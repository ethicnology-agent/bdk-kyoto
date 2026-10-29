use super::*;
use bdk_wallet::bitcoin::{self, Amount, Network, OutPoint, Transaction, TxIn, TxOut};
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

const EXTERNAL: &str = "wpkh([73c5da0a/84h/0h/0h]xpub6CatWdiZiodmUeTDp8LT5or8nmbKNcuyvz7WyksVFkKB4RHwCD3XyuvPEbvqAQY3rAPshWcMLoP2fMFMKHPJ4ZeZXYVUhLv1VMrjPC7PW6V/0/*)";
const INTERNAL: &str = "wpkh([73c5da0a/84h/0h/0h]xpub6CatWdiZiodmUeTDp8LT5or8nmbKNcuyvz7WyksVFkKB4RHwCD3XyuvPEbvqAQY3rAPshWcMLoP2fMFMKHPJ4ZeZXYVUhLv1VMrjPC7PW6V/1/*)";

fn wallet() -> bdk_wallet::Wallet {
    bdk_wallet::Wallet::create(EXTERNAL, INTERNAL)
        .network(Network::Bitcoin)
        .lookahead(25)
        .create_wallet_no_persist()
        .unwrap()
}

fn payment(script_pubkey: ScriptBuf, previous_output: OutPoint) -> Transaction {
    Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output,
            ..Default::default()
        }],
        output: vec![TxOut {
            value: Amount::from_sat(50_000),
            script_pubkey,
        }],
    }
}

fn block(tx: Transaction, height: u32) -> IndexedBlock {
    let mut block = bitcoin::blockdata::constants::genesis_block(Network::Bitcoin);
    block.header.nonce = height;
    block.txdata = vec![tx];
    block.header.merkle_root = block.compute_merkle_root().unwrap();
    IndexedBlock { height, block }
}

// Use the actual subscriber event loop; substitute only block transport, avoiding network I/O.
fn subscriber(
    wallet: &bdk_wallet::Wallet,
    policy: SyncPolicy,
    blocks: &[IndexedBlock],
) -> (
    UpdateSubscriber<wallets::Single>,
    UnboundedSender<Event>,
    Node,
) {
    let (node, client) = builder::Builder::new(Network::Bitcoin).build();
    let (sender, receiver) = unbounded_channel();
    let mut subscriber = UpdateSubscriber::<wallets::Single>::new(
        client.requester,
        policy,
        receiver,
        wallet.latest_checkpoint(),
        IndexedTxGraph::new(wallet.spk_index().clone()),
    );
    for block in blocks {
        subscriber.queue_block(block.height, block.block.block_hash());
    }
    sender
        .send(Event::FiltersSynced(SyncUpdate {
            tip: (0, wallet.latest_checkpoint().hash()).into(),
            recent_history: Default::default(),
        }))
        .unwrap();
    (subscriber, sender, node)
}

async fn run_update(
    wallet: &bdk_wallet::Wallet,
    policy: SyncPolicy,
    blocks: Vec<IndexedBlock>,
) -> Update {
    let (mut subscriber, _sender, _node) = subscriber(wallet, policy, &blocks);
    subscriber
        .sync_with_fetch(|hash| {
            let block = blocks
                .iter()
                .find(|b| b.block.block_hash() == hash)
                .cloned()
                .unwrap();
            std::future::ready(Ok(block))
        })
        .await
        .unwrap();
    subscriber.single_update_builder.as_mut().unwrap().finish()
}

#[tokio::test]
async fn recovery_records_matched_script_beyond_lookahead() {
    let wallet = wallet();
    let script = wallet
        .peek_address(KeychainKind::External, 50)
        .address
        .script_pubkey();
    let builder = UpdateBuilder::new(
        wallet.latest_checkpoint(),
        IndexedTxGraph::new(wallet.spk_index().clone()),
    );
    let policy = SyncPolicy::Recovery {
        used_script_index: 100,
        cp: (0, wallet.latest_checkpoint().hash()).into(),
    };
    assert!(
        builder.peek_scripts_from_policy(policy).contains(&script),
        "recovery filter policy must match script 50"
    );
    let tx = payment(script, OutPoint::null());
    let id = tx.compute_txid();
    let update = run_update(&wallet, policy, vec![block(tx, 1)]).await;
    assert!(
        update
            .tx_update
            .txs
            .iter()
            .any(|tx| tx.compute_txid() == id),
        "matched payment at index 50 was dropped by index lookahead 25"
    );
}

#[tokio::test]
async fn queued_payment_then_spend_keeps_spend() {
    let wallet = wallet();
    let funding = payment(
        wallet
            .peek_address(KeychainKind::External, 0)
            .address
            .script_pubkey(),
        OutPoint::null(),
    );
    let spend = payment(
        ScriptBuf::new(),
        OutPoint {
            txid: funding.compute_txid(),
            vout: 0,
        },
    );
    let id = spend.compute_txid();
    let update = run_update(
        &wallet,
        SyncPolicy::SyncFromLast,
        vec![block(funding, 1), block(spend, 2)],
    )
    .await;
    assert!(
        update
            .tx_update
            .txs
            .iter()
            .any(|tx| tx.compute_txid() == id),
        "newest-first application dropped the confirmed spend"
    );
}

#[tokio::test]
async fn queued_blocks_apply_by_height_despite_filter_arrival_order() {
    let wallet = wallet();
    let funding = payment(
        wallet
            .peek_address(KeychainKind::External, 0)
            .address
            .script_pubkey(),
        OutPoint::null(),
    );
    let spend = payment(
        ScriptBuf::new(),
        OutPoint {
            txid: funding.compute_txid(),
            vout: 0,
        },
    );
    let id = spend.compute_txid();
    let blocks = vec![block(spend, 2), block(funding, 1)];
    let (mut subscriber, _sender, _node) = subscriber(&wallet, SyncPolicy::SyncFromLast, &blocks);
    let mut heights = vec![];
    subscriber
        .sync_with_fetch(|hash| {
            let block = blocks
                .iter()
                .find(|b| b.block.block_hash() == hash)
                .cloned()
                .unwrap();
            heights.push(block.height);
            std::future::ready(Ok(block))
        })
        .await
        .unwrap();
    assert_eq!(heights, vec![1, 2]);
    let update = subscriber.single_update_builder.as_mut().unwrap().finish();
    assert!(update
        .tx_update
        .txs
        .iter()
        .any(|tx| tx.compute_txid() == id));
}

#[tokio::test]
async fn cancelled_fetch_retains_block_and_sync_boundary() {
    let wallet = wallet();
    let tx = payment(
        wallet
            .peek_address(KeychainKind::External, 0)
            .address
            .script_pubkey(),
        OutPoint::null(),
    );
    let id = tx.compute_txid();
    let blocks = vec![block(tx, 1)];
    let (mut subscriber, _sender, _node) = subscriber(&wallet, SyncPolicy::SyncFromLast, &blocks);
    let mut fetch_started = false;
    let cancelled = tokio::time::timeout(
        Duration::from_millis(10),
        subscriber.sync_with_fetch(|_| {
            fetch_started = true;
            std::future::pending::<Result<IndexedBlock, FetchBlockError>>()
        }),
    )
    .await;
    assert!(cancelled.is_err());
    assert!(fetch_started);
    assert_eq!(
        subscriber.queued_blocks.len(),
        1,
        "cancelled fetch must retain its block"
    );
    // No second FiltersSynced event is sent: the consumed boundary must survive cancellation.
    let resumed = tokio::time::timeout(
        Duration::from_millis(100),
        subscriber.sync_with_fetch(|_| std::future::ready(Ok(blocks[0].clone()))),
    )
    .await;
    assert!(
        resumed.is_ok(),
        "cancelled update lost its sync boundary and cannot resume"
    );
    resumed.unwrap().unwrap();
    assert!(subscriber.queued_blocks.is_empty());
    let update = subscriber.single_update_builder.as_mut().unwrap().finish();
    assert!(update
        .tx_update
        .txs
        .iter()
        .any(|tx| tx.compute_txid() == id));
}

#[tokio::test]
async fn queued_orphan_is_skipped_before_canonical_replacement() {
    let wallet = wallet();
    let orphan_tx = payment(
        wallet
            .peek_address(KeychainKind::External, 0)
            .address
            .script_pubkey(),
        OutPoint::null(),
    );
    let mut replacement_tx = orphan_tx.clone();
    replacement_tx.output[0].value = Amount::from_sat(40_000);
    let orphan_id = orphan_tx.compute_txid();
    let replacement_id = replacement_tx.compute_txid();
    let orphan = block(orphan_tx, 1);
    let replacement = block(replacement_tx, 1);
    let orphan_hash = orphan.block.block_hash();
    let blocks = vec![orphan, replacement.clone()];
    let (mut subscriber, _sender, _node) = subscriber(&wallet, SyncPolicy::SyncFromLast, &blocks);
    subscriber
        .sync_with_fetch(|hash| {
            std::future::ready(if hash == orphan_hash {
                Err(FetchBlockError::UnknownHash)
            } else {
                Ok(replacement.clone())
            })
        })
        .await
        .unwrap();
    let update = subscriber.single_update_builder.as_mut().unwrap().finish();
    assert!(!update
        .tx_update
        .txs
        .iter()
        .any(|tx| tx.compute_txid() == orphan_id));
    assert!(update
        .tx_update
        .txs
        .iter()
        .any(|tx| tx.compute_txid() == replacement_id));
}

fn multi_wallets() -> [bdk_wallet::Wallet; 2] {
    [
        bdk_wallet::Wallet::create(EXTERNAL, INTERNAL)
            .network(Network::Bitcoin)
            .lookahead(1000)
            .create_wallet_no_persist()
            .unwrap(),
        bdk_wallet::Wallet::create(
            EXTERNAL.replace("/0/*", "/2/*"),
            INTERNAL.replace("/1/*", "/3/*"),
        )
        .network(Network::Bitcoin)
        .lookahead(1000)
        .create_wallet_no_persist()
        .unwrap(),
    ]
}

fn multi_subscriber(
    wallets: &[bdk_wallet::Wallet],
) -> (
    UpdateSubscriber<wallets::Multiple>,
    UnboundedSender<Event>,
    Node,
) {
    use bdk_wallet::chain::DescriptorExt;
    let (node, client) = builder::Builder::new(Network::Bitcoin).build();
    let (sender, receiver) = unbounded_channel();
    let subscriber = UpdateSubscriber::<wallets::Multiple>::new_multiple(
        client.requester,
        receiver,
        wallets.iter().map(|wallet| {
            (
                wallet
                    .public_descriptor(KeychainKind::External)
                    .descriptor_id(),
                SyncPolicy::SyncFromLast,
                wallet.latest_checkpoint(),
                IndexedTxGraph::new(wallet.spk_index().clone()),
            )
        }),
    );
    (subscriber, sender, node)
}

fn send_boundary(sender: &UnboundedSender<Event>, block: &IndexedBlock) {
    sender
        .send(Event::ChainUpdate(BlockHeaderChanges::Connected(
            bip157::chain::IndexedHeader {
                height: block.height,
                header: block.block.header,
            },
        )))
        .unwrap();
    sender
        .send(Event::FiltersSynced(SyncUpdate {
            tip: (block.height, block.block.block_hash()).into(),
            recent_history: Default::default(),
        }))
        .unwrap();
}

fn finish_multiple(
    subscriber: &mut UpdateSubscriber<wallets::Multiple>,
) -> BTreeMap<DescriptorId, Update> {
    subscriber
        .multiple_updates_builder
        .as_mut()
        .unwrap()
        .iter_mut()
        .map(|(id, builder)| (*id, builder.finish()))
        .collect()
}

#[tokio::test]
async fn multi_updates_repeat_only_after_the_next_sync_boundary() {
    use bdk_wallet::chain::DescriptorExt;
    let wallets = multi_wallets();
    let expected_ids = wallets
        .iter()
        .map(|wallet| {
            wallet
                .public_descriptor(KeychainKind::External)
                .descriptor_id()
        })
        .collect::<BTreeSet<_>>();
    let (mut subscriber, sender, _node) = multi_subscriber(&wallets);
    for height in [1, 2] {
        // Cancellation while awaiting the next boundary must not replay the previous update.
        assert!(
            tokio::time::timeout(Duration::from_millis(10), subscriber.updates())
                .await
                .is_err()
        );
        let next = block(payment(ScriptBuf::new(), OutPoint::null()), height);
        send_boundary(&sender, &next);
        let updates = subscriber
            .updates()
            .await
            .unwrap()
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            updates.keys().copied().collect::<BTreeSet<_>>(),
            expected_ids
        );
        for update in updates.values() {
            assert_eq!(update.chain.as_ref().unwrap().height(), height);
        }
    }
    drop(sender);
    assert!(matches!(
        subscriber.updates().await,
        Err(UpdateError::NodeStopped)
    ));
}

#[tokio::test]
async fn multi_wallet_routing_keeps_script_999_and_later_spends() {
    use bdk_wallet::chain::DescriptorExt;
    let wallets = multi_wallets();
    let ids = wallets
        .iter()
        .map(|wallet| {
            wallet
                .public_descriptor(KeychainKind::External)
                .descriptor_id()
        })
        .collect::<Vec<_>>();
    let (mut subscriber, sender, _node) = multi_subscriber(&wallets);
    let mut funding = vec![];
    for (wallet, keychain) in wallets
        .iter()
        .zip([KeychainKind::External, KeychainKind::Internal])
    {
        let script = wallet.peek_address(keychain, 999).address.script_pubkey();
        assert!(subscriber.spk_cache.contains(&script));
        funding.push(payment(script, OutPoint::null()));
    }
    let blocks = funding
        .iter()
        .enumerate()
        .map(|(index, tx)| block(tx.clone(), index as u32 + 1))
        .collect::<Vec<_>>();
    for next in &blocks {
        subscriber.queue_block(next.height, next.block.block_hash());
    }
    send_boundary(&sender, blocks.last().unwrap());
    subscriber
        .sync_with_fetch(|hash| {
            std::future::ready(Ok(blocks
                .iter()
                .find(|block| block.block.block_hash() == hash)
                .unwrap()
                .clone()))
        })
        .await
        .unwrap();
    let updates = finish_multiple(&mut subscriber);
    for (index, id) in ids.iter().enumerate() {
        let txids = updates[id]
            .tx_update
            .txs
            .iter()
            .map(|tx| tx.compute_txid())
            .collect::<BTreeSet<_>>();
        assert_eq!(txids, BTreeSet::from([funding[index].compute_txid()]));
    }
    // The index must survive finish() so the next loop can recognize spends of prior outputs.
    let spend = payment(
        ScriptBuf::new(),
        OutPoint {
            txid: funding[0].compute_txid(),
            vout: 0,
        },
    );
    let next = block(spend.clone(), 3);
    subscriber.queue_block(next.height, next.block.block_hash());
    send_boundary(&sender, &next);
    subscriber
        .sync_with_fetch(|_| std::future::ready(Ok(next.clone())))
        .await
        .unwrap();
    let updates = finish_multiple(&mut subscriber);
    assert_eq!(updates[&ids[0]].tx_update.txs.len(), 1);
    assert_eq!(
        updates[&ids[0]].tx_update.txs[0].compute_txid(),
        spend.compute_txid()
    );
    assert!(updates[&ids[1]].tx_update.txs.is_empty());
}

#[tokio::test]
async fn multi_cancelled_fetch_retains_each_wallet_update() {
    use bdk_wallet::chain::DescriptorExt;
    let wallets = multi_wallets();
    let (mut subscriber, sender, _node) = multi_subscriber(&wallets);
    let blocks = wallets
        .iter()
        .enumerate()
        .map(|(index, wallet)| {
            block(
                payment(
                    wallet
                        .peek_address(KeychainKind::External, 0)
                        .address
                        .script_pubkey(),
                    OutPoint::null(),
                ),
                index as u32 + 1,
            )
        })
        .collect::<Vec<_>>();
    for block in &blocks {
        subscriber.queue_block(block.height, block.block.block_hash());
    }
    send_boundary(&sender, blocks.last().unwrap());
    let mut fetched = 0;
    assert!(tokio::time::timeout(
        Duration::from_millis(10),
        subscriber.sync_with_fetch(|_| {
            fetched += 1;
            let first = blocks[0].clone();
            let wait = fetched > 1;
            async move {
                if wait {
                    std::future::pending::<()>().await;
                }
                Ok(first)
            }
        })
    )
    .await
    .is_err());
    assert_eq!(fetched, 2);
    assert_eq!(subscriber.queued_blocks.len(), 1);
    assert!(subscriber.filters_synced);
    // Resume without another FiltersSynced; preserve the first wallet's already applied block.
    subscriber
        .sync_with_fetch(|_| std::future::ready(Ok(blocks[1].clone())))
        .await
        .unwrap();
    let updates = finish_multiple(&mut subscriber);
    for (wallet, block) in wallets.iter().zip(&blocks) {
        let id = wallet
            .public_descriptor(KeychainKind::External)
            .descriptor_id();
        assert_eq!(updates[&id].tx_update.txs.len(), 1);
        assert_eq!(
            updates[&id].tx_update.txs[0].compute_txid(),
            block.block.txdata[0].compute_txid()
        );
    }
}

// A checked historical range must be persistable even if the tip event never arrives.
#[tokio::test]
async fn cancellation_before_tip_yields_a_persistable_checkpoint() {
    let wallet = wallet();
    let (mut subscriber, sender, _node) = subscriber(&wallet, SyncPolicy::SyncFromLast, &[]);
    // Remove the helper's tip boundary: recovery is still in progress.
    subscriber.receiver.try_recv().unwrap();
    let mut previous = wallet.latest_checkpoint().hash();
    for height in 1..=1000 {
        let mut header = bitcoin::blockdata::constants::genesis_block(Network::Bitcoin).header;
        header.prev_blockhash = previous;
        header.nonce = height;
        previous = header.block_hash();
        sender
            .send(Event::ChainUpdate(BlockHeaderChanges::Connected(
                bip157::chain::IndexedHeader { height, header },
            )))
            .unwrap();
        subscriber.receive_filter(height, header, bip157::BlockFilter::new(&[0]));
    }
    let completed = tokio::time::timeout(
        Duration::from_millis(100),
        subscriber
            .sync_with_fetch(|_| std::future::pending::<Result<IndexedBlock, FetchBlockError>>()),
    )
    .await;
    assert!(
        completed.is_ok(),
        "cancelling recovery before the tip leaves no persistable update"
    );
    completed.unwrap().unwrap();
    let update = subscriber.single_update_builder.as_mut().unwrap().finish();
    assert_eq!(update.chain.as_ref().unwrap().height(), 1000);
    assert_eq!(update.chain.unwrap().hash(), previous);
}
