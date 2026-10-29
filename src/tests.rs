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
        match policy {
            SyncPolicy::Recovery { cp, .. } => cp,
            _ => builder::walk_back_max_reorg(wallet.latest_checkpoint()),
        },
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
        reuse_persisted_history: false,
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
    let script = wallet
        .peek_address(KeychainKind::External, 0)
        .address
        .script_pubkey();
    let orphan = block(payment(script.clone(), OutPoint::null()), 1);
    let mut replacement = block(payment(script, OutPoint::null()), 1);
    replacement.block.txdata[0].output[0].value = Amount::from_sat(40_000);
    replacement.block.header.merkle_root = replacement.block.compute_merkle_root().unwrap();
    let orphan_hash = orphan.block.block_hash();
    let replacement_id = replacement.block.txdata[0].compute_txid();
    let (mut subscriber, sender, _node) = subscriber(&wallet, SyncPolicy::SyncFromLast, &[]);
    subscriber.receiver.try_recv().unwrap();
    stage_blocks(&mut subscriber, std::slice::from_ref(&orphan));
    send_tip(&sender, &orphan);
    sender
        .send(Event::ChainUpdate(BlockHeaderChanges::Reorganized {
            accepted: vec![indexed_header(&replacement)],
            reorganized: vec![indexed_header(&orphan)],
        }))
        .unwrap();
    // The replacement filter is buffered before the reorg; the reorg clears it as stale.
    // Resupply it once the event has been consumed by a cancelled fetch.
    assert!(tokio::time::timeout(
        Duration::from_millis(10),
        subscriber.sync_with_fetch(|hash| {
            std::future::ready(if hash == orphan_hash {
                Err(FetchBlockError::UnknownHash)
            } else {
                Ok(replacement.clone())
            })
        })
    )
    .await
    .is_err());
    subscriber.receive_filter(1, replacement.block.header, basic_filter(&replacement));
    send_tip(&sender, &replacement);
    subscriber
        .sync_with_fetch(|_| std::future::ready(Ok(replacement.clone())))
        .await
        .unwrap();
    let update = subscriber.single_update_builder.as_mut().unwrap().finish();
    assert_eq!(update.tx_update.txs.len(), 1);
    assert_eq!(update.tx_update.txs[0].compute_txid(), replacement_id);
    assert_eq!(update.chain.unwrap().hash(), replacement.block.block_hash());
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

fn send_boundary<W: Wallets>(
    subscriber: &mut UpdateSubscriber<W>,
    sender: &UnboundedSender<Event>,
    block: &IndexedBlock,
) {
    subscriber.receive_filter(
        block.height,
        block.block.header,
        bip157::BlockFilter::new(&[0]),
    );
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
        send_boundary(&mut subscriber, &sender, &next);
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
        subscriber.apply_chain_event(&BlockHeaderChanges::Connected(
            bip157::chain::IndexedHeader {
                height: next.height,
                header: next.block.header,
            },
        ));
        subscriber.receive_filter(
            next.height,
            next.block.header,
            bip157::BlockFilter::new(&[0]),
        );
    }
    send_boundary(&mut subscriber, &sender, blocks.last().unwrap());
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
    send_boundary(&mut subscriber, &sender, &next);
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
        subscriber.apply_chain_event(&BlockHeaderChanges::Connected(
            bip157::chain::IndexedHeader {
                height: block.height,
                header: block.block.header,
            },
        ));
        subscriber.receive_filter(
            block.height,
            block.block.header,
            bip157::BlockFilter::new(&[0]),
        );
    }
    send_boundary(&mut subscriber, &sender, blocks.last().unwrap());
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

fn indexed_header(block: &IndexedBlock) -> bip157::chain::IndexedHeader {
    bip157::chain::IndexedHeader {
        height: block.height,
        header: block.block.header,
    }
}

fn basic_filter(block: &IndexedBlock) -> bip157::BlockFilter {
    bip157::BlockFilter::new_script_filter(&block.block, |_| {
        Ok::<_, bitcoin::bip158::Error>(ScriptBuf::new())
    })
    .unwrap()
}

fn historical_blocks(count: u32) -> Vec<IndexedBlock> {
    let mut previous = bitcoin::blockdata::constants::genesis_block(Network::Bitcoin).block_hash();
    (1..=count)
        .map(|height| {
            let mut next = bitcoin::blockdata::constants::genesis_block(Network::Bitcoin);
            next.txdata.clear();
            next.header.prev_blockhash = previous;
            next.header.nonce = height;
            previous = next.block_hash();
            IndexedBlock {
                height,
                block: next,
            }
        })
        .collect()
}

fn stage_blocks<W: Wallets>(subscriber: &mut UpdateSubscriber<W>, blocks: &[IndexedBlock]) {
    for block in blocks {
        subscriber.apply_chain_event(&BlockHeaderChanges::Connected(indexed_header(block)));
        subscriber.receive_filter(block.height, block.block.header, basic_filter(block));
    }
}

fn send_tip(sender: &UnboundedSender<Event>, block: &IndexedBlock) {
    sender
        .send(Event::FiltersSynced(SyncUpdate {
            tip: (block.height, block.block.block_hash()).into(),
            recent_history: Default::default(),
        }))
        .unwrap();
}

async fn fetch_boundary<W: Wallets>(
    subscriber: &mut UpdateSubscriber<W>,
    blocks: &[IndexedBlock],
) -> ScanBoundary {
    tokio::time::timeout(
        Duration::from_secs(1),
        subscriber.sync_with_fetch(|hash| {
            std::future::ready(Ok(blocks
                .iter()
                .find(|block| block.block.block_hash() == hash)
                .unwrap()
                .clone()))
        }),
    )
    .await
    .expect("safe boundary must complete")
    .unwrap()
}

#[tokio::test]
async fn intermediate_cadence_and_final_tip_are_distinct() {
    let wallet = wallet();
    let (mut subscriber, sender, _node) = subscriber(&wallet, SyncPolicy::SyncFromLast, &[]);
    subscriber.receiver.try_recv().unwrap();
    let blocks = historical_blocks(CHECKPOINT_INTERVAL * 2 + 17);
    stage_blocks(&mut subscriber, &blocks);
    send_tip(&sender, blocks.last().unwrap());
    for (height, synced) in [(1000, false), (2000, false), (2017, true)] {
        let boundary = fetch_boundary(&mut subscriber, &blocks).await;
        assert_eq!(boundary.scanned_tip.height, height);
        assert_eq!(boundary.synced_to_tip, synced);
        let update = subscriber.single_update_builder.as_mut().unwrap().finish();
        assert_eq!(update.chain.as_ref().unwrap().height(), height);
        assert_eq!(
            update.chain.unwrap().hash(),
            blocks[height as usize - 1].block.block_hash()
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(10), subscriber.update_batch())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn out_of_order_filters_cannot_cross_an_unchecked_height() {
    let wallet = wallet();
    let (mut subscriber, sender, _node) = subscriber(&wallet, SyncPolicy::SyncFromLast, &[]);
    subscriber.receiver.try_recv().unwrap();
    let blocks = historical_blocks(1000);
    for block in &blocks {
        subscriber.apply_chain_event(&BlockHeaderChanges::Connected(indexed_header(block)));
    }
    for block in blocks.iter().rev().filter(|block| block.height != 500) {
        subscriber.receive_filter(block.height, block.block.header, basic_filter(block));
    }
    send_tip(&sender, blocks.last().unwrap());
    assert!(
        tokio::time::timeout(Duration::from_millis(10), subscriber.update_batch())
            .await
            .is_err()
    );
    assert_eq!(subscriber.scanned_tip.height, 499);
    assert_eq!(
        subscriber
            .single_update_builder
            .as_ref()
            .unwrap()
            .cp
            .height(),
        0
    );
    let missing = &blocks[499];
    subscriber.receive_filter(missing.height, missing.block.header, basic_filter(missing));
    let batch = subscriber.update_batch().await.unwrap();
    assert_eq!(batch.scanned_tip.height, 1000);
    assert!(batch.synced_to_tip);
}

#[tokio::test]
async fn delayed_matched_block_prevents_checkpoint_and_survives_cancellation() {
    let wallet = wallet();
    let (mut subscriber, _sender, _node) = subscriber(&wallet, SyncPolicy::SyncFromLast, &[]);
    subscriber.receiver.try_recv().unwrap();
    let mut blocks = historical_blocks(1000);
    let tx = payment(
        wallet
            .peek_address(KeychainKind::External, 0)
            .address
            .script_pubkey(),
        OutPoint::null(),
    );
    blocks[998].block.txdata = vec![tx.clone()];
    blocks[998].block.header.merkle_root = blocks[998].block.compute_merkle_root().unwrap();
    blocks[999].block.header.prev_blockhash = blocks[998].block.block_hash();
    stage_blocks(&mut subscriber, &blocks);
    let mut called = false;
    assert!(tokio::time::timeout(
        Duration::from_millis(10),
        subscriber.sync_with_fetch(|_| {
            called = true;
            std::future::pending::<Result<IndexedBlock, FetchBlockError>>()
        })
    )
    .await
    .is_err());
    assert!(called);
    assert_eq!(
        subscriber
            .single_update_builder
            .as_ref()
            .unwrap()
            .cp
            .height(),
        0
    );
    assert_eq!(subscriber.queued_blocks.len(), 1);
    assert_eq!(
        subscriber.pending_boundary.unwrap().scanned_tip.height,
        1000
    );
    let boundary = fetch_boundary(&mut subscriber, &blocks).await;
    assert!(!boundary.synced_to_tip);
    let update = subscriber.single_update_builder.as_mut().unwrap().finish();
    assert_eq!(update.chain.unwrap().height(), 1000);
    assert!(update
        .tx_update
        .txs
        .iter()
        .any(|found| found.compute_txid() == tx.compute_txid()));
}

fn relink(blocks: &mut [IndexedBlock]) {
    let mut previous = bitcoin::blockdata::constants::genesis_block(Network::Bitcoin).block_hash();
    for block in blocks {
        if let Some(root) = block.block.compute_merkle_root() {
            block.block.header.merkle_root = root;
        }
        block.block.header.prev_blockhash = previous;
        previous = block.block.block_hash();
    }
}

#[tokio::test]
async fn lookahead_extends_before_scanning_beyond_a_partial_boundary() {
    let wallet = wallet();
    let (mut subscriber, sender, _node) = subscriber(&wallet, SyncPolicy::SyncFromLast, &[]);
    subscriber.receiver.try_recv().unwrap();
    let mut blocks = historical_blocks(1001);
    blocks[998].block.txdata = vec![payment(
        wallet
            .peek_address(KeychainKind::External, 24)
            .address
            .script_pubkey(),
        OutPoint::null(),
    )];
    let later_script = wallet
        .peek_address(KeychainKind::External, 40)
        .address
        .script_pubkey();
    assert!(!subscriber.spk_cache.contains(&later_script));
    let later = payment(later_script.clone(), OutPoint::null());
    blocks[1000].block.txdata = vec![later.clone()];
    relink(&mut blocks);
    // The future filter is already buffered, but must not be checked before the first boundary.
    stage_blocks(&mut subscriber, &blocks);
    let partial = fetch_boundary(&mut subscriber, &blocks).await;
    assert_eq!(partial.scanned_tip.height, 1000);
    assert!(subscriber.spk_cache.contains(&later_script));
    subscriber.single_update_builder.as_mut().unwrap().finish();
    send_tip(&sender, blocks.last().unwrap());
    let final_boundary = fetch_boundary(&mut subscriber, &blocks).await;
    assert!(final_boundary.synced_to_tip);
    let update = subscriber.single_update_builder.as_mut().unwrap().finish();
    assert!(update
        .tx_update
        .txs
        .iter()
        .any(|tx| tx.compute_txid() == later.compute_txid()));
}

#[tokio::test]
async fn persisted_intermediate_recovery_resumes_with_the_same_final_wallet_state() {
    use bdk_wallet::chain::Merge;
    let mut uninterrupted = wallet();
    let mut resumed = wallet();
    let mut persisted = resumed.take_staged().unwrap();
    let policy = SyncPolicy::Recovery {
        cp: (0, resumed.latest_checkpoint().hash()).into(),
        used_script_index: 100,
        reuse_persisted_history: false,
    };
    let mut blocks = historical_blocks(2017);
    let first = payment(
        resumed
            .peek_address(KeychainKind::External, 50)
            .address
            .script_pubkey(),
        OutPoint::null(),
    );
    let high = payment(
        resumed
            .peek_address(KeychainKind::External, 125)
            .address
            .script_pubkey(),
        OutPoint::null(),
    );
    let spend = payment(
        resumed
            .peek_address(KeychainKind::Internal, 10)
            .address
            .script_pubkey(),
        OutPoint {
            txid: first.compute_txid(),
            vout: 0,
        },
    );
    blocks[899].block.txdata = vec![first];
    blocks[1989].block.txdata = vec![high];
    blocks[1994].block.txdata = vec![payment(
        resumed
            .peek_address(KeychainKind::External, 200)
            .address
            .script_pubkey(),
        OutPoint::null(),
    )];
    blocks[1499].block.txdata = vec![spend];
    relink(&mut blocks);
    let (mut continuous, sender, _node) = subscriber(&uninterrupted, policy, &[]);
    continuous.receiver.try_recv().unwrap();
    stage_blocks(&mut continuous, &blocks);
    send_tip(&sender, blocks.last().unwrap());
    loop {
        let boundary = fetch_boundary(&mut continuous, &blocks).await;
        uninterrupted
            .apply_update(continuous.single_update_builder.as_mut().unwrap().finish())
            .unwrap();
        if boundary.synced_to_tip {
            break;
        }
    }
    let (mut partial, _sender, _node) = subscriber(&resumed, policy, &[]);
    partial.receiver.try_recv().unwrap();
    stage_blocks(&mut partial, &blocks[..1500]);
    assert_eq!(
        fetch_boundary(&mut partial, &blocks)
            .await
            .scanned_tip
            .height,
        1000
    );
    resumed
        .apply_update(partial.single_update_builder.as_mut().unwrap().finish())
        .unwrap();
    persisted.merge(resumed.take_staged().unwrap());
    // Work after H is cancelled before the next boundary. Persistence still ends exactly at H.
    assert!(tokio::time::timeout(
        Duration::from_millis(10),
        partial.sync_with_fetch(|hash| {
            std::future::ready(Ok(blocks
                .iter()
                .find(|block| block.block.block_hash() == hash)
                .unwrap()
                .clone()))
        })
    )
    .await
    .is_err());
    drop(partial);
    let mut resumed = bdk_wallet::Wallet::load()
        .lookahead(25)
        .load_wallet_no_persist(persisted)
        .unwrap()
        .unwrap();
    assert_eq!(resumed.latest_checkpoint().height(), 1000);
    assert_eq!(resumed.derivation_index(KeychainKind::External), Some(50));
    let start = builder::walk_back_max_reorg(resumed.latest_checkpoint());
    assert_eq!(start.height, 993);
    assert_eq!(start.hash, blocks[992].block.block_hash());
    // Same recovery script-count bound is retained despite the smaller persisted wallet lookahead.
    let resume_policy = SyncPolicy::Recovery {
        cp: start,
        used_script_index: 100,
        reuse_persisted_history: true,
    };
    let client = builder::Builder::new(Network::Bitcoin)
        .build_with_wallet(
            &resumed,
            SyncConfig::wallet_recovery_sync(start, 100)
                .reuse_persisted_history()
                .build(),
        )
        .unwrap();
    let (_client, _logs, actual) = client.subscribe();
    assert_eq!(actual.scanned_tip.height, 993);
    let (mut restarted, sender, _node) = subscriber(&resumed, resume_policy, &[]);
    restarted.receiver.try_recv().unwrap();
    stage_blocks(&mut restarted, &blocks[993..]);
    send_tip(&sender, blocks.last().unwrap());
    loop {
        let boundary = fetch_boundary(&mut restarted, &blocks).await;
        resumed
            .apply_update(restarted.single_update_builder.as_mut().unwrap().finish())
            .unwrap();
        if boundary.synced_to_tip {
            break;
        }
    }
    let ids = |wallet: &bdk_wallet::Wallet| {
        wallet
            .transactions()
            .map(|tx| tx.tx_node.txid)
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(ids(&resumed), ids(&uninterrupted));
    assert_eq!(ids(&resumed).len(), 3);
    assert_eq!(resumed.balance(), uninterrupted.balance());
    for keychain in [KeychainKind::External, KeychainKind::Internal] {
        assert_eq!(
            resumed.derivation_index(keychain),
            uninterrupted.derivation_index(keychain)
        );
    }
    assert_eq!(resumed.derivation_index(KeychainKind::External), Some(125));
    assert_eq!(
        resumed.latest_checkpoint().block_id(),
        uninterrupted.latest_checkpoint().block_id()
    );
    // Routine Sync also starts from H-7 when rebuilding from a persisted intermediate checkpoint.
    let client = builder::Builder::new(Network::Bitcoin)
        .build_with_wallet(&resumed, SyncConfig::sync_from_last_checkpoint().build())
        .unwrap();
    let (_client, _logs, actual) = client.subscribe();
    assert_eq!(actual.scanned_tip.height, 2010);
}

#[tokio::test]
async fn reorg_disconnects_a_persisted_intermediate_checkpoint() {
    let mut wallet = wallet();
    let (mut subscriber, sender, _node) = subscriber(&wallet, SyncPolicy::SyncFromLast, &[]);
    subscriber.receiver.try_recv().unwrap();
    let mut blocks = historical_blocks(1000);
    let orphan = payment(
        wallet
            .peek_address(KeychainKind::External, 0)
            .address
            .script_pubkey(),
        OutPoint::null(),
    );
    blocks[998].block.txdata = vec![orphan];
    relink(&mut blocks);
    stage_blocks(&mut subscriber, &blocks);
    assert!(!fetch_boundary(&mut subscriber, &blocks).await.synced_to_tip);
    wallet
        .apply_update(subscriber.single_update_builder.as_mut().unwrap().finish())
        .unwrap();
    assert_eq!(
        wallet.latest_checkpoint().hash(),
        blocks[999].block.block_hash()
    );
    assert_eq!(wallet.balance().total(), Amount::from_sat(50_000));
    let mut replacement = historical_blocks(1001);
    replacement[998].block.header.nonce = 99_999;
    relink(&mut replacement);
    subscriber.apply_chain_event(&BlockHeaderChanges::Reorganized {
        accepted: replacement[998..].iter().map(indexed_header).collect(),
        reorganized: blocks[998..].iter().map(indexed_header).collect(),
    });
    assert_eq!(subscriber.scanned_tip.height, 998);
    for block in &replacement[998..] {
        subscriber.receive_filter(block.height, block.block.header, basic_filter(block));
    }
    send_tip(&sender, replacement.last().unwrap());
    let partial = fetch_boundary(&mut subscriber, &replacement).await;
    assert_eq!(partial.scanned_tip.height, 1000);
    assert!(!partial.synced_to_tip);
    wallet
        .apply_update(subscriber.single_update_builder.as_mut().unwrap().finish())
        .unwrap();
    let boundary = fetch_boundary(&mut subscriber, &replacement).await;
    assert_eq!(boundary.scanned_tip.height, 1001);
    assert!(boundary.synced_to_tip);
    wallet
        .apply_update(subscriber.single_update_builder.as_mut().unwrap().finish())
        .unwrap();
    assert_eq!(
        wallet.latest_checkpoint().hash(),
        replacement[1000].block.block_hash()
    );
    assert_eq!(
        wallet.latest_checkpoint().get(1000).unwrap().hash(),
        replacement[999].block.block_hash()
    );
    assert_eq!(wallet.balance().total(), Amount::ZERO);
}

#[tokio::test]
async fn failed_canonical_fetch_cannot_recreate_an_unsafe_boundary() {
    let wallet = wallet();
    let (mut subscriber, sender, _node) = subscriber(&wallet, SyncPolicy::SyncFromLast, &[]);
    subscriber.receiver.try_recv().unwrap();
    let mut blocks = historical_blocks(1001);
    blocks[998].block.txdata = vec![payment(
        wallet
            .peek_address(KeychainKind::External, 0)
            .address
            .script_pubkey(),
        OutPoint::null(),
    )];
    relink(&mut blocks);
    stage_blocks(&mut subscriber, &blocks[..1000]);
    // Other old-chain events can be ahead of the reorg in the queue while the fetch fails.
    sender
        .send(Event::ChainUpdate(BlockHeaderChanges::Connected(
            indexed_header(&blocks[1000]),
        )))
        .unwrap();
    send_tip(&sender, blocks.last().unwrap());
    assert!(tokio::time::timeout(
        Duration::from_millis(10),
        subscriber.sync_with_fetch(|_| { std::future::ready(Err(FetchBlockError::UnknownHash)) })
    )
    .await
    .is_err());
    assert!(subscriber.awaiting_reorg);
    assert_eq!(
        subscriber
            .single_update_builder
            .as_ref()
            .unwrap()
            .cp
            .height(),
        0
    );
    let mut replacement = historical_blocks(1001);
    replacement[998].block.header.nonce = 99_999;
    relink(&mut replacement);
    subscriber.apply_chain_event(&BlockHeaderChanges::Reorganized {
        accepted: replacement[998..].iter().map(indexed_header).collect(),
        reorganized: blocks[998..].iter().map(indexed_header).collect(),
    });
    for block in &replacement[998..] {
        subscriber.receive_filter(block.height, block.block.header, basic_filter(block));
    }
    send_tip(&sender, replacement.last().unwrap());
    let replacement_partial = fetch_boundary(&mut subscriber, &replacement).await;
    assert_eq!(replacement_partial.scanned_tip.height, 1000);
    assert!(!replacement_partial.synced_to_tip);
    assert_eq!(
        subscriber.single_update_builder.as_ref().unwrap().cp.hash(),
        replacement[999].block.block_hash()
    );
    assert!(
        fetch_boundary(&mut subscriber, &replacement)
            .await
            .synced_to_tip
    );
    let update = subscriber.single_update_builder.as_mut().unwrap().finish();
    assert!(update.tx_update.txs.is_empty());
    assert_eq!(
        update.chain.unwrap().hash(),
        replacement[1000].block.block_hash()
    );
}

#[tokio::test]
async fn every_wallet_receives_the_same_intermediate_boundary() {
    use bdk_wallet::chain::DescriptorExt;
    let wallets = [
        wallet(),
        bdk_wallet::Wallet::create(
            EXTERNAL.replace("/0/*", "/2/*"),
            INTERNAL.replace("/1/*", "/3/*"),
        )
        .network(Network::Bitcoin)
        .lookahead(25)
        .create_wallet_no_persist()
        .unwrap(),
    ];
    let (mut subscriber, _sender, _node) = multi_subscriber(&wallets);
    let mut blocks = historical_blocks(1001);
    let txs = wallets
        .iter()
        .enumerate()
        .map(|(index, wallet)| {
            payment(
                wallet
                    .peek_address(KeychainKind::External, index as u32)
                    .address
                    .script_pubkey(),
                OutPoint::null(),
            )
        })
        .collect::<Vec<_>>();
    blocks[9].block.txdata = vec![txs[0].clone()];
    blocks[998].block.txdata = vec![txs[1].clone()];
    relink(&mut blocks);
    stage_blocks(&mut subscriber, &blocks);
    let boundary = fetch_boundary(&mut subscriber, &blocks).await;
    assert_eq!(boundary.scanned_tip.height, 1000);
    assert!(!boundary.synced_to_tip);
    let updates = finish_multiple(&mut subscriber);
    assert_eq!(updates.len(), 2);
    for (index, wallet) in wallets.iter().enumerate() {
        let id = wallet
            .public_descriptor(KeychainKind::External)
            .descriptor_id();
        assert_eq!(
            updates[&id].chain.as_ref().unwrap().block_id(),
            boundary.scanned_tip
        );
        assert_eq!(updates[&id].tx_update.txs.len(), 1);
        assert_eq!(
            updates[&id].tx_update.txs[0].compute_txid(),
            txs[index].compute_txid()
        );
    }
}

#[tokio::test]
async fn historical_rescan_rechecks_filters_with_the_extended_cache() {
    let wallet = wallet();
    let (mut subscriber, sender, _node) = subscriber(&wallet, SyncPolicy::SyncFromLast, &[]);
    subscriber.receiver.try_recv().unwrap();
    let mut blocks = historical_blocks(1001);
    let earlier = payment(
        wallet
            .peek_address(KeychainKind::External, 40)
            .address
            .script_pubkey(),
        OutPoint::null(),
    );
    blocks[1].block.txdata = vec![earlier.clone()];
    blocks[998].block.txdata = vec![payment(
        wallet
            .peek_address(KeychainKind::External, 24)
            .address
            .script_pubkey(),
        OutPoint::null(),
    )];
    relink(&mut blocks);
    stage_blocks(&mut subscriber, &blocks);
    fetch_boundary(&mut subscriber, &blocks).await;
    let first = subscriber.single_update_builder.as_mut().unwrap().finish();
    assert!(!first
        .tx_update
        .txs
        .iter()
        .any(|tx| tx.compute_txid() == earlier.compute_txid()));
    send_tip(&sender, blocks.last().unwrap());
    fetch_boundary(&mut subscriber, &blocks).await;
    subscriber.single_update_builder.as_mut().unwrap().finish();
    for block in &blocks {
        subscriber.receive_filter(block.height, block.block.header, basic_filter(block));
    }
    send_tip(&sender, blocks.last().unwrap());
    assert!(fetch_boundary(&mut subscriber, &blocks).await.synced_to_tip);
    let rescanned = subscriber.single_update_builder.as_mut().unwrap().finish();
    assert!(rescanned
        .tx_update
        .txs
        .iter()
        .any(|tx| tx.compute_txid() == earlier.compute_txid()));
}

#[tokio::test]
async fn new_wallet_skips_filters_only_for_its_initial_boundary() {
    let wallet = wallet();
    let (mut subscriber, sender, _node) = subscriber(&wallet, SyncPolicy::NewWallet, &[]);
    subscriber.receiver.try_recv().unwrap();
    let blocks = historical_blocks(2);
    subscriber.apply_chain_event(&BlockHeaderChanges::Connected(indexed_header(&blocks[0])));
    send_tip(&sender, &blocks[0]);
    let initial = subscriber.update_batch().await.unwrap();
    assert!(initial.synced_to_tip);
    assert_eq!(initial.scanned_tip.height, 1);
    subscriber.apply_chain_event(&BlockHeaderChanges::Connected(indexed_header(&blocks[1])));
    send_tip(&sender, &blocks[1]);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), subscriber.update_batch())
            .await
            .is_err()
    );
    subscriber.receive_filter(2, blocks[1].block.header, basic_filter(&blocks[1]));
    let next = subscriber.update_batch().await.unwrap();
    assert!(next.synced_to_tip);
    assert_eq!(next.scanned_tip.height, 2);
}

#[tokio::test]
async fn intermediate_batches_apply_to_an_ahead_sparse_wallet() {
    use bdk_wallet::chain::DescriptorExt;
    let blocks = historical_blocks(1600);
    let mut wallets = [
        wallet(),
        bdk_wallet::Wallet::create(
            EXTERNAL.replace("/0/*", "/2/*"),
            INTERNAL.replace("/1/*", "/3/*"),
        )
        .network(Network::Bitcoin)
        .lookahead(25)
        .create_wallet_no_persist()
        .unwrap(),
    ];
    let ahead = wallets[1].latest_checkpoint().insert(BlockId {
        height: 1500,
        hash: blocks[1499].block.block_hash(),
    });
    wallets[1]
        .apply_update(Update {
            chain: Some(ahead),
            ..Default::default()
        })
        .unwrap();
    let (mut subscriber, sender, _node) = multi_subscriber(&wallets);
    stage_blocks(&mut subscriber, &blocks);
    let partial = subscriber.updates_batch().await.unwrap();
    assert_eq!(partial.scanned_tip.height, 1000);
    assert!(!partial.synced_to_tip);
    for wallet in &mut wallets {
        let id = wallet
            .public_descriptor(KeychainKind::External)
            .descriptor_id();
        wallet.apply_update(partial.updates[&id].clone()).unwrap();
    }
    assert_eq!(wallets[0].latest_checkpoint().height(), 1000);
    assert_eq!(wallets[1].latest_checkpoint().height(), 1500);
    send_tip(&sender, blocks.last().unwrap());
    let final_batch = subscriber.updates_batch().await.unwrap();
    assert!(final_batch.synced_to_tip);
    assert_eq!(final_batch.scanned_tip.height, 1600);
    for wallet in &mut wallets {
        let id = wallet
            .public_descriptor(KeychainKind::External)
            .descriptor_id();
        wallet
            .apply_update(final_batch.updates[&id].clone())
            .unwrap();
        assert_eq!(wallet.latest_checkpoint().height(), 1600);
        assert_eq!(
            wallet.latest_checkpoint().hash(),
            blocks[1599].block.block_hash()
        );
    }
}

#[test]
fn multi_wallet_builder_rejects_conflicting_histories_below_the_scan_anchor() {
    use bdk_wallet::bitcoin::hashes::Hash;
    let mut wallets = multi_wallets();
    // Use different descriptor IDs; only the histories conflict.
    for (index, wallet) in wallets.iter_mut().enumerate() {
        let cp = wallet.latest_checkpoint().insert(BlockId {
            height: 1000,
            hash: BlockHash::from_byte_array([index as u8 + 1; 32]),
        });
        wallet
            .apply_update(Update {
                chain: Some(cp),
                ..Default::default()
            })
            .unwrap();
    }
    let result = builder::Builder::new(Network::Bitcoin).build_with_wallets(
        wallets
            .iter()
            .map(|wallet| (wallet, SyncConfig::sync_from_last_checkpoint().build()))
            .collect(),
    );
    assert!(matches!(
        result,
        Err(builder::BuilderError::ConflictingCheckpoints)
    ));
}

#[test]
fn recovery_initial_cache_keeps_at_least_the_wallet_lookahead() {
    let wallet = wallet();
    let policy = SyncPolicy::Recovery {
        cp: (0, wallet.latest_checkpoint().hash()).into(),
        used_script_index: 10,
        reuse_persisted_history: false,
    };
    let (subscriber, _sender, _node) = subscriber(&wallet, policy, &[]);
    let beyond_count = wallet
        .peek_address(KeychainKind::External, 20)
        .address
        .script_pubkey();
    assert!(subscriber.spk_cache.contains(&beyond_count));
    let builder = subscriber.single_update_builder.as_ref().unwrap();
    assert_eq!(
        builder.peek_scripts_from_policy(policy),
        builder.peek_script_to_keychain_lookahead()
    );
}

#[tokio::test]
async fn shorter_higher_work_reorg_replaces_an_intermediate_checkpoint() {
    let mut wallet = wallet();
    let (mut subscriber, sender, _node) = subscriber(&wallet, SyncPolicy::SyncFromLast, &[]);
    subscriber.receiver.try_recv().unwrap();
    let blocks = historical_blocks(1000);
    stage_blocks(&mut subscriber, &blocks);
    fetch_boundary(&mut subscriber, &blocks).await;
    wallet
        .apply_update(subscriber.single_update_builder.as_mut().unwrap().finish())
        .unwrap();
    assert_eq!(wallet.latest_checkpoint().height(), 1000);
    let mut replacement = historical_blocks(999);
    replacement[997].block.header.nonce = 99_999;
    relink(&mut replacement);
    subscriber.apply_chain_event(&BlockHeaderChanges::Reorganized {
        accepted: replacement[997..].iter().map(indexed_header).collect(),
        reorganized: blocks[997..].iter().map(indexed_header).collect(),
    });
    for block in &replacement[997..] {
        subscriber.receive_filter(block.height, block.block.header, basic_filter(block));
    }
    send_tip(&sender, replacement.last().unwrap());
    let boundary = fetch_boundary(&mut subscriber, &replacement).await;
    assert!(boundary.synced_to_tip);
    assert_eq!(boundary.scanned_tip.height, 999);
    let update = subscriber.single_update_builder.as_mut().unwrap().finish();
    assert_eq!(update.chain.as_ref().unwrap().height(), 999);
    wallet.apply_update(update).unwrap();
    assert_eq!(wallet.latest_checkpoint().height(), 999);
    assert_eq!(
        wallet.latest_checkpoint().hash(),
        replacement[998].block.block_hash()
    );
}

#[tokio::test]
async fn a_dense_ahead_wallet_rolls_back_when_a_partial_prefix_proves_a_reorg() {
    use bdk_wallet::chain::DescriptorExt;
    let mut wallets = [
        wallet(),
        bdk_wallet::Wallet::create(
            EXTERNAL.replace("/0/*", "/2/*"),
            INTERNAL.replace("/1/*", "/3/*"),
        )
        .network(Network::Bitcoin)
        .lookahead(25)
        .create_wallet_no_persist()
        .unwrap(),
    ];
    let old = historical_blocks(5000);
    let mut cp = wallets[1].latest_checkpoint();
    for block in &old {
        cp = cp.insert(BlockId {
            height: block.height,
            hash: block.block.block_hash(),
        });
    }
    wallets[1]
        .apply_update(Update {
            chain: Some(cp),
            ..Default::default()
        })
        .unwrap();
    let (mut subscriber, _sender, _node) = multi_subscriber(&wallets);
    stage_blocks(&mut subscriber, &old);
    let first = subscriber.updates_batch().await.unwrap();
    assert_eq!(first.scanned_tip.height, 1000);
    for wallet in &mut wallets {
        let id = wallet
            .public_descriptor(KeychainKind::External)
            .descriptor_id();
        wallet.apply_update(first.updates[&id].clone()).unwrap();
    }
    assert_eq!(wallets[1].latest_checkpoint().height(), 5000);
    let mut replacement = historical_blocks(5001);
    replacement[998].block.header.nonce = 99_999;
    relink(&mut replacement);
    subscriber.apply_chain_event(&BlockHeaderChanges::Reorganized {
        accepted: replacement[998..].iter().map(indexed_header).collect(),
        reorganized: old[998..].iter().map(indexed_header).collect(),
    });
    for block in &replacement[998..] {
        subscriber.receive_filter(block.height, block.block.header, basic_filter(block));
    }
    let partial = subscriber.updates_batch().await.unwrap();
    assert_eq!(partial.scanned_tip.height, 1000);
    assert!(!partial.synced_to_tip);
    for wallet in &mut wallets {
        let id = wallet
            .public_descriptor(KeychainKind::External)
            .descriptor_id();
        assert_eq!(partial.updates[&id].chain.as_ref().unwrap().height(), 1000);
        wallet.apply_update(partial.updates[&id].clone()).unwrap();
        assert_eq!(wallet.latest_checkpoint().height(), 1000);
        assert_eq!(
            wallet.latest_checkpoint().hash(),
            replacement[999].block.block_hash()
        );
    }
}

#[tokio::test]
async fn automatic_walkback_preserves_the_persisted_script_window() {
    use bdk_wallet::chain::Merge;
    let mut continuous_wallet = wallet();
    let mut saved_wallet = wallet();
    let mut persisted = saved_wallet.take_staged().unwrap();
    let policy = SyncPolicy::Recovery {
        cp: (0, saved_wallet.latest_checkpoint().hash()).into(),
        used_script_index: 100,
        reuse_persisted_history: false,
    };
    let mut blocks = historical_blocks(1001);
    let first = payment(
        saved_wallet
            .peek_address(KeychainKind::External, 50)
            .address
            .script_pubkey(),
        OutPoint::null(),
    );
    blocks[899].block.txdata = vec![first.clone()];
    // These addresses were outside the script window when their original filters were checked.
    blocks[994].block.txdata = vec![payment(
        saved_wallet
            .peek_address(KeychainKind::External, 125)
            .address
            .script_pubkey(),
        OutPoint::null(),
    )];
    blocks[1000].block.txdata = vec![payment(
        saved_wallet
            .peek_address(KeychainKind::External, 200)
            .address
            .script_pubkey(),
        OutPoint::null(),
    )];
    relink(&mut blocks);
    let (mut original, sender, _node) = subscriber(&continuous_wallet, policy, &[]);
    original.receiver.try_recv().unwrap();
    stage_blocks(&mut original, &blocks);
    fetch_boundary(&mut original, &blocks).await;
    let partial = original.single_update_builder.as_mut().unwrap().finish();
    continuous_wallet.apply_update(partial.clone()).unwrap();
    saved_wallet.apply_update(partial).unwrap();
    persisted.merge(saved_wallet.take_staged().unwrap());
    send_tip(&sender, blocks.last().unwrap());
    fetch_boundary(&mut original, &blocks).await;
    continuous_wallet
        .apply_update(original.single_update_builder.as_mut().unwrap().finish())
        .unwrap();
    let mut reloaded = bdk_wallet::Wallet::load()
        .lookahead(25)
        .load_wallet_no_persist(persisted)
        .unwrap()
        .unwrap();
    let start = builder::walk_back_max_reorg(reloaded.latest_checkpoint());
    assert_eq!(start.height, 993);
    let client = builder::Builder::new(Network::Bitcoin)
        .build_with_wallet(
            &reloaded,
            SyncConfig::wallet_recovery_sync(start, 100)
                .reuse_persisted_history()
                .build(),
        )
        .unwrap();
    let (_client, _logs, mut restarted) = client.subscribe();
    let (sender, receiver) = unbounded_channel();
    restarted.receiver = receiver;
    stage_blocks(&mut restarted, &blocks[993..]);
    send_tip(&sender, blocks.last().unwrap());
    loop {
        let boundary = fetch_boundary(&mut restarted, &blocks).await;
        reloaded
            .apply_update(restarted.single_update_builder.as_mut().unwrap().finish())
            .unwrap();
        if boundary.synced_to_tip {
            break;
        }
    }
    let ids = |wallet: &bdk_wallet::Wallet| {
        wallet
            .transactions()
            .map(|tx| tx.tx_node.txid)
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(
        ids(&continuous_wallet),
        BTreeSet::from([first.compute_txid()])
    );
    assert_eq!(ids(&reloaded), ids(&continuous_wallet));
    assert_eq!(reloaded.balance(), continuous_wallet.balance());
    assert_eq!(reloaded.derivation_index(KeychainKind::External), Some(50));
}

#[test]
fn persisted_walkback_reindexes_replacement_hashes_and_explicit_recovery() {
    let mut wallet = wallet();
    let blocks = historical_blocks(1000);
    let mut cp = wallet.latest_checkpoint();
    for block in &blocks {
        cp = cp.insert(BlockId {
            height: block.height,
            hash: block.block.block_hash(),
        });
    }
    wallet
        .apply_update(Update {
            chain: Some(cp),
            ..Default::default()
        })
        .unwrap();
    let start = builder::walk_back_max_reorg(wallet.latest_checkpoint());
    let mut builder = UpdateBuilder::new(
        wallet.latest_checkpoint(),
        IndexedTxGraph::new(wallet.spk_index().clone()),
    );
    builder.reuse_persisted_walkback(start);
    assert!(builder.persisted_walkback.is_some());
    let replacement = block(
        payment(
            wallet
                .peek_address(KeychainKind::External, 0)
                .address
                .script_pubkey(),
            OutPoint::null(),
        ),
        995,
    );
    assert_ne!(
        replacement.block.block_hash(),
        blocks[994].block.block_hash()
    );
    builder.apply_block_event(&replacement);
    assert!(builder
        .finish()
        .tx_update
        .txs
        .iter()
        .any(|tx| tx.compute_txid() == replacement.block.txdata[0].compute_txid()));
    let policy = SyncPolicy::Recovery {
        cp: start,
        used_script_index: 100,
        reuse_persisted_history: false,
    };
    let (subscriber, _sender, _node) = subscriber(&wallet, policy, &[]);
    assert!(subscriber
        .single_update_builder
        .as_ref()
        .unwrap()
        .persisted_walkback
        .is_none());
}
