#![doc = include_str!("../README.md")]
//! ## Examples
//!
//! If you have an existing project that leverages `bdk_wallet`, building the compact block filter
//! _node_ and _client_ is simple. You may construct and configure a node to integrate with your
//! wallet by using the [`BuilderExt`](crate::builder) and [`Builder`](crate::builder).
//!
//! ```no_run
//! # const RECEIVE: &str = "tr([7d94197e/86'/1'/0']tpubDCyQVJj8KzjiQsFjmb3KwECVXPvMwvAxxZGCP9XmWSopmjW3bCV3wD7TgxrUhiGSueDS1MU5X1Vb1YjYcp8jitXc5fXfdC1z68hDDEyKRNr/0/*)";
//! # const CHANGE: &str = "tr([7d94197e/86'/1'/0']tpubDCyQVJj8KzjiQsFjmb3KwECVXPvMwvAxxZGCP9XmWSopmjW3bCV3wD7TgxrUhiGSueDS1MU5X1Vb1YjYcp8jitXc5fXfdC1z68hDDEyKRNr/1/*)";
//! use bdk_wallet::Wallet;
//! use bdk_wallet::bitcoin::Network;
//! use bdk_kyoto::builder::{Builder, BuilderExt};
//! use bdk_kyoto::{LightClient, SyncConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let mut wallet = Wallet::create(RECEIVE, CHANGE)
//!         .network(Network::Signet)
//!         .create_wallet_no_persist()?;
//!
//!     let sync_config = SyncConfig::sync_from_last_checkpoint().build();
//!     let client = Builder::new(Network::Signet).build_with_wallet(&wallet, sync_config)?;
//!     let (client, _, mut update_subscriber) = client.subscribe();
//!     client.start();
//!
//!     loop {
//!         let update = update_subscriber.update().await?;
//!         wallet.apply_update(update)?;
//!         return Ok(());
//!     }
//! }
//! ```

#![warn(missing_docs)]
use std::collections::HashSet;
use std::collections::{BTreeMap, BTreeSet};

use bdk_wallet::chain::BlockId;
use bdk_wallet::chain::CheckPoint;
use bdk_wallet::chain::DescriptorId;
pub use bdk_wallet::Update;

use bdk_wallet::chain::{keychain_txout::KeychainTxOutIndex, IndexedTxGraph};
use bdk_wallet::chain::{ConfirmationBlockTime, TxUpdate};
use bdk_wallet::KeychainKind;

pub extern crate bip157;

use bip157::chain::BlockHeaderChanges;
use bip157::error::FetchBlockError;
use bip157::tokio;
use bip157::Event;
use bip157::IndexedBlock;
use bip157::ScriptBuf;
#[cfg(test)]
use bip157::SyncUpdate;
#[doc(inline)]
pub use bip157::{
    BlockHash, ClientError, FeeRate, HashCheckpoint, Info, Node, RejectPayload, RejectReason,
    Requester, TrustedPeer, Warning, Wtxid,
};

#[doc(inline)]
pub use bip157::Receiver;
#[doc(inline)]
pub use bip157::UnboundedReceiver;

#[doc(inline)]
pub use builder::BuilderExt;

use crate::sync_policy::NewWallet;
use crate::sync_policy::RecoverFromCheckpoint;
use crate::sync_policy::SyncFromLastCheckpoint;
pub mod builder;

/// State of the light client.
pub mod state {
    /// Client state when idle.
    pub struct Idle;
    /// Client state when subscribed to events.
    pub struct Subscribed;
    /// Client state when active.
    pub struct Active;
}

mod sealed {
    pub trait Sealed {}
}

impl sealed::Sealed for state::Idle {}
impl sealed::Sealed for state::Subscribed {}
impl sealed::Sealed for state::Active {}

/// State of the client.
pub trait State: sealed::Sealed {}

impl State for state::Idle {}
impl State for state::Subscribed {}
impl State for state::Active {}

/// Subscribe to events, notably
#[derive(Debug)]
pub struct LoggingSubscribers {
    /// Receive informational messages as the node runs.
    pub info_subscriber: Receiver<Info>,
    /// Receive warnings from the node as it runs.
    pub warning_subscriber: Receiver<Warning>,
}

/// A client and associated structs to send and receive events to and from a node process.
///
/// The client has three states:
/// - [`Idle`]: the client has been initialized.
/// - [`Subscribed`]: the application is ready to handle logs and updates, but the process is not
///   running yet
/// - [`Active`]: the client is actively fetching data and may now handle requests.
#[derive(Debug)]
pub struct LightClient<S: State, W: Wallets> {
    // Send events to a running node (i.e. broadcast a transaction).
    requester: Requester,
    // Receive info/warnings from the node as it runs.
    logging_subscribers: Option<LoggingSubscribers>,
    // Receive wallet updates from a node.
    update_subscriber: Option<UpdateSubscriber<W>>,
    // The underlying node that must be run to fetch blocks from peers.
    node: Option<Node>,
    _marker: core::marker::PhantomData<S>,
}

impl<W: Wallets> LightClient<state::Idle, W> {
    fn new(
        requester: Requester,
        logging: LoggingSubscribers,
        update: UpdateSubscriber<W>,
        node: bip157::Node,
    ) -> LightClient<state::Idle, W> {
        LightClient {
            requester,
            logging_subscribers: Some(logging),
            update_subscriber: Some(update),
            node: Some(node),
            _marker: core::marker::PhantomData,
        }
    }

    /// Subscribe to events emitted by the underlying data fetching process. This includes logging
    /// and wallet updates. During this step, one may start threads that log to a file and apply
    /// updates to a wallet.
    ///
    /// # Returns
    ///
    /// - [`LightClient<Subscribed>`], a client ready to start.
    /// - [`LoggingSubscribers`], info and warning messages to display to a user or write to file.
    /// - [`UpdateSubscriber`], used to await updates related to the user's wallet.
    pub fn subscribe(
        mut self,
    ) -> (
        LightClient<state::Subscribed, W>,
        LoggingSubscribers,
        UpdateSubscriber<W>,
    ) {
        let logging =
            core::mem::take(&mut self.logging_subscribers).expect("cannot subscribe twice.");
        let updates =
            core::mem::take(&mut self.update_subscriber).expect("cannot subscribe twice.");
        let client = LightClient {
            requester: self.requester,
            logging_subscribers: None,
            update_subscriber: None,
            node: self.node,
            _marker: core::marker::PhantomData,
        };
        (client, logging, updates)
    }
}

impl<W: Wallets> LightClient<state::Subscribed, W> {
    /// Start fetching data for the wallet on a dedicated [`tokio::task`]. This will continually
    /// run until terminated or no peers could be found.
    ///
    /// # Panics
    ///
    /// If there is no [`tokio::runtime::Runtime`] to drive execution. Common in synchronous
    /// setups.
    pub fn start(mut self) -> LightClient<state::Active, W> {
        let node = core::mem::take(&mut self.node).expect("cannot start twice.");
        tokio::task::spawn(async move { node.run().await });
        LightClient {
            requester: self.requester,
            logging_subscribers: None,
            update_subscriber: None,
            node: None,
            _marker: core::marker::PhantomData,
        }
    }

    /// Take the underlying node process to run in a custom way. Examples include using a dedicated
    /// [`tokio::runtime::Runtime`] or [`tokio::runtime::Handle`] to drive execution.
    pub fn managed_start(mut self) -> (LightClient<state::Active, W>, Node) {
        let node = core::mem::take(&mut self.node).expect("cannot start twice.");
        let client = LightClient {
            requester: self.requester,
            logging_subscribers: None,
            update_subscriber: None,
            node: None,
            _marker: core::marker::PhantomData,
        };
        (client, node)
    }
}

impl<W: Wallets> LightClient<state::Active, W> {
    /// The client is active and may now handle requests with a [`Requester`].
    pub fn requester(self) -> Requester {
        self.requester
    }
}

impl<W: Wallets> From<LightClient<state::Active, W>> for Requester {
    fn from(value: LightClient<state::Active, W>) -> Self {
        value.requester
    }
}

impl<W: Wallets> AsRef<Requester> for LightClient<state::Active, W> {
    fn as_ref(&self) -> &Requester {
        &self.requester
    }
}

/// Tag for number of wallets.
pub mod wallets {
    /// Client for a single wallet.
    #[derive(Debug)]
    pub struct Single;
    /// Client for multiple wallets.
    #[derive(Debug)]
    pub struct Multiple;
}

impl sealed::Sealed for wallets::Single {}
impl sealed::Sealed for wallets::Multiple {}

/// Number of wallets.
pub trait Wallets: sealed::Sealed {}

impl Wallets for wallets::Single {}
impl Wallets for wallets::Multiple {}

/// Interpret events from a node that is running to apply
/// updates to an underlying wallet.
#[derive(Debug)]
pub struct UpdateSubscriber<W: Wallets> {
    // request information from the client
    requester: Requester,
    // channel receiver
    receiver: UnboundedReceiver<Event>,
    // queued blocks to fetch
    queued_blocks: BTreeSet<(u32, BlockHash)>,
    // Retain a consumed sync boundary when a block fetch is cancelled.
    filters_synced: bool,
    // Verified headers are deliberately separate from the checkpoint delivered to wallets.
    header_chain: BTreeMap<u32, CheckPoint>,
    scanned_tip: BlockId,
    last_boundary_height: u32,
    pending_filters: BTreeMap<u32, (bip157::Header, bip157::BlockFilter)>,
    pending_boundary: Option<ScanBoundary>,
    tip_boundary: Option<BlockId>,
    headers_only: bool,
    awaiting_reorg: bool,
    // queued scripts to check filters
    spk_cache: HashSet<ScriptBuf>,
    // processes events for the wallet.
    single_update_builder: Option<UpdateBuilder>,
    // process events for multiple wallets.
    multiple_updates_builder: Option<BTreeMap<DescriptorId, UpdateBuilder>>,
    _marker: core::marker::PhantomData<W>,
}

impl<W: Wallets> UpdateSubscriber<W> {
    fn new(
        requester: Requester,
        policy: SyncPolicy,
        receiver: UnboundedReceiver<Event>,
        cp: CheckPoint,
        scan_start: HashCheckpoint,
        graph: IndexedTxGraph<ConfirmationBlockTime, KeychainTxOutIndex<KeychainKind>>,
    ) -> UpdateSubscriber<wallets::Single> {
        let start = cp
            .clone()
            .insert(BlockId {
                height: scan_start.height,
                hash: scan_start.hash,
            })
            .get(scan_start.height)
            .expect("inserted scan anchor");
        let header_chain = start.iter().map(|cp| (cp.height(), cp)).collect();
        let mut update_builder = UpdateBuilder::new(cp, graph);
        if matches!(
            policy,
            SyncPolicy::SyncFromLast
                | SyncPolicy::Recovery {
                    reuse_persisted_history: true,
                    ..
                }
        ) {
            update_builder.reuse_persisted_walkback(scan_start);
        }
        update_builder.extend_index_for_recovery(policy);
        let spk_cache = update_builder.peek_scripts_from_policy(policy);
        UpdateSubscriber {
            requester,
            receiver,
            single_update_builder: Some(update_builder),
            multiple_updates_builder: None,
            queued_blocks: BTreeSet::new(),
            filters_synced: false,
            header_chain,
            scanned_tip: BlockId {
                height: scan_start.height,
                hash: scan_start.hash,
            },
            last_boundary_height: scan_start.height,
            pending_filters: BTreeMap::new(),
            pending_boundary: None,
            tip_boundary: None,
            headers_only: matches!(policy, SyncPolicy::NewWallet),
            awaiting_reorg: false,
            spk_cache,
            _marker: core::marker::PhantomData,
        }
    }

    fn new_multiple(
        requester: Requester,
        receiver: UnboundedReceiver<Event>,
        wallet_iter: impl Iterator<
            Item = (
                DescriptorId,
                SyncPolicy,
                CheckPoint,
                IndexedTxGraph<ConfirmationBlockTime, KeychainTxOutIndex<KeychainKind>>,
            ),
        >,
    ) -> UpdateSubscriber<wallets::Multiple> {
        let wallets = wallet_iter.collect::<Vec<_>>();
        let (scan_start, mut header_chain) = wallets
            .iter()
            .map(|wallet| {
                (
                    builder::walk_back_max_reorg(wallet.2.clone()),
                    wallet.2.clone(),
                )
            })
            .min_by_key(|(cp, _)| cp.height)
            .expect("validated nonempty wallets");
        for wallet in &wallets {
            for cp in wallet
                .2
                .iter()
                .filter(|cp| cp.height() <= scan_start.height)
            {
                header_chain = header_chain.insert(cp.block_id());
            }
        }
        let header_chain = header_chain
            .get(scan_start.height)
            .expect("scan anchor belongs to its wallet")
            .iter()
            .map(|cp| (cp.height(), cp))
            .collect();
        let mut update_map = BTreeMap::new();
        let mut spk_cache = HashSet::new();
        for wallet in wallets {
            let mut update_builder = UpdateBuilder::new(wallet.2, wallet.3);
            update_builder.reuse_persisted_walkback(scan_start);
            update_builder.extend_index_for_recovery(wallet.1);
            spk_cache.extend(update_builder.peek_scripts_from_policy(wallet.1));
            update_map.insert(wallet.0, update_builder);
        }
        UpdateSubscriber {
            requester,
            receiver,
            single_update_builder: None,
            multiple_updates_builder: Some(update_map),
            queued_blocks: BTreeSet::new(),
            filters_synced: false,
            header_chain,
            scanned_tip: BlockId {
                height: scan_start.height,
                hash: scan_start.hash,
            },
            last_boundary_height: scan_start.height,
            pending_filters: BTreeMap::new(),
            pending_boundary: None,
            tip_boundary: None,
            headers_only: false,
            awaiting_reorg: false,
            spk_cache,
            _marker: core::marker::PhantomData,
        }
    }

    fn queue_block(&mut self, height: u32, hash: BlockHash) {
        self.queued_blocks.insert((height, hash));
    }

    fn receive_filter(&mut self, height: u32, header: bip157::Header, filter: bip157::BlockFilter) {
        if height > self.scanned_tip.height {
            self.pending_filters.insert(height, (header, filter));
        } else if self
            .header_chain
            .get(&height)
            .is_some_and(|cp| cp.hash() == header.block_hash())
        {
            // Explicit rescans may revisit already checked heights with an extended script cache.
            if filter
                .match_any(
                    &header.block_hash(),
                    self.spk_cache.iter().map(|script| script.as_bytes()),
                )
                .expect("verified filter must decode")
            {
                self.queue_block(height, header.block_hash());
            }
        }
    }

    fn apply_chain_event(&mut self, changes: &BlockHeaderChanges) {
        if let BlockHeaderChanges::Reorganized { accepted, .. } = changes {
            if let Some(first) = accepted.iter().min_by_key(|header| header.height) {
                let fork_height = first.height;
                self.pending_filters
                    .retain(|height, _| *height < fork_height);
                self.queued_blocks
                    .retain(|(height, _)| *height < fork_height);
                if self.scanned_tip.height >= fork_height {
                    // The preceding header is the common ancestor of both chains.
                    self.scanned_tip = BlockId {
                        height: fork_height.saturating_sub(1),
                        hash: first.header.prev_blockhash,
                    };
                }
                self.last_boundary_height = self.last_boundary_height.min(self.scanned_tip.height);
                self.pending_boundary = None;
                self.tip_boundary = None;
                self.filters_synced = false;
                self.awaiting_reorg = false;
                if let Some(single) = self.single_update_builder.as_mut() {
                    single.checkpoint_checked = false;
                }
                if let Some(multiple) = self.multiple_updates_builder.as_mut() {
                    for builder in multiple.values_mut() {
                        builder.checkpoint_checked = false;
                    }
                }
            }
        }
        match changes {
            BlockHeaderChanges::Connected(header) => {
                self.insert_header(header.height, header.block_hash());
            }
            BlockHeaderChanges::Reorganized { accepted, .. } => {
                let mut accepted = accepted.iter().collect::<Vec<_>>();
                accepted.sort_by_key(|header| header.height);
                for header in accepted {
                    self.insert_header(header.height, header.block_hash());
                }
            }
            BlockHeaderChanges::ForkAdded(_) => {}
        }
    }

    fn insert_header(&mut self, height: u32, hash: BlockHash) {
        if self
            .header_chain
            .get(&height)
            .is_some_and(|cp| cp.hash() == hash)
        {
            return;
        }
        if self.header_chain.contains_key(&height) {
            self.header_chain.split_off(&height);
        }
        let previous = self
            .header_chain
            .range(..height)
            .next_back()
            .expect("verified header follows the scan anchor")
            .1;
        let cp = previous.clone().insert(BlockId { height, hash });
        self.header_chain.insert(height, cp);
    }

    fn check_contiguous_filters(&mut self) {
        if self.awaiting_reorg {
            return;
        }
        let next_boundary = (self.last_boundary_height / CHECKPOINT_INTERVAL)
            .checked_add(1)
            .and_then(|period| period.checked_mul(CHECKPOINT_INTERVAL))
            .unwrap_or(u32::MAX);
        while self.scanned_tip.height < next_boundary {
            let Some(next_height) = self.scanned_tip.height.checked_add(1) else {
                break;
            };
            let Some((header, filter)) = self.pending_filters.get(&next_height) else {
                break;
            };
            let Some(verified) = self.header_chain.get(&next_height) else {
                break;
            };
            if verified.hash() != header.block_hash() {
                // A stale filter must not bridge a reorganization gap.
                self.pending_filters.remove(&next_height);
                break;
            }
            let hash = verified.hash();
            let matched = filter
                .match_any(&hash, self.spk_cache.iter().map(|script| script.as_bytes()))
                .expect("verified filter must decode");
            self.pending_filters.remove(&next_height);
            if matched {
                self.queue_block(next_height, hash);
            }
            self.scanned_tip = BlockId {
                height: next_height,
                hash,
            };
        }
        if self.scanned_tip.height >= next_boundary
            && self.scanned_tip.height > self.last_boundary_height
        {
            self.pending_boundary = Some(ScanBoundary {
                scanned_tip: self.scanned_tip,
                synced_to_tip: false,
            });
        }
        if let Some(tip) = self.tip_boundary {
            if self.headers_only
                || (self.scanned_tip.height >= tip.height && self.scanned_tip.hash == tip.hash)
            {
                self.pending_boundary = Some(ScanBoundary {
                    scanned_tip: tip,
                    synced_to_tip: true,
                });
            }
        }
    }

    async fn sync(&mut self) -> Result<ScanBoundary, UpdateError> {
        let requester = self.requester.clone();
        self.sync_with_fetch(|hash| requester.get_block(hash)).await
    }

    async fn sync_with_fetch<F, Fut>(&mut self, mut fetch: F) -> Result<ScanBoundary, UpdateError>
    where
        F: FnMut(BlockHash) -> Fut,
        Fut: core::future::Future<Output = Result<IndexedBlock, FetchBlockError>>,
    {
        loop {
            self.check_contiguous_filters();
            if let Some(boundary) = self.pending_boundary {
                // Apply funding outputs before later spends; retain every outstanding fetch
                // and its boundary across cancellation.
                let mut invalidated = false;
                while let Some(&(_, hash)) = self.queued_blocks.first() {
                    let block = match fetch(hash).await {
                        Ok(block) => block,
                        Err(FetchBlockError::UnknownHash) => {
                            invalidated = true;
                            break;
                        }
                        Err(FetchBlockError::RecvError) if self.requester.is_running() => {
                            invalidated = true;
                            break;
                        }
                        Err(_) => return Err(UpdateError::NodeStopped),
                    };
                    if let Some(single) = self.single_update_builder.as_mut() {
                        single.apply_block_event(&block);
                    }
                    if let Some(multiple) = self.multiple_updates_builder.as_mut() {
                        for builder in multiple.values_mut() {
                            builder.apply_block_event(&block);
                        }
                    }
                    self.queued_blocks.pop_first();
                }
                if invalidated {
                    // The canonical chain changed while fetching. Consume its queued chain
                    // event and replacement filters before allowing a persistable boundary.
                    self.pending_boundary = None;
                    self.tip_boundary = None;
                    self.filters_synced = false;
                    self.awaiting_reorg = true;
                } else {
                    let cp = self
                        .header_chain
                        .get(&boundary.scanned_tip.height)
                        .expect("boundary has a verified header");
                    if let Some(single) = self.single_update_builder.as_mut() {
                        // A wallet already ahead of the shared scan frontier retains its
                        // previously persisted checkpoint until the scan catches up.
                        single.advance_checkpoint(cp, &self.header_chain);
                        single.extend_active_script_window();
                        self.spk_cache
                            .extend(single.peek_script_to_keychain_lookahead());
                    }
                    if let Some(multiple) = self.multiple_updates_builder.as_mut() {
                        for builder in multiple.values_mut() {
                            builder.advance_checkpoint(cp, &self.header_chain);
                            builder.extend_active_script_window();
                            self.spk_cache
                                .extend(builder.peek_script_to_keychain_lookahead());
                        }
                    }
                    self.last_boundary_height = boundary.scanned_tip.height;
                    self.scanned_tip = boundary.scanned_tip;
                    self.pending_boundary = None;
                    if boundary.synced_to_tip {
                        self.filters_synced = false;
                        self.tip_boundary = None;
                        self.headers_only = false;
                    }
                    return Ok(boundary);
                }
            }
            let Some(message) = self.receiver.recv().await else {
                return Err(UpdateError::NodeStopped);
            };
            match message {
                Event::IndexedFilter(filter) => {
                    let height = filter.height();
                    let (header, filter) = filter.into_parts();
                    self.receive_filter(height, header, filter);
                }
                Event::ChainUpdate(changeset) => self.apply_chain_event(&changeset),
                Event::FiltersSynced(update) => {
                    self.filters_synced = true;
                    self.tip_boundary = Some(BlockId {
                        height: update.tip.height,
                        hash: update.tip.hash,
                    });
                }
            }
        }
    }
}

/// Height interval between persistable partial updates (at global multiples).
/// A scan starting between multiples emits its first partial after fewer filters.
/// Matched blocks are fetched before emitting the boundary, so download stalls may delay it.
pub const CHECKPOINT_INTERVAL: u32 = 1_000;

/// A persistable update boundary, shared by every configured wallet.
#[derive(Debug, Clone)]
pub struct UpdateBatch<T> {
    /// Wallet updates for this boundary.
    pub updates: T,
    /// Last fully scanned block, using the verified header chain's hash.
    /// A wallet previously persisted beyond this shared frontier retains its own higher
    /// checkpoint until the scan catches up; newly downloaded checkpoints never exceed this tip.
    pub scanned_tip: BlockId,
    /// True only for the final boundary signaled by the node's tip sync event.
    pub synced_to_tip: bool,
}

#[derive(Debug, Clone, Copy)]
struct ScanBoundary {
    scanned_tip: BlockId,
    synced_to_tip: bool,
}

impl UpdateSubscriber<wallets::Single> {
    /// Return the next persistable partial or tip update.
    /// Partial boundaries occur every [`CHECKPOINT_INTERVAL`] contiguous filters, after all
    /// matched blocks have been applied in chain order. Cancellation retains pending work.
    pub async fn update_batch(&mut self) -> Result<UpdateBatch<Update>, UpdateError> {
        let boundary = self.sync().await?;
        Ok(UpdateBatch {
            updates: self.single_update_builder.as_mut().unwrap().finish(),
            scanned_tip: boundary.scanned_tip,
            synced_to_tip: boundary.synced_to_tip,
        })
    }

    /// Return the most recent [`Update`] for a wallet once it has synced to the network's tip.
    /// This may take a significant portion of time during wallet recoveries or dormant wallets.
    /// Note that you may call this method in a loop as long as the node is running.
    pub async fn update(&mut self) -> Result<Update, UpdateError> {
        while !self.sync().await?.synced_to_tip {}
        Ok(self.single_update_builder.as_mut().unwrap().finish())
    }
}

impl UpdateSubscriber<wallets::Multiple> {
    /// Return every wallet's next persistable update at the same partial or tip boundary.
    pub async fn updates_batch(
        &mut self,
    ) -> Result<UpdateBatch<BTreeMap<DescriptorId, Update>>, UpdateError> {
        let boundary = self.sync().await?;
        let updates = self
            .multiple_updates_builder
            .as_mut()
            .unwrap()
            .iter_mut()
            .map(|(id, builder)| (*id, builder.finish()))
            .collect();
        Ok(UpdateBatch {
            updates,
            scanned_tip: boundary.scanned_tip,
            synced_to_tip: boundary.synced_to_tip,
        })
    }

    /// Return a set of [`Update`] for the configured wallets when synced to the network's tip. The
    /// [`Update`] are grouped with the [`DescriptorId`] of the external descriptor for each
    /// wallet.
    ///
    /// This may take a significant portion of time during wallet recoveries or dormant wallets.
    /// Note that you may call this method in a loop as long as the node is running.
    pub async fn updates(
        &mut self,
    ) -> Result<impl Iterator<Item = (DescriptorId, Update)>, UpdateError> {
        while !self.sync().await?.synced_to_tip {}
        let mut map = BTreeMap::new();
        for (id, builder) in self.multiple_updates_builder.as_mut().unwrap().iter_mut() {
            map.insert(*id, builder.finish());
        }
        Ok(map.into_iter())
    }
}

#[derive(Debug)]
struct UpdateBuilder {
    // Changes to the wallet local chain.
    cp: CheckPoint,
    // Transaction graph, required to process incoming blocks.
    graph: IndexedTxGraph<ConfirmationBlockTime, KeychainTxOutIndex<KeychainKind>>,
    recovery_script_count: Option<u32>,
    checkpoint_checked: bool,
    checkpoint_conflict_height: Option<u32>,
    persisted_walkback: Option<CheckPoint>,
}

impl UpdateBuilder {
    fn new(
        cp: CheckPoint,
        graph: IndexedTxGraph<ConfirmationBlockTime, KeychainTxOutIndex<KeychainKind>>,
    ) -> Self {
        Self {
            cp,
            graph,
            recovery_script_count: None,
            checkpoint_checked: false,
            checkpoint_conflict_height: None,
            persisted_walkback: None,
        }
    }

    fn reuse_persisted_walkback(&mut self, start: HashCheckpoint) {
        if start.height < self.cp.height() && builder::walk_back_max_reorg(self.cp.clone()) == start
        {
            self.persisted_walkback = Some(self.cp.clone());
        }
    }

    fn advance_checkpoint(&mut self, cp: &CheckPoint, headers: &BTreeMap<u32, CheckPoint>) {
        if self
            .persisted_walkback
            .as_ref()
            .is_some_and(|saved| cp.height() >= saved.height())
        {
            self.persisted_walkback = None;
        }
        if cp.height() >= self.cp.height() {
            self.cp = cp.clone();
            self.checkpoint_checked = true;
            self.checkpoint_conflict_height = None;
            return;
        }
        if !self.checkpoint_checked {
            // Header sync precedes filter sync. Check a pre-existing ahead wallet once,
            // and again after a reorg, rather than walking its chain for every filter.
            self.checkpoint_conflict_height = self
                .cp
                .iter()
                .filter_map(|known| {
                    headers
                        .get(&known.height())
                        .filter(|canonical| canonical.hash() != known.hash())
                        .map(|_| known.height())
                })
                .min();
            self.checkpoint_checked = true;
        }
        if self
            .checkpoint_conflict_height
            .is_some_and(|height| height <= cp.height())
        {
            // The safe prefix carries an explicit conflict, so BDK can invalidate every
            // disconnected higher checkpoint, including a shorter higher-work chain.
            self.cp = cp.clone();
            self.checkpoint_conflict_height = None;
        }
    }

    fn apply_block_event(&mut self, block: &IndexedBlock) {
        if self
            .persisted_walkback
            .as_ref()
            .and_then(|saved| saved.get(block.height))
            .is_some_and(|saved| saved.hash() == block.block.block_hash())
        {
            // Automatic rollback verifies the canonical identity of already persisted history.
            // Do not retroactively reindex it with a newer script window. Explicit earlier
            // recoveries and later rescans still reindex; replacement reorg hashes never skip.
            return;
        }
        let height = block.height;
        let block = &block.block;
        let _ = self.graph.apply_block_relevant(block, height);
    }

    // Derive the scripts a recovery checks filters against into the index, when they go beyond
    // the wallet's lookahead. Otherwise blocks matched by those scripts would be applied without
    // their transactions. Covers the same range as `peek_scripts`.
    fn extend_index_for_recovery(&mut self, policy: SyncPolicy) {
        let SyncPolicy::Recovery {
            used_script_index: to_index,
            ..
        } = policy
        else {
            return;
        };
        self.recovery_script_count = Some(to_index);
        self.extend_active_script_window();
    }

    fn extend_active_script_window(&mut self) {
        let Some(count) = self.recovery_script_count else {
            return;
        };
        let index = &mut self.graph.index;
        let to_index = count.max(index.lookahead());
        for keychain in [KeychainKind::External, KeychainKind::Internal] {
            let last_revealed = index.last_revealed_index(keychain).unwrap_or(0);
            if let Some(target) = last_revealed.saturating_add(to_index).checked_sub(1) {
                let _ = index.lookahead_to_target(keychain, target);
            }
        }
    }

    #[inline]
    fn peek_scripts_from_policy(&self, policy: SyncPolicy) -> HashSet<ScriptBuf> {
        match policy {
            SyncPolicy::NewWallet | SyncPolicy::SyncFromLast => {
                self.peek_script_to_keychain_lookahead()
            }
            SyncPolicy::Recovery {
                used_script_index, ..
            } => self.peek_scripts(used_script_index.max(self.graph.index.lookahead())),
        }
    }

    #[inline]
    fn peek_script_to_keychain_lookahead(&self) -> HashSet<ScriptBuf> {
        self.peek_scripts(
            self.recovery_script_count
                .unwrap_or(0)
                .max(self.graph.index.lookahead()),
        )
    }

    fn peek_scripts(&self, to_index: u32) -> HashSet<ScriptBuf> {
        let mut spk_cache = HashSet::new();
        let keychain = &self.graph.index;
        // We pre-compute an SPK cache so as to not call `unbounded_spk_iter` for each filter
        let last_revealed = keychain.last_revealed_indices();
        let ext_index = last_revealed
            .get(&KeychainKind::External)
            .copied()
            .unwrap_or(0);
        let unbounded_ext_spk_iter = keychain
            .unbounded_spk_iter(KeychainKind::External)
            .expect("wallet must have external keychain");
        let bound = (ext_index + to_index) as usize;
        let bounded_ext_iter = unbounded_ext_spk_iter.take(bound).map(|(_, script)| script);
        spk_cache.extend(bounded_ext_iter);
        let int_index = last_revealed
            .get(&KeychainKind::Internal)
            .copied()
            .unwrap_or(0);
        let unbounded_int_spk_iter = keychain.unbounded_spk_iter(KeychainKind::Internal);
        if let Some(int_spk_iter) = unbounded_int_spk_iter {
            let bound = (int_index + to_index) as usize;
            let bounded_int_iter = int_spk_iter.take(bound).map(|(_, script)| script);
            spk_cache.extend(bounded_int_iter);
        }
        spk_cache
    }

    fn finish(&mut self) -> Update {
        let tx_update = TxUpdate::from(self.graph.graph().clone());
        let graph = core::mem::take(&mut self.graph);
        let last_active_indices = graph.index.last_used_indices();
        self.graph = IndexedTxGraph::new(graph.index);
        Update {
            tx_update,
            last_active_indices,
            chain: Some(self.cp.clone()),
        }
    }
}

/// Errors encountered when attempting to construct a wallet update.
#[derive(Debug, Clone, Copy)]
pub enum UpdateError {
    /// The node has stopped running.
    NodeStopped,
}

impl std::fmt::Display for UpdateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateError::NodeStopped => write!(f, "the node halted execution."),
        }
    }
}

impl std::error::Error for UpdateError {}

/// How wallet syncing should behave.
pub mod sync_policy {
    /// A new wallet.
    #[derive(Debug, Clone, Copy)]
    pub struct NewWallet;
    /// Sync from the last checkpoint.
    #[derive(Debug, Clone, Copy)]
    pub struct SyncFromLastCheckpoint;
    /// Recover an existing wallet.
    #[derive(Debug, Clone, Copy)]
    pub struct RecoverFromCheckpoint;
}

impl sealed::Sealed for sync_policy::NewWallet {}
impl sealed::Sealed for sync_policy::SyncFromLastCheckpoint {}
impl sealed::Sealed for sync_policy::RecoverFromCheckpoint {}

/// State of the client.
pub trait SyncPolicyType: sealed::Sealed {}

impl SyncPolicyType for sync_policy::NewWallet {}
impl SyncPolicyType for sync_policy::SyncFromLastCheckpoint {}
impl SyncPolicyType for sync_policy::RecoverFromCheckpoint {}

#[derive(Debug, Clone, Copy)]
enum SyncPolicy {
    NewWallet,
    Recovery {
        used_script_index: u32,
        cp: HashCheckpoint,
        reuse_persisted_history: bool,
    },
    SyncFromLast,
}

/// The configuration for a sync with the blockchain.
#[derive(Debug, Clone, Copy)]
pub struct SyncConfig<P: SyncPolicyType>(SyncPolicy, core::marker::PhantomData<P>);

/// Build a new [`SyncConfig`].
pub struct SyncConfigBuilder<P: SyncPolicyType> {
    policy: SyncPolicy,
    _marker: core::marker::PhantomData<P>,
}

impl SyncConfig<SyncFromLastCheckpoint> {
    /// Sync a wallet that is guaranteed to be new. This implies no scripts
    /// have been revealed. The light client will sync block headers to the
    /// currently active chain type, but will **omit** filter downloads.
    ///
    /// Subsequent updates after new blocks are mined will include filter checks.
    pub fn new_wallet_sync() -> SyncConfigBuilder<sync_policy::NewWallet> {
        SyncConfigBuilder {
            policy: SyncPolicy::NewWallet,
            _marker: core::marker::PhantomData,
        }
    }

    /// Sync the wallet from the last time it was synced.
    ///
    /// Filters are checked against the wallet's revealed scripts plus its lookahead. To check
    /// more scripts (for instance for a wallet that has not been synced in a while), load the
    /// wallet with a larger lookahead.
    ///
    /// **Warning**: for a new wallet this will sync the entire blockchain from genesis!
    /// If a wallet is new or being recovered, try a different sync configuration.
    pub fn sync_from_last_checkpoint() -> SyncConfigBuilder<sync_policy::SyncFromLastCheckpoint> {
        SyncConfigBuilder {
            policy: SyncPolicy::SyncFromLast,
            _marker: core::marker::PhantomData,
        }
    }

    /// Recover an existing wallet that does not have local data.
    ///
    /// # Arguments
    ///
    /// - `cp`: [`HashCheckpoint`]: The point in the blockchain to begin the recovery. This is the
    ///   expected first use of the wallet.
    ///
    ///- `used_script_index`: [`u32`]: The number of scripts that were used by the wallet. This must
    ///  be known ahead of time, as querying filters requires all potential scripts in the case of a
    ///  recovery. A conservative estimate for this configuration is 1000, used by Bitcoin Core.
    ///  Subsequent syncs will query fewer scripts.
    pub fn wallet_recovery_sync(
        cp: HashCheckpoint,
        used_script_index: u32,
    ) -> SyncConfigBuilder<sync_policy::RecoverFromCheckpoint> {
        SyncConfigBuilder {
            policy: SyncPolicy::Recovery {
                used_script_index,
                cp,
                reuse_persisted_history: false,
            },
            _marker: core::marker::PhantomData,
        }
    }
}

impl SyncConfigBuilder<NewWallet> {
    /// Return the completed [`SyncConfig`].
    pub fn build(self) -> SyncConfig<NewWallet> {
        SyncConfig(self.policy, core::marker::PhantomData)
    }
}

impl SyncConfigBuilder<SyncFromLastCheckpoint> {
    /// Return the completed [`SyncConfig`].
    pub fn build(self) -> SyncConfig<SyncFromLastCheckpoint> {
        SyncConfig(self.policy, core::marker::PhantomData)
    }
}

impl SyncConfigBuilder<RecoverFromCheckpoint> {
    /// Reuse unchanged persisted history during the initial seven-height reorg walk-back.
    ///
    /// Use this only to resume the same recovery from the wallet's latest persisted
    /// checkpoint with its original script count. The configured checkpoint must equal the
    /// wallet's bounded reorg walk-back. Explicit recovery scans reindex history by default.
    pub fn reuse_persisted_history(mut self) -> Self {
        if let SyncPolicy::Recovery {
            reuse_persisted_history,
            ..
        } = &mut self.policy
        {
            *reuse_persisted_history = true;
        }
        self
    }

    /// Return the completed [`SyncConfig`].
    pub fn build(self) -> SyncConfig<RecoverFromCheckpoint> {
        SyncConfig(self.policy, core::marker::PhantomData)
    }
}

#[cfg(test)]
mod tests;
