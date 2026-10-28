//! Construct a [`LightClient`] by using a reference to a [`Wallet`].
//!
//! ## Details
//!
//! The node has a number of configurations. Notably, the height of in the blockchain to start a
//! wallet recovery and the nodes on the peer-to-peer network are both configurable.
//!
//! ```no_run
//! # const RECEIVE: &str = "tr([7d94197e/86'/1'/0']tpubDCyQVJj8KzjiQsFjmb3KwECVXPvMwvAxxZGCP9XmWSopmjW3bCV3wD7TgxrUhiGSueDS1MU5X1Vb1YjYcp8jitXc5fXfdC1z68hDDEyKRNr/0/*)";
//! # const CHANGE: &str = "tr([7d94197e/86'/1'/0']tpubDCyQVJj8KzjiQsFjmb3KwECVXPvMwvAxxZGCP9XmWSopmjW3bCV3wD7TgxrUhiGSueDS1MU5X1Vb1YjYcp8jitXc5fXfdC1z68hDDEyKRNr/1/*)";
//! use std::net::{IpAddr, Ipv4Addr};
//! use std::time::Duration;
//! use bdk_wallet::Wallet;
//! use bdk_kyoto::bip157::{Network, TrustedPeer};
//! use bdk_kyoto::builder::{Builder, BuilderExt};
//! use bdk_kyoto::{LightClient, SyncConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Add specific peers to connect to.
//!     let peer = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
//!     let trusted = TrustedPeer::from_ip(peer);
//!
//!     let mut wallet = Wallet::create(RECEIVE, CHANGE)
//!         .network(Network::Signet)
//!         .create_wallet_no_persist()?;
//!
//!     let sync_config = SyncConfig::sync_from_last_checkpoint().build();
//!
//!     let client = Builder::new(Network::Signet)
//!         // A node may handle multiple connections
//!         .required_peers(2)
//!         // How long peers have to respond messages
//!         .response_timeout(Duration::from_secs(2))
//!         // Added trusted peers to initialize the sync
//!         .add_peer(trusted)
//!         .build_with_wallet(&wallet, sync_config)?;
//!     Ok(())
//! }
//! ```

use std::collections::BTreeSet;
use std::fmt::Display;

use bdk_wallet::{
    chain::{
        keychain_txout::KeychainTxOutIndex, CheckPoint, ConfirmationBlockTime, DescriptorExt,
        DescriptorId, IndexedTxGraph,
    },
    KeychainKind, Wallet,
};
pub use bip157::Builder;
use bip157::{chain::ChainState, HashCheckpoint, Network};

use crate::{
    state::Idle, sync_policy::SyncFromLastCheckpoint, LightClient, LoggingSubscribers, SyncConfig,
    SyncPolicy, SyncPolicyType, UpdateSubscriber,
};

const IMPOSSIBLE_REORG_DEPTH: u32 = 7;

/// Build a compact block filter client and node for a specified wallet
pub trait BuilderExt {
    /// Attempt to build the node with scripts from a [`Wallet`] and following a [`SyncConfig`].
    fn build_with_wallet<P: SyncPolicyType>(
        self,
        wallet: &Wallet,
        sync_config: SyncConfig<P>,
    ) -> Result<LightClient<Idle, crate::wallets::Single>, BuilderError>;

    /// Attempt to build the node with scripts from multiple [`Wallet`]s and following a [`SyncConfig`].
    ///
    /// Only [`SyncFromLastCheckpoint`] configs are permitted. To recover from a birthday,
    /// insert its block checkpoint into the wallet's local chain first and configure a
    /// sufficient wallet lookahead (for example, 1000 scripts per keychain).
    /// The node starts from the earliest wallet checkpoint, allowing up to seven blocks of
    /// rollback where the wallet has retained checkpoints in that height range.
    fn build_with_wallets(
        self,
        wallets: Vec<(&Wallet, SyncConfig<SyncFromLastCheckpoint>)>,
    ) -> Result<LightClient<Idle, crate::wallets::Multiple>, BuilderError>;
}

impl BuilderExt for Builder {
    fn build_with_wallet<P: SyncPolicyType>(
        mut self,
        wallet: &Wallet,
        sync_config: SyncConfig<P>,
    ) -> Result<LightClient<Idle, crate::wallets::Single>, BuilderError> {
        let network = wallet.network();
        if self.network().ne(&network) {
            return Err(BuilderError::NetworkMismatch);
        }
        match sync_config.0 {
            SyncPolicy::NewWallet => {
                self = self
                    .chain_state(ChainState::Checkpoint(new_wallet_anchor(network)))
                    .headers_only_sync();
            }
            SyncPolicy::SyncFromLast => {
                let sync_start = walk_back_max_reorg(wallet.latest_checkpoint());
                self = self.chain_state(ChainState::Checkpoint(sync_start));
            }
            SyncPolicy::Recovery { cp, .. } => {
                self = self.chain_state(ChainState::Checkpoint(cp));
            }
        }
        let (node, client) = self.build();
        let bip157::Client {
            requester,
            info_rx,
            warn_rx,
            event_rx,
        } = client;
        let indexed_graph = IndexedTxGraph::new(wallet.spk_index().clone());
        let update_subscriber = UpdateSubscriber::<crate::wallets::Single>::new(
            requester.clone(),
            sync_config.0,
            event_rx,
            wallet.latest_checkpoint(),
            indexed_graph,
        );
        let client = LightClient::new(
            requester,
            LoggingSubscribers {
                info_subscriber: info_rx,
                warning_subscriber: warn_rx,
            },
            update_subscriber,
            node,
        );
        Ok(client)
    }

    fn build_with_wallets(
        mut self,
        wallets: Vec<(&Wallet, SyncConfig<SyncFromLastCheckpoint>)>,
    ) -> Result<LightClient<Idle, crate::wallets::Multiple>, BuilderError> {
        let network = wallets
            .first()
            .ok_or(BuilderError::EmptyIterator)?
            .0
            .network();
        if self.network().ne(&network) {
            return Err(BuilderError::NetworkMismatch);
        }
        let mut descriptor_ids = BTreeSet::new();
        for (wallet, _) in &wallets {
            if wallet.network() != network {
                return Err(BuilderError::NetworkMismatch);
            }
            let descriptor_id = wallet
                .public_descriptor(KeychainKind::External)
                .descriptor_id();
            if !descriptor_ids.insert(descriptor_id) {
                return Err(BuilderError::DuplicateDescriptor);
            }
        }
        let cp_min = wallets
            .iter()
            .map(|(wallet, _)| walk_back_max_reorg(wallet.latest_checkpoint()))
            .min()
            .ok_or(BuilderError::EmptyIterator)?;
        self = self.chain_state(ChainState::Checkpoint(cp_min));
        let (node, client) = self.build();
        let bip157::Client {
            requester,
            info_rx,
            warn_rx,
            event_rx,
        } = client;
        let wallet_iter = wallets
            .into_iter()
            .map(|(wallet, sync_config)| {
                (
                    wallet
                        .public_descriptor(KeychainKind::External)
                        .descriptor_id(),
                    sync_config.0,
                    wallet.latest_checkpoint(),
                    IndexedTxGraph::new(wallet.spk_index().clone()),
                )
            })
            .collect::<Vec<(
                DescriptorId,
                SyncPolicy,
                CheckPoint,
                IndexedTxGraph<ConfirmationBlockTime, KeychainTxOutIndex<KeychainKind>>,
            )>>();
        let update_subscriber = UpdateSubscriber::<crate::wallets::Multiple>::new_multiple(
            requester.clone(),
            event_rx,
            wallet_iter.into_iter(),
        );
        let client = LightClient::new(
            requester,
            LoggingSubscribers {
                info_subscriber: info_rx,
                warning_subscriber: warn_rx,
            },
            update_subscriber,
            node,
        );
        Ok(client)
    }
}

fn new_wallet_anchor(network: Network) -> HashCheckpoint {
    match network {
        Network::Bitcoin => HashCheckpoint::taproot_activation(),
        net => HashCheckpoint::from_genesis(net),
    }
}

/// Walk back at most 7 block heights in case the last sync was an orphan block.
/// Sparse chains must not jump from a birthday checkpoint all the way to genesis.
fn walk_back_max_reorg(checkpoint: CheckPoint) -> HashCheckpoint {
    let minimum_height = checkpoint.height().saturating_sub(IMPOSSIBLE_REORG_DEPTH);
    let mut ret_cp = HashCheckpoint::new(checkpoint.height(), checkpoint.hash());
    for next in checkpoint.iter() {
        if next.height() < minimum_height {
            break;
        }
        ret_cp = HashCheckpoint::new(next.height(), next.hash());
    }
    ret_cp
}

/// Errors the builder may throw.
#[derive(Debug)]
pub enum BuilderError {
    /// The wallet network and node network do not match.
    NetworkMismatch,
    /// The wallet iterator is empty.
    EmptyIterator,
    /// More than one wallet has the same external descriptor identifier.
    DuplicateDescriptor,
}

impl Display for BuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuilderError::NetworkMismatch => {
                write!(f, "wallet network and node network do not match")
            }
            BuilderError::EmptyIterator => write!(f, "empty wallet iterator."),
            BuilderError::DuplicateDescriptor => {
                write!(f, "wallet external descriptors must be distinct")
            }
        }
    }
}

impl std::error::Error for BuilderError {}

#[cfg(test)]
mod tests {
    use super::*;
    use bdk_wallet::bitcoin::{hashes::Hash, BlockHash};
    use bdk_wallet::chain::BlockId;

    fn checkpoints(heights: &[u32]) -> CheckPoint {
        CheckPoint::from_block_ids(heights.iter().map(|height| BlockId {
            height: *height,
            hash: BlockHash::from_byte_array([(*height % 256) as u8; 32]),
        }))
        .unwrap()
    }

    #[test]
    fn sparse_birthday_checkpoint_does_not_rewind_to_genesis() {
        let cp = checkpoints(&[0, 970_580]);
        assert_eq!(walk_back_max_reorg(cp).height, 970_580);
    }

    #[test]
    fn reorg_walkback_is_bounded_by_block_height() {
        let cp = checkpoints(&[0, 100, 194, 197, 198, 200]);
        assert_eq!(walk_back_max_reorg(cp).height, 194);
    }

    #[test]
    fn contiguous_checkpoints_keep_seven_block_reorg_margin() {
        let heights = (0..=20).collect::<Vec<_>>();
        assert_eq!(walk_back_max_reorg(checkpoints(&heights)).height, 13);
    }

    fn public_wallet(network: Network) -> Wallet {
        const EXTERNAL: &str = "wpkh([73c5da0a/84h/0h/0h]xpub6CatWdiZiodmUeTDp8LT5or8nmbKNcuyvz7WyksVFkKB4RHwCD3XyuvPEbvqAQY3rAPshWcMLoP2fMFMKHPJ4ZeZXYVUhLv1VMrjPC7PW6V/0/*)";
        const INTERNAL: &str = "wpkh([73c5da0a/84h/0h/0h]xpub6CatWdiZiodmUeTDp8LT5or8nmbKNcuyvz7WyksVFkKB4RHwCD3XyuvPEbvqAQY3rAPshWcMLoP2fMFMKHPJ4ZeZXYVUhLv1VMrjPC7PW6V/1/*)";
        Wallet::create(EXTERNAL, INTERNAL)
            .network(network)
            .create_wallet_no_persist()
            .unwrap()
    }

    #[test]
    fn multi_wallet_builder_rejects_later_network_mismatch() {
        let first = public_wallet(Network::Bitcoin);
        let second = Wallet::create(
            "tr([7d94197e/86'/1'/0']tpubDCyQVJj8KzjiQsFjmb3KwECVXPvMwvAxxZGCP9XmWSopmjW3bCV3wD7TgxrUhiGSueDS1MU5X1Vb1YjYcp8jitXc5fXfdC1z68hDDEyKRNr/0/*)",
            "tr([7d94197e/86'/1'/0']tpubDCyQVJj8KzjiQsFjmb3KwECVXPvMwvAxxZGCP9XmWSopmjW3bCV3wD7TgxrUhiGSueDS1MU5X1Vb1YjYcp8jitXc5fXfdC1z68hDDEyKRNr/1/*)",
        )
        .network(Network::Signet)
        .create_wallet_no_persist()
        .unwrap();
        let result = Builder::new(Network::Bitcoin).build_with_wallets(vec![
            (&first, SyncConfig::sync_from_last_checkpoint().build()),
            (&second, SyncConfig::sync_from_last_checkpoint().build()),
        ]);
        assert_eq!(
            result
                .err()
                .expect("invalid wallet set must be rejected")
                .to_string(),
            "wallet network and node network do not match"
        );
    }

    #[test]
    fn multi_wallet_builder_rejects_duplicate_external_descriptors() {
        let first = public_wallet(Network::Bitcoin);
        let second = public_wallet(Network::Bitcoin);
        let result = Builder::new(Network::Bitcoin).build_with_wallets(vec![
            (&first, SyncConfig::sync_from_last_checkpoint().build()),
            (&second, SyncConfig::sync_from_last_checkpoint().build()),
        ]);
        assert_eq!(
            result
                .err()
                .expect("invalid wallet set must be rejected")
                .to_string(),
            "wallet external descriptors must be distinct"
        );
    }
}
