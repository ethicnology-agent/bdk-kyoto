# BDK Kyoto

BDK-Kyoto is an extension of [Kyoto](https://github.com/2140-dev/kyoto), a client-side implementation of BIP157/BIP158.
These proposals define a way for users to fetch transactions privately, using _compact block filters_.
You may want to read the specification [here](https://github.com/bitcoin/bips/blob/master/bip-0158.mediawiki).
Kyoto runs as a psuedo-node, sending messages over the Bitcoin peer-to-peer layer, finding new peers to connect to, and managing a
light-weight database of Bitcoin block headers. As such, developing a wallet application using this crate is distinct from a typical
client/server relationship. Esplora and Electrum offer _proactive_ APIs, in that the servers will respond to events as they are requested.

In the case of running a node as a background process, the developer experience is far more _reactive_, in that the node may emit any number of events, and the application may respond to them. BDK-Kyoto curates these events into structures that are easily handled by BDK APIs, making integration of compact block filters easily understood.

## Persistable recovery progress

`UpdateSubscriber::update_batch()` and `updates_batch()` return `UpdateBatch<T>` with `updates`, `scanned_tip: BlockId`, and `synced_to_tip: bool`. The multiple-wallet form returns a descriptor-keyed `BTreeMap`. Apply and persist every returned wallet update in order. Existing `update()` and `updates()` remain tip-only APIs.

Partial boundaries occur at global multiples of 1,000 filter heights, after every preceding height in the requested range has been checked. The first boundary can be closer than 1,000 heights, and a resumed seven-height walk-back may repeat the last persisted boundary. This fixed phase keeps the script-window extension points stable across restarts. A boundary waits for every matched block through that height to be fetched and applied in chronological order, then extends the script index and cache before checking later filters. Raw out-of-order filters remain buffered until missing earlier heights arrive. The checkpoint hash comes from the verified canonical header chain. Header download progress alone never advances a persistable wallet checkpoint. A stalled filter or matched block can delay a boundary; there is no wall-clock completion guarantee.

This cadence amortizes wallet persistence across 1,000 filters while bounding completed scan work lost between persisted boundaries. Block downloads can be much more expensive than filter checks, so a boundary cannot safely bypass an outstanding matched block. The verified header map and pending filter map use logarithmic height lookup; stored checkpoint prefixes share their history. These structures and upstream event queues do not impose a hard byte-memory bound.

Every configured wallet participates in the same batch boundary. A wallet already persisted beyond the shared `scanned_tip` retains its previously known higher checkpoint until the shared scan catches up or a safe scanned prefix proves that checkpoint disconnected; newly downloaded checkpoints never exceed the scanned frontier. Contradictory wallet checkpoints at or below the trusted node starting anchor are rejected, since headers downloaded strictly after that anchor cannot reconcile them. Differences above the anchor are resolved by the canonical header download.

Rebuild routine sync from the wallet's latest persisted checkpoint. The existing walk-back selects a retained checkpoint within seven block heights, without jumping across a sparse birthday gap to genesis. During recovery, retain the original `used_script_index` bound and use that latest checkpoint (with the same bounded walk-back) as the new recovery checkpoint and opt into `SyncConfig::wallet_recovery_sync(...).reuse_persisted_history()`. Reusing an explicit original birthday intentionally rescans from that birthday; the default recovery configuration always reindexes, including an explicit checkpoint equal to the walk-back. The recovery script-count window and ordinary lookahead are extended from newly revealed indices at each boundary. During the initial automatic seven-height walk-back, an exact unchanged checkpoint hash reuses the already persisted scan result rather than retroactively reindexing that block with a later script window; changed reorg hashes and explicit earlier recoveries are processed normally. Together with the fixed boundary heights, this keeps uninterrupted and resumed recovery coverage consistent. This remains a finite script window per boundary, rather than an exhaustive search over every possible address.

A disconnected intermediate checkpoint is replaced through the ordinary canonical chain update. A failed canonical block fetch prevents delivery until a reorganization removes that queued match and replacement filters are checked. Cancellation preserves an in-flight boundary inside the subscriber; process loss requires rebuilding from the last wallet state actually persisted by the application.

## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.
