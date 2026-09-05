//! FFI-backed Celeborn shuffle client implementation.
//!
//! This bridges our async [`CelebornClient`] trait to the synchronous
//! `celeborn_client::ShuffleClient` (C++ FFI). All FFI calls are dispatched
//! to a blocking thread via `tokio::task::spawn_blocking` so they never block
//! the async runtime.

use std::{
    collections::HashMap,
    io::{BufReader, Read},
    sync::{Arc, Condvar, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use celeborn_client::{Config as CelebornConfig, ShuffleClient};
use common_error::{DaftError, DaftResult};

use super::client::{CelebornClient, CelebornClientConfig, PartitionDataStream};

/// Convert a value to `i32`, returning a descriptive error on overflow.
///
/// The Celeborn C++ FFI uses `i32` for all ID parameters while the Daft
/// trait uses wider unsigned types. This helper centralises the checked
/// conversion so callers don't repeat the same boilerplate.
fn to_ffi_i32(value: impl TryInto<i32> + std::fmt::Display + Copy, name: &str) -> DaftResult<i32> {
    value.try_into().map_err(|_| {
        DaftError::External(format!("{name} {value} overflows i32 (Celeborn FFI limit)").into())
    })
}

/// Run a synchronous FFI closure on the tokio blocking thread pool and
/// map JoinError (panic) into a [`DaftError`].
async fn run_blocking<F, R>(op_name: &str, f: F) -> DaftResult<R>
where
    F: FnOnce() -> DaftResult<R> + Send + 'static,
    R: Send + 'static,
{
    let op = op_name.to_owned();
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| DaftError::External(format!("Celeborn {op} task panicked: {e}").into()))?
}

/// Thread-safe wrapper around `celeborn_client::ShuffleClient`.
///
/// `ShuffleClient` holds an opaque raw pointer to the C++ FFI handle, which
/// prevents auto `Send`/`Sync`. As of the celeborn-client "parallel read/write"
/// revision, every `ShuffleClient` method takes `&self` and the underlying C++
/// `ShuffleClientImpl` synchronises all shared state internally (folly
/// concurrent maps, per-shuffle registration mutex, per-call compressor).
/// Concurrent `push_data` / `read_partition` from multiple threads is therefore
/// safe, so this wrapper is both `Send` and `Sync` and can be shared via a
/// plain `Arc` without any external lock.
struct CelebornShuffleClient(ShuffleClient);

// SAFETY: `ShuffleClient` is `!Send`/`!Sync` at the type level only because it
// holds an opaque raw pointer to the C++ FFI handle. That handle owns its own
// thread pool and synchronises every shared structure internally, and all
// methods take `&self`, so it is safe to both move the wrapper between threads
// (`Send`) and share `&CelebornShuffleClient` across threads (`Sync`). This
// mirrors the `unsafe impl Send + Sync for ShuffleClient` in celeborn-client
// itself.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for CelebornShuffleClient {}
// SAFETY: see the `Send` impl above; the inner C++ client synchronises all
// shared state internally and exposes every operation through `&self`.
unsafe impl Sync for CelebornShuffleClient {}

/// State of a shuffle's reducer-file-group fetch.
///
/// * `Idle` — no fetch is in flight and none has succeeded (the initial state,
///   and the state a failed fetch resets to so the next reader retries).
/// * `Fetching` — one reader holds the fetch; concurrent readers park on the
///   latch's condvar until it resolves, so only one LM RPC is ever in flight.
/// * `Done` — the `shuffleId`-keyed cache is populated; reads proceed with no
///   further fetch, ever.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FileGroupState {
    Idle,
    Fetching,
    Done,
}

/// Per-shuffle latch guarding the reducer-file-group fetch.
///
/// `ShuffleClientImpl::updateReducerFileGroup` (C++) performs a **blocking**
/// `askSync` RPC to the LifecycleManager on *every* call with no cache check,
/// yet it stores the result in a `shuffleId`-keyed cache that `open_partition`
/// reads from. Calling it once per `read_partition` therefore issued one
/// blocking LM round-trip per reduce partition (e.g. ~8.6k RPCs for a 10k-way
/// repartition on a single worker), turning the reduce phase into tens of
/// seconds of pure RPC latency. That latency made the data-bearing partition
/// reads the slow stragglers, so they were still in flight — and got torn down
/// — when the pipeline completed on the fast (empty) reads, silently dropping
/// all real data.
///
/// This latch collapses that to **one fetch per successful shuffle** while
/// keeping failures retriable. The first reader transitions `Idle -> Fetching`
/// and runs the RPC with the lock released; concurrent readers park on `cv`
/// until it resolves. On success the state latches to `Done` and no reader ever
/// fetches again; on failure it resets to `Idle` so the next reader — a parked
/// waiter, or a later task retry reusing this long-lived client — re-attempts.
/// Unlike a `Once`, a transient LM error therefore does not permanently poison
/// the shuffle. See [`FileGroupLatch::ensure_fetched`].
struct FileGroupLatch {
    state: Mutex<FileGroupState>,
    cv: Condvar,
}

/// Reset-on-drop guard for the `Fetching` claim. If the fetch closure panics
/// (rather than returning `Err`), unwinding drops this with `armed == true`,
/// releasing the claim back to `Idle` and waking parked readers — otherwise the
/// latch would stay `Fetching` forever and every reader would deadlock. The
/// normal (return) paths disarm it before setting the final state themselves.
struct FetchGuard<'a> {
    latch: &'a FileGroupLatch,
    armed: bool,
}

impl Drop for FetchGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            if let Ok(mut state) = self.latch.state.lock() {
                *state = FileGroupState::Idle;
            }
            self.latch.cv.notify_all();
        }
    }
}

impl FileGroupLatch {
    fn new() -> Self {
        Self {
            state: Mutex::new(FileGroupState::Idle),
            cv: Condvar::new(),
        }
    }

    /// Ensure the reducer file group has been fetched, running `fetch` exactly
    /// once **on success** across all concurrent readers of this shuffle.
    ///
    /// Blocks while another reader is fetching. Returns immediately once a fetch
    /// has succeeded. On failure the latch is reset to `Idle` and the error is
    /// returned to *this* caller, so the read fails loudly (and is retried)
    /// rather than silently proceeding against an empty file-group cache.
    fn ensure_fetched<F>(&self, fetch: F) -> DaftResult<()>
    where
        F: FnOnce() -> Result<(), String>,
    {
        let poisoned = |e| DaftError::External(format!("file_group latch poisoned: {e}").into());
        let mut state = self.state.lock().map_err(poisoned)?;
        loop {
            match *state {
                FileGroupState::Done => return Ok(()),
                FileGroupState::Fetching => {
                    state = self.cv.wait(state).map_err(poisoned)?;
                }
                FileGroupState::Idle => {
                    // Claim the fetch and run it with the lock released so
                    // parked readers are not blocked on the mutex, only on `cv`.
                    *state = FileGroupState::Fetching;
                    drop(state);
                    let mut guard = FetchGuard {
                        latch: self,
                        armed: true,
                    };
                    let res = fetch();
                    let mut state = self.state.lock().map_err(poisoned)?;
                    guard.armed = false;
                    return match res {
                        Ok(()) => {
                            *state = FileGroupState::Done;
                            self.cv.notify_all();
                            Ok(())
                        }
                        Err(e) => {
                            *state = FileGroupState::Idle;
                            self.cv.notify_all();
                            Err(DaftError::External(
                                format!("Celeborn update_reducer_file_group failed: {e}").into(),
                            ))
                        }
                    };
                }
            }
        }
    }
}

/// Celeborn shuffle client backed by the C++ FFI implementation.
///
/// Thread-safety: as of the celeborn-client "parallel read/write" revision the
/// underlying `ShuffleClient` exposes every operation through `&self` and
/// synchronises internally, so we share it via a plain `Arc` with **no external
/// lock**. This lets multiple partitions be pushed and read truly concurrently.
///
/// This is a **connection-level** object: one instance per Worker, shared
/// across all shuffles. Per-shuffle metadata (`num_mappers`, `num_partitions`)
/// is stored in `shuffle_meta`, which is still guarded by a `Mutex` because it
/// is plain Rust state mutated by `register_shuffle` / `unregister_shuffle`.
pub struct ShuffleCelebornClient {
    inner: Arc<CelebornShuffleClient>,
    shuffle_meta: Arc<Mutex<HashMap<u64, (u32, u32)>>>,
    /// One [`FileGroupLatch`] per shuffle, so `update_reducer_file_group` runs
    /// exactly once per shuffle instead of once per partition read.
    file_group_latches: Arc<Mutex<HashMap<u64, Arc<FileGroupLatch>>>>,
}

// SAFETY: both fields are `Arc<T>` where `T: Send + Sync`
// (`CelebornShuffleClient` via its manual impls above, `Mutex<HashMap<..>>`
// inherently), so the struct is safe to send and share across threads.
unsafe impl Send for ShuffleCelebornClient {}
unsafe impl Sync for ShuffleCelebornClient {}

impl ShuffleCelebornClient {
    /// Connect to a running Celeborn LifecycleManager and return a new client.
    ///
    /// # Arguments
    /// * `config` - Connection-level Celeborn configuration (lm_host, lm_port,
    ///   app_id) plus native `celeborn.*` `properties` forwarded to the client.
    pub fn connect(config: &CelebornClientConfig) -> DaftResult<Self> {
        let celeborn_config = CelebornConfig {
            app_id: config.app_id.clone(),
            properties: config.properties.clone(),
        };

        let client = ShuffleClient::connect(celeborn_config, &config.lm_host, config.lm_port)
            .map_err(|e| {
                DaftError::External(
                    format!(
                        "Failed to connect to Celeborn LifecycleManager at {}:{}: {e}",
                        config.lm_host, config.lm_port
                    )
                    .into(),
                )
            })?;

        Ok(Self {
            inner: Arc::new(CelebornShuffleClient(client)),
            shuffle_meta: Arc::new(Mutex::new(HashMap::new())),
            file_group_latches: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl CelebornClient for ShuffleCelebornClient {
    async fn register_shuffle(
        &self,
        shuffle_id: u64,
        num_mappers: u32,
        num_partitions: u32,
    ) -> DaftResult<()> {
        let mut meta = self
            .shuffle_meta
            .lock()
            .map_err(|e| DaftError::External(format!("shuffle_meta lock poisoned: {e}").into()))?;
        meta.insert(shuffle_id, (num_mappers, num_partitions));
        Ok(())
    }

    async fn push_data(
        &self,
        shuffle_id: u64,
        map_id: u32,
        attempt_id: u32,
        partition_id: u32,
        data: &[u8],
    ) -> DaftResult<()> {
        let (num_mappers_raw, num_partitions_raw) = {
            let meta = self.shuffle_meta.lock().map_err(|e| {
                DaftError::External(format!("shuffle_meta lock poisoned: {e}").into())
            })?;
            *meta.get(&shuffle_id).ok_or_else(|| {
                DaftError::External(
                    format!("shuffle {shuffle_id} not registered; call register_shuffle first")
                        .into(),
                )
            })?
        };
        let inner = Arc::clone(&self.inner);
        let shuffle_id = to_ffi_i32(shuffle_id, "shuffle_id")?;
        let map_id = to_ffi_i32(map_id, "map_id")?;
        let attempt_id = to_ffi_i32(attempt_id, "attempt_id")?;
        let partition_id = to_ffi_i32(partition_id, "partition_id")?;
        let num_mappers = to_ffi_i32(num_mappers_raw, "num_mappers")?;
        let num_partitions = to_ffi_i32(num_partitions_raw, "num_partitions")?;
        let data_owned = data.to_vec();

        run_blocking("push_data", move || {
            inner
                .0
                .push_data(
                    shuffle_id,
                    map_id,
                    attempt_id,
                    partition_id,
                    &data_owned,
                    num_mappers,
                    num_partitions,
                )
                .map_err(|e| DaftError::External(format!("Celeborn push_data failed: {e}").into()))
        })
        .await
    }

    async fn mapper_end(&self, shuffle_id: u64, map_id: u32, attempt_id: u32) -> DaftResult<()> {
        let (num_mappers_raw, _) = {
            let meta = self.shuffle_meta.lock().map_err(|e| {
                DaftError::External(format!("shuffle_meta lock poisoned: {e}").into())
            })?;
            *meta.get(&shuffle_id).ok_or_else(|| {
                DaftError::External(
                    format!("shuffle {shuffle_id} not registered; call register_shuffle first")
                        .into(),
                )
            })?
        };
        let inner = Arc::clone(&self.inner);
        let shuffle_id = to_ffi_i32(shuffle_id, "shuffle_id")?;
        let map_id = to_ffi_i32(map_id, "map_id")?;
        let attempt_id = to_ffi_i32(attempt_id, "attempt_id")?;
        let num_mappers = to_ffi_i32(num_mappers_raw, "num_mappers")?;

        run_blocking("mapper_end", move || {
            inner
                .0
                .mapper_end(shuffle_id, map_id, attempt_id, num_mappers)
                .map_err(|e| DaftError::External(format!("Celeborn mapper_end failed: {e}").into()))
        })
        .await
    }

    async fn read_partition(
        &self,
        shuffle_id: u64,
        partition_id: u32,
    ) -> DaftResult<PartitionDataStream> {
        let (num_mappers_raw, _) = {
            let meta = self.shuffle_meta.lock().map_err(|e| {
                DaftError::External(format!("shuffle_meta lock poisoned: {e}").into())
            })?;
            *meta.get(&shuffle_id).ok_or_else(|| {
                DaftError::External(
                    format!("shuffle {shuffle_id} not registered; call register_shuffle first")
                        .into(),
                )
            })?
        };
        let inner = Arc::clone(&self.inner);
        let shuffle_id_ffi = to_ffi_i32(shuffle_id, "shuffle_id")?;
        let partition_id = to_ffi_i32(partition_id, "partition_id")?;
        let num_mappers = to_ffi_i32(num_mappers_raw, "num_mappers")?;

        // Grab (or create) the per-shuffle latch so the reducer-file-group is
        // fetched exactly once per shuffle instead of once per partition read.
        let file_group_latch = {
            let mut latches = self.file_group_latches.lock().map_err(|e| {
                DaftError::External(format!("file_group_latches lock poisoned: {e}").into())
            })?;
            Arc::clone(
                latches
                    .entry(shuffle_id)
                    .or_insert_with(|| Arc::new(FileGroupLatch::new())),
            )
        };

        let (tx, rx) = async_channel::bounded(4);

        tokio::task::spawn_blocking(move || {
            let run = || -> DaftResult<()> {
                // No external lock: the celeborn-client `ShuffleClient` takes
                // `&self` and synchronises internally, so multiple partitions
                // can be opened and read concurrently from different blocking
                // threads sharing the same `Arc<CelebornShuffleClient>`.
                //
                // `update_reducer_file_group` issues a blocking LM RPC on every
                // call (no cache check) but populates a `shuffleId`-keyed cache
                // that `open_partition` reads from, so we run it exactly once per
                // shuffle. `ensure_fetched` blocks every concurrent reader until
                // the single fetch completes (so no `open_partition` observes an
                // unpopulated cache) and, on failure, resets the latch so a later
                // reader/retry re-attempts instead of the shuffle being poisoned.
                file_group_latch.ensure_fetched(|| {
                    inner
                        .0
                        .update_reducer_file_group(shuffle_id_ffi)
                        .map_err(|e| format!("{e}"))
                })?;

                let reader = inner
                    .0
                    .open_partition(shuffle_id_ffi, partition_id, 0, 0, num_mappers)
                    .map_err(|e| {
                        DaftError::External(format!("Celeborn open_partition failed: {e}").into())
                    })?;

                // Read the whole partition's raw bytes (a concatenation of one
                // self-contained Arrow IPC stream per map-side push) in one shot.
                // Framing + decoding is done once on the reduce reader
                // (`MicroPartition::read_record_batches_from_ipc_streams`); this side no
                // longer parses IPC just to re-frame it (which decoded twice).
                let mut reader = BufReader::with_capacity(64 * 1024, reader);
                let mut buf = Vec::new();
                if let Err(e) = reader.read_to_end(&mut buf) {
                    let _ = tx.send_blocking(Err(DaftError::External(
                        format!(
                            "Celeborn read_partition: failed to read partition bytes \
                             (shuffle_id={shuffle_id_ffi}, partition_id={partition_id}, \
                             num_mappers={num_mappers}): {e}"
                        )
                        .into(),
                    )));
                    return Ok(());
                }
                if !buf.is_empty() {
                    let _ = tx.send_blocking(Ok(Bytes::from(buf)));
                }
                Ok(())
            };

            if let Err(e) = run() {
                let _ = tx.send_blocking(Err(e));
            }
        });

        Ok(Box::pin(rx))
    }

    /// Clean up local per-shuffle metadata.
    ///
    /// The underlying `celeborn_client::ShuffleClient` C++ FFI does not expose
    /// an explicit `unregister_shuffle` API, so server-side cleanup relies on
    /// the Celeborn cluster's own garbage-collection mechanism
    /// (LifecycleManager timeout / application heartbeat expiry). However, we
    /// still remove the local `shuffle_meta` entry to avoid unbounded memory
    /// growth when many shuffles are executed through the same client instance.
    async fn unregister_shuffle(&self, shuffle_id: u64) -> DaftResult<()> {
        {
            let mut meta = self.shuffle_meta.lock().map_err(|e| {
                DaftError::External(format!("shuffle_meta lock poisoned: {e}").into())
            })?;
            meta.remove(&shuffle_id);
        }
        // Drop the per-shuffle file-group latch so the map does not grow
        // unbounded when many shuffles run through the same client instance.
        if let Ok(mut latches) = self.file_group_latches.lock() {
            latches.remove(&shuffle_id);
        }
        Ok(())
    }
}
