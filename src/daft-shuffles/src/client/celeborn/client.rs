//! Celeborn shuffle client abstraction.
//!
//! This module defines the [`CelebornClient`] interface used by Daft's shuffle
//! operators. [`ShuffleCelebornClient`](super::ffi::ShuffleCelebornClient)
//! implements it using the Celeborn C++ client over FFI.

use async_trait::async_trait;
use bytes::Bytes;
use common_error::DaftResult;
use futures::stream::BoxStream;

/// Stream of partition data chunks returned from `read_partition`.
///
/// Each chunk is a contiguous slice of bytes in Arrow IPC stream format.
/// Multiple chunks may originate from different map tasks but logically belong
/// to the same reduce partition.
pub type PartitionDataStream = BoxStream<'static, DaftResult<Bytes>>;

/// Connection-level configuration used to construct a [`CelebornClient`].
///
/// Contains all the parameters needed to establish a connection to the
/// Celeborn LifecycleManager. Per-shuffle metadata such as `num_mappers` and
/// `num_partitions` are registered via [`CelebornClient::register_shuffle`]
/// instead, allowing a single client instance to serve multiple shuffles.
#[derive(Clone, Debug, Default)]
pub struct CelebornClientConfig {
    /// LifecycleManager hostname or IP address.
    pub lm_host: String,
    /// LifecycleManager port.
    pub lm_port: i32,
    /// Application-level identifier. Celeborn keys shuffle data by
    /// `(app_id, shuffle_id)`, so this must be unique per application run;
    /// it comes from `CelebornConfig::app_id`, which defaults to a value
    /// unique per Daft process.
    pub app_id: String,
    /// Native `celeborn.*` properties (key, value) forwarded verbatim to the
    /// C++ client (compression codec, push/fetch timeouts, inflight backpressure, …).
    pub properties: Vec<(String, String)>,
}

/// Abstract Celeborn shuffle client.
///
/// All methods are `async` to accommodate both pure Rust implementations
/// (which may use `tonic` gRPC) and FFI-backed implementations (which may
/// dispatch to a thread pool internally).
///
/// Implementations must be `Send + Sync` so that an `Arc<dyn CelebornClient>`
/// can be shared across the Daft pipeline (multiple Map tasks of the same
/// shuffle share one client instance).
#[async_trait]
pub trait CelebornClient: Send + Sync {
    /// Register a shuffle with the Celeborn cluster.
    ///
    /// Must be called once per shuffle before any `push_data` or `mapper_end`
    /// calls. The client stores `num_mappers` and `num_partitions` internally
    /// so that subsequent per-record calls do not need to repeat them.
    async fn register_shuffle(
        &self,
        shuffle_id: u64,
        num_mappers: u32,
        num_partitions: u32,
    ) -> DaftResult<()>;

    /// Push a single partition payload to the Celeborn cluster.
    ///
    /// `register_shuffle` must have been called for this `shuffle_id` before
    /// calling `push_data`. The client retrieves `num_mappers` and
    /// `num_partitions` from the internally stored metadata.
    ///
    /// * `shuffle_id` - Logical shuffle identifier shared by all mappers/reducers
    ///   participating in this shuffle.
    /// * `map_id` - Index of the current map task, in `[0, num_mappers)`.
    /// * `attempt_id` - Attempt index for the map task; used by Celeborn for
    ///   deduplication when speculative execution is enabled.
    /// * `partition_id` - Target reduce partition index, in `[0, num_partitions)`.
    /// * `data` - Arrow IPC stream bytes for the partition slice.
    async fn push_data(
        &self,
        shuffle_id: u64,
        map_id: u32,
        attempt_id: u32,
        partition_id: u32,
        data: &[u8],
    ) -> DaftResult<()>;

    /// Notify the Celeborn cluster that this map task has finished pushing all
    /// partitions. Must be called exactly once per map attempt.
    async fn mapper_end(&self, shuffle_id: u64, map_id: u32, attempt_id: u32) -> DaftResult<()>;

    /// Read all blocks for a single reduce partition from the Celeborn cluster.
    /// Returns a stream of byte chunks holding the per-push Arrow IPC streams
    /// `push_data` wrote, concatenated in push order.
    ///
    /// Each chunk must be cut on an IPC stream boundary — a whole number of
    /// complete streams, never a partial one. The reduce reader decodes chunk by
    /// chunk (`MicroPartition::read_record_batches_from_ipc_streams`), so a chunk
    /// ending mid-message fails the read rather than being carried over. Yielding
    /// the whole partition as a single chunk trivially satisfies this.
    async fn read_partition(
        &self,
        shuffle_id: u64,
        partition_id: u32,
    ) -> DaftResult<PartitionDataStream>;

    /// Release all resources (memory, disk, replicas) associated with this
    /// shuffle on the Celeborn cluster. Idempotent; safe to call multiple times.
    async fn unregister_shuffle(&self, shuffle_id: u64) -> DaftResult<()>;
}
