use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use daft_stats::{PartitionSpec, TableStatistics};
use futures::stream::BoxStream;

use crate::{ScanTaskRef, partitioning::PartitionField, pushdowns::Pushdowns};

/// Reference to a [`DataSource`].
pub type DataSourceRef = Arc<dyn DataSource>;

/// Reference to a [`DataSourceTask`].
pub type DataSourceTaskRef = Arc<dyn DataSourceTask>;

/// Stream of [`DataSourceTask`]s.
pub type DataSourceTaskStream = BoxStream<'static, DaftResult<DataSourceTaskRef>>;

/// Stream of [`RecordBatch`]s.
pub type RecordBatchStream = BoxStream<'static, DaftResult<RecordBatch>>;

/// Base trait for reading tabular data; new sources implement this trait.
#[async_trait]
pub trait DataSource: Send + Sync + Debug {
    /// The name of the data source, typically a `'static` string, useful for debugging.
    fn name(&self) -> String;

    /// The schema of the data source.
    fn schema(&self) -> SchemaRef;

    /// The partitioning fields of the data source, used in pushdown splitting.
    fn partition_fields(&self) -> Vec<PartitionField> {
        vec![]
    }

    /// Pre-computed statistics for query optimization.
    ///
    /// The optimizer uses these to eliminate aggregations that can be answered
    /// from metadata alone (e.g. `COUNT(*)` when `num_rows` is `Exact`), without
    /// reading any data. Returning `None` disables all statistics-based rewrites
    /// for this source.
    fn statistics(&self) -> Option<DataSourceStatistics> {
        None
    }

    /// Stream tasks during execution.
    ///
    /// The outer `DaftResult` captures setup errors (e.g. metadata lookup failures)
    /// before any tasks are produced. Per-task errors appear as items in the stream.
    async fn get_tasks(&self, pushdowns: &Pushdowns) -> DaftResult<DataSourceTaskStream>;
}

/// Pre-computed statistics exposed by a [`DataSource`] for query optimization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSourceStatistics {
    /// Total number of rows across all tasks produced by this source.
    pub num_rows: Precision<u64>,
}

/// Exactness annotation for a statistic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precision<T> {
    /// The value is exact. Safe to substitute for a full scan.
    Exact(T),
    /// The value is an estimate. Not safe to substitute; usable for heuristics only.
    Inexact(T),
    /// No value is available.
    Absent,
}

impl<T> Precision<T> {
    /// Returns a reference to the inner value regardless of exactness, or `None` if absent.
    pub fn get(&self) -> Option<&T> {
        match self {
            Self::Exact(v) | Self::Inexact(v) => Some(v),
            Self::Absent => None,
        }
    }
}

/// Metadata about a [`DataSourceTask`] used for planning and optimization.
#[derive(Debug, Clone)]
pub struct DataSourceTaskStatistics {
    /// On-disk size of the data this task will read, used for task coalescing.
    pub size_bytes: Precision<u64>,
    /// Column-level range statistics for predicate pushdown evaluation.
    pub column_stats: Option<TableStatistics>,
}

/// Options controlling how a [`DataSourceTask`] reads its data.
#[derive(Debug, Clone, Copy)]
pub struct ReadOptions {
    /// Whether the task must emit batches in the order they appear in the source.
    pub maintain_order: bool,
    /// Target number of rows per emitted [`RecordBatch`].
    pub batch_size: usize,
}

/// A single unit of work produced by a [`DataSource`]. Self-contained and distributable.
#[async_trait]
pub trait DataSourceTask: Send + Sync + Debug {
    /// The schema of records this task produces.
    fn schema(&self) -> SchemaRef;

    /// Metadata about this task for planning and optimization.
    ///
    /// Returning `None` disables all statistics-based optimizations for this task,
    /// including task coalescing and predicate pushdown evaluation.
    fn statistics(&self) -> Option<DataSourceTaskStatistics> {
        None
    }

    /// Partition values for this task, injected into output records.
    fn partition_values(&self) -> Option<&PartitionSpec> {
        None
    }

    /// TEMPORARY DURING MIGRATION
    ///
    /// Returns the underlying [`ScanTask`] if this task wraps one.
    ///
    /// Used by the [`ScanOperator`] bridge to extract native scan tasks
    /// without going through the `read()` path.
    fn as_scan_task(&self) -> Option<&ScanTaskRef> {
        None
    }

    /// Returns the Python object backing this task, if any.
    ///
    /// Used by the [`ScanOperator`] bridge to create a
    /// `python_factory_func_scan_task` for pure-Python tasks.
    #[cfg(feature = "python")]
    fn to_py(&self, _py: pyo3::Python<'_>) -> Option<pyo3::Py<pyo3::PyAny>> {
        None
    }

    /// Read this task, producing a stream of [`RecordBatch`]es.
    ///
    /// The framework calls this from within an async I/O context — do not
    /// block the calling thread. The outer `DaftResult` captures setup errors
    /// (e.g. file not found, invalid configuration) before any data is read.
    /// Data-read errors are items in the returned stream.
    ///
    /// ## Lifetime note
    ///
    /// The returned stream must be `'static`: it outlives the `&self` borrow
    /// and may be polled on a different thread. Any data from `self` needed by
    /// the stream must be cloned or wrapped in `Arc` before being moved into it.
    ///
    /// ## Blocking I/O (e.g. Python sources)
    ///
    /// Bridge blocking work to the stream with a channel rather than buffering:
    ///
    /// ```ignore
    /// async fn read(&self, opts: ReadOptions) -> DaftResult<RecordBatchStream> {
    ///     let (tx, rx) = unbounded_channel();
    ///     tokio::task::spawn_blocking(move || {
    ///         for batch in blocking_iter {
    ///             if tx.send(Ok(batch)).is_err() { break; }
    ///         }
    ///     });
    ///     Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    /// }
    /// ```
    async fn read(&self, options: ReadOptions) -> DaftResult<RecordBatchStream>;
}

/// A [`DataSourceTask`] backed by a native [`ScanTask`].
///
/// Created by [`DataSourceTask::parquet()`] (and future factory methods).
/// The [`ScanOperator`] bridge extracts the inner [`ScanTask`] via
/// [`as_scan_task()`](DataSourceTask::as_scan_task) so it flows through
/// the existing execution path without calling [`read()`](DataSourceTask::read).
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ShimSourceTask(ScanTaskRef);

impl ShimSourceTask {
    pub fn new(scan_task: ScanTaskRef) -> Self {
        Self(scan_task)
    }
}

#[async_trait]
impl DataSourceTask for ShimSourceTask {
    fn schema(&self) -> SchemaRef {
        self.0.schema.clone()
    }

    fn statistics(&self) -> Option<DataSourceTaskStatistics> {
        Some(DataSourceTaskStatistics {
            size_bytes: match self.0.size_bytes_on_disk {
                Some(n) => Precision::Exact(n),
                None => Precision::Absent,
            },
            column_stats: self.0.statistics.clone(),
        })
    }

    fn partition_values(&self) -> Option<&PartitionSpec> {
        self.0
            .sources
            .first()
            .and_then(|s| s.partition_spec.as_ref())
    }

    fn as_scan_task(&self) -> Option<&ScanTaskRef> {
        Some(&self.0)
    }

    async fn read(&self, _options: ReadOptions) -> DaftResult<RecordBatchStream> {
        unreachable!("ShimSourceTask is executed via the native ScanTask path, not read()")
    }
}
