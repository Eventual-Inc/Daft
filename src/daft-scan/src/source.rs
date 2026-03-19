use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use daft_stats::{PartitionSpec, TableStatistics};
use futures::stream::BoxStream;

use crate::{partitioning::PartitionField, pushdowns::Pushdowns};

/// Base trait for reading tabular data; new sources implement this trait.
pub trait DataSource: Send + Sync + Debug {
    /// The name of the data source, useful for debugging.
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

    /// Split this source into independently-executable tasks given the pushdowns.
    fn get_tasks(&self, pushdowns: &Pushdowns) -> DaftResult<Vec<Arc<dyn DataSourceTask>>>;
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
    /// async fn read(&self, opts: ReadOptions)
    ///     -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>>
    /// {
    ///     let (tx, rx) = unbounded_channel();
    ///     tokio::task::spawn_blocking(move || {
    ///         for batch in blocking_iter {
    ///             if tx.send(Ok(batch)).is_err() { break; }
    ///         }
    ///     });
    ///     Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    /// }
    /// ```
    async fn read(
        &self,
        options: ReadOptions,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>>;
}
