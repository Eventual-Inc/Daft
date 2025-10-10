use std::{
    any::Any,
    collections::{BTreeMap, HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_display::{DisplayAs, DisplayLevel};
use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormatConfig;
use common_runtime::get_io_runtime;
use common_scan_info::{
    PartitionField, Pushdowns, ScanOperator, ScanTaskLike, ScanTaskLikeRef, StorageConfig,
    SupportsPushdownFilters,
};
use daft_core::{count_mode::CountMode, prelude::SchemaRef};
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{
    ExprRef,
    functions::prelude::{Deserialize, Serialize},
    optimization::get_required_columns,
};
use daft_io::IOStatsRef;
use daft_recordbatch::RecordBatch;
use daft_stats::{TableMetadata, TableStatistics};
use futures::{StreamExt, stream::BoxStream};
use lance::Dataset;
#[cfg(feature = "python")]
use pyo3::{
    PyResult, Python,
    prelude::{PyAnyMethods, PyModule},
};

pub async fn stream_lance_fragments<F>(
    ds: &Dataset,
    fragment_ids: &[usize],
    columns: Option<Vec<String>>,
    filter: Option<String>,
    _io_stats: Option<IOStatsRef>, // TODO(zhenchao) support io stats
    record_processor: F,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>>
where
    F: Fn(RecordBatch) -> DaftResult<RecordBatch> + Send + Sync + 'static,
{
    if fragment_ids.is_empty() {
        return Err(DaftError::ValueError(
            "Lance fragment id must not be empty".to_string(),
        ));
    }

    let fragments =
        fragment_ids
            .iter()
            .try_fold(vec![], |mut fs, fid| match ds.get_fragment(*fid) {
                Some(f) => {
                    fs.push(f.metadata().clone());
                    Ok(fs)
                }
                None => Err(DaftError::ValueError(format!(
                    "No lance fragment found with id {}",
                    fid
                ))),
            })?;

    let mut scanner = ds.scan();
    scanner.with_fragments(fragments);

    if let Some(columns) = columns
        && let Err(e) = scanner.project(columns.as_slice())
    {
        return Err(DaftError::External(Box::new(e)));
    }

    if let Some(filter) = filter
        && let Err(e) = scanner.filter(&filter)
    {
        return Err(DaftError::External(Box::new(e)));
    }

    let stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| DaftError::External(Box::new(e)))?;
    Ok(Box::pin(stream.map(move |r| match r {
        Ok(batch) => record_processor(batch.try_into()?),
        Err(e) => Err(DaftError::External(Box::new(e))),
    })))
}

/// ScanTaskLike impl for Lance format.
///
/// It's mainly used to encapsulate related configurations to build ScanTask.
/// It is not a ScanTaskLike implementation that can be executed directly.
/// It ultimately needs to be converted into an executable ScanTask.
#[derive(PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct LanceScanTask {
    uri: String,
    fragment_ids: Vec<usize>,
    columns: Option<Vec<String>>,
    filter: Option<String>,
    num_rows: Option<usize>,
    size_bytes: Option<usize>,
    metadata: Option<TableMetadata>,
    statistics: Option<TableStatistics>,

    schema: SchemaRef,
    file_format_config: Arc<FileFormatConfig>,
    storage_config: Arc<StorageConfig>,
    pushdowns: Arc<Pushdowns>,
    generated_fields: Option<SchemaRef>,

    storage_options: Option<BTreeMap<String, String>>,
}

impl LanceScanTask {
    pub fn fragment_ids(&self) -> Vec<usize> {
        self.fragment_ids.clone()
    }

    pub fn columns(&self) -> Option<Vec<String>> {
        self.columns.clone()
    }

    pub fn filter(&self) -> Option<String> {
        self.filter.clone()
    }

    pub fn storage_options(&self) -> Option<BTreeMap<String, String>> {
        self.storage_options.clone()
    }

    pub fn metadata(&self) -> Option<TableMetadata> {
        self.metadata.clone()
    }

    pub fn statistics(&self) -> Option<TableStatistics> {
        self.statistics.clone()
    }

    pub fn storage_config(&self) -> Arc<StorageConfig> {
        self.storage_config.clone()
    }
}

#[typetag::serde]
impl ScanTaskLike for LanceScanTask {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn dyn_eq(&self, other: &dyn ScanTaskLike) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|a| a == self)
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.hash(&mut state);
    }

    fn materialized_schema(&self) -> SchemaRef {
        todo!()
    }

    fn num_rows(&self) -> Option<usize> {
        self.num_rows
    }

    fn approx_num_rows(&self, _config: Option<&DaftExecutionConfig>) -> Option<f64> {
        self.num_rows().map(|r| r as f64)
    }

    fn upper_bound_rows(&self) -> Option<usize> {
        todo!()
    }

    fn size_bytes_on_disk(&self) -> Option<usize> {
        self.size_bytes
    }

    fn estimate_in_memory_size_bytes(
        &self,
        _config: Option<&DaftExecutionConfig>,
    ) -> Option<usize> {
        self.size_bytes_on_disk()
    }

    fn file_format_config(&self) -> Arc<FileFormatConfig> {
        self.file_format_config.clone()
    }

    fn pushdowns(&self) -> &Pushdowns {
        self.pushdowns.as_ref()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn get_file_paths(&self) -> Vec<String> {
        vec![self.uri.clone()]
    }
}

#[cfg(not(debug_assertions))]
impl std::fmt::Debug for LanceScanTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScanTask")
    }
}

impl DisplayAs for LanceScanTask {
    fn display_as(&self, _level: DisplayLevel) -> String {
        todo!()
    }
}

#[derive(Debug)]
pub struct LanceScanOperator {
    uri: String,
    file_format_config: Arc<FileFormatConfig>,
    storage_config: Arc<StorageConfig>,

    ds: Arc<Dataset>,
    storage_options: Arc<HashMap<String, String>>,
}

impl LanceScanOperator {
    pub fn try_new(
        uri: String,
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
        storage_options: Arc<HashMap<String, String>>,
    ) -> DaftResult<Self> {
        let cloned_uri = uri.clone();
        let rt = get_io_runtime(true);

        let lance_cfg = match file_format_config.as_ref() {
            FileFormatConfig::Lance(cfg) => cfg.clone(),
            _ => {
                return Err(DaftError::InternalError(format!(
                    "Expected Lance File Format Config, but got {}",
                    file_format_config.file_format()
                )));
            }
        };

        let cloned_storage_options = storage_options.as_ref().clone();
        let ds = rt
            .block_within_async_context(async move {
                crate::dataset::try_open_dataset(
                    cloned_uri.as_str(),
                    lance_cfg.version,
                    lance_cfg.tag.clone(),
                    lance_cfg.block_size,
                    lance_cfg.index_cache_size,
                    lance_cfg.metadata_cache_size,
                    Some(cloned_storage_options.clone()),
                    true,
                )
                .await
            })
            .map_err(|e| DaftError::External(Box::new(e)))??;

        Ok(Self {
            uri,
            file_format_config,
            storage_config,
            ds,
            storage_options,
        })
    }
}

impl ScanOperator for LanceScanOperator {
    fn name(&self) -> &'static str {
        "LanceScanOperator"
    }

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(crate::dataset::to_daft_schema(self.ds.schema()))
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &[]
    }

    fn file_path_column(&self) -> Option<&str> {
        None
    }

    fn generated_fields(&self) -> Option<SchemaRef> {
        None
    }

    fn can_absorb_filter(&self) -> bool {
        true
    }

    fn can_absorb_select(&self) -> bool {
        true
    }

    fn can_absorb_limit(&self) -> bool {
        false
    }

    fn can_absorb_shard(&self) -> bool {
        false
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!("{}({})", self.name(), self.uri),
            format!("Schema = {}", self.schema().short_string()),
        ]
    }

    fn supports_count_pushdown(&self) -> bool {
        true
    }

    fn supported_count_modes(&self) -> Vec<CountMode> {
        vec![CountMode::All]
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskLikeRef>> {
        let required_columns = pushdowns.columns.clone().map(|columns| {
            let mut columns = columns.iter().cloned().collect::<HashSet<String>>();
            if let Some(filter_columns) = pushdowns.filters.as_ref().map(get_required_columns) {
                columns.extend(filter_columns);
            }
            columns.into_iter().collect()
        });

        let filter_expr = pushdowns.pushed_filters.as_ref().map(|filters| {
            filters
                .iter()
                .map(|filter| filter.to_string())
                .collect::<Vec<_>>()
                .join(" & ")
        });

        let pushdowns_ref = Arc::new(pushdowns);
        let rt = get_io_runtime(true);
        let mut scan_tasks = vec![];
        for fragment in self.ds.get_fragments() {
            let fragment_id = fragment.id();
            let size_bytes: Option<u64> = fragment
                .metadata()
                .files
                .iter()
                .map(|file| file.file_size_bytes.get().map(u64::from))
                .sum();

            let cloned_expr = filter_expr.clone();
            let num_rows = rt
                .block_within_async_context(async move {
                    fragment
                        .count_rows(cloned_expr)
                        .await
                        .map_err(|e| DaftError::External(Box::new(e)))
                })
                .map_err(|e| DaftError::External(Box::new(e)))??;
            if num_rows == 0 {
                continue;
            }

            let scan_task = LanceScanTask {
                uri: self.uri.clone(),
                fragment_ids: vec![fragment_id],
                columns: required_columns.clone(),
                filter: filter_expr.clone(),
                storage_options: Some(
                    self.storage_options
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                ),
                num_rows: Some(num_rows),
                size_bytes: size_bytes.map(|sb| sb as usize),
                metadata: None,
                statistics: None,
                schema: self.schema(),
                file_format_config: self.file_format_config.clone(),
                storage_config: self.storage_config.clone(),
                pushdowns: pushdowns_ref.clone(),
                generated_fields: self.generated_fields(),
            };
            scan_tasks.push(Arc::new(scan_task) as Arc<dyn ScanTaskLike>);
        }

        Ok(scan_tasks)
    }

    fn as_pushdown_filter(&self) -> Option<&dyn SupportsPushdownFilters> {
        Some(self)
    }
}

impl SupportsPushdownFilters for LanceScanOperator {
    fn push_filters(&self, filters: &[ExprRef]) -> (Vec<ExprRef>, Vec<ExprRef>) {
        #[cfg(feature = "python")]
        {
            let is_arrow_expr = |expr: &ExprRef| -> bool {
                Python::attach(|py| -> PyResult<bool> {
                    let expr_module =
                        PyModule::import(py, pyo3::intern!(py, "daft.expressions.expressions"))?;
                    let expr_class = expr_module.getattr(pyo3::intern!(py, "Expression"))?;
                    expr_class
                        .getattr(pyo3::intern!(py, "is_arrow_expr"))?
                        .call1((PyExpr::from(expr.clone()),))?
                        .extract::<bool>()
                })
                .unwrap_or(false)
            };

            let (pushed_exprs, remain_exprs): (Vec<_>, Vec<_>) =
                filters.iter().cloned().partition(is_arrow_expr);

            (pushed_exprs, remain_exprs)
        }
        #[cfg(not(feature = "python"))]
        {
            (vec![], filters.to_vec())
        }
    }
}
