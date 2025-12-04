use std::{collections::HashSet, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormatConfig;
use common_runtime::get_io_runtime;
use common_scan_info::{
    PartitionField, Pushdowns, ScanOperator, ScanTaskLike, ScanTaskLikeRef, SupportsPushdownFilters,
};
use daft_core::{count_mode::CountMode, prelude::SchemaRef};
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{ExprRef, optimization::get_required_columns};
use itertools::Itertools;
use lance::Dataset;
#[cfg(feature = "python")]
use pyo3::{
    PyResult, Python,
    prelude::{PyAnyMethods, PyModule},
};

use crate::{DataSource, ScanTask, storage_config::StorageConfig};

#[derive(Debug)]
pub struct LanceScanOperator {
    uri: String,
    file_format_config: Arc<FileFormatConfig>,
    storage_config: Arc<StorageConfig>,

    ds: Arc<Dataset>,
}

impl LanceScanOperator {
    pub fn try_new(
        uri: String,
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
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

        let ds = Arc::new(
            rt.block_within_async_context(async move {
                daft_lance::schema::try_open_dataset(
                    cloned_uri.as_str(),
                    lance_cfg.version,
                    lance_cfg.tag.clone(),
                    lance_cfg.block_size,
                    lance_cfg.index_cache_size,
                    lance_cfg.metadata_cache_size,
                    lance_cfg.storage_options,
                )
                .await
            })
            .map_err(|e| DaftError::External(Box::new(e)))??,
        );

        Ok(Self {
            uri,
            file_format_config,
            storage_config,
            ds,
        })
    }
}

impl ScanOperator for LanceScanOperator {
    fn name(&self) -> &'static str {
        "LanceScanOperator"
    }

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(daft_lance::schema::to_daft_schema(self.ds.schema()))
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

        let filter_expr = pushdowns
            .pushed_filters
            .as_ref()
            .map(|filters| filters.iter().map(|filter| filter.to_string()).join(" & "));

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

            let scan_task = ScanTask::new(
                vec![DataSource::Fragment {
                    uri: self.uri.clone(),
                    fragment_ids: vec![fragment_id],
                    columns: required_columns.clone(),
                    filter: filter_expr.clone(),
                    size_bytes,
                    metadata: None,
                    statistics: None,
                }],
                self.file_format_config.clone(),
                self.schema(),
                self.storage_config.clone(),
                pushdowns.clone(),
                self.generated_fields(),
            );
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

            // FIXME by zhenchao
            let mut pushed_exprs = vec![];
            let mut remain_exprs = vec![];
            for expr in filters {
                if is_arrow_expr(expr) {
                    pushed_exprs.push(expr.clone());
                } else {
                    remain_exprs.push(expr.clone());
                }
            }

            (pushed_exprs, remain_exprs)
        }
        #[cfg(not(feature = "python"))]
        {
            (vec![], filters.to_vec())
        }
    }
}
