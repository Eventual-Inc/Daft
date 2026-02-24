use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use common_hashable_float_wrapper::FloatWrapper;
use common_io_config::IOConfig;
#[cfg(feature = "python")]
use common_py_serde::PyObjectWrapper;
use daft_schema::schema::SchemaRef;
use educe::Educe;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{LogicalPlan, stats::StatsState};

#[derive(Educe, Clone, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[educe(PartialEq, Eq, Hash)]
pub struct SkipExistingSpec {
    pub existing_path: Vec<String>,
    pub file_format: FileFormat,
    pub key_column: Vec<String>,
    pub io_config: Option<IOConfig>,
    pub num_workers: Option<usize>,
    pub cpus_per_worker: Option<FloatWrapper<f64>>,
    pub filter_batch_size: Option<usize>,
    pub keys_load_batch_size: Option<usize>,
    pub max_concurrency_per_worker: Option<usize>,
    pub strict_path_check: bool,
    #[cfg(feature = "python")]
    pub read_kwargs: PyObjectWrapper,
}

impl SkipExistingSpec {
    fn validate_inputs(
        existing_path: &[String],
        key_column: &[String],
        num_workers: Option<usize>,
        cpus_per_worker: Option<f64>,
        filter_batch_size: Option<usize>,
        keys_load_batch_size: Option<usize>,
        max_concurrency_per_worker: Option<usize>,
    ) -> DaftResult<()> {
        if existing_path.is_empty() || existing_path.iter().any(|p| p.is_empty()) {
            return Err(DaftError::ValueError(
                "[skip_existing] existing_path must be a non-empty list of non-empty paths"
                    .to_string(),
            ));
        }
        if key_column.is_empty() || key_column.iter().any(|c| c.is_empty()) {
            return Err(DaftError::ValueError(
                "[skip_existing] key_column must be a non-empty list of non-empty column names"
                    .to_string(),
            ));
        }
        if matches!(num_workers, Some(0)) {
            return Err(DaftError::ValueError(
                "[skip_existing] num_workers must be > 0".to_string(),
            ));
        }
        if matches!(cpus_per_worker, Some(v) if v <= 0.0) {
            return Err(DaftError::ValueError(
                "[skip_existing] cpus_per_worker must be > 0".to_string(),
            ));
        }
        if matches!(filter_batch_size, Some(0)) {
            return Err(DaftError::ValueError(
                "[skip_existing] filter_batch_size must be > 0".to_string(),
            ));
        }
        if matches!(keys_load_batch_size, Some(0)) {
            return Err(DaftError::ValueError(
                "[skip_existing] keys_load_batch_size must be > 0".to_string(),
            ));
        }
        if matches!(max_concurrency_per_worker, Some(0)) {
            return Err(DaftError::ValueError(
                "[skip_existing] max_concurrency_per_worker must be > 0".to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(feature = "python")]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        existing_path: Vec<String>,
        file_format: FileFormat,
        key_column: Vec<String>,
        io_config: Option<IOConfig>,
        read_kwargs: PyObjectWrapper,
        num_workers: Option<usize>,
        cpus_per_worker: Option<f64>,
        filter_batch_size: Option<usize>,
        keys_load_batch_size: Option<usize>,
        max_concurrency_per_worker: Option<usize>,
        strict_path_check: bool,
    ) -> DaftResult<Self> {
        Self::validate_inputs(
            &existing_path,
            &key_column,
            num_workers,
            cpus_per_worker,
            filter_batch_size,
            keys_load_batch_size,
            max_concurrency_per_worker,
        )?;
        Ok(Self {
            existing_path,
            file_format,
            key_column,
            io_config,
            read_kwargs,
            num_workers,
            cpus_per_worker: cpus_per_worker.map(FloatWrapper),
            filter_batch_size,
            keys_load_batch_size,
            max_concurrency_per_worker,
            strict_path_check,
        })
    }

    #[cfg(not(feature = "python"))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        existing_path: Vec<String>,
        file_format: FileFormat,
        key_column: Vec<String>,
        io_config: Option<IOConfig>,
        num_workers: Option<usize>,
        cpus_per_worker: Option<f64>,
        filter_batch_size: Option<usize>,
        keys_load_batch_size: Option<usize>,
        max_concurrency_per_worker: Option<usize>,
        strict_path_check: bool,
    ) -> DaftResult<Self> {
        Self::validate_inputs(
            &existing_path,
            &key_column,
            num_workers,
            cpus_per_worker,
            filter_batch_size,
            keys_load_batch_size,
            max_concurrency_per_worker,
        )?;
        Ok(Self {
            existing_path,
            file_format,
            key_column,
            io_config,
            num_workers,
            cpus_per_worker: cpus_per_worker.map(FloatWrapper),
            filter_batch_size,
            keys_load_batch_size,
            max_concurrency_per_worker,
            strict_path_check,
        })
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct SkipExisting {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    pub input: Arc<LogicalPlan>,
    pub spec: SkipExistingSpec,
    pub output_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl SkipExisting {
    pub fn try_new(input: Arc<LogicalPlan>, spec: SkipExistingSpec) -> DaftResult<Self> {
        let input_schema = input.schema();
        for col in &spec.key_column {
            if input_schema.get_field(col).is_err() {
                return Err(DaftError::ValueError(format!(
                    "[skip_existing] key column not found in schema: {col}",
                )));
            }
        }
        Ok(Self {
            plan_id: None,
            node_id: None,
            output_schema: input_schema,
            input,
            spec,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let path_display = if self.spec.existing_path.len() == 1 {
            self.spec.existing_path[0].as_str().to_string()
        } else {
            format!("{:?}", self.spec.existing_path)
        };
        let key_display = if self.spec.key_column.len() == 1 {
            self.spec.key_column[0].as_str().to_string()
        } else {
            format!("{:?}", self.spec.key_column)
        };
        let mut res = vec![format!(
            "[skip_existing] SkipExisting: path = {}, format = {:?}, on = {}",
            path_display, self.spec.file_format, key_display
        )];
        if let Some(io_config) = &self.spec.io_config {
            res.push(format!("IOConfig = {}", io_config));
        }
        if let Some(batch_size) = self.spec.filter_batch_size {
            res.push(format!("Key Filter Batch Size = {}", batch_size));
        }
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        sync::Arc,
    };

    use common_file_formats::FileFormat;
    #[cfg(feature = "python")]
    use pyo3::types::PyDictMethods;

    use super::SkipExistingSpec;

    #[cfg(not(feature = "python"))]
    #[test]
    fn test_skip_existing_spec_eq_hash() {
        let spec_a = SkipExistingSpec::new(
            vec!["root".to_string()],
            FileFormat::Csv,
            vec!["id".to_string()],
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();

        let spec_b = SkipExistingSpec::new(
            vec!["root2".to_string()],
            FileFormat::Csv,
            vec!["id".to_string()],
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();

        assert_ne!(spec_a, spec_b);

        let mut h1 = DefaultHasher::new();
        spec_a.hash(&mut h1);
        let mut h2 = DefaultHasher::new();
        spec_b.hash(&mut h2);
        assert_ne!(h1.finish(), h2.finish());
    }

    #[cfg(feature = "python")]
    #[test]
    fn test_skip_existing_spec_eq_hash_kwargs() {
        pyo3::prepare_freethreaded_python();
        pyo3::Python::with_gil(|py| {
            let kwargs_a = {
                let d = pyo3::types::PyDict::new(py);
                d.set_item("delimiter", "|").unwrap();
                common_py_serde::PyObjectWrapper(Arc::new(d.unbind().into()))
            };
            let kwargs_b = {
                let d = pyo3::types::PyDict::new(py);
                d.set_item("delimiter", "|").unwrap();
                common_py_serde::PyObjectWrapper(Arc::new(d.unbind().into()))
            };
            let kwargs_c = {
                let d = pyo3::types::PyDict::new(py);
                d.set_item("delimiter", ",").unwrap();
                common_py_serde::PyObjectWrapper(Arc::new(d.unbind().into()))
            };

            let spec_a = SkipExistingSpec::new(
                vec!["root".to_string()],
                FileFormat::Csv,
                vec!["id".to_string()],
                None,
                kwargs_a,
                None,
                None,
                None,
                None,
                None,
                false,
            )
            .unwrap();

            let spec_b = SkipExistingSpec::new(
                vec!["root".to_string()],
                FileFormat::Csv,
                vec!["id".to_string()],
                None,
                kwargs_b,
                None,
                None,
                None,
                None,
                None,
                false,
            )
            .unwrap();

            let spec_c = SkipExistingSpec::new(
                vec!["root".to_string()],
                FileFormat::Csv,
                vec!["id".to_string()],
                None,
                kwargs_c,
                None,
                None,
                None,
                None,
                None,
                false,
            )
            .unwrap();

            assert_eq!(spec_a, spec_b);

            let mut h1 = DefaultHasher::new();
            spec_a.hash(&mut h1);
            let mut h2 = DefaultHasher::new();
            spec_b.hash(&mut h2);
            assert_eq!(h1.finish(), h2.finish());

            assert_ne!(spec_a, spec_c);

            let mut h1 = DefaultHasher::new();
            spec_a.hash(&mut h1);
            let mut h2 = DefaultHasher::new();
            spec_c.hash(&mut h2);
            assert_ne!(h1.finish(), h2.finish());
        });
    }

    #[test]
    fn skip_existing_filter_batch_size_applied_to_filter() -> common_error::DaftResult<()> {
        use daft_core::prelude::{DataType, Field};
        use daft_dsl::{lit, resolved_col};

        use crate::{
            LogicalPlan,
            test::{dummy_scan_node, dummy_scan_operator},
        };

        let scan_builder =
            dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)]));

        #[cfg(feature = "python")]
        let builder_with_skip_existing =
            pyo3::Python::with_gil(|py| -> common_error::DaftResult<_> {
                let kwargs = common_py_serde::PyObjectWrapper(Arc::new(py.None()));
                let spec = SkipExistingSpec::new(
                    vec!["root".to_string()],
                    FileFormat::Csv,
                    vec!["a".to_string()],
                    None,
                    kwargs,
                    None,
                    None,
                    Some(10),
                    None,
                    None,
                    false,
                )?;
                scan_builder.skip_existing(spec)
            })?;

        #[cfg(not(feature = "python"))]
        let builder_with_skip_existing = {
            let spec = SkipExistingSpec::new(
                vec!["root".to_string()],
                FileFormat::Csv,
                vec!["a".to_string()],
                None,
                None,
                None,
                Some(10),
                None,
                None,
                false,
            )?;
            scan_builder.skip_existing(spec)?
        };

        let predicate = resolved_col("a").lt(lit(2));
        let applied =
            builder_with_skip_existing.apply_skip_existing_predicates(vec![Some(predicate)])?;
        let plan = applied.build();

        match plan.as_ref() {
            LogicalPlan::Filter(filter) => {
                assert_eq!(filter.batch_size, Some(10));
                Ok(())
            }
            other => Err(common_error::DaftError::InternalError(format!(
                "[skip_existing] Expected Filter after applying skip_existing predicates, got {other:?}"
            ))),
        }
    }

    #[cfg(not(feature = "python"))]
    #[test]
    fn skip_existing_multiline_display_includes_filter_batch_size() -> common_error::DaftResult<()>
    {
        use daft_core::prelude::{DataType, Field};

        use crate::{
            LogicalPlan,
            test::{dummy_scan_node, dummy_scan_operator},
        };

        let scan_builder =
            dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)]));

        let spec = SkipExistingSpec::new(
            vec!["root".to_string()],
            FileFormat::Parquet,
            vec!["a".to_string()],
            None,
            None,
            None,
            Some(10),
            None,
            None,
            false,
        )?;

        let plan = scan_builder.skip_existing(spec)?.build();
        match plan.as_ref() {
            LogicalPlan::SkipExisting(op) => {
                let lines = op.multiline_display();
                assert!(lines.iter().any(|l| l == "Key Filter Batch Size = 10"));
                Ok(())
            }
            other => Err(common_error::DaftError::InternalError(format!(
                "Expected SkipExisting, got {other:?}"
            ))),
        }
    }
}

#[cfg(feature = "python")]
#[pyclass(module = "daft.daft", name = "SkipExistingSpec")]
#[derive(Clone)]
pub struct PySkipExistingSpec {
    pub spec: SkipExistingSpec,
}

#[cfg(feature = "python")]
#[pymethods]
impl PySkipExistingSpec {
    #[getter]
    pub fn existing_path(&self) -> Vec<String> {
        self.spec.existing_path.clone()
    }

    #[getter]
    pub fn file_format(&self) -> FileFormat {
        self.spec.file_format
    }

    #[getter]
    pub fn key_column(&self) -> Vec<String> {
        self.spec.key_column.clone()
    }

    #[getter]
    pub fn io_config(&self) -> Option<common_io_config::python::IOConfig> {
        self.spec
            .io_config
            .as_ref()
            .map(|c| common_io_config::python::IOConfig { config: c.clone() })
    }

    #[getter]
    pub fn num_workers(&self) -> Option<usize> {
        self.spec.num_workers
    }

    #[getter]
    pub fn cpus_per_worker(&self) -> Option<f64> {
        self.spec.cpus_per_worker.as_ref().map(|v| v.0)
    }

    #[getter]
    pub fn filter_batch_size(&self) -> Option<usize> {
        self.spec.filter_batch_size
    }

    #[getter]
    pub fn keys_load_batch_size(&self) -> Option<usize> {
        self.spec.keys_load_batch_size
    }

    #[getter]
    pub fn max_concurrency_per_worker(&self) -> Option<usize> {
        self.spec.max_concurrency_per_worker
    }

    #[getter]
    pub fn strict_path_check(&self) -> bool {
        self.spec.strict_path_check
    }

    #[getter]
    pub fn read_kwargs(&self, py: Python) -> Py<PyAny> {
        self.spec.read_kwargs.0.as_ref().clone_ref(py)
    }
}

#[cfg(feature = "python")]
impl From<SkipExistingSpec> for PySkipExistingSpec {
    fn from(spec: SkipExistingSpec) -> Self {
        Self { spec }
    }
}
