use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use common_hashable_float_wrapper::FloatWrapper;
use common_io_config::IOConfig;
use daft_schema::schema::SchemaRef;
use educe::Educe;
use serde::{Deserialize, Serialize};

use crate::{LogicalPlan, stats::StatsState};

#[cfg(feature = "python")]
use common_py_serde::PyObjectWrapper;

#[derive(Educe, Clone, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[educe(PartialEq, Eq, Hash)]
pub struct ResumeCheckpointSpec {
    pub root_dir: String,
    pub file_format: FileFormat,
    pub key_column: String,
    pub io_config: Option<IOConfig>,
    pub num_buckets: Option<usize>,
    pub num_cpus: Option<FloatWrapper<f64>>,
    #[cfg(feature = "python")]
    pub read_kwargs: PyObjectWrapper,
}

impl ResumeCheckpointSpec {
    #[cfg(feature = "python")]
    pub fn new(
        root_dir: String,
        file_format: FileFormat,
        key_column: String,
        io_config: Option<IOConfig>,
        read_kwargs: PyObjectWrapper,
        num_buckets: Option<usize>,
        num_cpus: Option<f64>,
    ) -> DaftResult<Self> {
        if root_dir.is_empty() {
            return Err(DaftError::ValueError(
                "resume checkpoint root_dir must be non-empty".to_string(),
            ));
        }
        if key_column.is_empty() {
            return Err(DaftError::ValueError(
                "resume checkpoint key_column must be non-empty".to_string(),
            ));
        }
        if matches!(num_buckets, Some(0)) {
            return Err(DaftError::ValueError(
                "resume checkpoint num_buckets must be > 0".to_string(),
            ));
        }
        if matches!(num_cpus, Some(v) if v <= 0.0) {
            return Err(DaftError::ValueError(
                "resume checkpoint num_cpus must be > 0".to_string(),
            ));
        }
        Ok(Self {
            root_dir,
            file_format,
            key_column,
            io_config,
            read_kwargs,
            num_buckets,
            num_cpus: num_cpus.map(FloatWrapper),
        })
    }

    #[cfg(not(feature = "python"))]
    pub fn new(
        root_dir: String,
        file_format: FileFormat,
        key_column: String,
        io_config: Option<IOConfig>,
        num_buckets: Option<usize>,
        num_cpus: Option<f64>,
    ) -> DaftResult<Self> {
        if root_dir.is_empty() {
            return Err(DaftError::ValueError(
                "resume checkpoint root_dir must be non-empty".to_string(),
            ));
        }
        if key_column.is_empty() {
            return Err(DaftError::ValueError(
                "resume checkpoint key_column must be non-empty".to_string(),
            ));
        }
        if matches!(num_buckets, Some(0)) {
            return Err(DaftError::ValueError(
                "resume checkpoint num_buckets must be > 0".to_string(),
            ));
        }
        if matches!(num_cpus, Some(v) if v <= 0.0) {
            return Err(DaftError::ValueError(
                "resume checkpoint num_cpus must be > 0".to_string(),
            ));
        }
        Ok(Self {
            root_dir,
            file_format,
            key_column,
            io_config,
            num_buckets,
            num_cpus: num_cpus.map(FloatWrapper),
        })
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ResumeCheckpoint {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    pub input: Arc<LogicalPlan>,
    pub spec: ResumeCheckpointSpec,
    pub output_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl ResumeCheckpoint {
    pub fn try_new(input: Arc<LogicalPlan>, spec: ResumeCheckpointSpec) -> DaftResult<Self> {
        let input_schema = input.schema();
        if input_schema.get_field(&spec.key_column).is_err() {
            return Err(DaftError::ValueError(format!(
                "resume checkpoint key column not found in schema: {}",
                spec.key_column
            )));
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
        let mut res = vec![format!(
            "ResumeCheckpoint: path = {}, format = {:?}, on = {}",
            self.spec.root_dir, self.spec.file_format, self.spec.key_column
        )];
        if let Some(io_config) = &self.spec.io_config {
            res.push(format!("IOConfig = {}", io_config));
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

    use super::ResumeCheckpointSpec;

    #[cfg(feature = "python")]
    use pyo3::types::PyDictMethods;

    #[cfg(not(feature = "python"))]
    #[test]
    fn test_resume_checkpoint_spec_eq_hash() {
        let spec_a = ResumeCheckpointSpec::new(
            "root".to_string(),
            FileFormat::Csv,
            "id".to_string(),
            None,
            None,
            None,
        )
        .unwrap();

        let spec_b = ResumeCheckpointSpec::new(
            "root2".to_string(),
            FileFormat::Csv,
            "id".to_string(),
            None,
            None,
            None,
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
    fn test_resume_checkpoint_spec_eq_hash_kwargs() {
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

            let spec_a = ResumeCheckpointSpec::new(
                "root".to_string(),
                FileFormat::Csv,
                "id".to_string(),
                None,
                kwargs_a,
                None,
                None,
            )
            .unwrap();

            let spec_b = ResumeCheckpointSpec::new(
                "root".to_string(),
                FileFormat::Csv,
                "id".to_string(),
                None,
                kwargs_b,
                None,
                None,
            )
            .unwrap();

            let spec_c = ResumeCheckpointSpec::new(
                "root".to_string(),
                FileFormat::Csv,
                "id".to_string(),
                None,
                kwargs_c,
                None,
                None,
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
}
