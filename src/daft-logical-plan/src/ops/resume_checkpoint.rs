use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use common_hashable_float_wrapper::FloatWrapper;
use common_io_config::IOConfig;
#[cfg(feature = "python")]
use common_py_serde::PyObjectWrapper;
use daft_schema::schema::SchemaRef;
use educe::Educe;
use serde::{Deserialize, Serialize};

use crate::{LogicalPlan, stats::StatsState};

#[derive(Educe, Clone, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[educe(PartialEq, Eq, Hash)]
pub struct ResumeCheckpointSpec {
    pub root_dir: Vec<String>,
    pub file_format: FileFormat,
    pub key_column: Vec<String>,
    pub io_config: Option<IOConfig>,
    pub num_buckets: Option<usize>,
    pub num_cpus: Option<FloatWrapper<f64>>,
    pub resume_filter_batch_size: Option<usize>,
    pub checkpoint_loading_batch_size: Option<usize>,
    pub checkpoint_actor_max_concurrency: Option<usize>,
    #[cfg(feature = "python")]
    pub read_kwargs: PyObjectWrapper,
}

impl ResumeCheckpointSpec {
    #[cfg(feature = "python")]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        root_dir: Vec<String>,
        file_format: FileFormat,
        key_column: Vec<String>,
        io_config: Option<IOConfig>,
        read_kwargs: PyObjectWrapper,
        num_buckets: Option<usize>,
        num_cpus: Option<f64>,
        resume_filter_batch_size: Option<usize>,
        checkpoint_loading_batch_size: Option<usize>,
        checkpoint_actor_max_concurrency: Option<usize>,
    ) -> DaftResult<Self> {
        if root_dir.is_empty() || root_dir.iter().any(|p| p.is_empty()) {
            return Err(DaftError::ValueError(
                "resume checkpoint root_dir must be a non-empty list of non-empty paths"
                    .to_string(),
            ));
        }
        if key_column.is_empty() || key_column.iter().any(|c| c.is_empty()) {
            return Err(DaftError::ValueError(
                "resume checkpoint key_column must be a non-empty list of non-empty column names"
                    .to_string(),
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
        if matches!(resume_filter_batch_size, Some(0)) {
            return Err(DaftError::ValueError(
                "resume checkpoint resume_filter_batch_size must be > 0".to_string(),
            ));
        }
        if matches!(checkpoint_loading_batch_size, Some(0)) {
            return Err(DaftError::ValueError(
                "resume checkpoint checkpoint_loading_batch_size must be > 0".to_string(),
            ));
        }
        if matches!(checkpoint_actor_max_concurrency, Some(0)) {
            return Err(DaftError::ValueError(
                "resume checkpoint checkpoint_actor_max_concurrency must be > 0".to_string(),
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
            resume_filter_batch_size,
            checkpoint_loading_batch_size,
            checkpoint_actor_max_concurrency,
        })
    }

    #[cfg(not(feature = "python"))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        root_dir: Vec<String>,
        file_format: FileFormat,
        key_column: Vec<String>,
        io_config: Option<IOConfig>,
        num_buckets: Option<usize>,
        num_cpus: Option<f64>,
        resume_filter_batch_size: Option<usize>,
        checkpoint_loading_batch_size: Option<usize>,
        checkpoint_actor_max_concurrency: Option<usize>,
    ) -> DaftResult<Self> {
        if root_dir.is_empty() || root_dir.iter().any(|p| p.is_empty()) {
            return Err(DaftError::ValueError(
                "resume checkpoint root_dir must be a non-empty list of non-empty paths"
                    .to_string(),
            ));
        }
        if key_column.is_empty() || key_column.iter().any(|c| c.is_empty()) {
            return Err(DaftError::ValueError(
                "resume checkpoint key_column must be a non-empty list of non-empty column names"
                    .to_string(),
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
        if matches!(resume_filter_batch_size, Some(0)) {
            return Err(DaftError::ValueError(
                "resume checkpoint resume_filter_batch_size must be > 0".to_string(),
            ));
        }
        if matches!(checkpoint_loading_batch_size, Some(0)) {
            return Err(DaftError::ValueError(
                "resume checkpoint checkpoint_loading_batch_size must be > 0".to_string(),
            ));
        }
        if matches!(checkpoint_actor_max_concurrency, Some(0)) {
            return Err(DaftError::ValueError(
                "resume checkpoint checkpoint_actor_max_concurrency must be > 0".to_string(),
            ));
        }
        Ok(Self {
            root_dir,
            file_format,
            key_column,
            io_config,
            num_buckets,
            num_cpus: num_cpus.map(FloatWrapper),
            resume_filter_batch_size,
            checkpoint_loading_batch_size,
            checkpoint_actor_max_concurrency,
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
        for col in &spec.key_column {
            if input_schema.get_field(col).is_err() {
                return Err(DaftError::ValueError(format!(
                    "resume checkpoint key column not found in schema: {col}",
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
        let path_display = if self.spec.root_dir.len() == 1 {
            self.spec.root_dir[0].as_str().to_string()
        } else {
            format!("{:?}", self.spec.root_dir)
        };
        let key_display = if self.spec.key_column.len() == 1 {
            self.spec.key_column[0].as_str().to_string()
        } else {
            format!("{:?}", self.spec.key_column)
        };
        let mut res = vec![format!(
            "ResumeCheckpoint: path = {}, format = {:?}, on = {}",
            path_display, self.spec.file_format, key_display
        )];
        if let Some(io_config) = &self.spec.io_config {
            res.push(format!("IOConfig = {}", io_config));
        }
        if let Some(batch_size) = self.spec.resume_filter_batch_size {
            res.push(format!("Resume Filter Batch Size = {}", batch_size));
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

    use super::ResumeCheckpointSpec;

    #[cfg(not(feature = "python"))]
    #[test]
    fn test_resume_checkpoint_spec_eq_hash() {
        let spec_a = ResumeCheckpointSpec::new(
            vec!["root".to_string()],
            FileFormat::Csv,
            vec!["id".to_string()],
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let spec_b = ResumeCheckpointSpec::new(
            vec!["root2".to_string()],
            FileFormat::Csv,
            vec!["id".to_string()],
            None,
            None,
            None,
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
            )
            .unwrap();

            let spec_b = ResumeCheckpointSpec::new(
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
            )
            .unwrap();

            let spec_c = ResumeCheckpointSpec::new(
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
    fn resume_checkpoint_resume_filter_batch_size_applied_to_filter() -> common_error::DaftResult<()>
    {
        use daft_core::prelude::{DataType, Field};
        use daft_dsl::{lit, resolved_col};

        use crate::{
            LogicalPlan,
            test::{dummy_scan_node, dummy_scan_operator},
        };

        let scan_builder =
            dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)]));

        #[cfg(feature = "python")]
        let builder_with_resume = pyo3::Python::with_gil(|py| -> common_error::DaftResult<_> {
            let kwargs = common_py_serde::PyObjectWrapper(Arc::new(py.None()));
            let spec = ResumeCheckpointSpec::new(
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
            )?;
            scan_builder.resume_checkpoint(spec)
        })?;

        #[cfg(not(feature = "python"))]
        let builder_with_resume = {
            let spec = ResumeCheckpointSpec::new(
                vec!["root".to_string()],
                FileFormat::Csv,
                vec!["a".to_string()],
                None,
                None,
                None,
                Some(10),
                None,
                None,
            )?;
            scan_builder.resume_checkpoint(spec)?
        };

        let predicate = resolved_col("a").lt(lit(2));
        let applied =
            builder_with_resume.apply_resume_checkpoint_predicates(vec![Some(predicate)])?;
        let plan = applied.build();

        match plan.as_ref() {
            LogicalPlan::Filter(filter) => {
                assert_eq!(filter.batch_size, Some(10));
                Ok(())
            }
            other => Err(common_error::DaftError::InternalError(format!(
                "Expected Filter after applying resume predicates, got {other:?}"
            ))),
        }
    }

    #[cfg(not(feature = "python"))]
    #[test]
    fn resume_checkpoint_multiline_display_includes_resume_filter_batch_size()
    -> common_error::DaftResult<()> {
        use daft_core::prelude::{DataType, Field};

        use crate::{
            LogicalPlan,
            test::{dummy_scan_node, dummy_scan_operator},
        };

        let scan_builder =
            dummy_scan_node(dummy_scan_operator(vec![Field::new("a", DataType::Int64)]));

        let spec = ResumeCheckpointSpec::new(
            vec!["root".to_string()],
            FileFormat::Parquet,
            vec!["a".to_string()],
            None,
            None,
            None,
            Some(10),
            None,
            None,
        )?;

        let plan = scan_builder.resume_checkpoint(spec)?.build();
        match plan.as_ref() {
            LogicalPlan::ResumeCheckpoint(op) => {
                let lines = op.multiline_display();
                assert!(lines.iter().any(|l| l == "Resume Filter Batch Size = 10"));
                Ok(())
            }
            other => Err(common_error::DaftError::InternalError(format!(
                "Expected ResumeCheckpoint, got {other:?}"
            ))),
        }
    }
}
