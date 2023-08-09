use std::sync::Arc;

use daft_dsl::Expr;

use crate::{logical_plan::LogicalPlan, ops};

#[cfg(feature = "python")]
use {
    crate::{
        planner::plan,
        source_info::{ExternalInfo, FileInfo, InMemoryInfo, PyFileFormatConfig, SourceInfo},
        PartitionScheme, PartitionSpec,
    },
    daft_core::python::schema::PySchema,
    daft_dsl::python::PyExpr,
    pyo3::{exceptions::PyValueError, prelude::*},
    std::collections::HashMap,
};

#[cfg_attr(feature = "python", pyclass)]
#[derive(Debug)]
pub struct LogicalPlanBuilder {
    plan: Arc<LogicalPlan>,
}

impl LogicalPlanBuilder {
    pub fn new(plan: Arc<LogicalPlan>) -> Self {
        Self { plan }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl LogicalPlanBuilder {
    #[staticmethod]
    pub fn in_memory_scan(
        partition_key: &str,
        cache_entry: &PyAny,
        schema: &PySchema,
        partition_spec: &PartitionSpec,
    ) -> PyResult<LogicalPlanBuilder> {
        let source_info = SourceInfo::InMemoryInfo(InMemoryInfo::new(
            partition_key.into(),
            cache_entry.to_object(cache_entry.py()),
        ));
        let logical_plan: LogicalPlan = ops::Source::new(
            schema.schema.clone(),
            source_info.into(),
            partition_spec.clone().into(),
        )
        .into();
        let logical_plan_builder = LogicalPlanBuilder::new(logical_plan.into());
        Ok(logical_plan_builder)
    }

    #[staticmethod]
    pub fn table_scan(
        file_paths: Vec<String>,
        schema: &PySchema,
        file_format_config: PyFileFormatConfig,
    ) -> PyResult<LogicalPlanBuilder> {
        let num_partitions = file_paths.len();
        let source_info = SourceInfo::ExternalInfo(ExternalInfo::new(
            schema.schema.clone(),
            FileInfo::new(file_paths, None, None, None).into(),
            file_format_config.into(),
        ));
        let partition_spec = PartitionSpec::new(PartitionScheme::Unknown, num_partitions, None);
        let logical_plan: LogicalPlan = ops::Source::new(
            schema.schema.clone(),
            source_info.into(),
            partition_spec.into(),
        )
        .into();
        let logical_plan_builder = LogicalPlanBuilder::new(logical_plan.into());
        Ok(logical_plan_builder)
    }

    pub fn filter(&self, predicate: &PyExpr) -> PyResult<LogicalPlanBuilder> {
        let logical_plan: LogicalPlan =
            ops::Filter::new(predicate.expr.clone(), self.plan.clone()).into();
        let logical_plan_builder = LogicalPlanBuilder::new(logical_plan.into());
        Ok(logical_plan_builder)
    }

    pub fn limit(&self, limit: i64) -> PyResult<LogicalPlanBuilder> {
        let logical_plan: LogicalPlan = ops::Limit::new(limit, self.plan.clone()).into();
        let logical_plan_builder = LogicalPlanBuilder::new(logical_plan.into());
        Ok(logical_plan_builder)
    }

    pub fn aggregate(&self, agg_exprs: Vec<PyExpr>) -> PyResult<LogicalPlanBuilder> {
        use crate::ops::Aggregate;
        let agg_exprs = agg_exprs
            .iter()
            .map(|expr| match &expr.expr {
                Expr::Agg(agg_expr) => Ok(agg_expr.clone()),
                _ => Err(PyValueError::new_err(format!(
                    "Expected aggregation expression, but got: {}",
                    expr.expr
                ))),
            })
            .collect::<PyResult<Vec<daft_dsl::AggExpr>>>()?;
        let logical_plan: LogicalPlan = Aggregate::new(agg_exprs, self.plan.clone()).into();
        let logical_plan_builder = LogicalPlanBuilder::new(logical_plan.into());
        Ok(logical_plan_builder)
    }

    pub fn schema(&self) -> PyResult<PySchema> {
        Ok(self.plan.schema().into())
    }

    pub fn partition_spec(&self) -> PyResult<PartitionSpec> {
        Ok(self.plan.partition_spec().as_ref().clone())
    }

    pub fn to_partition_tasks(&self, psets: HashMap<String, Vec<PyObject>>) -> PyResult<PyObject> {
        let physical_plan = plan(self.plan.as_ref())?;
        Python::with_gil(|py| physical_plan.to_partition_tasks(py, &psets))
    }

    pub fn repr_ascii(&self) -> PyResult<String> {
        Ok(self.plan.repr_ascii())
    }
}
