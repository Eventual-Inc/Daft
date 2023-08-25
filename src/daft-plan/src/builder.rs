use std::sync::Arc;

use crate::{logical_plan::LogicalPlan, optimization::Optimizer, ResourceRequest};

#[cfg(feature = "python")]
use {
    crate::{
        ops,
        planner::plan,
        sink_info::{OutputFileInfo, SinkInfo},
        source_info::{
            ExternalInfo as ExternalSourceInfo, FileInfo as InputFileInfo, InMemoryInfo,
            PyFileFormatConfig, SourceInfo,
        },
        FileFormat, JoinType, PartitionScheme, PartitionSpec, PhysicalPlanScheduler,
    },
    daft_core::{datatypes::Field, python::schema::PySchema, schema::Schema, DataType},
    daft_dsl::{python::PyExpr, Expr},
    pyo3::{exceptions::PyValueError, prelude::*},
};

#[cfg_attr(feature = "python", pyclass)]
#[derive(Debug)]
pub struct LogicalPlanBuilder {
    // The current root of the logical plan in this builder.
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
        Ok(logical_plan.into())
    }

    #[staticmethod]
    pub fn table_scan(
        file_paths: Vec<String>,
        file_sizes: Vec<Option<i64>>,
        file_rows: Vec<Option<i64>>,
        schema: &PySchema,
        file_format_config: PyFileFormatConfig,
    ) -> PyResult<LogicalPlanBuilder> {
        let num_partitions = file_paths.len();
        let source_info = SourceInfo::ExternalInfo(ExternalSourceInfo::new(
            schema.schema.clone(),
            InputFileInfo::new(file_paths, file_sizes, file_rows).into(),
            file_format_config.into(),
        ));
        let partition_spec = PartitionSpec::new(PartitionScheme::Unknown, num_partitions, None);
        let logical_plan: LogicalPlan = ops::Source::new(
            schema.schema.clone(),
            source_info.into(),
            partition_spec.into(),
        )
        .into();
        Ok(logical_plan.into())
    }

    pub fn project(
        &self,
        projection: Vec<PyExpr>,
        resource_request: ResourceRequest,
    ) -> PyResult<LogicalPlanBuilder> {
        let projection_exprs = projection
            .iter()
            .map(|e| e.clone().into())
            .collect::<Vec<Expr>>();
        let logical_plan: LogicalPlan =
            ops::Project::try_new(self.plan.clone(), projection_exprs, resource_request)?.into();
        Ok(logical_plan.into())
    }

    pub fn filter(&self, predicate: &PyExpr) -> PyResult<LogicalPlanBuilder> {
        let logical_plan: LogicalPlan =
            ops::Filter::new(predicate.expr.clone(), self.plan.clone()).into();
        Ok(logical_plan.into())
    }

    pub fn limit(&self, limit: i64) -> PyResult<LogicalPlanBuilder> {
        let logical_plan: LogicalPlan = ops::Limit::new(limit, self.plan.clone()).into();
        Ok(logical_plan.into())
    }

    pub fn explode(&self, to_explode_pyexprs: Vec<PyExpr>) -> PyResult<LogicalPlanBuilder> {
        let to_explode = to_explode_pyexprs
            .iter()
            .map(|e| e.clone().into())
            .collect::<Vec<Expr>>();
        let logical_plan: LogicalPlan =
            ops::Explode::try_new(self.plan.clone(), to_explode)?.into();
        Ok(logical_plan.into())
    }

    pub fn sort(
        &self,
        sort_by: Vec<PyExpr>,
        descending: Vec<bool>,
    ) -> PyResult<LogicalPlanBuilder> {
        if sort_by.is_empty() {
            return Err(PyValueError::new_err(
                "df.sort() must be given at least one column/expression to sort by",
            ));
        }
        let sort_by_exprs: Vec<Expr> = sort_by.iter().map(|expr| expr.clone().into()).collect();
        let logical_plan: LogicalPlan =
            ops::Sort::new(sort_by_exprs, descending, self.plan.clone()).into();
        Ok(logical_plan.into())
    }

    pub fn repartition(
        &self,
        num_partitions: usize,
        partition_by: Vec<PyExpr>,
        scheme: PartitionScheme,
    ) -> PyResult<LogicalPlanBuilder> {
        let partition_by_exprs: Vec<Expr> = partition_by
            .iter()
            .map(|expr| expr.clone().into())
            .collect();
        let logical_plan: LogicalPlan = ops::Repartition::new(
            num_partitions,
            partition_by_exprs,
            scheme,
            self.plan.clone(),
        )
        .into();
        Ok(logical_plan.into())
    }

    pub fn coalesce(&self, num_partitions: usize) -> PyResult<LogicalPlanBuilder> {
        let logical_plan: LogicalPlan =
            ops::Coalesce::new(num_partitions, self.plan.clone()).into();
        Ok(logical_plan.into())
    }

    pub fn distinct(&self) -> PyResult<LogicalPlanBuilder> {
        let logical_plan: LogicalPlan = ops::Distinct::new(self.plan.clone()).into();
        Ok(logical_plan.into())
    }

    pub fn aggregate(
        &self,
        agg_exprs: Vec<PyExpr>,
        groupby_exprs: Vec<PyExpr>,
    ) -> PyResult<LogicalPlanBuilder> {
        use crate::ops::Aggregate;
        // TODO(Clark): Move validation into Aggregate::new() (changing it to a ::try_new()).
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
        let groupby_exprs = groupby_exprs
            .iter()
            .map(|expr| expr.clone().into())
            .collect::<Vec<Expr>>();

        let logical_plan: LogicalPlan =
            Aggregate::try_new(self.plan.clone(), agg_exprs, groupby_exprs)?.into();
        Ok(logical_plan.into())
    }

    pub fn join(
        &self,
        other: &Self,
        left_on: Vec<PyExpr>,
        right_on: Vec<PyExpr>,
        join_type: JoinType,
    ) -> PyResult<LogicalPlanBuilder> {
        let left_on_exprs = left_on
            .iter()
            .map(|e| e.clone().into())
            .collect::<Vec<Expr>>();
        let right_on_exprs = right_on
            .iter()
            .map(|e| e.clone().into())
            .collect::<Vec<Expr>>();
        let logical_plan: LogicalPlan = ops::Join::try_new(
            self.plan.clone(),
            other.plan.clone(),
            left_on_exprs,
            right_on_exprs,
            join_type,
        )?
        .into();
        Ok(logical_plan.into())
    }

    pub fn concat(&self, other: &Self) -> PyResult<LogicalPlanBuilder> {
        let self_schema = self.plan.schema();
        let other_schema = other.plan.schema();
        // TODO(Clark): Move validation into Concat::new() (changing it to a ::try_new()).
        if self_schema != other_schema {
            return Err(PyValueError::new_err(format!(
                "Both DataFrames must have the same schema to concatenate them, but got: {}, {}",
                self_schema, other_schema
            )));
        }
        let logical_plan: LogicalPlan =
            ops::Concat::new(other.plan.clone(), self.plan.clone()).into();
        Ok(logical_plan.into())
    }

    pub fn table_write(
        &self,
        root_dir: &str,
        file_format: FileFormat,
        partition_cols: Option<Vec<PyExpr>>,
        compression: Option<String>,
    ) -> PyResult<LogicalPlanBuilder> {
        let part_cols =
            partition_cols.map(|cols| cols.iter().map(|e| e.clone().into()).collect::<Vec<Expr>>());
        let sink_info = SinkInfo::OutputFileInfo(OutputFileInfo::new(
            root_dir.into(),
            file_format,
            part_cols,
            compression,
        ));
        let fields = vec![Field::new("file_paths", DataType::Utf8)];
        let logical_plan: LogicalPlan = ops::Sink::new(
            Schema::new(fields)?.into(),
            sink_info.into(),
            self.plan.clone(),
        )
        .into();
        Ok(logical_plan.into())
    }

    pub fn optimize(&self) -> PyResult<LogicalPlanBuilder> {
        let optimizer = Optimizer::new(Default::default());
        let new_plan = optimizer.optimize(self.plan.clone(), |_, _, _, _, _| {})?;
        Ok(Self::new(new_plan))
    }

    pub fn schema(&self) -> PyResult<PySchema> {
        Ok(self.plan.schema().into())
    }

    pub fn partition_spec(&self) -> PyResult<PartitionSpec> {
        Ok(self.plan.partition_spec().as_ref().clone())
    }

    pub fn to_physical_plan_scheduler(&self) -> PyResult<PhysicalPlanScheduler> {
        let physical_plan = plan(self.plan.as_ref())?;
        Ok(Arc::new(physical_plan).into())
    }

    pub fn repr_ascii(&self) -> PyResult<String> {
        Ok(self.plan.repr_ascii())
    }
}

impl From<LogicalPlan> for LogicalPlanBuilder {
    fn from(plan: LogicalPlan) -> Self {
        Self::new(plan.into())
    }
}
