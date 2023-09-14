use std::sync::Arc;

use crate::{
    logical_ops,
    logical_plan::LogicalPlan,
    optimization::Optimizer,
    planner::plan,
    sink_info::{OutputFileInfo, SinkInfo},
    source_info::{
        ExternalInfo as ExternalSourceInfo, FileFormatConfig, FileInfos as InputFileInfos,
        PyStorageConfig, SourceInfo, StorageConfig,
    },
    FileFormat, JoinType, PartitionScheme, PartitionSpec, PhysicalPlanScheduler, ResourceRequest,
};
use common_error::{DaftError, DaftResult};
use daft_core::schema::SchemaRef;
use daft_core::{datatypes::Field, schema::Schema, DataType};
use daft_dsl::Expr;

#[cfg(feature = "python")]
use {
    crate::{
        physical_plan::PhysicalPlan,
        source_info::{InMemoryInfo, PyFileFormatConfig},
    },
    daft_core::python::schema::PySchema,
    daft_dsl::python::PyExpr,
    pyo3::prelude::*,
};

/// A logical plan builder, which simplifies constructing logical plans via
/// a fluent interface. E.g., LogicalPlanBuilder::table_scan(..).project(..).filter(..).build().
///
/// This builder holds the current root (sink) of the logical plan, and the building methods return
/// a brand new builder holding a new plan; i.e., this is an immutable builder.
#[derive(Debug)]
pub struct LogicalPlanBuilder {
    // The current root of the logical plan in this builder.
    pub plan: Arc<LogicalPlan>,
}

impl LogicalPlanBuilder {
    pub fn new(plan: Arc<LogicalPlan>) -> Self {
        Self { plan }
    }
}

impl LogicalPlanBuilder {
    #[cfg(feature = "python")]
    pub fn in_memory_scan(
        partition_key: &str,
        cache_entry: PyObject,
        schema: Arc<Schema>,
        partition_spec: PartitionSpec,
    ) -> DaftResult<Self> {
        let source_info = SourceInfo::InMemoryInfo(InMemoryInfo::new(
            schema.clone(),
            partition_key.into(),
            cache_entry,
        ));
        let logical_plan: LogicalPlan = logical_ops::Source::new(
            schema.clone(),
            source_info.into(),
            partition_spec.clone().into(),
            None,
        )
        .into();
        Ok(logical_plan.into())
    }

    pub fn table_scan(
        file_infos: InputFileInfos,
        schema: Arc<Schema>,
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
    ) -> DaftResult<Self> {
        Self::table_scan_with_limit(file_infos, schema, file_format_config, storage_config, None)
    }

    pub fn table_scan_with_limit(
        file_infos: InputFileInfos,
        schema: Arc<Schema>,
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
        limit: Option<usize>,
    ) -> DaftResult<Self> {
        let num_partitions = file_infos.len();
        let source_info = SourceInfo::ExternalInfo(ExternalSourceInfo::new(
            schema.clone(),
            file_infos.into(),
            file_format_config,
            storage_config,
        ));
        let partition_spec =
            PartitionSpec::new_internal(PartitionScheme::Unknown, num_partitions, None);
        let logical_plan: LogicalPlan = logical_ops::Source::new(
            schema.clone(),
            source_info.into(),
            partition_spec.into(),
            limit,
        )
        .into();
        Ok(logical_plan.into())
    }

    pub fn project(
        &self,
        projection: Vec<Expr>,
        resource_request: ResourceRequest,
    ) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Project::try_new(self.plan.clone(), projection, resource_request)?.into();
        Ok(logical_plan.into())
    }

    pub fn filter(&self, predicate: Expr) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Filter::try_new(self.plan.clone(), predicate)?.into();
        Ok(logical_plan.into())
    }

    pub fn limit(&self, limit: i64) -> DaftResult<Self> {
        let logical_plan: LogicalPlan = logical_ops::Limit::new(self.plan.clone(), limit).into();
        Ok(logical_plan.into())
    }

    pub fn explode(&self, to_explode: Vec<Expr>) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Explode::try_new(self.plan.clone(), to_explode)?.into();
        Ok(logical_plan.into())
    }

    pub fn sort(&self, sort_by: Vec<Expr>, descending: Vec<bool>) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Sort::try_new(self.plan.clone(), sort_by, descending)?.into();
        Ok(logical_plan.into())
    }

    pub fn repartition(
        &self,
        num_partitions: usize,
        partition_by: Vec<Expr>,
        scheme: PartitionScheme,
    ) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Repartition::new(self.plan.clone(), num_partitions, partition_by, scheme)
                .into();
        Ok(logical_plan.into())
    }

    pub fn coalesce(&self, num_partitions: usize) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Coalesce::new(self.plan.clone(), num_partitions).into();
        Ok(logical_plan.into())
    }

    pub fn distinct(&self) -> DaftResult<Self> {
        let logical_plan: LogicalPlan = logical_ops::Distinct::new(self.plan.clone()).into();
        Ok(logical_plan.into())
    }

    pub fn aggregate(&self, agg_exprs: Vec<Expr>, groupby_exprs: Vec<Expr>) -> DaftResult<Self> {
        let agg_exprs = agg_exprs
            .iter()
            .map(|expr| match expr {
                Expr::Agg(agg_expr) => Ok(agg_expr.clone()),
                _ => Err(DaftError::ValueError(format!(
                    "Expected aggregation expression, but got: {expr}"
                ))),
            })
            .collect::<DaftResult<Vec<daft_dsl::AggExpr>>>()?;

        let logical_plan: LogicalPlan =
            logical_ops::Aggregate::try_new(self.plan.clone(), agg_exprs, groupby_exprs)?.into();
        Ok(logical_plan.into())
    }

    pub fn join(
        &self,
        right: &Self,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        join_type: JoinType,
    ) -> DaftResult<Self> {
        let logical_plan: LogicalPlan = logical_ops::Join::try_new(
            self.plan.clone(),
            right.plan.clone(),
            left_on,
            right_on,
            join_type,
        )?
        .into();
        Ok(logical_plan.into())
    }

    pub fn concat(&self, other: &Self) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Concat::try_new(self.plan.clone(), other.plan.clone())?.into();
        Ok(logical_plan.into())
    }

    pub fn table_write(
        &self,
        root_dir: &str,
        file_format: FileFormat,
        partition_cols: Option<Vec<Expr>>,
        compression: Option<String>,
    ) -> DaftResult<Self> {
        let sink_info = SinkInfo::OutputFileInfo(OutputFileInfo::new(
            root_dir.into(),
            file_format,
            partition_cols,
            compression,
        ));
        let fields = vec![Field::new("path", DataType::Utf8)];
        let logical_plan: LogicalPlan = logical_ops::Sink::new(
            self.plan.clone(),
            Schema::new(fields)?.into(),
            sink_info.into(),
        )
        .into();
        Ok(logical_plan.into())
    }

    pub fn build(&self) -> Arc<LogicalPlan> {
        self.plan.clone()
    }

    pub fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    pub fn partition_spec(&self) -> PartitionSpec {
        self.plan.partition_spec().as_ref().clone()
    }

    pub fn repr_ascii(&self, simple: bool) -> String {
        self.plan.repr_ascii(simple)
    }
}

impl From<LogicalPlan> for LogicalPlanBuilder {
    fn from(plan: LogicalPlan) -> Self {
        Self::new(plan.into())
    }
}

/// A Python-facing wrapper of the LogicalPlanBuilder.
///
/// This lightweight proxy interface should hold as much of the Python-specific logic
/// as possible, converting pyo3 wrapper type arguments into their underlying Rust-native types
/// (e.g. PySchema -> Schema).
#[cfg_attr(feature = "python", pyclass(name = "LogicalPlanBuilder"))]
#[derive(Debug)]
pub struct PyLogicalPlanBuilder {
    // Internal logical plan builder.
    builder: LogicalPlanBuilder,
}

impl PyLogicalPlanBuilder {
    pub fn new(builder: LogicalPlanBuilder) -> Self {
        Self { builder }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl PyLogicalPlanBuilder {
    #[staticmethod]
    pub fn in_memory_scan(
        partition_key: &str,
        cache_entry: &PyAny,
        schema: PySchema,
        partition_spec: PartitionSpec,
    ) -> PyResult<Self> {
        Ok(LogicalPlanBuilder::in_memory_scan(
            partition_key,
            cache_entry.to_object(cache_entry.py()),
            schema.into(),
            partition_spec,
        )?
        .into())
    }

    #[staticmethod]
    pub fn table_scan(
        file_infos: InputFileInfos,
        schema: PySchema,
        file_format_config: PyFileFormatConfig,
        storage_config: PyStorageConfig,
    ) -> PyResult<Self> {
        Ok(LogicalPlanBuilder::table_scan(
            file_infos,
            schema.into(),
            file_format_config.into(),
            storage_config.into(),
        )?
        .into())
    }

    pub fn project(
        &self,
        projection: Vec<PyExpr>,
        resource_request: ResourceRequest,
    ) -> PyResult<Self> {
        let projection_exprs = projection
            .iter()
            .map(|e| e.clone().into())
            .collect::<Vec<Expr>>();
        Ok(self
            .builder
            .project(projection_exprs, resource_request)?
            .into())
    }

    pub fn filter(&self, predicate: PyExpr) -> PyResult<Self> {
        Ok(self.builder.filter(predicate.expr)?.into())
    }

    pub fn limit(&self, limit: i64) -> PyResult<Self> {
        Ok(self.builder.limit(limit)?.into())
    }

    pub fn explode(&self, to_explode: Vec<PyExpr>) -> PyResult<Self> {
        let to_explode_exprs = to_explode
            .iter()
            .map(|e| e.clone().into())
            .collect::<Vec<Expr>>();
        Ok(self.builder.explode(to_explode_exprs)?.into())
    }

    pub fn sort(&self, sort_by: Vec<PyExpr>, descending: Vec<bool>) -> PyResult<Self> {
        let sort_by_exprs: Vec<Expr> = sort_by.iter().map(|expr| expr.clone().into()).collect();
        Ok(self.builder.sort(sort_by_exprs, descending)?.into())
    }

    pub fn repartition(
        &self,
        num_partitions: usize,
        partition_by: Vec<PyExpr>,
        scheme: PartitionScheme,
    ) -> PyResult<Self> {
        let partition_by_exprs: Vec<Expr> = partition_by
            .iter()
            .map(|expr| expr.clone().into())
            .collect();
        Ok(self
            .builder
            .repartition(num_partitions, partition_by_exprs, scheme)?
            .into())
    }

    pub fn coalesce(&self, num_partitions: usize) -> PyResult<Self> {
        Ok(self.builder.coalesce(num_partitions)?.into())
    }

    pub fn distinct(&self) -> PyResult<Self> {
        Ok(self.builder.distinct()?.into())
    }

    pub fn aggregate(&self, agg_exprs: Vec<PyExpr>, groupby_exprs: Vec<PyExpr>) -> PyResult<Self> {
        let agg_exprs = agg_exprs
            .iter()
            .map(|expr| expr.clone().into())
            .collect::<Vec<Expr>>();
        let groupby_exprs = groupby_exprs
            .iter()
            .map(|expr| expr.clone().into())
            .collect::<Vec<Expr>>();
        Ok(self.builder.aggregate(agg_exprs, groupby_exprs)?.into())
    }

    pub fn join(
        &self,
        right: &Self,
        left_on: Vec<PyExpr>,
        right_on: Vec<PyExpr>,
        join_type: JoinType,
    ) -> PyResult<Self> {
        let left_on = left_on
            .iter()
            .map(|e| e.clone().into())
            .collect::<Vec<Expr>>();
        let right_on = right_on
            .iter()
            .map(|e| e.clone().into())
            .collect::<Vec<Expr>>();
        Ok(self
            .builder
            .join(&right.builder, left_on, right_on, join_type)?
            .into())
    }

    pub fn concat(&self, other: &Self) -> DaftResult<Self> {
        Ok(self.builder.concat(&other.builder)?.into())
    }

    pub fn table_write(
        &self,
        root_dir: &str,
        file_format: FileFormat,
        partition_cols: Option<Vec<PyExpr>>,
        compression: Option<String>,
    ) -> PyResult<Self> {
        let partition_cols =
            partition_cols.map(|cols| cols.iter().map(|e| e.clone().into()).collect::<Vec<Expr>>());
        Ok(self
            .builder
            .table_write(root_dir, file_format, partition_cols, compression)?
            .into())
    }

    pub fn schema(&self) -> PyResult<PySchema> {
        Ok(self.builder.schema().into())
    }

    pub fn partition_spec(&self) -> PyResult<PartitionSpec> {
        Ok(self.builder.partition_spec())
    }

    /// Optimize the underlying logical plan, returning a new plan builder containing the optimized plan.
    pub fn optimize(&self) -> PyResult<Self> {
        let optimizer = Optimizer::new(Default::default());
        let unoptimized_plan = self.builder.build();
        let optimized_plan = optimizer.optimize(
            unoptimized_plan,
            |new_plan, rule_batch, pass, transformed, seen| {
                if transformed {
                    log::debug!(
                        "Rule batch {:?} transformed plan on pass {}, and produced {} plan:\n{}",
                        rule_batch,
                        pass,
                        if seen { "an already seen" } else { "a new" },
                        new_plan.repr_ascii(true),
                    );
                } else {
                    log::debug!(
                        "Rule batch {:?} did NOT transform plan on pass {} for plan:\n{}",
                        rule_batch,
                        pass,
                        new_plan.repr_ascii(true),
                    );
                }
            },
        )?;
        let builder = LogicalPlanBuilder::new(optimized_plan);
        Ok(builder.into())
    }

    /// Finalize the logical plan, translate the logical plan to a physical plan, and return
    /// a physical plan scheduler that's capable of launching the work necessary to compute the output
    /// of the physical plan.
    pub fn to_physical_plan_scheduler(&self) -> PyResult<PhysicalPlanScheduler> {
        let logical_plan = self.builder.build();
        let physical_plan: Arc<PhysicalPlan> = plan(logical_plan.as_ref())?.into();
        Ok(physical_plan.into())
    }

    pub fn repr_ascii(&self, simple: bool) -> PyResult<String> {
        Ok(self.builder.repr_ascii(simple))
    }
}

impl From<LogicalPlanBuilder> for PyLogicalPlanBuilder {
    fn from(plan: LogicalPlanBuilder) -> Self {
        Self::new(plan)
    }
}
