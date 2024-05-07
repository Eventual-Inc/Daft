use std::{collections::HashSet, sync::Arc};

use crate::{
    logical_ops,
    logical_optimization::Optimizer,
    logical_plan::LogicalPlan,
    partitioning::{
        HashRepartitionConfig, IntoPartitionsConfig, RandomShuffleConfig, RepartitionSpec,
    },
    physical_planner::plan,
    sink_info::{OutputFileInfo, SinkInfo},
    source_info::SourceInfo,
    PhysicalPlanScheduler, ResourceRequest,
};
use common_error::{DaftError, DaftResult};
use common_io_config::IOConfig;
use daft_core::{
    join::{JoinStrategy, JoinType},
    schema::{Schema, SchemaRef},
};
use daft_dsl::{col, ApproxPercentileParams, Expr, ExprRef};
use daft_scan::{file_format::FileFormat, Pushdowns, ScanExternalInfo, ScanOperatorRef};

#[cfg(feature = "python")]
use {
    crate::physical_planner::python::AdaptivePhysicalPlanScheduler,
    crate::sink_info::{CatalogInfo, IcebergCatalogInfo},
    crate::{physical_plan::PhysicalPlanRef, source_info::InMemoryInfo},
    common_daft_config::PyDaftExecutionConfig,
    daft_core::python::schema::PySchema,
    daft_dsl::python::PyExpr,
    daft_scan::python::pylib::ScanOperatorHandle,
    pyo3::prelude::*,
};

/// A logical plan builder, which simplifies constructing logical plans via
/// a fluent interface. E.g., LogicalPlanBuilder::table_scan(..).project(..).filter(..).build().
///
/// This builder holds the current root (sink) of the logical plan, and the building methods return
/// a brand new builder holding a new plan; i.e., this is an immutable builder.
#[derive(Debug, Clone)]
pub struct LogicalPlanBuilder {
    // The current root of the logical plan in this builder.
    pub plan: Arc<LogicalPlan>,
}

impl LogicalPlanBuilder {
    pub fn new(plan: Arc<LogicalPlan>) -> Self {
        Self { plan }
    }
}

fn check_for_agg(expr: &ExprRef) -> bool {
    use Expr::*;

    match expr.as_ref() {
        Agg(_) => true,
        Column(_) | Literal(_) => false,
        Alias(e, _) | Cast(e, _) | Not(e) | IsNull(e) | NotNull(e) => check_for_agg(e),
        BinaryOp { left, right, .. } => check_for_agg(left) || check_for_agg(right),
        Function { inputs, .. } => inputs.iter().any(check_for_agg),
        IsIn(l, r) | FillNull(l, r) => check_for_agg(l) || check_for_agg(r),
        IfElse {
            if_true,
            if_false,
            predicate,
        } => check_for_agg(if_true) || check_for_agg(if_false) || check_for_agg(predicate),
    }
}

fn err_if_agg(fn_name: &str, exprs: &Vec<ExprRef>) -> DaftResult<()> {
    for e in exprs {
        if check_for_agg(e) {
            return Err(DaftError::ValueError(format!(
                "Aggregation expressions are not currently supported in {fn_name}: {e}\nIf you would like to have this feature, please see https://github.com/Eventual-Inc/Daft/issues/1979#issue-2170913383",
                fn_name=fn_name,
                e=e
            )));
        }
    }
    Ok(())
}

fn extract_agg_expr(expr: &Expr) -> DaftResult<daft_dsl::AggExpr> {
    use Expr::*;

    match expr {
        Agg(agg_expr) => Ok(agg_expr.clone()),
        Function { func, inputs } => Ok(daft_dsl::AggExpr::MapGroups {
            func: func.clone(),
            inputs: inputs.clone(),
        }),
        Alias(e, name) => extract_agg_expr(e).map(|agg_expr| {
            use daft_dsl::AggExpr::*;

            // reorder expressions so that alias goes before agg
            match agg_expr {
                Count(e, count_mode) => Count(Alias(e, name.clone()).into(), count_mode),
                Sum(e) => Sum(Alias(e, name.clone()).into()),
                ApproxSketch(e) => ApproxSketch(Alias(e, name.clone()).into()),
                ApproxPercentile(ApproxPercentileParams {
                    child: e,
                    percentiles,
                    force_list_output,
                }) => ApproxPercentile(ApproxPercentileParams {
                    child: Alias(e, name.clone()).into(),
                    percentiles,
                    force_list_output,
                }),
                MergeSketch(e) => MergeSketch(Alias(e, name.clone()).into()),
                Mean(e) => Mean(Alias(e, name.clone()).into()),
                Min(e) => Min(Alias(e, name.clone()).into()),
                Max(e) => Max(Alias(e, name.clone()).into()),
                AnyValue(e, ignore_nulls) => AnyValue(Alias(e, name.clone()).into(), ignore_nulls),
                List(e) => List(Alias(e, name.clone()).into()),
                Concat(e) => Concat(Alias(e, name.clone()).into()),
                MapGroups { func, inputs } => MapGroups {
                    func,
                    inputs: inputs
                        .into_iter()
                        .map(|input| input.alias(name.clone()))
                        .collect(),
                },
            }
        }),
        // TODO(Kevin): Support a mix of aggregation and non-aggregation expressions
        // as long as the final value always has a cardinality of 1.
        _ => Err(DaftError::ValueError(format!(
            "Expected aggregation expression, but got: {expr}"
        ))),
    }
}

fn extract_and_check_agg_expr(expr: &Expr) -> DaftResult<daft_dsl::AggExpr> {
    let agg_expr = extract_agg_expr(expr)?;
    let has_nested_agg = agg_expr.children().iter().any(check_for_agg);

    if has_nested_agg {
        Err(DaftError::ValueError(format!(
            "Nested aggregation expressions are not supported: {expr}\nIf you would like to have this feature, please see https://github.com/Eventual-Inc/Daft/issues/1979#issue-2170913383"
        )))
    } else {
        Ok(agg_expr)
    }
}

impl LogicalPlanBuilder {
    #[cfg(feature = "python")]
    pub fn in_memory_scan(
        partition_key: &str,
        cache_entry: PyObject,
        schema: Arc<Schema>,
        num_partitions: usize,
        size_bytes: usize,
        num_rows: usize,
    ) -> DaftResult<Self> {
        let source_info = SourceInfo::InMemory(InMemoryInfo::new(
            schema.clone(),
            partition_key.into(),
            cache_entry,
            num_partitions,
            size_bytes,
            num_rows,
            None, // TODO(sammy) thread through clustering spec to Python
        ));
        let logical_plan: LogicalPlan =
            logical_ops::Source::new(schema.clone(), source_info.into()).into();
        Ok(logical_plan.into())
    }

    pub fn table_scan(
        scan_operator: ScanOperatorRef,
        pushdowns: Option<Pushdowns>,
    ) -> DaftResult<Self> {
        let schema = scan_operator.0.schema();
        let partitioning_keys = scan_operator.0.partitioning_keys();
        let source_info = SourceInfo::External(ScanExternalInfo::new(
            scan_operator.clone(),
            schema.clone(),
            partitioning_keys.into(),
            pushdowns.clone().unwrap_or_default(),
        ));
        // If column selection (projection) pushdown is specified, prune unselected columns from the schema.
        let output_schema = if let Some(Pushdowns {
            columns: Some(columns),
            ..
        }) = &pushdowns
            && columns.len() < schema.fields.len()
        {
            let pruned_upstream_schema = schema
                .fields
                .iter()
                .filter(|&(name, _)| columns.contains(name))
                .map(|(_, field)| field.clone())
                .collect::<Vec<_>>();
            Arc::new(Schema::new(pruned_upstream_schema)?)
        } else {
            schema.clone()
        };
        let logical_plan: LogicalPlan =
            logical_ops::Source::new(output_schema, source_info.into()).into();
        Ok(logical_plan.into())
    }

    pub fn select(&self, to_select: Vec<ExprRef>) -> DaftResult<Self> {
        err_if_agg("project", &to_select)?;

        let logical_plan: LogicalPlan =
            logical_ops::Project::try_new(self.plan.clone(), to_select, Default::default())?.into();
        Ok(logical_plan.into())
    }

    pub fn with_columns(
        &self,
        columns: Vec<ExprRef>,
        resource_request: ResourceRequest,
    ) -> DaftResult<Self> {
        err_if_agg("with_columns", &columns)?;

        let new_col_names = columns.iter().map(|e| e.name()).collect::<HashSet<&str>>();

        let mut exprs = self
            .schema()
            .fields
            .iter()
            .filter_map(|(name, _)| {
                if new_col_names.contains(name.as_str()) {
                    None
                } else {
                    Some(col(name.clone()))
                }
            })
            .collect::<Vec<_>>();

        exprs.extend(columns);

        let logical_plan: LogicalPlan =
            logical_ops::Project::try_new(self.plan.clone(), exprs, resource_request)?.into();
        Ok(logical_plan.into())
    }

    pub fn exclude(&self, to_exclude: Vec<String>) -> DaftResult<Self> {
        let to_exclude = HashSet::<_>::from_iter(to_exclude.iter());

        let exprs = self
            .schema()
            .fields
            .iter()
            .filter_map(|(name, _)| {
                if to_exclude.contains(name) {
                    None
                } else {
                    Some(col(name.clone()))
                }
            })
            .collect::<Vec<_>>();

        let logical_plan: LogicalPlan =
            logical_ops::Project::try_new(self.plan.clone(), exprs, Default::default())?.into();
        Ok(logical_plan.into())
    }

    pub fn filter(&self, predicate: ExprRef) -> DaftResult<Self> {
        err_if_agg("filter", &vec![predicate.to_owned()])?;

        let logical_plan: LogicalPlan =
            logical_ops::Filter::try_new(self.plan.clone(), predicate)?.into();
        Ok(logical_plan.into())
    }

    pub fn limit(&self, limit: i64, eager: bool) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Limit::new(self.plan.clone(), limit, eager).into();
        Ok(logical_plan.into())
    }

    pub fn explode(&self, to_explode: Vec<ExprRef>) -> DaftResult<Self> {
        err_if_agg("explode", &to_explode)?;

        let logical_plan: LogicalPlan =
            logical_ops::Explode::try_new(self.plan.clone(), to_explode)?.into();
        Ok(logical_plan.into())
    }

    pub fn unpivot(
        &self,
        ids: Vec<ExprRef>,
        values: Vec<ExprRef>,
        variable_name: &str,
        value_name: &str,
    ) -> DaftResult<Self> {
        let values = if values.is_empty() {
            let ids_set = HashSet::<_>::from_iter(ids.iter());

            self.schema()
                .fields
                .iter()
                .filter_map(|(name, _)| {
                    let column = col(name.clone());

                    if ids_set.contains(&column) {
                        None
                    } else {
                        Some(column)
                    }
                })
                .collect()
        } else {
            values
        };

        let logical_plan: LogicalPlan = logical_ops::Unpivot::try_new(
            self.plan.clone(),
            ids,
            values,
            variable_name,
            value_name,
        )?
        .into();
        Ok(logical_plan.into())
    }

    pub fn sort(&self, sort_by: Vec<ExprRef>, descending: Vec<bool>) -> DaftResult<Self> {
        err_if_agg("sort", &sort_by)?;

        let logical_plan: LogicalPlan =
            logical_ops::Sort::try_new(self.plan.clone(), sort_by, descending)?.into();
        Ok(logical_plan.into())
    }

    pub fn hash_repartition(
        &self,
        num_partitions: Option<usize>,
        partition_by: Vec<ExprRef>,
    ) -> DaftResult<Self> {
        err_if_agg("hash_repartition", &partition_by)?;

        let logical_plan: LogicalPlan = logical_ops::Repartition::try_new(
            self.plan.clone(),
            RepartitionSpec::Hash(HashRepartitionConfig::new(num_partitions, partition_by)),
        )?
        .into();
        Ok(logical_plan.into())
    }

    pub fn random_shuffle(&self, num_partitions: Option<usize>) -> DaftResult<Self> {
        let logical_plan: LogicalPlan = logical_ops::Repartition::try_new(
            self.plan.clone(),
            RepartitionSpec::Random(RandomShuffleConfig::new(num_partitions)),
        )?
        .into();
        Ok(logical_plan.into())
    }

    pub fn into_partitions(&self, num_partitions: usize) -> DaftResult<Self> {
        let logical_plan: LogicalPlan = logical_ops::Repartition::try_new(
            self.plan.clone(),
            RepartitionSpec::IntoPartitions(IntoPartitionsConfig::new(num_partitions)),
        )?
        .into();
        Ok(logical_plan.into())
    }

    pub fn distinct(&self) -> DaftResult<Self> {
        let logical_plan: LogicalPlan = logical_ops::Distinct::new(self.plan.clone()).into();
        Ok(logical_plan.into())
    }

    pub fn sample(
        &self,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Sample::new(self.plan.clone(), fraction, with_replacement, seed).into();
        Ok(logical_plan.into())
    }

    pub fn aggregate(
        &self,
        agg_exprs: Vec<ExprRef>,
        groupby_exprs: Vec<ExprRef>,
    ) -> DaftResult<Self> {
        let agg_exprs = agg_exprs
            .iter()
            .map(|v| v.as_ref())
            .map(extract_and_check_agg_expr)
            .collect::<DaftResult<Vec<daft_dsl::AggExpr>>>()?;

        let logical_plan: LogicalPlan =
            logical_ops::Aggregate::try_new(self.plan.clone(), agg_exprs, groupby_exprs)?.into();
        Ok(logical_plan.into())
    }

    pub fn pivot(
        &self,
        group_by: Vec<ExprRef>,
        pivot_column: ExprRef,
        value_column: ExprRef,
        agg_expr: ExprRef,
        names: Vec<String>,
    ) -> DaftResult<Self> {
        let agg_expr = extract_and_check_agg_expr(agg_expr.as_ref())?;
        let pivot_logical_plan: LogicalPlan = logical_ops::Pivot::try_new(
            self.plan.clone(),
            group_by,
            pivot_column,
            value_column,
            agg_expr,
            names,
        )?
        .into();
        Ok(pivot_logical_plan.into())
    }

    pub fn join(
        &self,
        right: &Self,
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        join_type: JoinType,
        join_strategy: Option<JoinStrategy>,
    ) -> DaftResult<Self> {
        err_if_agg("join", &left_on)?;
        err_if_agg("join", &right_on)?;

        let logical_plan: LogicalPlan = logical_ops::Join::try_new(
            self.plan.clone(),
            right.plan.clone(),
            left_on,
            right_on,
            join_type,
            join_strategy,
        )?
        .into();
        Ok(logical_plan.into())
    }

    pub fn concat(&self, other: &Self) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Concat::try_new(self.plan.clone(), other.plan.clone())?.into();
        Ok(logical_plan.into())
    }

    pub fn add_monotonically_increasing_id(&self, column_name: Option<&str>) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::MonotonicallyIncreasingId::new(self.plan.clone(), column_name).into();
        Ok(logical_plan.into())
    }

    pub fn table_write(
        &self,
        root_dir: &str,
        file_format: FileFormat,
        partition_cols: Option<Vec<ExprRef>>,
        compression: Option<String>,
        io_config: Option<IOConfig>,
    ) -> DaftResult<Self> {
        if let Some(partition_cols) = &partition_cols {
            err_if_agg("table_write", partition_cols)?;
        }

        let sink_info = SinkInfo::OutputFileInfo(OutputFileInfo::new(
            root_dir.into(),
            file_format,
            partition_cols,
            compression,
            io_config,
        ));

        let logical_plan: LogicalPlan =
            logical_ops::Sink::try_new(self.plan.clone(), sink_info.into())?.into();
        Ok(logical_plan.into())
    }

    #[cfg(feature = "python")]
    #[allow(clippy::too_many_arguments)]
    pub fn iceberg_write(
        &self,
        table_name: String,
        table_location: String,
        spec_id: i64,
        iceberg_schema: PyObject,
        iceberg_properties: PyObject,
        io_config: Option<IOConfig>,
        catalog_columns: Vec<String>,
    ) -> DaftResult<Self> {
        let sink_info = SinkInfo::CatalogInfo(CatalogInfo {
            catalog: crate::sink_info::CatalogType::Iceberg(IcebergCatalogInfo {
                table_name,
                table_location,
                spec_id,
                iceberg_schema,
                iceberg_properties,
                io_config,
            }),
            catalog_columns,
        });

        let logical_plan: LogicalPlan =
            logical_ops::Sink::try_new(self.plan.clone(), sink_info.into())?.into();
        Ok(logical_plan.into())
    }

    pub fn build(&self) -> Arc<LogicalPlan> {
        self.plan.clone()
    }

    pub fn schema(&self) -> SchemaRef {
        self.plan.schema()
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
fn pyexprs_to_exprs(vec: Vec<PyExpr>) -> Vec<ExprRef> {
    vec.into_iter().map(|e| e.into()).collect()
}

#[cfg(feature = "python")]
#[pymethods]
impl PyLogicalPlanBuilder {
    #[staticmethod]
    pub fn in_memory_scan(
        partition_key: &str,
        cache_entry: &PyAny,
        schema: PySchema,
        num_partitions: usize,
        size_bytes: usize,
        num_rows: usize,
    ) -> PyResult<Self> {
        Ok(LogicalPlanBuilder::in_memory_scan(
            partition_key,
            cache_entry.to_object(cache_entry.py()),
            schema.into(),
            num_partitions,
            size_bytes,
            num_rows,
        )?
        .into())
    }

    #[staticmethod]
    pub fn table_scan(scan_operator: ScanOperatorHandle) -> PyResult<Self> {
        Ok(LogicalPlanBuilder::table_scan(scan_operator.into(), None)?.into())
    }

    pub fn select(&self, to_select: Vec<PyExpr>) -> PyResult<Self> {
        Ok(self.builder.select(pyexprs_to_exprs(to_select))?.into())
    }

    pub fn with_columns(
        &self,
        columns: Vec<PyExpr>,
        resource_request: ResourceRequest,
    ) -> PyResult<Self> {
        Ok(self
            .builder
            .with_columns(pyexprs_to_exprs(columns), resource_request)?
            .into())
    }

    pub fn exclude(&self, to_exclude: Vec<String>) -> PyResult<Self> {
        Ok(self.builder.exclude(to_exclude)?.into())
    }

    pub fn filter(&self, predicate: PyExpr) -> PyResult<Self> {
        Ok(self.builder.filter(predicate.expr)?.into())
    }

    pub fn limit(&self, limit: i64, eager: bool) -> PyResult<Self> {
        Ok(self.builder.limit(limit, eager)?.into())
    }

    pub fn explode(&self, to_explode: Vec<PyExpr>) -> PyResult<Self> {
        Ok(self.builder.explode(pyexprs_to_exprs(to_explode))?.into())
    }

    pub fn unpivot(
        &self,
        ids: Vec<PyExpr>,
        values: Vec<PyExpr>,
        variable_name: &str,
        value_name: &str,
    ) -> PyResult<Self> {
        let ids_exprs = ids
            .iter()
            .map(|e| e.clone().into())
            .collect::<Vec<ExprRef>>();
        let values_exprs = values
            .iter()
            .map(|e| e.clone().into())
            .collect::<Vec<ExprRef>>();
        Ok(self
            .builder
            .unpivot(ids_exprs, values_exprs, variable_name, value_name)?
            .into())
    }

    pub fn sort(&self, sort_by: Vec<PyExpr>, descending: Vec<bool>) -> PyResult<Self> {
        Ok(self
            .builder
            .sort(pyexprs_to_exprs(sort_by), descending)?
            .into())
    }

    pub fn hash_repartition(
        &self,
        partition_by: Vec<PyExpr>,
        num_partitions: Option<usize>,
    ) -> PyResult<Self> {
        Ok(self
            .builder
            .hash_repartition(num_partitions, pyexprs_to_exprs(partition_by))?
            .into())
    }

    pub fn random_shuffle(&self, num_partitions: Option<usize>) -> PyResult<Self> {
        Ok(self.builder.random_shuffle(num_partitions)?.into())
    }

    pub fn into_partitions(&self, num_partitions: usize) -> PyResult<Self> {
        Ok(self.builder.into_partitions(num_partitions)?.into())
    }

    pub fn distinct(&self) -> PyResult<Self> {
        Ok(self.builder.distinct()?.into())
    }

    pub fn sample(
        &self,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> PyResult<Self> {
        Ok(self
            .builder
            .sample(fraction, with_replacement, seed)?
            .into())
    }

    pub fn aggregate(&self, agg_exprs: Vec<PyExpr>, groupby_exprs: Vec<PyExpr>) -> PyResult<Self> {
        Ok(self
            .builder
            .aggregate(pyexprs_to_exprs(agg_exprs), pyexprs_to_exprs(groupby_exprs))?
            .into())
    }

    pub fn pivot(
        &self,
        group_by: Vec<PyExpr>,
        pivot_column: PyExpr,
        value_column: PyExpr,
        agg_expr: PyExpr,
        names: Vec<String>,
    ) -> PyResult<Self> {
        Ok(self
            .builder
            .pivot(
                pyexprs_to_exprs(group_by),
                pivot_column.into(),
                value_column.into(),
                agg_expr.into(),
                names,
            )?
            .into())
    }

    pub fn join(
        &self,
        right: &Self,
        left_on: Vec<PyExpr>,
        right_on: Vec<PyExpr>,
        join_type: JoinType,
        join_strategy: Option<JoinStrategy>,
    ) -> PyResult<Self> {
        Ok(self
            .builder
            .join(
                &right.builder,
                pyexprs_to_exprs(left_on),
                pyexprs_to_exprs(right_on),
                join_type,
                join_strategy,
            )?
            .into())
    }

    pub fn concat(&self, other: &Self) -> DaftResult<Self> {
        Ok(self.builder.concat(&other.builder)?.into())
    }

    pub fn add_monotonically_increasing_id(&self, column_name: Option<&str>) -> PyResult<Self> {
        Ok(self
            .builder
            .add_monotonically_increasing_id(column_name)?
            .into())
    }

    pub fn table_write(
        &self,
        root_dir: &str,
        file_format: FileFormat,
        partition_cols: Option<Vec<PyExpr>>,
        compression: Option<String>,
        io_config: Option<common_io_config::python::IOConfig>,
    ) -> PyResult<Self> {
        Ok(self
            .builder
            .table_write(
                root_dir,
                file_format,
                partition_cols.map(pyexprs_to_exprs),
                compression,
                io_config.map(|cfg| cfg.config),
            )?
            .into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn iceberg_write(
        &self,
        table_name: String,
        table_location: String,
        spec_id: i64,
        iceberg_schema: PyObject,
        iceberg_properties: PyObject,
        catalog_columns: Vec<String>,
        io_config: Option<common_io_config::python::IOConfig>,
    ) -> PyResult<Self> {
        Ok(self
            .builder
            .iceberg_write(
                table_name,
                table_location,
                spec_id,
                iceberg_schema,
                iceberg_properties,
                io_config.map(|cfg| cfg.config),
                catalog_columns,
            )?
            .into())
    }

    pub fn schema(&self) -> PyResult<PySchema> {
        Ok(self.builder.schema().into())
    }

    /// Optimize the underlying logical plan, returning a new plan builder containing the optimized plan.
    pub fn optimize(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
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
        })
    }

    /// Finalize the logical plan, translate the logical plan to a physical plan, and return
    /// a physical plan scheduler that's capable of launching the work necessary to compute the output
    /// of the physical plan.
    pub fn to_physical_plan_scheduler(
        &self,
        py: Python,
        cfg: PyDaftExecutionConfig,
    ) -> PyResult<PhysicalPlanScheduler> {
        py.allow_threads(|| {
            let logical_plan = self.builder.build();
            let physical_plan: PhysicalPlanRef = plan(logical_plan, cfg.config.clone())?;
            Ok(physical_plan.into())
        })
    }

    pub fn repr_ascii(&self, simple: bool) -> PyResult<String> {
        Ok(self.builder.repr_ascii(simple))
    }

    pub fn to_adaptive_physical_plan_scheduler(
        &self,
        py: Python,
        cfg: PyDaftExecutionConfig,
    ) -> PyResult<AdaptivePhysicalPlanScheduler> {
        py.allow_threads(|| {
            let logical_plan = self.builder.build();
            Ok(AdaptivePhysicalPlanScheduler::new(
                logical_plan,
                cfg.config.clone(),
            ))
        })
    }
}

impl From<LogicalPlanBuilder> for PyLogicalPlanBuilder {
    fn from(plan: LogicalPlanBuilder) -> Self {
        Self::new(plan)
    }
}
