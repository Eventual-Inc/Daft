use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use common_daft_config::DaftPlanningConfig;
use common_display::mermaid::MermaidDisplayOptions;
use common_error::DaftResult;
use common_file_formats::{FileFormat, FileFormatConfig, ParquetSourceConfig};
use common_io_config::IOConfig;
use daft_core::{
    join::{JoinStrategy, JoinType},
    prelude::TimeUnit,
};
use daft_dsl::{col, ExprRef};
use daft_scan::{
    glob::GlobScanOperator,
    storage_config::{NativeStorageConfig, StorageConfig},
    PhysicalScanInfo, Pushdowns, ScanOperatorRef,
};
use daft_schema::{
    field::Field,
    schema::{Schema, SchemaRef},
};
#[cfg(feature = "python")]
use {
    crate::sink_info::{CatalogInfo, IcebergCatalogInfo},
    crate::source_info::InMemoryInfo,
    common_daft_config::PyDaftPlanningConfig,
    daft_dsl::python::PyExpr,
    daft_scan::python::pylib::ScanOperatorHandle,
    daft_schema::python::schema::PySchema,
    pyo3::prelude::*,
};

use crate::{
    logical_ops,
    logical_optimization::{Optimizer, OptimizerConfig},
    logical_plan::LogicalPlan,
    partitioning::{
        HashRepartitionConfig, IntoPartitionsConfig, RandomShuffleConfig, RepartitionSpec,
    },
    sink_info::{OutputFileInfo, SinkInfo},
    source_info::SourceInfo,
    LogicalPlanRef,
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
    config: Option<Arc<DaftPlanningConfig>>,
}

impl LogicalPlanBuilder {
    pub fn new(plan: Arc<LogicalPlan>, config: Option<Arc<DaftPlanningConfig>>) -> Self {
        Self { plan, config }
    }
}

impl From<&Self> for LogicalPlanBuilder {
    fn from(builder: &Self) -> Self {
        Self {
            plan: builder.plan.clone(),
            config: builder.config.clone(),
        }
    }
}

impl From<LogicalPlanBuilder> for LogicalPlanRef {
    fn from(value: LogicalPlanBuilder) -> Self {
        value.plan
    }
}

impl From<&LogicalPlanBuilder> for LogicalPlanRef {
    fn from(value: &LogicalPlanBuilder) -> Self {
        value.plan.clone()
    }
}
pub trait IntoGlobPath {
    fn into_glob_path(self) -> Vec<String>;
}
impl IntoGlobPath for Vec<String> {
    fn into_glob_path(self) -> Vec<String> {
        self
    }
}
impl IntoGlobPath for String {
    fn into_glob_path(self) -> Vec<String> {
        vec![self]
    }
}
impl IntoGlobPath for &str {
    fn into_glob_path(self) -> Vec<String> {
        vec![self.to_string()]
    }
}
impl IntoGlobPath for Vec<&str> {
    fn into_glob_path(self) -> Vec<String> {
        self.iter().map(|s| (*s).to_string()).collect()
    }
}
impl LogicalPlanBuilder {
    /// Replace the LogicalPlanBuilder's plan with the provided plan
    pub fn with_new_plan<LP: Into<Arc<LogicalPlan>>>(&self, plan: LP) -> Self {
        Self::new(plan.into(), self.config.clone())
    }

    /// Parametrize the LogicalPlanBuilder with a DaftPlanningConfig
    pub fn with_config(&self, config: Arc<DaftPlanningConfig>) -> Self {
        Self::new(self.plan.clone(), Some(config))
    }

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
        let logical_plan: LogicalPlan = logical_ops::Source::new(schema, source_info.into()).into();

        Ok(Self::new(logical_plan.into(), None))
    }

    #[cfg(feature = "python")]
    pub fn delta_scan<T: AsRef<str>>(
        glob_path: T,
        io_config: Option<IOConfig>,
        multithreaded_io: bool,
    ) -> DaftResult<Self> {
        use daft_scan::storage_config::PyStorageConfig;

        Python::with_gil(|py| {
            let io_config = io_config.unwrap_or_default();

            let native_storage_config = NativeStorageConfig {
                io_config: Some(io_config),
                multithreaded_io,
            };

            let py_storage_config: PyStorageConfig =
                Arc::new(StorageConfig::Native(Arc::new(native_storage_config))).into();

            // let py_io_config = PyIOConfig { config: io_config };
            let delta_lake_scan = PyModule::import_bound(py, "daft.delta_lake.delta_lake_scan")?;
            let delta_lake_scan_operator =
                delta_lake_scan.getattr(pyo3::intern!(py, "DeltaLakeScanOperator"))?;
            let delta_lake_operator = delta_lake_scan_operator
                .call1((glob_path.as_ref(), py_storage_config))?
                .to_object(py);
            let scan_operator_handle =
                ScanOperatorHandle::from_python_scan_operator(delta_lake_operator, py)?;
            Self::table_scan(scan_operator_handle.into(), None)
        })
    }

    #[cfg(not(feature = "python"))]
    pub fn delta_scan<T: IntoGlobPath>(
        glob_path: T,
        io_config: Option<IOConfig>,
        multithreaded_io: bool,
    ) -> DaftResult<Self> {
        panic!("Delta Lake scan requires the 'python' feature to be enabled.")
    }

    pub fn table_scan(
        scan_operator: ScanOperatorRef,
        pushdowns: Option<Pushdowns>,
    ) -> DaftResult<Self> {
        let schema = scan_operator.0.schema();
        let partitioning_keys = scan_operator.0.partitioning_keys();
        let source_info = SourceInfo::Physical(PhysicalScanInfo::new(
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
            schema
        };
        let logical_plan: LogicalPlan =
            logical_ops::Source::new(output_schema, source_info.into()).into();
        Ok(Self::new(logical_plan.into(), None))
    }

    pub fn parquet_scan<T: IntoGlobPath>(glob_path: T) -> ParquetScanBuilder {
        ParquetScanBuilder::new(glob_path)
    }

    pub fn select(&self, to_select: Vec<ExprRef>) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Project::try_new(self.plan.clone(), to_select)?.into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn with_columns(&self, columns: Vec<ExprRef>) -> DaftResult<Self> {
        let fields = &self.schema().fields;
        let current_col_names = fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<HashSet<_>>();
        let new_col_name_and_exprs = columns
            .iter()
            .map(|e| (e.name(), e.clone()))
            .collect::<HashMap<_, _>>();

        let mut exprs = fields
            .iter()
            .map(|(name, _)| {
                new_col_name_and_exprs
                    .get(name.as_str())
                    .cloned()
                    .unwrap_or_else(|| col(name.clone()))
            })
            .collect::<Vec<_>>();

        exprs.extend(
            columns
                .iter()
                .filter(|e| !current_col_names.contains(e.name()))
                .cloned(),
        );

        let logical_plan: LogicalPlan =
            logical_ops::Project::try_new(self.plan.clone(), exprs)?.into();
        Ok(self.with_new_plan(logical_plan))
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
            logical_ops::Project::try_new(self.plan.clone(), exprs)?.into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn filter(&self, predicate: ExprRef) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Filter::try_new(self.plan.clone(), predicate)?.into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn limit(&self, limit: i64, eager: bool) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Limit::new(self.plan.clone(), limit, eager).into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn explode(&self, to_explode: Vec<ExprRef>) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Explode::try_new(self.plan.clone(), to_explode)?.into();
        Ok(self.with_new_plan(logical_plan))
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
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn sort(&self, sort_by: Vec<ExprRef>, descending: Vec<bool>) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Sort::try_new(self.plan.clone(), sort_by, descending)?.into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn hash_repartition(
        &self,
        num_partitions: Option<usize>,
        partition_by: Vec<ExprRef>,
    ) -> DaftResult<Self> {
        let logical_plan: LogicalPlan = logical_ops::Repartition::try_new(
            self.plan.clone(),
            RepartitionSpec::Hash(HashRepartitionConfig::new(num_partitions, partition_by)),
        )?
        .into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn random_shuffle(&self, num_partitions: Option<usize>) -> DaftResult<Self> {
        let logical_plan: LogicalPlan = logical_ops::Repartition::try_new(
            self.plan.clone(),
            RepartitionSpec::Random(RandomShuffleConfig::new(num_partitions)),
        )?
        .into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn into_partitions(&self, num_partitions: usize) -> DaftResult<Self> {
        let logical_plan: LogicalPlan = logical_ops::Repartition::try_new(
            self.plan.clone(),
            RepartitionSpec::IntoPartitions(IntoPartitionsConfig::new(num_partitions)),
        )?
        .into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn distinct(&self) -> DaftResult<Self> {
        let logical_plan: LogicalPlan = logical_ops::Distinct::new(self.plan.clone()).into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn sample(
        &self,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Sample::new(self.plan.clone(), fraction, with_replacement, seed).into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn aggregate(
        &self,
        agg_exprs: Vec<ExprRef>,
        groupby_exprs: Vec<ExprRef>,
    ) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Aggregate::try_new(self.plan.clone(), agg_exprs, groupby_exprs)?.into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn pivot(
        &self,
        group_by: Vec<ExprRef>,
        pivot_column: ExprRef,
        value_column: ExprRef,
        agg_expr: ExprRef,
        names: Vec<String>,
    ) -> DaftResult<Self> {
        let pivot_logical_plan: LogicalPlan = logical_ops::Pivot::try_new(
            self.plan.clone(),
            group_by,
            pivot_column,
            value_column,
            agg_expr,
            names,
        )?
        .into();
        Ok(self.with_new_plan(pivot_logical_plan))
    }

    pub fn join<Right: Into<LogicalPlanRef>>(
        &self,
        right: Right,
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        join_type: JoinType,
        join_strategy: Option<JoinStrategy>,
    ) -> DaftResult<Self> {
        let logical_plan: LogicalPlan = logical_ops::Join::try_new(
            self.plan.clone(),
            right.into(),
            left_on,
            right_on,
            join_type,
            join_strategy,
        )?
        .into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn concat(&self, other: &Self) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::Concat::try_new(self.plan.clone(), other.plan.clone())?.into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn add_monotonically_increasing_id(&self, column_name: Option<&str>) -> DaftResult<Self> {
        let logical_plan: LogicalPlan =
            logical_ops::MonotonicallyIncreasingId::new(self.plan.clone(), column_name).into();
        Ok(self.with_new_plan(logical_plan))
    }

    pub fn table_write(
        &self,
        root_dir: &str,
        file_format: FileFormat,
        partition_cols: Option<Vec<ExprRef>>,
        compression: Option<String>,
        io_config: Option<IOConfig>,
    ) -> DaftResult<Self> {
        let sink_info = SinkInfo::OutputFileInfo(OutputFileInfo::new(
            root_dir.into(),
            file_format,
            partition_cols,
            compression,
            io_config,
        ));

        let logical_plan: LogicalPlan =
            logical_ops::Sink::try_new(self.plan.clone(), sink_info.into())?.into();
        Ok(self.with_new_plan(logical_plan))
    }

    #[cfg(feature = "python")]
    #[allow(clippy::too_many_arguments)]
    pub fn iceberg_write(
        &self,
        table_name: String,
        table_location: String,
        partition_spec: PyObject,
        iceberg_schema: PyObject,
        iceberg_properties: PyObject,
        io_config: Option<IOConfig>,
        catalog_columns: Vec<String>,
    ) -> DaftResult<Self> {
        let sink_info = SinkInfo::CatalogInfo(CatalogInfo {
            catalog: crate::sink_info::CatalogType::Iceberg(IcebergCatalogInfo {
                table_name,
                table_location,
                partition_spec,
                iceberg_schema,
                iceberg_properties,
                io_config,
            }),
            catalog_columns,
        });

        let logical_plan: LogicalPlan =
            logical_ops::Sink::try_new(self.plan.clone(), sink_info.into())?.into();
        Ok(self.with_new_plan(logical_plan))
    }

    #[cfg(feature = "python")]
    #[allow(clippy::too_many_arguments)]
    pub fn delta_write(
        &self,
        path: String,
        columns_name: Vec<String>,
        mode: String,
        version: i32,
        large_dtypes: bool,
        partition_cols: Option<Vec<String>>,
        io_config: Option<IOConfig>,
    ) -> DaftResult<Self> {
        use crate::sink_info::DeltaLakeCatalogInfo;
        let sink_info = SinkInfo::CatalogInfo(CatalogInfo {
            catalog: crate::sink_info::CatalogType::DeltaLake(DeltaLakeCatalogInfo {
                path,
                mode,
                version,
                large_dtypes,
                partition_cols,
                io_config,
            }),
            catalog_columns: columns_name,
        });

        let logical_plan: LogicalPlan =
            logical_ops::Sink::try_new(self.plan.clone(), sink_info.into())?.into();
        Ok(self.with_new_plan(logical_plan))
    }

    #[cfg(feature = "python")]
    #[allow(clippy::too_many_arguments)]
    pub fn lance_write(
        &self,
        path: String,
        columns_name: Vec<String>,
        mode: String,
        io_config: Option<IOConfig>,
        kwargs: PyObject,
    ) -> DaftResult<Self> {
        use crate::sink_info::LanceCatalogInfo;

        let sink_info = SinkInfo::CatalogInfo(CatalogInfo {
            catalog: crate::sink_info::CatalogType::Lance(LanceCatalogInfo {
                path,
                mode,
                io_config,
                kwargs,
            }),
            catalog_columns: columns_name,
        });

        let logical_plan: LogicalPlan =
            logical_ops::Sink::try_new(self.plan.clone(), sink_info.into())?.into();
        Ok(self.with_new_plan(logical_plan))
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

    pub fn repr_mermaid(&self, opts: MermaidDisplayOptions) -> String {
        use common_display::mermaid::MermaidDisplay;
        self.plan.repr_mermaid(opts)
    }
}

pub struct ParquetScanBuilder {
    pub glob_paths: Vec<String>,
    pub infer_schema: bool,
    pub coerce_int96_timestamp_unit: TimeUnit,
    pub field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    pub row_groups: Option<Vec<Option<Vec<i64>>>>,
    pub chunk_size: Option<usize>,
    pub io_config: Option<IOConfig>,
    pub multithreaded: bool,
    pub schema: Option<SchemaRef>,
}

impl ParquetScanBuilder {
    pub fn new<T: IntoGlobPath>(glob_paths: T) -> Self {
        let glob_paths = glob_paths.into_glob_path();
        Self::new_impl(glob_paths)
    }

    // concrete implementation to reduce LLVM code duplication
    fn new_impl(glob_paths: Vec<String>) -> Self {
        Self {
            glob_paths,
            infer_schema: true,
            coerce_int96_timestamp_unit: TimeUnit::Nanoseconds,
            field_id_mapping: None,
            row_groups: None,
            chunk_size: None,
            multithreaded: true,
            schema: None,
            io_config: None,
        }
    }
    pub fn infer_schema(mut self, infer_schema: bool) -> Self {
        self.infer_schema = infer_schema;
        self
    }
    pub fn coerce_int96_timestamp_unit(mut self, unit: TimeUnit) -> Self {
        self.coerce_int96_timestamp_unit = unit;
        self
    }
    pub fn field_id_mapping(mut self, field_id_mapping: Arc<BTreeMap<i32, Field>>) -> Self {
        self.field_id_mapping = Some(field_id_mapping);
        self
    }
    pub fn row_groups(mut self, row_groups: Vec<Option<Vec<i64>>>) -> Self {
        self.row_groups = Some(row_groups);
        self
    }
    pub fn chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = Some(chunk_size);
        self
    }

    pub fn io_config(mut self, io_config: IOConfig) -> Self {
        self.io_config = Some(io_config);
        self
    }

    pub fn multithreaded(mut self, multithreaded: bool) -> Self {
        self.multithreaded = multithreaded;
        self
    }
    pub fn schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn finish(self) -> DaftResult<LogicalPlanBuilder> {
        let cfg = ParquetSourceConfig {
            coerce_int96_timestamp_unit: self.coerce_int96_timestamp_unit,
            field_id_mapping: self.field_id_mapping,
            row_groups: self.row_groups,
            chunk_size: self.chunk_size,
        };

        let operator = Arc::new(GlobScanOperator::try_new(
            self.glob_paths,
            Arc::new(FileFormatConfig::Parquet(cfg)),
            Arc::new(StorageConfig::Native(Arc::new(
                NativeStorageConfig::new_internal(self.multithreaded, self.io_config),
            ))),
            self.infer_schema,
            self.schema,
        )?);

        LogicalPlanBuilder::table_scan(ScanOperatorRef(operator), None)
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
    pub builder: LogicalPlanBuilder,
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
        cache_entry: PyObject,
        schema: PySchema,
        num_partitions: usize,
        size_bytes: usize,
        num_rows: usize,
    ) -> PyResult<Self> {
        Ok(LogicalPlanBuilder::in_memory_scan(
            partition_key,
            cache_entry,
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

    pub fn with_planning_config(
        &self,
        daft_planning_config: PyDaftPlanningConfig,
    ) -> PyResult<Self> {
        Ok(self.builder.with_config(daft_planning_config.config).into())
    }

    pub fn select(&self, to_select: Vec<PyExpr>) -> PyResult<Self> {
        Ok(self.builder.select(pyexprs_to_exprs(to_select))?.into())
    }

    pub fn with_columns(&self, columns: Vec<PyExpr>) -> PyResult<Self> {
        Ok(self.builder.with_columns(pyexprs_to_exprs(columns))?.into())
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
        partition_spec: PyObject,
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
                partition_spec,
                iceberg_schema,
                iceberg_properties,
                io_config.map(|cfg| cfg.config),
                catalog_columns,
            )?
            .into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn delta_write(
        &self,
        path: String,
        columns_name: Vec<String>,
        mode: String,
        version: i32,
        large_dtypes: bool,
        partition_cols: Option<Vec<String>>,
        io_config: Option<common_io_config::python::IOConfig>,
    ) -> PyResult<Self> {
        Ok(self
            .builder
            .delta_write(
                path,
                columns_name,
                mode,
                version,
                large_dtypes,
                partition_cols,
                io_config.map(|cfg| cfg.config),
            )?
            .into())
    }

    pub fn lance_write(
        &self,
        py: Python,
        path: String,
        columns_name: Vec<String>,
        mode: String,
        io_config: Option<common_io_config::python::IOConfig>,
        kwargs: Option<PyObject>,
    ) -> PyResult<Self> {
        let kwargs = kwargs.unwrap_or_else(|| py.None());
        Ok(self
            .builder
            .lance_write(
                path,
                columns_name,
                mode,
                io_config.map(|cfg| cfg.config),
                kwargs,
            )?
            .into())
    }
    pub fn schema(&self) -> PyResult<PySchema> {
        Ok(self.builder.schema().into())
    }

    /// Optimize the underlying logical plan, returning a new plan builder containing the optimized plan.
    pub fn optimize(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            // Create optimizer
            let default_optimizer_config: OptimizerConfig = Default::default();
            let optimizer_config = OptimizerConfig { enable_actor_pool_projections: self.builder.config.as_ref().map(|planning_cfg| planning_cfg.enable_actor_pool_projections).unwrap_or(default_optimizer_config.enable_actor_pool_projections), ..default_optimizer_config };
            let optimizer = Optimizer::new(optimizer_config);

            // Run LogicalPlan optimizations
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

            let builder = LogicalPlanBuilder::new(optimized_plan, self.builder.config.clone());
            Ok(builder.into())
        })
    }

    pub fn repr_ascii(&self, simple: bool) -> PyResult<String> {
        Ok(self.builder.repr_ascii(simple))
    }

    pub fn repr_mermaid(&self, opts: MermaidDisplayOptions) -> String {
        self.builder.repr_mermaid(opts)
    }
}

impl From<LogicalPlanBuilder> for PyLogicalPlanBuilder {
    fn from(plan: LogicalPlanBuilder) -> Self {
        Self::new(plan)
    }
}
