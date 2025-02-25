//! Translation between Spark Connect and Daft

mod datatype;
mod literal;

use std::{collections::HashMap, io::Cursor, rc::Rc, sync::Arc};

use arrow2::io::ipc::read::{read_stream_metadata, StreamReader, StreamState};
use daft_core::series::Series;
use daft_dsl::unresolved_col;
use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
use daft_micropartition::{self, python::PyMicroPartition, MicroPartition};
use daft_recordbatch::RecordBatch;
use daft_scan::builder::{delta_scan, CsvScanBuilder, JsonScanBuilder, ParquetScanBuilder};
use daft_schema::schema::{Schema, SchemaRef};
use daft_sql::SQLPlanner;
use datatype::to_daft_datatype;
pub use datatype::to_spark_datatype;
use itertools::zip_eq;
use literal::to_daft_literal;
use pyo3::{intern, prelude::*};
use spark_connect::{
    aggregate::GroupType,
    data_type::StructField,
    expression::{
        self as spark_expr,
        cast::{CastToType, EvalMode},
        sort_order::{NullOrdering, SortDirection},
        ExprType, SortOrder, UnresolvedFunction,
    },
    read::ReadType,
    relation::RelType,
    set_operation::SetOpType,
    Deduplicate, Expression, Limit, Range, Relation, SetOperation, Sort, Sql,
};
use tracing::debug;

use crate::{
    ensure,
    error::{ConnectError, ConnectResult, Context},
    functions::CONNECT_FUNCTIONS,
    internal_err, invalid_argument_err, not_yet_implemented,
    session::ConnectSession,
    util::FromOptionalField,
};

#[derive(Clone)]
pub struct SparkAnalyzer<'a> {
    pub session: &'a ConnectSession,
}

impl SparkAnalyzer<'_> {
    pub fn new(session: &ConnectSession) -> SparkAnalyzer<'_> {
        SparkAnalyzer { session }
    }

    /// Creates a logical source (scan) operator from a vec of tables.
    ///
    /// Consider moving into LogicalBuilder, but this would re-introduce the daft-recordbatch dependency.
    ///
    /// TODOs
    ///   * https://github.com/Eventual-Inc/Daft/pull/3250
    ///   * https://github.com/Eventual-Inc/Daft/issues/3718
    ///
    pub fn create_in_memory_scan(
        &self,
        _plan_id: usize,
        schema: Arc<Schema>,
        tables: Vec<RecordBatch>,
    ) -> ConnectResult<LogicalPlanBuilder> {
        let mp = MicroPartition::new_loaded(schema, Arc::new(tables), None);
        Python::with_gil(|py| {
            // Convert MicroPartition to a logical plan using Python interop.
            let py_micropartition = py
                .import(intern!(py, "daft.recordbatch"))?
                .getattr(intern!(py, "MicroPartition"))?
                .getattr(intern!(py, "_from_pymicropartition"))?
                .call1((PyMicroPartition::from(mp),))?;

            let py_plan_builder = py
                .import(intern!(py, "daft.dataframe.dataframe"))?
                .getattr(intern!(py, "to_logical_plan_builder"))?
                .call1((py_micropartition,))?;
            let py_plan_builder = py_plan_builder.getattr(intern!(py, "_builder"))?;
            let plan: PyLogicalPlanBuilder = py_plan_builder.extract()?;

            Ok::<_, ConnectError>(plan.builder)
        })
    }

    pub async fn to_logical_plan(&self, relation: Relation) -> ConnectResult<LogicalPlanBuilder> {
        let common = relation.common.required("common")?;

        if common.origin.is_some() {
            debug!("Ignoring common metadata for relation: {common:?}; not yet implemented");
        }

        let rel_type = relation.rel_type.required("rel_type")?;

        match rel_type {
            RelType::Limit(l) => self.limit(*l).await,
            RelType::Range(r) => self.range(r),
            RelType::Project(p) => self.project(*p).await,
            RelType::Aggregate(a) => self.aggregate(*a).await,
            RelType::WithColumns(w) => self.with_columns(*w).await,
            RelType::ToDf(t) => self.to_df(*t).await,
            RelType::LocalRelation(l) => {
                let plan_id = common.plan_id.required("plan_id")?;
                self.local_relation(plan_id, l)
            }
            RelType::WithColumnsRenamed(w) => self.with_columns_renamed(*w).await,
            RelType::Read(r) => self.read(r).await,
            RelType::Drop(d) => self.drop(*d).await,
            RelType::Filter(f) => self.filter(*f).await,
            RelType::ShowString(_) => unreachable!("should already be handled in execute"),
            RelType::Deduplicate(rel) => self.deduplicate(*rel).await,
            RelType::Sort(rel) => self.sort(*rel).await,
            RelType::Sql(sql) => self.sql(sql).await,
            RelType::SetOp(set_op) => self.set_op(*set_op).await,
            plan => not_yet_implemented!(r#"relation type: "{}""#, rel_name(&plan)),
        }
    }

    async fn limit(&self, limit: Limit) -> ConnectResult<LogicalPlanBuilder> {
        let Limit { input, limit } = limit;
        let input = input.required("input")?;

        let plan = Box::pin(self.to_logical_plan(*input)).await?;

        plan.limit(i64::from(limit), false).map_err(Into::into)
    }

    async fn deduplicate(&self, deduplicate: Deduplicate) -> ConnectResult<LogicalPlanBuilder> {
        let Deduplicate {
            input,
            column_names,
            ..
        } = deduplicate;

        if !column_names.is_empty() {
            not_yet_implemented!("Deduplicate with column names");
        }

        let input = input.required("input")?;

        let plan = Box::pin(self.to_logical_plan(*input)).await?;

        plan.distinct().map_err(Into::into)
    }

    async fn sort(&self, sort: Sort) -> ConnectResult<LogicalPlanBuilder> {
        let Sort {
            input,
            order,
            is_global,
        } = sort;

        let input = input.required("input")?;

        if is_global == Some(false) {
            not_yet_implemented!("Non Global sort");
        }

        let plan = Box::pin(self.to_logical_plan(*input)).await?;
        if order.is_empty() {
            return plan
                .sort(vec![unresolved_col("*")], vec![false], vec![false])
                .map_err(Into::into);
        }
        let mut sort_by = Vec::with_capacity(order.len());
        let mut descending = Vec::with_capacity(order.len());
        let mut nulls_first = Vec::with_capacity(order.len());

        for SortOrder {
            child,
            direction,
            null_ordering,
        } in order
        {
            let expr = child.required("child")?;
            let expr = self.to_daft_expr(&expr)?;

            let sort_direction = SortDirection::try_from(direction).map_err(|e| {
                ConnectError::invalid_relation(format!("Unknown sort direction: {e}"))
            })?;

            let desc = match sort_direction {
                SortDirection::Ascending => false,
                SortDirection::Descending | SortDirection::Unspecified => true,
            };

            let null_ordering = NullOrdering::try_from(null_ordering).map_err(|e| {
                ConnectError::invalid_relation(format!("Unknown null ordering: {e}"))
            })?;

            let nf = match null_ordering {
                NullOrdering::SortNullsUnspecified => desc,
                NullOrdering::SortNullsFirst => true,
                NullOrdering::SortNullsLast => false,
            };

            sort_by.push(expr);
            descending.push(desc);
            nulls_first.push(nf);
        }

        plan.sort(sort_by, descending, nulls_first)
            .map_err(Into::into)
    }

    fn range(&self, range: Range) -> ConnectResult<LogicalPlanBuilder> {
        use daft_scan::python::pylib::ScanOperatorHandle;
        let Range {
            start,
            end,
            step,
            num_partitions,
        } = range;

        let partitions = num_partitions.unwrap_or(1);

        ensure!(partitions > 0, "num_partitions must be greater than 0");

        let start = start.unwrap_or(0);

        let step = usize::try_from(step).wrap_err("step must be a positive integer")?;
        ensure!(step > 0, "step must be greater than 0");

        let plan = Python::with_gil(|py| {
            let range_module =
                PyModule::import(py, "daft.io._range").wrap_err("Failed to import range module")?;

            let range = range_module
                .getattr(pyo3::intern!(py, "RangeScanOperator"))
                .wrap_err("Failed to get range function")?;

            let range = range
                .call1((start, end, step, partitions))
                .wrap_err("Failed to create range scan operator")?
                .into_pyobject(py)
                .unwrap()
                .unbind();

            let scan_operator_handle = ScanOperatorHandle::from_python_scan_operator(range, py)?;

            let plan = LogicalPlanBuilder::table_scan(scan_operator_handle.into(), None)?;

            ConnectResult::<_>::Ok(plan)
        })
        .wrap_err("Failed to create range scan")?;

        Ok(plan)
    }

    async fn read(&self, read: spark_connect::Read) -> ConnectResult<LogicalPlanBuilder> {
        let spark_connect::Read {
            is_streaming,
            read_type,
        } = read;

        if is_streaming {
            not_yet_implemented!("Streaming read");
        }

        let read_type = read_type.required("read_type")?;

        match read_type {
            ReadType::NamedTable(_) => not_yet_implemented!("NamedTable"),
            ReadType::DataSource(source) => self.read_datasource(source).await,
        }
    }

    async fn read_datasource(
        &self,
        data_source: spark_connect::read::DataSource,
    ) -> ConnectResult<LogicalPlanBuilder> {
        let spark_connect::read::DataSource {
            format,
            schema,
            mut options,
            paths,
            predicates: _,
        } = data_source;
        let format = format.required("format")?;

        ensure!(!paths.is_empty(), "Paths are required");

        if let Some(schema) = schema {
            debug!("Ignoring schema: {schema:?}; not yet implemented");
        }

        if !options.is_empty() {
            debug!("Ignoring options: {options:?}; not yet implemented");
        }

        // TODO: allow user to set handling for unused options. either ignore or error.
        fn check_unused_options(
            format: &str,
            options: &HashMap<String, String>,
        ) -> ConnectResult<()> {
            if options.is_empty() {
                Ok(())
            } else {
                let unimplemented_options = options.keys().cloned().collect::<Vec<_>>();
                Err(ConnectError::not_yet_implemented(format!(
                    "[{unimplemented_options}] options for {format}",
                    unimplemented_options = unimplemented_options.join(", ")
                )))
            }
        }

        let format = &*format;
        Ok(match format {
            "parquet" => {
                let chunk_size = options
                    .remove("chunk_size")
                    .map(|v| v.parse())
                    .transpose()
                    .wrap_err("invalid chunk_size option")?;

                let hive_partitioning = options
                    .remove("hive_partitioning")
                    .map(|v| v.parse())
                    .transpose()
                    .wrap_err("invalid hive_partitioning option")?;

                let mut builder = ParquetScanBuilder::new(paths);
                builder.chunk_size = chunk_size;

                if let Some(hive_partitioning) = hive_partitioning {
                    builder.hive_partitioning = hive_partitioning;
                }
                check_unused_options(format, &options)?;

                builder.finish().await?
            }
            "csv" => {
                // reference for csv options:
                // https://spark.apache.org/docs/latest/sql-data-sources-csv.html
                let mut builder = CsvScanBuilder::new(paths);

                if let Some(sep) = options.remove("sep").and_then(|s| s.chars().next()) {
                    builder = builder.delimiter(sep);
                }

                // spark sets this to false by default, so we'll do the same
                let header = options
                    .remove("header")
                    .map(|v| v.to_lowercase().parse())
                    .transpose()
                    .wrap_err("invalid header value")?
                    .unwrap_or(false);

                builder = builder.has_headers(header);

                let infer_schema = options
                    .remove("inferSchema")
                    .map(|v| v.to_lowercase().parse())
                    .transpose()
                    .wrap_err("invalid inferSchema value")?
                    .unwrap_or(true);

                builder = builder.infer_schema(infer_schema);

                if let Some(quote) = options.remove("quote").and_then(|s| s.chars().next()) {
                    builder = builder.quote(quote);
                }

                if let Some(comment) = options.remove("comment").and_then(|s| s.chars().next()) {
                    builder = builder.comment(comment);
                }

                if let Some(escape_char) = options.remove("escape").and_then(|s| s.chars().next()) {
                    builder = builder.escape_char(escape_char);
                }

                if let Some(hive_partitioning) = options
                    .remove("hive_partitioning")
                    .map(|v| v.parse())
                    .transpose()
                    .wrap_err("invalid hive_partitioning option")?
                {
                    builder = builder.hive_partitioning(hive_partitioning);
                }
                if let Some(chunk_size) = options
                    .remove("chunk_size")
                    .map(|v| v.parse())
                    .transpose()
                    .wrap_err("invalid chunk_size option")?
                {
                    builder = builder.chunk_size(chunk_size);
                }

                if let Some(buffer_size) = options
                    .remove("buffer_size")
                    .map(|v| v.parse())
                    .transpose()
                    .wrap_err("invalid buffer_size option")?
                {
                    builder = builder.buffer_size(buffer_size);
                }

                check_unused_options(format, &options)?;

                builder.finish().await?
            }
            "json" => {
                check_unused_options(format, &options)?;
                JsonScanBuilder::new(paths).finish().await?
            }
            "delta" => {
                if paths.len() != 1 {
                    invalid_argument_err!(
                        "Delta format only supports a single path; got {paths:?}"
                    );
                }
                let path = paths.first().unwrap();
                check_unused_options(format, &options)?;

                delta_scan(path, None, true)?
            }

            other => invalid_argument_err!("Unsupported format: {other};"),
        })
    }

    async fn aggregate(
        &self,
        aggregate: spark_connect::Aggregate,
    ) -> ConnectResult<LogicalPlanBuilder> {
        fn check_grouptype(group_type: GroupType) -> ConnectResult<()> {
            match group_type {
                GroupType::Groupby => {}
                GroupType::Unspecified => {
                    invalid_argument_err!("GroupType must be specified; got Unspecified");
                }
                GroupType::Rollup => {
                    not_yet_implemented!("GroupType.Rollup not yet supported");
                }
                GroupType::Cube => {
                    not_yet_implemented!("GroupType.Cube");
                }
                GroupType::Pivot => {
                    not_yet_implemented!("GroupType.Pivot");
                }
                GroupType::GroupingSets => {
                    not_yet_implemented!("GroupType.GroupingSets");
                }
            };
            Ok(())
        }

        let spark_connect::Aggregate {
            input,
            group_type,
            grouping_expressions,
            aggregate_expressions,
            pivot,
            grouping_sets,
        } = aggregate;

        let input = input.required("input")?;

        let mut plan = Box::pin(self.to_logical_plan(*input)).await?;

        let group_type = GroupType::try_from(group_type).wrap_err("Invalid group type")?;

        check_grouptype(group_type)?;

        if let Some(pivot) = pivot {
            not_yet_implemented!("Pivot not yet supported; got {pivot:?}");
        }

        if !grouping_sets.is_empty() {
            not_yet_implemented!("Grouping sets not yet supported; got {grouping_sets:?}");
        }

        let grouping_expressions: Vec<_> = grouping_expressions
            .iter()
            .map(|e| self.to_daft_expr(e))
            .try_collect()?;

        let aggregate_expressions: Vec<_> = aggregate_expressions
            .iter()
            .map(|e| self.to_daft_expr(e))
            .try_collect()?;

        plan = plan.aggregate(aggregate_expressions, grouping_expressions)?;

        Ok(plan)
    }

    async fn drop(&self, drop: spark_connect::Drop) -> ConnectResult<LogicalPlanBuilder> {
        let spark_connect::Drop {
            input,
            columns,
            column_names,
        } = drop;

        let input = input.required("input")?;

        if !columns.is_empty() {
            not_yet_implemented!("columns is not supported; use column_names instead");
        }

        let plan = Box::pin(self.to_logical_plan(*input)).await?;

        let to_select = plan
            .schema()
            .exclude(&column_names)?
            .names()
            .into_iter()
            .map(unresolved_col)
            .collect();

        // Use select to keep only the columns we want
        Ok(plan.select(to_select)?)
    }

    pub async fn filter(&self, filter: spark_connect::Filter) -> ConnectResult<LogicalPlanBuilder> {
        let spark_connect::Filter { input, condition } = filter;

        let input = input.required("input")?;
        let condition = condition.required("condition")?;
        let condition = self.to_daft_expr(&condition)?;

        let plan = Box::pin(self.to_logical_plan(*input)).await?;
        Ok(plan.filter(condition)?)
    }

    pub fn local_relation(
        &self,
        plan_id: i64,
        plan: spark_connect::LocalRelation,
    ) -> ConnectResult<LogicalPlanBuilder> {
        // We can ignore spark schema. The true schema is sent in the
        // arrow data. (see read_stream_metadata)
        // the schema inside the plan is actually wrong. See https://issues.apache.org/jira/browse/SPARK-50627
        let spark_connect::LocalRelation { data, schema: _ } = plan;

        let data = data.required("data")?;

        let mut reader = Cursor::new(&data);
        let metadata =
            read_stream_metadata(&mut reader).wrap_err("Failed to read stream metadata")?;

        let arrow_schema = metadata.schema.clone();
        let daft_schema = Arc::new(
            Schema::try_from(&arrow_schema)
                .wrap_err("Failed to convert Arrow schema to Daft schema.")?,
        );

        let reader = StreamReader::new(reader, metadata, None);

        let tables = reader.into_iter().map(|ss| {
            let ss = ss.wrap_err("Failed to read next chunk from StreamReader.")?;

            let chunk = match ss {
                StreamState::Some(chunk) => chunk,
                StreamState::Waiting => {
                    internal_err!("StreamReader is waiting for data, but a chunk was expected. This likely indicates that the spark provided data is incomplete.")
                }
            };


            let arrays = chunk.into_arrays();
            let columns = zip_eq(arrays, &arrow_schema.fields)
                .map(|(array, arrow_field)| {
                    let field = Arc::new(arrow_field.into());

                    let series = Series::from_arrow(field, array)
                        .wrap_err("Failed to create Series from Arrow array.")?;

                    Ok(series)
                })
                .collect::<ConnectResult<Vec<_>>>()?;

            let batch = RecordBatch::from_nonempty_columns(columns)?;

            Ok(batch)
         }).collect::<ConnectResult<Vec<_>>>()?;

        self.create_in_memory_scan(plan_id as _, daft_schema, tables)
    }

    async fn project(&self, project: spark_connect::Project) -> ConnectResult<LogicalPlanBuilder> {
        let spark_connect::Project { input, expressions } = project;

        let input = input.required("input")?;

        let mut plan = Box::pin(self.to_logical_plan(*input)).await?;

        let daft_exprs: Vec<_> = expressions
            .iter()
            .map(|e| self.to_daft_expr(e))
            .try_collect()?;
        plan = plan.select(daft_exprs)?;

        Ok(plan)
    }

    async fn with_columns(
        &self,
        with_columns: spark_connect::WithColumns,
    ) -> ConnectResult<LogicalPlanBuilder> {
        let spark_connect::WithColumns { input, aliases } = with_columns;

        let input = input.required("input")?;

        let plan = Box::pin(self.to_logical_plan(*input)).await?;

        let daft_exprs: Vec<_> = aliases
            .into_iter()
            .map(|alias| {
                let expression = Expression {
                    common: None,
                    expr_type: Some(ExprType::Alias(Box::new(alias))),
                };

                self.to_daft_expr(&expression)
            })
            .try_collect()?;

        Ok(plan.with_columns(daft_exprs)?)
    }

    async fn with_columns_renamed(
        &self,
        with_columns_renamed: spark_connect::WithColumnsRenamed,
    ) -> ConnectResult<LogicalPlanBuilder> {
        let spark_connect::WithColumnsRenamed {
            input,
            rename_columns_map,
            renames,
        } = with_columns_renamed;

        let input = input.required("input")?;

        let plan = Box::pin(self.to_logical_plan(*input)).await?;

        // todo: let's implement this directly into daft

        // Convert the rename mappings into expressions
        let rename_exprs = if !rename_columns_map.is_empty() {
            // Use rename_columns_map if provided (legacy format)
            rename_columns_map
                .into_iter()
                .map(|(old_name, new_name)| {
                    unresolved_col(old_name.as_str()).alias(new_name.as_str())
                })
                .collect()
        } else {
            // Use renames if provided (new format)
            renames
                .into_iter()
                .map(|rename| {
                    unresolved_col(rename.col_name.as_str()).alias(rename.new_col_name.as_str())
                })
                .collect()
        };

        // Apply the rename expressions to the plan
        let plan = plan
            .select(rename_exprs)
            .wrap_err("Failed to apply rename expressions to logical plan")?;

        Ok(plan)
    }

    async fn to_df(&self, to_df: spark_connect::ToDf) -> ConnectResult<LogicalPlanBuilder> {
        let spark_connect::ToDf {
            input,
            column_names,
        } = to_df;

        let input = input.required("input")?;

        let mut plan = Box::pin(self.to_logical_plan(*input)).await?;

        let column_names: Vec<_> = column_names.into_iter().map(unresolved_col).collect();

        plan = plan
            .select(column_names)
            .wrap_err("Failed to add columns to logical plan")?;
        Ok(plan)
    }

    async fn set_op(&self, set_op: SetOperation) -> ConnectResult<LogicalPlanBuilder> {
        let set_op_type = set_op.set_op_type;
        let left = set_op.left_input.required("left_input")?;
        let right = set_op.right_input.required("right_input")?;
        let is_all = set_op.is_all.required("is_all")?;

        let set_op_type =
            SetOpType::try_from(set_op_type).wrap_err("Invalid set operation type")?;

        let left = Box::pin(self.to_logical_plan(*left)).await?;
        let right = Box::pin(self.to_logical_plan(*right)).await?;

        match set_op_type {
            SetOpType::Except => left.except(&right, is_all),
            SetOpType::Intersect => left.intersect(&right, is_all),
            SetOpType::Union => left.union(&right, is_all),
            SetOpType::Unspecified => {
                invalid_argument_err!("SetOpType must be specified; got Unspecified")
            }
        }
        .map_err(Into::into)
    }

    pub async fn relation_to_spark_schema(
        &self,
        input: Relation,
    ) -> ConnectResult<spark_connect::DataType> {
        let result = self.relation_to_daft_schema(input).await?;

        let fields: ConnectResult<Vec<StructField>> = result
            .fields
            .iter()
            .map(|(name, field)| {
                let field_type = to_spark_datatype(&field.dtype);
                Ok(StructField {
                    name: name.clone(), // todo(correctness): name vs field.name... will they always be the same?
                    data_type: Some(field_type),
                    nullable: true, // todo(correctness): is this correct?
                    metadata: None, // todo(completeness): might want to add metadata here
                })
            })
            .collect();

        Ok(spark_connect::DataType {
            kind: Some(spark_connect::data_type::Kind::Struct(
                spark_connect::data_type::Struct {
                    fields: fields?,
                    type_variation_reference: 0,
                },
            )),
        })
    }

    pub async fn relation_to_daft_schema(&self, input: Relation) -> ConnectResult<SchemaRef> {
        if let Some(common) = &input.common {
            if common.origin.is_some() {
                debug!("Ignoring common metadata for relation: {common:?}; not yet implemented");
            }
        }

        let plan = Box::pin(self.to_logical_plan(input)).await?;

        let result = plan.schema();

        Ok(result)
    }

    #[allow(deprecated)]
    async fn sql(&self, sql: Sql) -> ConnectResult<LogicalPlanBuilder> {
        let Sql {
            query,
            args,
            pos_args,
            named_arguments,
            pos_arguments,
        } = sql;
        if !args.is_empty() {
            not_yet_implemented!("args");
        }
        if !pos_args.is_empty() {
            not_yet_implemented!("pos_args");
        }
        if !named_arguments.is_empty() {
            not_yet_implemented!("named_arguments");
        }
        if !pos_arguments.is_empty() {
            not_yet_implemented!("pos_arguments");
        }

        // TODO: converge Session and ConnectSession
        let session = self.session.session().clone();
        let session = Rc::new(session);

        let mut planner = SQLPlanner::new(session);
        let plan = planner.plan_sql(&query)?;
        Ok(plan.into())
    }

    pub fn to_daft_expr(&self, expression: &Expression) -> ConnectResult<daft_dsl::ExprRef> {
        if let Some(common) = &expression.common {
            if common.origin.is_some() {
                debug!("Ignoring common metadata for relation: {common:?}; not yet implemented");
            }
        };

        let Some(expr) = &expression.expr_type else {
            not_yet_implemented!("Expression is required");
        };

        match expr {
            spark_expr::ExprType::Literal(l) => to_daft_literal(l),
            spark_expr::ExprType::UnresolvedAttribute(attr) => {
                let spark_expr::UnresolvedAttribute {
                    unparsed_identifier,
                    plan_id,
                    is_metadata_column,
                } = attr;

                if let Some(plan_id) = plan_id {
                    debug!(
                        "Ignoring plan_id {plan_id} for attribute expressions; not yet implemented"
                    );
                }

                if let Some(is_metadata_column) = is_metadata_column {
                    debug!("Ignoring is_metadata_column {is_metadata_column} for attribute expressions; not yet implemented");
                }

                Ok(daft_dsl::unresolved_col(unparsed_identifier.as_str()))
            }
            spark_expr::ExprType::UnresolvedFunction(f) => self.process_function(f),
            spark_expr::ExprType::ExpressionString(_) => {
                not_yet_implemented!("Expression string not yet supported")
            }
            spark_expr::ExprType::UnresolvedStar(_) => {
                not_yet_implemented!("Unresolved star expressions not yet supported")
            }
            spark_expr::ExprType::Alias(alias) => {
                let spark_expr::Alias {
                    expr,
                    name,
                    metadata,
                } = &**alias;
                let Some(expr) = expr else {
                    invalid_argument_err!("Alias expression is required");
                };

                let [name] = name.as_slice() else {
                    invalid_argument_err!("Alias name is required and currently only works with a single string; got {name:?}");
                };

                if let Some(metadata) = metadata {
                    not_yet_implemented!("Alias metadata: {metadata:?}");
                }

                let child = self.to_daft_expr(expr)?;

                let name = Arc::from(name.as_str());

                Ok(child.alias(name))
            }
            spark_expr::ExprType::Cast(c) => {
                let spark_expr::Cast {
                    expr,
                    eval_mode,
                    cast_to_type,
                } = &**c;

                let Some(expr) = expr else {
                    invalid_argument_err!("Cast expression is required");
                };

                let expr = self.to_daft_expr(expr)?;

                let Some(cast_to_type) = cast_to_type else {
                    invalid_argument_err!("Cast to type is required");
                };

                let data_type = match &cast_to_type {
                    CastToType::Type(kind) => to_daft_datatype(kind)?,
                    CastToType::TypeStr(s) => {
                        not_yet_implemented!(
                            "Cast to type string not yet supported; tried to cast to {s}"
                        );
                    }
                };

                let eval_mode = EvalMode::try_from(*eval_mode).map_err(|e| {
                    ConnectError::invalid_relation(format!("Unknown eval mode: {e}"))
                })?;

                debug!("Ignoring cast eval mode: {eval_mode:?}");

                Ok(expr.cast(&data_type))
            }
            spark_expr::ExprType::SortOrder(s) => {
                let spark_expr::SortOrder {
                    child,
                    direction,
                    null_ordering,
                } = &**s;

                let Some(_child) = child else {
                    invalid_argument_err!("Sort order child is required");
                };

                let _sort_direction = SortDirection::try_from(*direction).map_err(|e| {
                    ConnectError::invalid_relation(format!("Unknown sort direction: {e}"))
                })?;

                let _sort_nulls = NullOrdering::try_from(*null_ordering).map_err(|e| {
                    ConnectError::invalid_relation(format!("Unknown null ordering: {e}"))
                })?;

                not_yet_implemented!("Sort order expressions not yet supported");
            }
            other => not_yet_implemented!("expression type: {other:?}"),
        }
    }

    fn process_function(&self, f: &UnresolvedFunction) -> ConnectResult<daft_dsl::ExprRef> {
        let UnresolvedFunction {
            function_name,
            arguments,
            is_distinct,
            is_user_defined_function,
        } = f;

        if *is_distinct {
            not_yet_implemented!("Distinct");
        }

        if *is_user_defined_function {
            not_yet_implemented!("User-defined functions");
        }

        let Some(f) = CONNECT_FUNCTIONS.get(function_name.as_str()) else {
            not_yet_implemented!("function: {function_name}");
        };

        f.to_expr(arguments, self)
    }
}

fn rel_name(rel: &RelType) -> &str {
    match rel {
        RelType::Read(_) => "Read",
        RelType::Project(_) => "Project",
        RelType::Filter(_) => "Filter",
        RelType::Join(_) => "Join",
        RelType::SetOp(_) => "SetOp",
        RelType::Sort(_) => "Sort",
        RelType::Limit(_) => "Limit",
        RelType::Aggregate(_) => "Aggregate",
        RelType::Sql(_) => "Sql",
        RelType::LocalRelation(_) => "LocalRelation",
        RelType::Sample(_) => "Sample",
        RelType::Offset(_) => "Offset",
        RelType::Deduplicate(_) => "Deduplicate",
        RelType::Range(_) => "Range",
        RelType::SubqueryAlias(_) => "SubqueryAlias",
        RelType::Repartition(_) => "Repartition",
        RelType::ToDf(_) => "ToDf",
        RelType::WithColumnsRenamed(_) => "WithColumnsRenamed",
        RelType::ShowString(_) => "ShowString",
        RelType::Drop(_) => "Drop",
        RelType::Tail(_) => "Tail",
        RelType::WithColumns(_) => "WithColumns",
        RelType::Hint(_) => "Hint",
        RelType::Unpivot(_) => "Unpivot",
        RelType::ToSchema(_) => "ToSchema",
        RelType::RepartitionByExpression(_) => "RepartitionByExpression",
        RelType::MapPartitions(_) => "MapPartitions",
        RelType::CollectMetrics(_) => "CollectMetrics",
        RelType::Parse(_) => "Parse",
        RelType::GroupMap(_) => "GroupMap",
        RelType::CoGroupMap(_) => "CoGroupMap",
        RelType::WithWatermark(_) => "WithWatermark",
        RelType::ApplyInPandasWithState(_) => "ApplyInPandasWithState",
        RelType::HtmlString(_) => "HtmlString",
        RelType::CachedLocalRelation(_) => "CachedLocalRelation",
        RelType::CachedRemoteRelation(_) => "CachedRemoteRelation",
        RelType::CommonInlineUserDefinedTableFunction(_) => "CommonInlineUserDefinedTableFunction",
        RelType::AsOfJoin(_) => "AsOfJoin",
        RelType::CommonInlineUserDefinedDataSource(_) => "CommonInlineUserDefinedDataSource",
        RelType::WithRelations(_) => "WithRelations",
        RelType::Transpose(_) => "Transpose",
        RelType::FillNa(_) => "FillNa",
        RelType::DropNa(_) => "DropNa",
        RelType::Replace(_) => "Replace",
        RelType::Summary(_) => "Summary",
        RelType::Crosstab(_) => "Crosstab",
        RelType::Describe(_) => "Describe",
        RelType::Cov(_) => "Cov",
        RelType::Corr(_) => "Corr",
        RelType::ApproxQuantile(_) => "ApproxQuantile",
        RelType::FreqItems(_) => "FreqItems",
        RelType::SampleBy(_) => "SampleBy",
        RelType::Catalog(_) => "Catalog",
        RelType::Extension(_) => "Extension",
        RelType::Unknown(_) => "Unknown",
    }
}
