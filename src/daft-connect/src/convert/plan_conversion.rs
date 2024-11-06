use std::{collections::HashSet, sync::Arc};

use daft_plan::{LogicalPlanBuilder, ParquetScanBuilder};
use eyre::{bail, eyre, Result, WrapErr};
use spark_connect::{
    expression::Alias,
    read::{DataSource, ReadType},
    relation::RelType,
    Filter, Read, Relation, WithColumns,
};
use tracing::warn;

use crate::convert::expression;

pub fn to_logical_plan(plan: Relation) -> Result<LogicalPlanBuilder> {
    let scope = std::thread::spawn(|| {
        let rel_type = plan.rel_type.ok_or_else(|| eyre!("rel_type is None"))?;

        match rel_type {
            RelType::ShowString(..) => {
                bail!("ShowString is only supported as a top-level relation")
            }
            RelType::Filter(filter) => parse_filter(*filter).wrap_err("parsing Filter"),
            RelType::WithColumns(with_columns) => {
                parse_with_columns(*with_columns).wrap_err("parsing WithColumns")
            }
            RelType::Read(read) => parse_read(read),
            _ => bail!("Unsupported relation type: {rel_type:?}"),
        }
    });

    scope.join().unwrap()
}

fn parse_filter(filter: Filter) -> Result<LogicalPlanBuilder> {
    let Filter { input, condition } = filter;
    let input = *input.ok_or_else(|| eyre!("input is None"))?;
    let input_plan = to_logical_plan(input).wrap_err("parsing input")?;

    let condition = condition.ok_or_else(|| eyre!("condition is None"))?;
    let condition =
        expression::convert_expression(condition).wrap_err("converting to daft expression")?;
    let condition = Arc::new(condition);

    input_plan.filter(condition).wrap_err("applying filter")
}

fn parse_with_columns(with_columns: WithColumns) -> Result<LogicalPlanBuilder> {
    let WithColumns { input, aliases } = with_columns;
    let input = *input.ok_or_else(|| eyre!("input is None"))?;
    let input_plan = to_logical_plan(input).wrap_err("parsing input")?;

    let mut new_exprs = Vec::new();
    let mut existing_columns: HashSet<_> = input_plan.schema().names().into_iter().collect();

    for alias in aliases {
        let Alias {
            expr,
            name,
            metadata,
        } = alias;

        if name.len() != 1 {
            bail!("Alias name must have exactly one element");
        }
        let name = name[0].as_str();

        if metadata.is_some() {
            bail!("Metadata is not yet supported");
        }

        let expr = expr.ok_or_else(|| eyre!("expression is None"))?;
        let expr =
            expression::convert_expression(*expr).wrap_err("converting to daft expression")?;
        let expr = Arc::new(expr);

        new_exprs.push(expr.alias(name));

        if existing_columns.contains(name) {
            existing_columns.remove(name);
        }
    }

    // Add remaining existing columns
    for col_name in existing_columns {
        new_exprs.push(daft_dsl::col(col_name));
    }

    input_plan
        .select(new_exprs)
        .wrap_err("selecting new expressions")
}

fn parse_read(read: Read) -> Result<LogicalPlanBuilder> {
    let Read {
        is_streaming,
        read_type,
    } = read;

    warn!("Ignoring is_streaming: {is_streaming}");

    let read_type = read_type.ok_or_else(|| eyre!("type is None"))?;

    match read_type {
        ReadType::NamedTable(_) => bail!("Named tables are not yet supported"),
        ReadType::DataSource(data_source) => parse_data_source(data_source),
    }
}

fn parse_data_source(data_source: DataSource) -> Result<LogicalPlanBuilder> {
    let DataSource {
        format,
        options,
        paths,
        predicates,
        ..
    } = data_source;

    let format = format.ok_or_else(|| eyre!("format is None"))?;
    if format != "parquet" {
        bail!("Only parquet is supported; got {format}");
    }

    if !options.is_empty() {
        bail!("Options are not yet supported");
    }
    if !predicates.is_empty() {
        bail!("Predicates are not yet supported");
    }

    ParquetScanBuilder::new(paths)
        .finish()
        .wrap_err("creating ParquetScanBuilder")
}
