use std::{collections::HashSet, sync::Arc};

use anyhow::{bail, ensure, Context};
use daft_plan::{LogicalPlanBuilder, ParquetScanBuilder};

use crate::spark_connect::{
    expression::Alias,
    read::{DataSource, ReadType},
    relation::RelType,
    Filter, Read, Relation, ShowString, WithColumns,
};

mod expr;

// todo: a way to do something like tracing scopes but with errors?
pub fn to_logical_plan(plan: Relation) -> anyhow::Result<LogicalPlanBuilder> {
    let result = match plan.rel_type.context("rel_type is None")? {
        RelType::ShowString(show_string) => {
            let ShowString {
                input, num_rows, ..
            } = *show_string;
            // todo: support more truncate options
            let input = *input.context("input is None")?;

            let builder = to_logical_plan(input)?;

            let num_rows = i64::from(num_rows);
            builder.limit(num_rows, false)?.add_show_string_column()?
        }
        RelType::Filter(filter) => {
            let Filter { input, condition } = *filter;

            let input = input.context("input is None")?;
            let input_plan = to_logical_plan(*input)?;

            let condition = condition.context("condition is None")?;
            let condition = expr::to_daft_expr(condition)?;
            let condition = Arc::new(condition);

            input_plan.filter(condition)?
        }
        RelType::WithColumns(with_columns) => {
            let WithColumns { input, aliases } = *with_columns;

            let input = input.context("input is None")?;
            let input_plan = to_logical_plan(*input)?;

            let mut new_exprs = Vec::new();
            let mut existing_columns: HashSet<_> =
                input_plan.schema().names().into_iter().collect();

            for alias in aliases {
                let Alias {
                    expr,
                    name,
                    metadata,
                } = alias;

                let [name] = name.as_slice() else {
                    bail!("Alias name must have exactly one element");
                };

                let name = name.as_str();

                ensure!(metadata.is_none(), "Metadata is not yet supported");

                let expr = expr.context("expr is None")?;

                let expr = expr::to_daft_expr(*expr)?;
                let expr = Arc::new(expr);

                // todo: test
                new_exprs.push(expr.alias(name));

                if existing_columns.contains(name) {
                    // Replace existing column
                    existing_columns.remove(name);
                }
            }

            // Add remaining existing columns
            for col_name in existing_columns {
                new_exprs.push(daft_dsl::col(col_name));
            }

            input_plan.select(new_exprs)?
        }
        RelType::Read(Read {
            is_streaming,
            read_type,
        }) => {
            ensure!(!is_streaming, "Streaming reads are not yet supported");
            let read_type = read_type.context("read_type is None")?;

            match read_type {
                ReadType::NamedTable(_) => {
                    bail!("Named tables are not yet supported");
                }
                ReadType::DataSource(DataSource {
                    format,
                    schema,
                    options,
                    paths,
                    predicates,
                }) => {
                    let format = format.context("format is None")?;

                    ensure!(
                        format == "parquet",
                        "Only parquet is supported; got {format}"
                    );

                    // let schema = schema.context("schema is None")?;

                    ensure!(options.is_empty(), "Options are not yet supported");

                    ensure!(predicates.is_empty(), "Predicates are not yet supported");

                    ParquetScanBuilder::new(paths).finish()?
                }
            }
        }
        plan => bail!("Unsupported relation type: {:?}", plan),
    };

    Ok(result)
}

#[cfg(test)]
mod tests;
