use std::sync::Arc;

use eyre::{bail, Context};
use spark_connect::{
    expression as spark_expr,
    expression::{
        cast::{CastToType, EvalMode},
        sort_order::{NullOrdering, SortDirection},
    },
    Expression,
};
use tracing::warn;
use unresolved_function::unresolved_to_daft_expr;

use crate::translation::{to_daft_datatype, to_daft_literal};

mod unresolved_function;

pub fn to_daft_expr(expression: &Expression) -> eyre::Result<daft_dsl::ExprRef> {
    if let Some(common) = &expression.common {
        if common.origin.is_some() {
            warn!("Ignoring common metadata for relation: {common:?}; not yet implemented");
        }
    };

    let Some(expr) = &expression.expr_type else {
        bail!("Expression is required");
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
                warn!("Ignoring plan_id {plan_id} for attribute expressions; not yet implemented");
            }

            if let Some(is_metadata_column) = is_metadata_column {
                warn!("Ignoring is_metadata_column {is_metadata_column} for attribute expressions; not yet implemented");
            }

            Ok(daft_dsl::col(unparsed_identifier.as_str()))
        }
        spark_expr::ExprType::UnresolvedFunction(f) => {
            unresolved_to_daft_expr(f).wrap_err("Failed to handle unresolved function")
        }
        spark_expr::ExprType::ExpressionString(_) => bail!("Expression string not yet supported"),
        spark_expr::ExprType::UnresolvedStar(_) => {
            bail!("Unresolved star expressions not yet supported")
        }
        spark_expr::ExprType::Alias(alias) => {
            let spark_expr::Alias {
                expr,
                name,
                metadata,
            } = &**alias;

            let Some(expr) = expr else {
                bail!("Alias expr is required");
            };

            let [name] = name.as_slice() else {
                bail!("Alias name is required and currently only works with a single string; got {name:?}");
            };

            if let Some(metadata) = metadata {
                bail!("Alias metadata is not yet supported; got {metadata:?}");
            }

            let child = to_daft_expr(expr)?;

            let name = Arc::from(name.as_str());

            Ok(child.alias(name))
        }
        spark_expr::ExprType::Cast(c) => {
            // Cast { expr: Some(Expression { common: None, expr_type: Some(UnresolvedAttribute(UnresolvedAttribute { unparsed_identifier: "id", plan_id: None, is_metadata_column: None })) }), eval_mode: Unspecified, cast_to_type: Some(Type(DataType { kind: Some(String(String { type_variation_reference: 0, collation: "" })) })) }
            // thread 'tokio-runtime-worker' panicked at src/daft-connect/src/trans
            let spark_expr::Cast {
                expr,
                eval_mode,
                cast_to_type,
            } = &**c;

            let Some(expr) = expr else {
                bail!("Cast expression is required");
            };

            let expr = to_daft_expr(expr)?;

            let Some(cast_to_type) = cast_to_type else {
                bail!("Cast to type is required");
            };

            let data_type = match cast_to_type {
                CastToType::Type(kind) => to_daft_datatype(kind).wrap_err_with(|| {
                    format!("Failed to convert spark datatype to daft datatype: {kind:?}")
                })?,
                CastToType::TypeStr(s) => {
                    bail!("Cast to type string not yet supported; tried to cast to {s}");
                }
            };

            let eval_mode = EvalMode::try_from(*eval_mode)
                .wrap_err_with(|| format!("Invalid cast eval mode: {eval_mode}"))?;

            warn!("Ignoring cast eval mode: {eval_mode:?}");

            Ok(expr.cast(&data_type))
        }
        spark_expr::ExprType::UnresolvedRegex(_) => {
            bail!("Unresolved regex expressions not yet supported")
        }
        spark_expr::ExprType::SortOrder(s) => {
            let spark_expr::SortOrder {
                child,
                direction,
                null_ordering,
            } = &**s;

            let Some(_child) = child else {
                bail!("Sort order child is required");
            };

            let _sort_direction = SortDirection::try_from(*direction)
                .wrap_err_with(|| format!("Invalid sort direction: {direction}"))?;

            let _sort_nulls = NullOrdering::try_from(*null_ordering)
                .wrap_err_with(|| format!("Invalid sort nulls: {null_ordering}"))?;

            bail!("Sort order expressions not yet supported");
        }
        spark_expr::ExprType::LambdaFunction(_) => {
            bail!("Lambda function expressions not yet supported")
        }
        spark_expr::ExprType::Window(_) => bail!("Window expressions not yet supported"),
        spark_expr::ExprType::UnresolvedExtractValue(_) => {
            bail!("Unresolved extract value expressions not yet supported")
        }
        spark_expr::ExprType::UpdateFields(_) => {
            bail!("Update fields expressions not yet supported")
        }
        spark_expr::ExprType::UnresolvedNamedLambdaVariable(_) => {
            bail!("Unresolved named lambda variable expressions not yet supported")
        }
        spark_expr::ExprType::CommonInlineUserDefinedFunction(_) => {
            bail!("Common inline user defined function expressions not yet supported")
        }
        spark_expr::ExprType::CallFunction(_) => {
            bail!("Call function expressions not yet supported")
        }
        spark_expr::ExprType::NamedArgumentExpression(_) => {
            bail!("Named argument expressions not yet supported")
        }
        spark_expr::ExprType::MergeAction(_) => bail!("Merge action expressions not yet supported"),
        spark_expr::ExprType::TypedAggregateExpression(_) => {
            bail!("Typed aggregate expressions not yet supported")
        }
        spark_expr::ExprType::Extension(_) => bail!("Extension expressions not yet supported"),
    }
}
