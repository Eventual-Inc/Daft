use std::sync::Arc;

use daft_core::datatypes::IntervalValue;
use daft_dsl::{PlanRef, UnresolvedColumn};
use spark_connect::{
    expression::{
        self as spark_expr,
        cast::{CastToType, EvalMode},
        literal::LiteralType,
        sort_order::{NullOrdering, SortDirection},
        Literal, UnresolvedFunction,
    },
    Expression,
};
use tracing::debug;

use super::to_daft_datatype;
use crate::{
    error::{ConnectError, ConnectResult},
    functions::CONNECT_FUNCTIONS,
    invalid_argument_err, invalid_relation_err, not_yet_implemented,
};

pub(crate) fn analyze_expr(expression: &Expression) -> ConnectResult<daft_dsl::ExprRef> {
    if let Some(common) = &expression.common {
        if common.origin.is_some() {
            debug!("Ignoring common metadata for relation: {common:?}; not yet implemented");
        }
    }

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
            if let Some(is_metadata_column) = is_metadata_column {
                debug!("Ignoring is_metadata_column {is_metadata_column} for attribute expressions; not yet implemented");
            }
            if let Some(id) = plan_id {
                let id = *id;
                let id: usize = id.try_into().map_err(|_| {
                    ConnectError::invalid_relation(format!("Invalid plan ID: {id}"))
                })?;

                Ok(UnresolvedColumn {
                    name: Arc::from(unparsed_identifier.as_str()),
                    plan_ref: PlanRef::Id(id),
                    plan_schema: None,
                }
                .into())
            } else {
                Ok(daft_dsl::unresolved_col(unparsed_identifier.as_str()))
            }
        }
        spark_expr::ExprType::UnresolvedFunction(f) => process_function(f),
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

            let child = analyze_expr(expr)?;

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

            let expr = analyze_expr(expr)?;

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

            let eval_mode = EvalMode::try_from(*eval_mode)
                .map_err(|e| ConnectError::invalid_relation(format!("Unknown eval mode: {e}")))?;

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

fn process_function(f: &UnresolvedFunction) -> ConnectResult<daft_dsl::ExprRef> {
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

    f.to_expr(arguments)
}

// todo(test): add tests for this esp in Python
pub fn to_daft_literal(literal: &Literal) -> ConnectResult<daft_dsl::ExprRef> {
    let Some(literal) = &literal.literal_type else {
        invalid_relation_err!("Literal is required");
    };

    match literal {
        LiteralType::Array(_) => not_yet_implemented!("array literals"),
        LiteralType::Binary(bytes) => Ok(daft_dsl::lit(bytes.as_slice())),
        LiteralType::Boolean(b) => Ok(daft_dsl::lit(*b)),
        LiteralType::Byte(_) => not_yet_implemented!("Byte literals"),
        LiteralType::CalendarInterval(_) => not_yet_implemented!("Calendar interval literals"),
        LiteralType::Date(d) => Ok(daft_dsl::lit(*d)),
        LiteralType::DayTimeInterval(_) => not_yet_implemented!("Day-time interval literals"),
        LiteralType::Decimal(_) => not_yet_implemented!("Decimal literals"),
        LiteralType::Double(d) => Ok(daft_dsl::lit(*d)),
        LiteralType::Float(f) => {
            let f = f64::from(*f);
            Ok(daft_dsl::lit(f))
        }
        LiteralType::Integer(i) => Ok(daft_dsl::lit(*i)),
        LiteralType::Long(l) => Ok(daft_dsl::lit(*l)),
        LiteralType::Map(_) => not_yet_implemented!("Map literals"),
        LiteralType::Null(_) => {
            // todo(correctness): is it ok to assume type is i32 here?
            Ok(daft_dsl::null_lit())
        }
        LiteralType::Short(_) => not_yet_implemented!("Short literals"),
        LiteralType::String(s) => Ok(daft_dsl::lit(s.as_str())),
        LiteralType::Struct(_) => not_yet_implemented!("Struct literals"),
        LiteralType::Timestamp(ts) => {
            // todo(correctness): is it ok that the type is different logically?
            Ok(daft_dsl::lit(*ts))
        }
        LiteralType::TimestampNtz(ts) => {
            // todo(correctness): is it ok that the type is different logically?
            Ok(daft_dsl::lit(*ts))
        }
        LiteralType::YearMonthInterval(value) => {
            let interval = IntervalValue::new(*value, 0, 0);
            Ok(daft_dsl::lit(interval))
        }
    }
}
