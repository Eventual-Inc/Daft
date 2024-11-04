use daft_dsl::{Expr as DaftExpr, Operator};
use eyre::{bail, ensure, eyre, Result};
use spark_connect::{expression, expression::literal::LiteralType, Expression};

pub fn convert_expression(expr: Expression) -> Result<DaftExpr> {
    match expr.expr_type {
        Some(expression::ExprType::Literal(lit)) => Ok(DaftExpr::Literal(convert_literal(lit)?)),

        Some(expression::ExprType::UnresolvedAttribute(attr)) => {
            Ok(DaftExpr::Column(attr.unparsed_identifier.into()))
        }

        Some(expression::ExprType::Alias(alias)) => {
            let expression::Alias {
                expr,
                name,
                metadata,
            } = *alias;
            let expr = *expr.ok_or_else(|| eyre!("expr is None"))?;

            // Convert alias
            let expr = convert_expression(expr)?;

            if let Some(metadata) = metadata
                && !metadata.is_empty()
            {
                bail!("Metadata is not yet supported");
            }

            // ignore metadata for now

            let [name] = name.as_slice() else {
                bail!("Alias name must have exactly one element");
            };

            Ok(DaftExpr::Alias(expr.into(), name.as_str().into()))
        }

        Some(expression::ExprType::UnresolvedFunction(expression::UnresolvedFunction {
            function_name,
            arguments,
            is_distinct,
            is_user_defined_function,
        })) => {
            ensure!(!is_distinct, "Distinct is not yet supported");
            ensure!(
                !is_user_defined_function,
                "User-defined functions are not yet supported"
            );

            let op = function_name.as_str();
            match op {
                ">" | "<" | "<=" | ">=" | "+" | "-" | "*" | "/" => {
                    let arr: [Expression; 2] = arguments
                        .try_into()
                        .map_err(|_| eyre!("Expected 2 arguments"))?;
                    let [left, right] = arr;

                    let left = convert_expression(left)?;
                    let right = convert_expression(right)?;

                    let op = match op {
                        ">" => Operator::Gt,
                        "<" => Operator::Lt,
                        "<=" => Operator::LtEq,
                        ">=" => Operator::GtEq,
                        "+" => Operator::Plus,
                        "-" => Operator::Minus,
                        "*" => Operator::Multiply,
                        "/" => Operator::FloorDivide, // todo is this what we want?
                        _ => unreachable!(),
                    };

                    Ok(DaftExpr::BinaryOp {
                        left: left.into(),
                        op,
                        right: right.into(),
                    })
                }
                other => bail!("Unsupported function name: {other}"),
            }
        }

        // Handle other expression types...
        _ => Err(eyre!("Unsupported expression type")),
    }
}

// Helper functions to convert literals, function names, operators etc.

fn convert_literal(lit: expression::Literal) -> Result<daft_dsl::LiteralValue> {
    let literal_type = lit
        .literal_type
        .ok_or_else(|| eyre!("literal_type is None"))?;

    let result = match literal_type {
        LiteralType::Null(..) => daft_dsl::LiteralValue::Null,
        LiteralType::Binary(input) => daft_dsl::LiteralValue::Binary(input),
        LiteralType::Boolean(input) => daft_dsl::LiteralValue::Boolean(input),
        LiteralType::Byte(input) => daft_dsl::LiteralValue::Int32(input),
        LiteralType::Short(input) => daft_dsl::LiteralValue::Int32(input),
        LiteralType::Integer(input) => daft_dsl::LiteralValue::Int32(input),
        LiteralType::Long(input) => daft_dsl::LiteralValue::Int64(input),
        LiteralType::Float(input) => daft_dsl::LiteralValue::Float64(f64::from(input)),
        LiteralType::Double(input) => daft_dsl::LiteralValue::Float64(input),
        LiteralType::String(input) => daft_dsl::LiteralValue::Utf8(input),
        LiteralType::Date(input) => daft_dsl::LiteralValue::Date(input),
        LiteralType::Decimal(_)
        | LiteralType::Timestamp(_)
        | LiteralType::TimestampNtz(_)
        | LiteralType::CalendarInterval(_)
        | LiteralType::YearMonthInterval(_)
        | LiteralType::DayTimeInterval(_)
        | LiteralType::Array(_)
        | LiteralType::Map(_)
        | LiteralType::Struct(_) => bail!("unimplemented"),
    };

    Ok(result)
}
