/// TODOs
///  - the from_proto helpers are weird.
///  - consider using macros or something better.
///  - having things return box/arc is odd and less flexible.
use crate::{from_proto, from_proto_arc, FromToProto};

/// Export daft_ir types under an `ir` namespace to concisely disambiguate domains.
#[rustfmt::skip]
mod ir {
    pub use daft_ir::rex::*;
}

/// Export daft_proto types under a `proto` namespace because prost is heinous.
#[rustfmt::skip]
mod proto {
    pub use crate::v1::protos::rex::*;
    pub use crate::v1::protos::rex::expr::*;
    pub use crate::v1::protos::rex::literal::*;
}

impl FromToProto for ir::Expr {
    type Message = proto::Expr;

    fn from_proto(message: Self::Message) -> crate::ProtoResult<Self>
    where
        Self: Sized,
    {
        let expr_variant: ir::Expr = match message.expr_variant.unwrap() {
            proto::ExprVariant::Column(column) => {
                //
                let name = column.name;
                let qualifier = column.qualifier;
                let alias = column.alias;
                ir::Expr::Column(ir::Column::Unresolved(ir::UnresolvedColumn {
                    name: name.into(),
                    plan_ref: get_plan_ref(qualifier, alias),
                    plan_schema: None,
                }))
            }
            proto::ExprVariant::Alias(alias) => {
                //
                let name = alias.name;
                let expr: ir::ExprRef = from_proto_arc(alias.expr)?;
                ir::Expr::Alias(expr, name.into())
            }
            proto::ExprVariant::Agg(agg_expr) => todo!(),
            proto::ExprVariant::BinaryOp(binary_op) => {
                //
                let op = ir::Operator::from_proto(binary_op.op)?;
                let lhs = from_proto_arc(binary_op.lhs)?;
                let rhs = from_proto_arc(binary_op.rhs)?;
                ir::Expr::BinaryOp {
                    op,
                    left: lhs,
                    right: rhs,
                }
            }
            proto::ExprVariant::Cast(cast) => todo!(),
            proto::ExprVariant::Function(function) => todo!(),
            proto::ExprVariant::Over(over) => todo!(),
            proto::ExprVariant::WindowFunction(window_function) => todo!(),
            proto::ExprVariant::Not(not) => todo!(),
            proto::ExprVariant::IsNull(is_null) => todo!(),
            proto::ExprVariant::NotNull(not_null) => todo!(),
            proto::ExprVariant::FillNull(fill_null) => todo!(),
            proto::ExprVariant::IsIn(is_in) => todo!(),
            proto::ExprVariant::Between(between) => todo!(),
            proto::ExprVariant::List(list) => todo!(),
            proto::ExprVariant::Literal(literal) => todo!(),
            proto::ExprVariant::IfElse(if_else) => todo!(),
            proto::ExprVariant::ScalarFunction(scalar_function) => todo!(),
            proto::ExprVariant::Subquery(subquery) => todo!(),
            proto::ExprVariant::InSubquery(in_subquery) => todo!(),
            proto::ExprVariant::Exists(exists) => todo!(),
        };
        Ok(expr_variant)
    }

    fn to_proto(&self) -> crate::ProtoResult<Self::Message> {
        let expr_variant = match self {
            ir::Expr::Column(column) => to_proto_expr_column(column),
            ir::Expr::Alias(expr, name) => to_proto_expr_alias(expr, name),
            ir::Expr::Agg(agg_expr) => to_proto_expr_agg(agg_expr),
            ir::Expr::BinaryOp { op, left, right } => to_proto_expr_binary_op(op, left, right),
            ir::Expr::Cast(expr, data_type) => to_proto_expr_cast(expr, data_type),
            ir::Expr::Function { func, inputs } => to_proto_expr_function(func, inputs),
            ir::Expr::Over(window_expr, window_spec) => {
                to_proto_expr_over(window_expr, window_spec)
            }
            ir::Expr::WindowFunction(window_expr) => to_proto_expr_window_function(window_expr),
            ir::Expr::Not(expr) => to_proto_expr_not(expr),
            ir::Expr::IsNull(expr) => to_proto_expr_is_null(expr),
            ir::Expr::NotNull(expr) => to_proto_expr_not_null(expr),
            ir::Expr::FillNull(expr, fill_value) => to_proto_expr_fill_null(expr, fill_value),
            ir::Expr::IsIn(expr, items) => to_proto_expr_is_in(expr, items),
            ir::Expr::Between(expr, lower, upper) => to_proto_expr_between(expr, lower, upper),
            ir::Expr::List(items) => to_proto_expr_list(items),
            ir::Expr::Literal(literal_value) => to_proto_expr_literal(literal_value),
            ir::Expr::IfElse {
                if_true,
                if_false,
                predicate,
            } => to_proto_expr_if_else(if_true, if_false, predicate),
            ir::Expr::ScalarFunction(scalar_function) => {
                to_proto_expr_scalar_function(scalar_function)
            }
            ir::Expr::Subquery(subquery) => to_proto_expr_subquery(subquery),
            ir::Expr::InSubquery(expr, subquery) => to_proto_expr_in_subquery(expr, subquery),
            ir::Expr::Exists(subquery) => to_proto_expr_exists(subquery),
        }?;
        Ok(Self::Message {
            expr_variant: Some(expr_variant),
        })
    }
}

/// Proto translation for column.
fn to_proto_expr_column(column: &ir::Column) -> crate::ProtoResult<proto::ExprVariant> {
    if let ir::Column::Unresolved(colum) = column {
        let column = proto::Column {
            name: colum.name.to_string(),
            qualifier: match &colum.plan_ref {
                ir::PlanRef::Id(qualifier) => Some(*qualifier as u64),
                _ => None,
            },
            alias: match &colum.plan_ref {
                ir::PlanRef::Alias(alias) => Some(alias.to_string()),
                _ => None,
            },
        };
        return Ok(proto::ExprVariant::Column(column));
    }
    todo!("other columns variants...")
}

/// Proto translation for alias.
fn to_proto_expr_alias(expr: &ir::ExprRef, name: &str) -> crate::ProtoResult<proto::ExprVariant> {
    let alias = proto::Alias {
        expr: Some(Box::new(expr.to_proto()?)),
        name: name.to_string(),
    };
    Ok(proto::ExprVariant::Alias(alias.into()))
}

/// Proto translation for aggregate expression.
fn to_proto_expr_agg(agg_expr: &ir::AggExpr) -> crate::ProtoResult<proto::ExprVariant> {
    todo!()
    // let agg = proto::AggExpr {
    //     name: agg_expr.name().to_string(),
    //     args: agg_expr
    //         .children()
    //         .iter()
    //         .map(|e| Ok(Some(Box::new(e.to_proto()?))))
    //         .collect::<crate::ProtoResult<Vec<_>>>()?,
    //     distinct: false, // TODO: Add distinct flag to AggExpr
    // };
    // Ok(proto::ExprVariant::Agg(agg))
}

/// Proto translation for binary operation.
fn to_proto_expr_binary_op(
    op: &ir::Operator,
    left: &ir::ExprRef,
    right: &ir::ExprRef,
) -> crate::ProtoResult<proto::ExprVariant> {
    let binary_op = proto::BinaryOp {
        op: op.to_proto()?,
        lhs: Some(Box::new(left.to_proto()?)),
        rhs: Some(Box::new(right.to_proto()?)),
    };
    Ok(proto::ExprVariant::BinaryOp(binary_op.into()))
}

/// Proto translation for cast expression.
fn to_proto_expr_cast(
    expr: &ir::ExprRef,
    data_type: &daft_ir::schema::DataType,
) -> crate::ProtoResult<proto::ExprVariant> {
    let cast = proto::Cast {
        expr: Some(Box::new(expr.to_proto()?)),
        dtype: Some(data_type.to_proto()?),
    };
    Ok(proto::ExprVariant::Cast(cast.into()))
}

/// Proto translation for function expression.
fn to_proto_expr_function(
    func: &ir::functions::FunctionExpr,
    inputs: &[ir::ExprRef],
) -> crate::ProtoResult<proto::ExprVariant> {
    todo!()
    // let function = proto::Function {
    //     func: func.to_proto()?,
    //     inputs: inputs
    //         .iter()
    //         .map(|e| Ok(Some(Box::new(e.to_proto()?))))
    //         .collect::<crate::ProtoResult<Vec<_>>>()?,
    // };
    // Ok(proto::ExprVariant::Function(function))
}

/// Proto translation for window over expression.
fn to_proto_expr_over(
    window_expr: &ir::WindowExpr,
    window_spec: &ir::WindowSpec,
) -> crate::ProtoResult<proto::ExprVariant> {
    todo!();
    // let over = proto::Over {
    //     expr: Some(Box::new(window_expr.to_proto()?)),
    //     spec: window_spec.to_proto()?,
    // };
    // Ok(proto::ExprVariant::Over(over))
}

/// Proto translation for window function.
fn to_proto_expr_window_function(
    window_expr: &ir::WindowExpr,
) -> crate::ProtoResult<proto::ExprVariant> {
    todo!();
    // let window_function = proto::WindowFunction {
    //     expr: Some(Box::new(window_expr.to_proto()?)),
    // };
    // Ok(proto::ExprVariant::WindowFunction(window_function))
}

/// Proto translation for not expression.
fn to_proto_expr_not(expr: &ir::ExprRef) -> crate::ProtoResult<proto::ExprVariant> {
    let not = proto::Not {
        expr: Some(Box::new(expr.to_proto()?)),
    };
    Ok(proto::ExprVariant::Not(not.into()))
}

/// Proto translation for is null expression.
fn to_proto_expr_is_null(expr: &ir::ExprRef) -> crate::ProtoResult<proto::ExprVariant> {
    let is_null = proto::IsNull {
        expr: Some(Box::new(expr.to_proto()?)),
    };
    Ok(proto::ExprVariant::IsNull(is_null.into()))
}

/// Proto translation for not null expression.
fn to_proto_expr_not_null(expr: &ir::ExprRef) -> crate::ProtoResult<proto::ExprVariant> {
    let not_null = proto::NotNull {
        expr: Some(Box::new(expr.to_proto()?)),
    };
    Ok(proto::ExprVariant::NotNull(not_null.into()))
}

/// Proto translation for fill null expression.
fn to_proto_expr_fill_null(
    expr: &ir::ExprRef,
    fill_value: &ir::ExprRef,
) -> crate::ProtoResult<proto::ExprVariant> {
    let fill_null = proto::FillNull {
        expr: Some(Box::new(expr.to_proto()?)),
        fill_value: Some(Box::new(fill_value.to_proto()?)),
    };
    Ok(proto::ExprVariant::FillNull(fill_null.into()))
}

/// Proto translation for is in expression.
fn to_proto_expr_is_in(
    expr: &ir::ExprRef,
    items: &[ir::ExprRef],
) -> crate::ProtoResult<proto::ExprVariant> {
    todo!()
    // let is_in = proto::IsIn {
    //     expr: Some(Box::new(expr.to_proto()?)),
    //     items: items
    //         .iter()
    //         .map(|e| Ok(Some(Box::new(e.to_proto()?))))
    //         .collect::<crate::ProtoResult<Vec<_>>>()?,
    // };
    // Ok(proto::ExprVariant::IsIn(is_in))
}

/// Proto translation for between expression.
fn to_proto_expr_between(
    expr: &ir::ExprRef,
    lower: &ir::ExprRef,
    upper: &ir::ExprRef,
) -> crate::ProtoResult<proto::ExprVariant> {
    let between = proto::Between {
        expr: Some(Box::new(expr.to_proto()?)),
        lower: Some(Box::new(lower.to_proto()?)),
        upper: Some(Box::new(upper.to_proto()?)),
    };
    Ok(proto::ExprVariant::Between(between.into()))
}

/// Proto translation for list expression.
fn to_proto_expr_list(items: &[ir::ExprRef]) -> crate::ProtoResult<proto::ExprVariant> {
    todo!();
    // let list = proto::List {
    //     items: items
    //         .iter()
    //         .map(|e| Ok(Some(Box::new(e.to_proto()?))))
    //         .collect::<crate::ProtoResult<Vec<_>>>()?,
    // };
    // Ok(proto::ExprVariant::List(list))
}

/// Proto translation for literal expression.
fn to_proto_expr_literal(
    literal_value: &ir::LiteralValue,
) -> crate::ProtoResult<proto::ExprVariant> {
    let literal = literal_value.to_proto()?;
    Ok(proto::ExprVariant::Literal(literal))
}

/// Proto translation for if else expression.
fn to_proto_expr_if_else(
    if_true: &ir::ExprRef,
    if_false: &ir::ExprRef,
    predicate: &ir::ExprRef,
) -> crate::ProtoResult<proto::ExprVariant> {
    let if_else = proto::IfElse {
        if_true: Some(Box::new(if_true.to_proto()?)),
        if_false: Some(Box::new(if_false.to_proto()?)),
        predicate: Some(Box::new(predicate.to_proto()?)),
    };
    Ok(proto::ExprVariant::IfElse(if_else.into()))
}

/// Proto translation for scalar function expression.
fn to_proto_expr_scalar_function(
    scalar_function: &ir::functions::ScalarFunction,
) -> crate::ProtoResult<proto::ExprVariant> {
    todo!();
    // let scalar_function = proto::ScalarFunction {
    //     name: scalar_function.name().to_string(),
    //     args: scalar_function
    //         .inputs
    //         .iter()
    //         .map(|e| Ok(Some(Box::new(e.to_proto()?))))
    //         .collect::<crate::ProtoResult<Vec<_>>>()?,
    // };
    // Ok(proto::ExprVariant::ScalarFunction(scalar_function))
}

/// Proto translation for subquery expression.
fn to_proto_expr_subquery(subquery: &ir::Subquery) -> crate::ProtoResult<proto::ExprVariant> {
    todo!();
    // let subquery = proto::Subquery {
    //     plan: subquery.plan.to_proto()?,
    // };
    // Ok(proto::ExprVariant::Subquery(subquery))
}

/// Proto translation for in subquery expression.
fn to_proto_expr_in_subquery(
    expr: &ir::ExprRef,
    subquery: &ir::Subquery,
) -> crate::ProtoResult<proto::ExprVariant> {
    todo!();
    // let in_subquery = proto::InSubquery {
    //     expr: Some(Box::new(expr.to_proto()?)),
    //     plan: subquery.plan.to_proto()?,
    // };
    // Ok(proto::ExprVariant::InSubquery(in_subquery))
}

/// Proto translation for exists expression.
fn to_proto_expr_exists(subquery: &ir::Subquery) -> crate::ProtoResult<proto::ExprVariant> {
    todo!();
    // let exists = proto::Exists {
    //     plan: subquery.plan.to_proto()?,
    // };
    // Ok(proto::ExprVariant::Exists(exists))
}

impl FromToProto for ir::Operator {
    type Message = i32;

    fn from_proto(message: Self::Message) -> crate::ProtoResult<Self>
    where
        Self: Sized,
    {
        let operator = proto::Operator::try_from(message).unwrap_or(proto::Operator::Unspecified);
        let operator = match operator {
            proto::Operator::Unspecified => {
                return Err(crate::ProtoError::FromProtoError(
                    "Unspecified operator".to_string(),
                ))
            }
            proto::Operator::Eq => Self::Eq,
            proto::Operator::EqNullSafe => Self::EqNullSafe,
            proto::Operator::NotEq => Self::NotEq,
            proto::Operator::Lt => Self::Lt,
            proto::Operator::LtEq => Self::LtEq,
            proto::Operator::Gt => Self::Gt,
            proto::Operator::GtEq => Self::GtEq,
            proto::Operator::Plus => Self::Plus,
            proto::Operator::Minus => Self::Minus,
            proto::Operator::Multiply => Self::Multiply,
            proto::Operator::TrueDivide => Self::TrueDivide,
            proto::Operator::FloorDivide => Self::FloorDivide,
            proto::Operator::Modulus => Self::Modulus,
            proto::Operator::And => Self::And,
            proto::Operator::Or => Self::Or,
            proto::Operator::Xor => Self::Xor,
            proto::Operator::ShiftLeft => Self::ShiftLeft,
            proto::Operator::ShiftRight => Self::ShiftRight,
        };
        Ok(operator)
    }

    fn to_proto(&self) -> crate::ProtoResult<Self::Message> {
        let operator = match self {
            Self::Eq => proto::Operator::Eq,
            Self::EqNullSafe => proto::Operator::EqNullSafe,
            Self::NotEq => proto::Operator::NotEq,
            Self::Lt => proto::Operator::Lt,
            Self::LtEq => proto::Operator::LtEq,
            Self::Gt => proto::Operator::Gt,
            Self::GtEq => proto::Operator::GtEq,
            Self::Plus => proto::Operator::Plus,
            Self::Minus => proto::Operator::Minus,
            Self::Multiply => proto::Operator::Multiply,
            Self::TrueDivide => proto::Operator::TrueDivide,
            Self::FloorDivide => proto::Operator::FloorDivide,
            Self::Modulus => proto::Operator::Modulus,
            Self::And => proto::Operator::And,
            Self::Or => proto::Operator::Or,
            Self::Xor => proto::Operator::Xor,
            Self::ShiftLeft => proto::Operator::ShiftLeft,
            Self::ShiftRight => proto::Operator::ShiftRight,
        };
        Ok(operator as i32)
    }
}

impl FromToProto for ir::LiteralValue {
    type Message = proto::Literal;

    fn from_proto(message: Self::Message) -> crate::ProtoResult<Self>
    where
        Self: Sized,
    {
        let literal_value = match message.literal_variant.unwrap() {
            proto::LiteralVariant::Null(_) => ir::LiteralValue::Null,
            proto::LiteralVariant::Boolean(b) => ir::LiteralValue::Boolean(b),
            proto::LiteralVariant::Utf8(s) => ir::LiteralValue::Utf8(s.into()),
            proto::LiteralVariant::Binary(items) => ir::LiteralValue::Binary(items),
            proto::LiteralVariant::FixedSizeBinary(fixed_size_binary) => {
                ir::LiteralValue::FixedSizeBinary(
                    fixed_size_binary.value,
                    fixed_size_binary.size as usize,
                )
            }
            proto::LiteralVariant::Int8(i) => ir::LiteralValue::Int8(i as i8),
            proto::LiteralVariant::Uint8(i) => ir::LiteralValue::UInt8(i as u8),
            proto::LiteralVariant::Int16(i) => ir::LiteralValue::Int16(i as i16),
            proto::LiteralVariant::Uint16(i) => ir::LiteralValue::UInt16(i as u16),
            proto::LiteralVariant::Int32(i) => ir::LiteralValue::Int32(i),
            proto::LiteralVariant::Uint32(i) => ir::LiteralValue::UInt32(i),
            proto::LiteralVariant::Int64(i) => ir::LiteralValue::Int64(i),
            proto::LiteralVariant::Uint64(i) => ir::LiteralValue::UInt64(i),
            proto::LiteralVariant::Timestamp(timestamp) => {
                todo!()
                // let unit = daft_ir::schema::TimeUnit::from_proto(timestamp.unit)?;
                // LiteralValue::Timestamp(timestamp.value, unit, timestamp.timezone)
            }
            proto::LiteralVariant::Date(days) => ir::LiteralValue::Date(days as i32),
            proto::LiteralVariant::Time(time) => {
                todo!()
                // let unit = daft_ir::schema::TimeUnit::from_proto(time.unit)?;
                // LiteralValue::Time(time.value, unit)
            }
            proto::LiteralVariant::Duration(duration) => {
                todo!()
                // let unit = daft_ir::schema::TimeUnit::from_proto(duration.unit)?;
                // LiteralValue::Duration(duration.value, unit)
            }
            proto::LiteralVariant::Interval(interval) => {
                todo!()
                // LiteralValue::Interval(daft_ir::rex::IntervalValue {
                //     months: interval.months,
                //     days: interval.days,
                //     nanoseconds: interval.nanoseconds,
                // })
            }
            proto::LiteralVariant::Float64(f) => ir::LiteralValue::Float64(f),
            proto::LiteralVariant::Decimal(decimal) => {
                // TODO: Implement decimal parsing from string
                todo!("Decimal parsing from string not implemented")
            }
            proto::LiteralVariant::Series(series) => {
                let dtype = series.dtype.as_ref().ok_or_else(|| {
                    crate::ProtoError::FromProtoError("Missing dtype in Series".to_string())
                })?;
                let dtype = daft_ir::schema::DataType::from_proto(dtype.clone())?;
                // TODO: Implement series creation from bytes
                todo!("Series creation from bytes not implemented")
            }
            proto::LiteralVariant::Struct(struct_) => {
                let mut fields = vec![];
                for field in struct_.fields {
                    let f = field.name;
                    let v: ir::LiteralValue = from_proto(field.value)?;
                    fields.push((f, v));
                }
                ir::LiteralValue::new_struct(fields)
            }
        };
        Ok(literal_value)
    }

    fn to_proto(&self) -> crate::ProtoResult<Self::Message> {
        let literal_variant = match self {
            ir::LiteralValue::Null => proto::LiteralVariant::Null(true),
            ir::LiteralValue::Boolean(bool) => proto::LiteralVariant::Boolean(*bool),
            ir::LiteralValue::Utf8(s) => proto::LiteralVariant::Utf8(s.to_string()),
            ir::LiteralValue::Binary(items) => proto::LiteralVariant::Binary(items.to_vec()),
            ir::LiteralValue::FixedSizeBinary(items, size) => {
                proto::LiteralVariant::FixedSizeBinary(proto::literal::FixedSizeBinary {
                    value: items.to_vec(),
                    size: *size as u64,
                })
            }
            ir::LiteralValue::Int8(i) => proto::LiteralVariant::Int8(*i as i32),
            ir::LiteralValue::UInt8(i) => proto::LiteralVariant::Uint8(*i as u32),
            ir::LiteralValue::Int16(i) => proto::LiteralVariant::Int16(*i as i32),
            ir::LiteralValue::UInt16(i) => proto::LiteralVariant::Uint16(*i as u32),
            ir::LiteralValue::Int32(i) => proto::LiteralVariant::Int32(*i),
            ir::LiteralValue::UInt32(i) => proto::LiteralVariant::Uint32(*i),
            ir::LiteralValue::Int64(i) => proto::LiteralVariant::Int64(*i),
            ir::LiteralValue::UInt64(i) => proto::LiteralVariant::Uint64(*i),
            ir::LiteralValue::Timestamp(value, time_unit, timezone) => {
                proto::LiteralVariant::Timestamp(proto::literal::Timestamp {
                    value: *value,
                    unit: time_unit.to_proto()?,
                    timezone: timezone.clone(),
                })
            }
            ir::LiteralValue::Date(days) => proto::LiteralVariant::Date(*days as i32),
            ir::LiteralValue::Time(value, time_unit) => {
                proto::LiteralVariant::Time(proto::literal::Time {
                    value: *value,
                    unit: time_unit.to_proto()?,
                })
            }
            ir::LiteralValue::Duration(value, time_unit) => {
                proto::LiteralVariant::Duration(proto::literal::Duration {
                    value: *value,
                    unit: time_unit.to_proto()?,
                })
            }
            ir::LiteralValue::Interval(interval_value) => {
                proto::LiteralVariant::Interval(proto::literal::Interval {
                    months: interval_value.months,
                    days: interval_value.days,
                    nanoseconds: interval_value.nanoseconds,
                })
            }
            ir::LiteralValue::Float64(f) => proto::LiteralVariant::Float64(*f),
            ir::LiteralValue::Decimal(value, precision, scale) => {
                proto::LiteralVariant::Decimal(proto::literal::Decimal {
                    value: display_decimal128(*value, *precision, *scale),
                })
            }
            ir::LiteralValue::Series(_) => {
                todo!()
                // proto::LiteralVariant::Series(proto::literal::Series {
                //     name: series.name().to_string(),
                //     dtype: Some(series.data_type().to_proto()?),
                //     data: series.to_bytes()?,
                // })
            }
            ir::LiteralValue::Python(_) => todo!("Python object serialization not implemented"),
            ir::LiteralValue::Struct(struct_) => {
                let mut fields = vec![];
                for field in struct_.iter() {
                    fields.push(proto::r#struct::Field {
                        name: field.0.to_string(),
                        value: Some(field.1.to_proto()?),
                    });
                }
                proto::LiteralVariant::Struct(proto::literal::Struct { fields })
            }
        };
        Ok(proto::Literal {
            literal_variant: Some(literal_variant),
        })
    }
}

// ---------------------------------------------
//
//                   HELPERS
//
// ---------------------------------------------

/// no util import because I just need one function.
fn display_decimal128(val: i128, _precision: u8, scale: i8) -> String {
    if scale < 0 {
        unimplemented!();
    } else {
        let modulus = i128::pow(10, scale as u32);
        let integral = val / modulus;
        if scale == 0 {
            format!("{}", integral)
        } else {
            let sign = if val < 0 { "-" } else { "" };
            let integral = integral.abs();
            let decimals = (val % modulus).abs();
            let scale = scale as usize;
            format!("{}{}.{:0scale$}", sign, integral, decimals)
        }
    }
}

///
fn get_plan_ref(qualifier: Option<u64>, alias: Option<String>) -> ir::PlanRef {
    // df["x"]
    if let Some(qualifier) = qualifier {
        return ir::PlanRef::Id(qualifier as usize);
    }
    // no idea why columns would have an alias.
    if let Some(alias) = alias {
        return ir::PlanRef::Alias(alias.into());
    }
    unreachable!("expected either")
}
