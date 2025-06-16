use super::{from_proto, from_proto_arc, FromToProto, ProtoResult};
use crate::{from_proto_err, proto::UNIT, unsupported_err};

/// Export daft_ir types under an `ir` namespace to concisely disambiguate domains.
#[rustfmt::skip]
mod ir {
    pub use crate::rex::*;
    pub use crate::schema::DataType;
}

/// Export daft_proto types under a `proto` namespace because prost is heinous.
#[rustfmt::skip]
mod proto {
    pub use daft_proto::protos::daft::v1::*;
    pub use daft_proto::protos::daft::v1::expr::Variant as ExprVariant;
    pub use daft_proto::protos::daft::v1::literal::Variant as LiteralVariant;
}

impl FromToProto for ir::Expr {
    type Message = proto::Expr;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let expr: Self = match message.variant.unwrap() {
            proto::ExprVariant::Column(column) => {
                //
                let name = column.name;
                let qualifier = column.qualifier;
                let alias = column.alias;
                Self::Column(ir::Column::Unresolved(ir::UnresolvedColumn {
                    name: name.into(),
                    plan_ref: get_plan_ref(qualifier, alias),
                    plan_schema: None,
                }))
            }
            proto::ExprVariant::Alias(alias) => {
                //
                let name = alias.name;
                let expr: ir::ExprRef = from_proto_arc(alias.expr)?;
                Self::Alias(expr, name.into())
            }
            proto::ExprVariant::Agg(_) => todo!(),
            proto::ExprVariant::BinaryOp(binary_op) => {
                //
                let op = ir::Operator::from_proto(binary_op.op)?;
                let lhs = from_proto_arc(binary_op.lhs)?;
                let rhs = from_proto_arc(binary_op.rhs)?;
                Self::BinaryOp {
                    op,
                    left: lhs,
                    right: rhs,
                }
            }
            proto::ExprVariant::Cast(_) => unsupported_err!("cast"),
            proto::ExprVariant::Function(_) => unsupported_err!("function"),
            proto::ExprVariant::Over(_) => unsupported_err!("over"),
            proto::ExprVariant::WindowFunction(_) => unsupported_err!("window_function"),
            proto::ExprVariant::Not(_) => unsupported_err!("not"),
            proto::ExprVariant::IsNull(_) => unsupported_err!("is_null"),
            proto::ExprVariant::NotNull(_) => unsupported_err!("not_null"),
            proto::ExprVariant::FillNull(_) => unsupported_err!("fill_null"),
            proto::ExprVariant::IsIn(_) => unsupported_err!("is_in"),
            proto::ExprVariant::Between(_) => unsupported_err!("between"),
            proto::ExprVariant::List(_) => unsupported_err!("list"),
            proto::ExprVariant::Literal(_) => unsupported_err!("literal"),
            proto::ExprVariant::IfElse(_) => unsupported_err!("if_else"),
            proto::ExprVariant::ScalarFunction(_) => unsupported_err!("scalar_function"),
            proto::ExprVariant::Subquery(_) => unsupported_err!("subquery"),
            proto::ExprVariant::InSubquery(_) => unsupported_err!("in_subquery"),
            proto::ExprVariant::Exists(_) => unsupported_err!("exists"),
        };
        Ok(expr)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let variant = match self {
            Self::Column(column) => to_proto_expr_column(column),
            Self::Alias(expr, name) => to_proto_expr_alias(expr, name),
            Self::Agg(agg_expr) => to_proto_expr_agg(agg_expr),
            Self::BinaryOp { op, left, right } => to_proto_expr_binary_op(op, left, right),
            Self::Cast(expr, data_type) => to_proto_expr_cast(expr, data_type),
            Self::Function { func, inputs } => to_proto_expr_function(func, inputs),
            Self::Over(window_expr, window_spec) => to_proto_expr_over(window_expr, window_spec),
            Self::WindowFunction(window_expr) => to_proto_expr_window_function(window_expr),
            Self::Not(expr) => to_proto_expr_not(expr),
            Self::IsNull(expr) => to_proto_expr_is_null(expr),
            Self::NotNull(expr) => to_proto_expr_not_null(expr),
            Self::FillNull(expr, fill_value) => to_proto_expr_fill_null(expr, fill_value),
            Self::IsIn(expr, items) => to_proto_expr_is_in(expr, items),
            Self::Between(expr, lower, upper) => to_proto_expr_between(expr, lower, upper),
            Self::List(items) => to_proto_expr_list(items),
            Self::Literal(literal_value) => to_proto_expr_literal(literal_value),
            Self::IfElse {
                if_true,
                if_false,
                predicate,
            } => to_proto_expr_if_else(if_true, if_false, predicate),
            Self::ScalarFunction(scalar_function) => to_proto_expr_scalar_function(scalar_function),
            Self::Subquery(subquery) => to_proto_expr_subquery(subquery),
            Self::InSubquery(expr, subquery) => to_proto_expr_in_subquery(expr, subquery),
            Self::Exists(subquery) => to_proto_expr_exists(subquery),
        }?;
        Ok(Self::Message {
            variant: Some(variant),
        })
    }
}

/// Proto translation for column.
fn to_proto_expr_column(column: &ir::Column) -> ProtoResult<proto::ExprVariant> {
    let column_variant = match column {
        ir::Column::Unresolved(column) => {
            // handle unresolved columns
            proto::Column {
                name: column.name.to_string(),
                qualifier: match &column.plan_ref {
                    ir::PlanRef::Id(qualifier) => Some(*qualifier as u64),
                    _ => None,
                },
                alias: match &column.plan_ref {
                    ir::PlanRef::Alias(alias) => Some(alias.to_string()),
                    _ => None,
                },
            }
        }
        ir::Column::Resolved(column) => {
            // handle resolved column
            proto::Column {
                name: match column {
                    ir::ResolvedColumn::Basic(name) => name.to_string(),
                    ir::ResolvedColumn::JoinSide(_, _) => todo!(),
                    ir::ResolvedColumn::OuterRef(_, _) => todo!(),
                },
                qualifier: None,
                alias: None,
            }
        }
        ir::Column::Bound(_) => {
            //
            todo!("bound columns variants...")
        }
    };
    Ok(proto::ExprVariant::Column(column_variant))
}

/// Proto translation for alias.
fn to_proto_expr_alias(expr: &ir::ExprRef, name: &str) -> ProtoResult<proto::ExprVariant> {
    let alias = proto::Alias {
        expr: Some(Box::new(expr.to_proto()?)),
        name: name.to_string(),
    };
    Ok(proto::ExprVariant::Alias(alias.into()))
}

/// Proto translation for aggregate expression.
fn to_proto_expr_agg(_: &ir::AggExpr) -> ProtoResult<proto::ExprVariant> {
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
    unsupported_err!("agg_expr");
}

/// Proto translation for binary operation.
fn to_proto_expr_binary_op(
    op: &ir::Operator,
    left: &ir::ExprRef,
    right: &ir::ExprRef,
) -> ProtoResult<proto::ExprVariant> {
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
    data_type: &ir::DataType,
) -> ProtoResult<proto::ExprVariant> {
    let cast = proto::Cast {
        expr: Some(Box::new(expr.to_proto()?)),
        dtype: Some(data_type.to_proto()?),
    };
    Ok(proto::ExprVariant::Cast(cast.into()))
}

/// Proto translation for function expression.
fn to_proto_expr_function(
    _func: &ir::functions::FunctionExpr,
    _inputs: &[ir::ExprRef],
) -> ProtoResult<proto::ExprVariant> {
    // let function = proto::Function {
    //     func: func.to_proto()?,
    //     inputs: inputs
    //         .iter()
    //         .map(|e| Ok(Some(Box::new(e.to_proto()?))))
    //         .collect::<crate::ProtoResult<Vec<_>>>()?,
    // };
    // Ok(proto::ExprVariant::Function(function))
    unsupported_err!("expr_function");
}

/// Proto translation for window over expression.
fn to_proto_expr_over(
    _window_expr: &ir::WindowExpr,
    _window_spec: &ir::WindowSpec,
) -> ProtoResult<proto::ExprVariant> {
    // let over = proto::Over {
    //     expr: Some(Box::new(window_expr.to_proto()?)),
    //     spec: window_spec.to_proto()?,
    // };
    // Ok(proto::ExprVariant::Over(over))
    unsupported_err!("expr_over");
}

/// Proto translation for window function.
fn to_proto_expr_window_function(_window_expr: &ir::WindowExpr) -> ProtoResult<proto::ExprVariant> {
    // let window_function = proto::WindowFunction {
    //     expr: Some(Box::new(window_expr.to_proto()?)),
    // };
    // Ok(proto::ExprVariant::WindowFunction(window_function))
    unsupported_err!("expr_window_function");
}

/// Proto translation for not expression.
fn to_proto_expr_not(expr: &ir::ExprRef) -> ProtoResult<proto::ExprVariant> {
    let not = proto::Not {
        expr: Some(Box::new(expr.to_proto()?)),
    };
    Ok(proto::ExprVariant::Not(not.into()))
}

/// Proto translation for is null expression.
fn to_proto_expr_is_null(expr: &ir::ExprRef) -> ProtoResult<proto::ExprVariant> {
    let is_null = proto::IsNull {
        expr: Some(Box::new(expr.to_proto()?)),
    };
    Ok(proto::ExprVariant::IsNull(is_null.into()))
}

/// Proto translation for not null expression.
fn to_proto_expr_not_null(expr: &ir::ExprRef) -> ProtoResult<proto::ExprVariant> {
    let not_null = proto::NotNull {
        expr: Some(Box::new(expr.to_proto()?)),
    };
    Ok(proto::ExprVariant::NotNull(not_null.into()))
}

/// Proto translation for fill null expression.
fn to_proto_expr_fill_null(
    expr: &ir::ExprRef,
    fill_value: &ir::ExprRef,
) -> ProtoResult<proto::ExprVariant> {
    let fill_null = proto::FillNull {
        expr: Some(Box::new(expr.to_proto()?)),
        fill_value: Some(Box::new(fill_value.to_proto()?)),
    };
    Ok(proto::ExprVariant::FillNull(fill_null.into()))
}

/// Proto translation for is in expression.
fn to_proto_expr_is_in(
    _expr: &ir::ExprRef,
    _items: &[ir::ExprRef],
) -> ProtoResult<proto::ExprVariant> {
    // let is_in = proto::IsIn {
    //     expr: Some(Box::new(expr.to_proto()?)),
    //     items: items
    //         .iter()
    //         .map(|e| Ok(Some(Box::new(e.to_proto()?))))
    //         .collect::<crate::ProtoResult<Vec<_>>>()?,
    // };
    // Ok(proto::ExprVariant::IsIn(is_in))
    unsupported_err!("expr_is_in");
}

/// Proto translation for between expression.
fn to_proto_expr_between(
    expr: &ir::ExprRef,
    lower: &ir::ExprRef,
    upper: &ir::ExprRef,
) -> ProtoResult<proto::ExprVariant> {
    let between = proto::Between {
        expr: Some(Box::new(expr.to_proto()?)),
        lower: Some(Box::new(lower.to_proto()?)),
        upper: Some(Box::new(upper.to_proto()?)),
    };
    Ok(proto::ExprVariant::Between(between.into()))
}

/// Proto translation for list expression.
fn to_proto_expr_list(_items: &[ir::ExprRef]) -> ProtoResult<proto::ExprVariant> {
    // let list = proto::List {
    //     items: items
    //         .iter()
    //         .map(|e| Ok(Some(Box::new(e.to_proto()?))))
    //         .collect::<crate::ProtoResult<Vec<_>>>()?,
    // };
    // Ok(proto::ExprVariant::List(list))
    unsupported_err!("expr_list");
}

/// Proto translation for literal expression.
fn to_proto_expr_literal(literal_value: &ir::LiteralValue) -> ProtoResult<proto::ExprVariant> {
    let literal = literal_value.to_proto()?;
    Ok(proto::ExprVariant::Literal(literal))
}

/// Proto translation for if else expression.
fn to_proto_expr_if_else(
    if_true: &ir::ExprRef,
    if_false: &ir::ExprRef,
    predicate: &ir::ExprRef,
) -> ProtoResult<proto::ExprVariant> {
    let if_else = proto::IfElse {
        if_true: Some(Box::new(if_true.to_proto()?)),
        if_false: Some(Box::new(if_false.to_proto()?)),
        predicate: Some(Box::new(predicate.to_proto()?)),
    };
    Ok(proto::ExprVariant::IfElse(if_else.into()))
}

/// Proto translation for scalar function expression.
fn to_proto_expr_scalar_function(
    _scalar_function: &ir::functions::ScalarFunction,
) -> ProtoResult<proto::ExprVariant> {
    // let scalar_function = proto::ScalarFunction {
    //     name: scalar_function.name().to_string(),
    //     args: scalar_function
    //         .inputs
    //         .iter()
    //         .map(|e| Ok(Some(Box::new(e.to_proto()?))))
    //         .collect::<crate::ProtoResult<Vec<_>>>()?,
    // };
    // Ok(proto::ExprVariant::ScalarFunction(scalar_function))
    unsupported_err!("expr_scalar_function");
}

/// Proto translation for subquery expression.
fn to_proto_expr_subquery(_subquery: &ir::Subquery) -> ProtoResult<proto::ExprVariant> {
    // let subquery = proto::Subquery {
    //     plan: subquery.plan.to_proto()?,
    // };
    // Ok(proto::ExprVariant::Subquery(subquery))
    unsupported_err!("expr_subquery");
}

/// Proto translation for in subquery expression.
fn to_proto_expr_in_subquery(
    _expr: &ir::ExprRef,
    _subquery: &ir::Subquery,
) -> ProtoResult<proto::ExprVariant> {
    // let in_subquery = proto::InSubquery {
    //     expr: Some(Box::new(expr.to_proto()?)),
    //     plan: subquery.plan.to_proto()?,
    // };
    // Ok(proto::ExprVariant::InSubquery(in_subquery))
    unsupported_err!("expr_in_subquery");
}

/// Proto translation for exists expression.
fn to_proto_expr_exists(_subquery: &ir::Subquery) -> ProtoResult<proto::ExprVariant> {
    // let exists = proto::Exists {
    //     plan: subquery.plan.to_proto()?,
    // };
    // Ok(proto::ExprVariant::Exists(exists))
    unsupported_err!("expr_exists");
}

impl FromToProto for ir::Operator {
    type Message = i32;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let operator = proto::Operator::try_from(message).unwrap_or(proto::Operator::Unspecified);
        let operator = match operator {
            proto::Operator::Unspecified => from_proto_err!("Unspecified operator."),
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

    fn to_proto(&self) -> ProtoResult<Self::Message> {
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

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let literal_value = match message.variant.unwrap() {
            proto::LiteralVariant::Null(_) => Self::Null,
            proto::LiteralVariant::Boolean(b) => Self::Boolean(b),
            proto::LiteralVariant::Utf8(s) => Self::Utf8(s.into()),
            proto::LiteralVariant::Binary(items) => Self::Binary(items),
            proto::LiteralVariant::FixedSizeBinary(fixed_size_binary) => {
                Self::FixedSizeBinary(fixed_size_binary.value, fixed_size_binary.size as usize)
            }
            proto::LiteralVariant::Int8(i) => Self::Int8(i as i8),
            proto::LiteralVariant::Uint8(i) => Self::UInt8(i as u8),
            proto::LiteralVariant::Int16(i) => Self::Int16(i as i16),
            proto::LiteralVariant::Uint16(i) => Self::UInt16(i as u16),
            proto::LiteralVariant::Int32(i) => Self::Int32(i),
            proto::LiteralVariant::Uint32(i) => Self::UInt32(i),
            proto::LiteralVariant::Int64(i) => Self::Int64(i),
            proto::LiteralVariant::Uint64(i) => Self::UInt64(i),
            proto::LiteralVariant::Timestamp(_) => {
                // let unit = daft_ir::schema::TimeUnit::from_proto(timestamp.unit)?;
                // LiteralValue::Timestamp(timestamp.value, unit, timestamp.timezone)
                unsupported_err!("literal_timestamp")
            }
            proto::LiteralVariant::Date(days) => Self::Date(days),
            proto::LiteralVariant::Time(_) => {
                // let unit = daft_ir::schema::TimeUnit::from_proto(time.unit)?;
                // LiteralValue::Time(time.value, unit)
                unsupported_err!("literal_time")
            }
            proto::LiteralVariant::Duration(_duration) => {
                // let unit = daft_ir::schema::TimeUnit::from_proto(duration.unit)?;
                // LiteralValue::Duration(duration.value, unit)
                unsupported_err!("literal_duration")
            }
            proto::LiteralVariant::Interval(_interval) => {
                // LiteralValue::Interval(daft_ir::IntervalValue {
                //     months: interval.months,
                //     days: interval.days,
                //     nanoseconds: interval.nanoseconds,
                // })
                unsupported_err!("literal_interval")
            }
            proto::LiteralVariant::Float64(f) => Self::Float64(f),
            proto::LiteralVariant::Decimal(_decimal) => {
                // TODO: Implement decimal parsing from string
                unsupported_err!("literal_decimal")
            }
            proto::LiteralVariant::Struct(struct_) => {
                let mut fields = vec![];
                for field in struct_.fields {
                    let f = field.name;
                    let v: Self = from_proto(field.value)?;
                    fields.push((f, v));
                }
                Self::new_struct(fields)
            }
        };
        Ok(literal_value)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let variant = match self {
            Self::Null => proto::LiteralVariant::Null(UNIT),
            Self::Boolean(bool) => proto::LiteralVariant::Boolean(*bool),
            Self::Utf8(s) => proto::LiteralVariant::Utf8(s.to_string()),
            Self::Binary(items) => proto::LiteralVariant::Binary(items.clone()),
            Self::FixedSizeBinary(items, size) => {
                proto::LiteralVariant::FixedSizeBinary(proto::literal::FixedSizeBinary {
                    value: items.clone(),
                    size: *size as u64,
                })
            }
            Self::Int8(i) => proto::LiteralVariant::Int8(*i as i32),
            Self::UInt8(i) => proto::LiteralVariant::Uint8(*i as u32),
            Self::Int16(i) => proto::LiteralVariant::Int16(*i as i32),
            Self::UInt16(i) => proto::LiteralVariant::Uint16(*i as u32),
            Self::Int32(i) => proto::LiteralVariant::Int32(*i),
            Self::UInt32(i) => proto::LiteralVariant::Uint32(*i),
            Self::Int64(i) => proto::LiteralVariant::Int64(*i),
            Self::UInt64(i) => proto::LiteralVariant::Uint64(*i),
            Self::Timestamp(value, time_unit, timezone) => {
                proto::LiteralVariant::Timestamp(proto::literal::Timestamp {
                    value: *value,
                    unit: time_unit.to_proto()?,
                    timezone: timezone.clone(),
                })
            }
            Self::Date(days) => proto::LiteralVariant::Date(*days),
            Self::Time(value, time_unit) => proto::LiteralVariant::Time(proto::literal::Time {
                value: *value,
                unit: time_unit.to_proto()?,
            }),
            Self::Duration(value, time_unit) => {
                proto::LiteralVariant::Duration(proto::literal::Duration {
                    value: *value,
                    unit: time_unit.to_proto()?,
                })
            }
            Self::Interval(interval_value) => {
                proto::LiteralVariant::Interval(proto::literal::Interval {
                    months: interval_value.months,
                    days: interval_value.days,
                    nanoseconds: interval_value.nanoseconds,
                })
            }
            Self::Float64(f) => proto::LiteralVariant::Float64(*f),
            Self::Decimal(value, precision, scale) => {
                proto::LiteralVariant::Decimal(proto::literal::Decimal {
                    value: display_decimal128(*value, *precision, *scale),
                })
            }
            Self::Struct(struct_) => {
                let mut fields = vec![];
                for field in struct_ {
                    fields.push(proto::literal::r#struct::Field {
                        name: field.0.to_string(),
                        value: Some(field.1.to_proto()?),
                    });
                }
                proto::LiteralVariant::Struct(proto::literal::Struct { fields })
            }
            Self::Series(_) => unsupported_err!("series literal"),
            #[cfg(feature = "python")]
            Self::Python(_) => unsupported_err!("python literal"),
        };
        Ok(proto::Literal {
            variant: Some(variant),
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

/// Gets an ir::PlanRef from the proto column fields.
fn get_plan_ref(qualifier: Option<u64>, alias: Option<String>) -> ir::PlanRef {
    // df["x"]
    if let Some(qualifier) = qualifier {
        return ir::PlanRef::Id(qualifier as usize);
    }
    if let Some(alias) = alias {
        return ir::PlanRef::Alias(alias.into());
    }
    unreachable!("expected either")
}
