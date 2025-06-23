use std::sync::Arc;

use super::{from_proto, from_proto_arc, ProtoResult, ToFromProto};
use crate::{
    from_proto_err, non_null, not_implemented_err, not_optimized_err,
    proto::{
        from_proto_vec,
        functions::{from_proto_function, function_expr_to_proto},
        to_proto_vec, UNIT,
    },
};

/// Export daft_ir types under an `ir` namespace to concisely disambiguate domains.
#[rustfmt::skip]
mod ir {
    pub use crate::rex::*;
    pub use crate::CountMode;
}

/// Export daft_proto types under a `proto` namespace because prost is heinous.
#[rustfmt::skip]
mod proto {
    pub use daft_proto::protos::daft::v1::*;
    pub use daft_proto::protos::daft::v1::agg::Variant as AggVariant;
    pub use daft_proto::protos::daft::v1::expr::Variant as ExprVariant;
    pub use daft_proto::protos::daft::v1::literal::Variant as LiteralVariant;
}

impl ToFromProto for ir::Expr {
    type Message = proto::Expr;

    fn from_proto(message: Self::Message) -> ProtoResult<Self> {
        let expr = match non_null!(message.variant) {
            proto::ExprVariant::Column(column) => {
                let column = ir::Column::from_proto(column)?;
                Self::Column(column)
            }
            proto::ExprVariant::Alias(alias) => {
                let expr = from_proto_arc(alias.expr)?;
                let name = alias.name.into();
                Self::Alias(expr, name)
            }
            proto::ExprVariant::Agg(agg) => {
                let agg = ir::AggExpr::from_proto(*agg)?;
                Self::Agg(agg)
            }
            proto::ExprVariant::BinaryOp(binary_op) => {
                let op = ir::Operator::from_proto(binary_op.op)?;
                let lhs = from_proto_arc(binary_op.lhs)?;
                let rhs = from_proto_arc(binary_op.rhs)?;
                Self::BinaryOp {
                    op,
                    left: lhs,
                    right: rhs,
                }
            }
            proto::ExprVariant::Cast(cast) => {
                let expr = from_proto_arc(cast.expr)?;
                let dtype = from_proto(cast.dtype)?;
                Self::Cast(expr, dtype)
            }
            proto::ExprVariant::Function(function) => {
                // there are various function expression types hidden within this method.
                from_proto_function(function)?
            }
            proto::ExprVariant::Over(_) => {
                not_implemented_err!("over")
                // let window_expr = from_proto_arc(over.window_expr)?;
                // let window_spec = ir::WindowSpec::from_proto(over.window_spec)?;
                // Self::Over(window_expr, window_spec)
            }
            proto::ExprVariant::WindowFunction(_) => {
                not_implemented_err!("window")
                // let window_expr = from_proto_arc(window_function.window_expr)?;
                // Self::WindowFunction(window_expr)
            }
            proto::ExprVariant::Not(not) => {
                let expr = from_proto_arc(not.expr)?;
                Self::Not(expr)
            }
            proto::ExprVariant::IsNull(is_null) => {
                let expr = from_proto_arc(is_null.expr)?;
                Self::IsNull(expr)
            }
            proto::ExprVariant::NotNull(not_null) => {
                let expr = from_proto_arc(not_null.expr)?;
                Self::NotNull(expr)
            }
            proto::ExprVariant::FillNull(fill_null) => {
                let expr = from_proto_arc(fill_null.expr)?;
                let fill_value = from_proto_arc(fill_null.fill_value)?;
                Self::FillNull(expr, fill_value)
            }
            proto::ExprVariant::IsIn(is_in) => {
                let expr = from_proto_arc(is_in.expr)?;
                let values = from_proto_vec(is_in.items)?
                    .into_iter()
                    .map(Arc::new)
                    .collect();
                Self::IsIn(expr, values)
            }
            proto::ExprVariant::Between(between) => {
                let expr = from_proto_arc(between.expr)?;
                let lower = from_proto_arc(between.lower)?;
                let upper = from_proto_arc(between.upper)?;
                Self::Between(expr, lower, upper)
            }
            proto::ExprVariant::List(list) => {
                let values = from_proto_vec(list.items)?
                    .into_iter()
                    .map(Arc::new)
                    .collect();
                Self::List(values)
            }
            proto::ExprVariant::Literal(literal) => {
                let literal = ir::LiteralValue::from_proto(literal)?;
                Self::Literal(literal)
            }
            proto::ExprVariant::IfElse(if_else) => {
                let if_true = from_proto_arc(if_else.if_true)?;
                let if_false = from_proto_arc(if_else.if_false)?;
                let predicate = from_proto_arc(if_else.predicate)?;
                Self::IfElse {
                    if_true,
                    if_false,
                    predicate,
                }
            }
            proto::ExprVariant::Subquery(_) => {
                not_implemented_err!("subquery")
                // Self::Subquery(ir::Subquery::from_proto(subquery)?)
            }
            proto::ExprVariant::SubqueryIn(_) => {
                not_implemented_err!("subquery_in")
                // let expr = from_proto_arc(in_subquery.expr)?;
                // let subquery = ir::Subquery::from_proto(in_subquery.subquery)?;
                // Self::InSubquery(expr, subquery)
            }
            proto::ExprVariant::SubqueryComp(_) => {
                not_implemented_err!("subquery_comp")
                // Self::Exists(ir::Subquery::from_proto(exists.subquery)?)
            }
            proto::ExprVariant::SubqueryTest(_) => {
                not_implemented_err!("subquery_test")
                // Self::Exists(ir::Subquery::from_proto(exists.subquery)?)
            }
        };
        Ok(expr)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let variant = match self {
            Self::Column(column) => {
                let column = column.to_proto()?;
                proto::ExprVariant::Column(column)
            }
            Self::Alias(expr, name) => {
                let expr = expr.to_proto()?.into();
                let name = name.to_string();
                proto::ExprVariant::Alias(
                    proto::Alias {
                        expr: Some(expr),
                        name,
                    }
                    .into(),
                )
            }
            Self::Agg(agg_expr) => {
                let agg = agg_expr.to_proto()?.into();
                proto::ExprVariant::Agg(agg)
            }
            Self::BinaryOp { op, left, right } => {
                let op = op.to_proto()?;
                let lhs = left.to_proto()?.into();
                let rhs = right.to_proto()?.into();
                proto::ExprVariant::BinaryOp(
                    proto::BinaryOp {
                        op,
                        lhs: Some(lhs),
                        rhs: Some(rhs),
                    }
                    .into(),
                )
            }
            Self::Cast(expr, dtype) => {
                let expr = expr.to_proto()?.into();
                let dtype = dtype.to_proto()?.into();
                proto::ExprVariant::Cast(Box::new(proto::Cast {
                    expr: Some(expr),
                    dtype: Some(dtype),
                }))
            }
            Self::Function { func, inputs } => {
                let function = function_expr_to_proto(func, inputs)?;
                proto::ExprVariant::Function(function)
            }
            Self::Over(..) => {
                not_implemented_err!("over")
            }
            Self::WindowFunction(_) => {
                not_implemented_err!("window")
            }
            Self::Not(expr) => {
                let expr = expr.to_proto()?.into();
                proto::ExprVariant::Not(proto::Not { expr: Some(expr) }.into())
            }
            Self::IsNull(expr) => {
                let expr = expr.to_proto()?.into();
                proto::ExprVariant::IsNull(proto::IsNull { expr: Some(expr) }.into())
            }
            Self::NotNull(expr) => {
                let expr = expr.to_proto()?.into();
                proto::ExprVariant::NotNull(proto::NotNull { expr: Some(expr) }.into())
            }
            Self::FillNull(expr, fill_value) => {
                let expr = expr.to_proto()?.into();
                let fill_value = fill_value.to_proto()?.into();
                proto::ExprVariant::FillNull(
                    proto::FillNull {
                        expr: Some(expr),
                        fill_value: Some(fill_value),
                    }
                    .into(),
                )
            }
            Self::IsIn(expr, values) => {
                let expr = expr.to_proto()?.into();
                let items = to_proto_vec(values)?;
                proto::ExprVariant::IsIn(
                    proto::IsIn {
                        expr: Some(expr),
                        items,
                    }
                    .into(),
                )
            }
            Self::Between(expr, lower, upper) => {
                let expr = expr.to_proto()?.into();
                let lower = lower.to_proto()?.into();
                let upper = upper.to_proto()?.into();
                proto::ExprVariant::Between(
                    proto::Between {
                        expr: Some(expr),
                        lower: Some(lower),
                        upper: Some(upper),
                    }
                    .into(),
                )
            }
            Self::List(values) => {
                let items = to_proto_vec(values)?;
                proto::ExprVariant::List(proto::List { items }.into())
            }
            Self::Literal(value) => {
                let literal = value.to_proto()?;
                proto::ExprVariant::Literal(literal)
            }
            Self::IfElse {
                if_true,
                if_false,
                predicate,
            } => {
                let if_true = if_true.to_proto()?.into();
                let if_false = if_false.to_proto()?.into();
                let predicate = predicate.to_proto()?.into();
                proto::ExprVariant::IfElse(
                    proto::IfElse {
                        if_true: Some(if_true),
                        if_false: Some(if_false),
                        predicate: Some(predicate),
                    }
                    .into(),
                )
            }
            Self::ScalarFunction(scalar_function) => {
                let function = scalar_function.to_proto()?;
                proto::ExprVariant::Function(function)
            }
            Self::Subquery(_) => {
                // todo(conner)
                not_implemented_err!("subquery")
            }
            Self::InSubquery(..) => {
                // todo(conner)
                not_implemented_err!("in_subquery")
            }
            Self::Exists(_) => {
                // todo(conner)
                not_implemented_err!("exists")
            }
        };
        Ok(proto::Expr {
            variant: Some(variant),
        })
    }
}

// Implement ToFromProto for each variant type
impl ToFromProto for ir::Column {
    type Message = proto::Column;

    fn from_proto(message: Self::Message) -> ProtoResult<Self> {
        // we only ever produce resolved columns
        let column = ir::ResolvedColumn::Basic(message.name.into());
        Ok(Self::Resolved(column))
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        // validate the column is resolved
        let column = match self {
            Self::Bound(_) => not_implemented_err!("column::bound"),
            Self::Unresolved(_) => not_optimized_err!("unresolved column in optimized plan"),
            Self::Resolved(column) => column,
        };
        // convert the resolved column to proto
        let column = match column {
            ir::ResolvedColumn::Basic(name) => proto::Column {
                name: name.to_string(),
                qualifier: None,
            },
            ir::ResolvedColumn::JoinSide(_, _) => not_implemented_err!("column::join_side"),
            ir::ResolvedColumn::OuterRef(_, _) => not_implemented_err!("column::outer_ref"),
        };
        Ok(column)
    }
}

impl ToFromProto for ir::AggExpr {
    type Message = proto::Agg;

    fn from_proto(message: Self::Message) -> ProtoResult<Self> {
        let agg = match message.variant.unwrap() {
            proto::AggVariant::SetFunction(set_function) => {
                let name = set_function.name.as_str();
                let arg = set_function.args[0].clone(); // hack because I know we only support one atm.
                let arg = ir::Expr::from_proto(arg)?.into();
                let is_all = set_function.is_all;
                // aggregations needs some work
                if !is_all && name != "count" {
                    not_implemented_err!(
                        "daft does not support DISTINCT in aggregations other than count."
                    );
                }
                // behold, the aggregation registry!
                match name {
                    "count" => match is_all {
                        true => Self::Count(arg, ir::CountMode::Valid),
                        false => Self::CountDistinct(arg), // no mode?
                    },
                    "count_star" => Self::Count(arg, ir::CountMode::All),
                    "count_nulls" => Self::Count(arg, ir::CountMode::Null),
                    "sum" => Self::Sum(arg),
                    "mean" => Self::Mean(arg),
                    "stddev" => Self::Stddev(arg),
                    "min" => Self::Min(arg),
                    "max" => Self::Max(arg),
                    "bool_and" => Self::BoolAnd(arg),
                    "bool_or" => Self::BoolOr(arg),
                    "any_value" => Self::AnyValue(arg, true), // TODO: handle ignore_nulls
                    "agg_list" => Self::List(arg),
                    "agg_set" => Self::Set(arg),
                    "agg_concat" => Self::Concat(arg),
                    "skew" => Self::Skew(arg),
                    _ => not_implemented_err!("unrecognized aggregation function: {}", name),
                }
            }
            proto::AggVariant::ApproxPercentile(_) => {
                not_implemented_err!("approx_percentile");
                // let expr = from_proto_arc(non_null!(approx_percentile.expr))?;
                // let percentiles = approx_percentile.percentiles.iter().map(|p| FloatWrapper(*p)).collect();
                // Self::ApproxPercentile(ApproxPercentileParams {
                //     child: expr,
                //     percentiles,
                //     force_list_output: approx_percentile.force_list_output,
                // })
            }
            proto::AggVariant::ApproxSketch(_) => {
                not_implemented_err!("approx_sketch");
                // let expr = from_proto_arc(non_null!(approx_sketch.expr))?;
                // let sketch_type = match approx_sketch.sketch_type {
                //     1 => SketchType::DDSketch,
                //     2 => SketchType::HyperLogLog,
                //     _ => return Err(ProtoError::FromProto("Invalid sketch type".to_string())),
                // };
                // Self::ApproxSketch(expr, sketch_type)
            }
            proto::AggVariant::MergeSketch(_) => {
                not_implemented_err!("merge_sketch");
                // let expr = from_proto_arc(non_null!(merge_sketch.expr))?;
                // let sketch_type = match merge_sketch.sketch_type {
                //     1 => SketchType::DDSketch,
                //     2 => SketchType::HyperLogLog,
                //     _ => return Err(ProtoError::FromProto("Invalid sketch type".to_string())),
                // };
                // Self::MergeSketch(expr, sketch_type)
            }
            proto::AggVariant::MapGroups(_) => {
                not_implemented_err!("map_groups");
                // let func = from_proto_arc(non_null!(map_groups.func))?;
                // let inputs = map_groups.inputs.into_iter()
                //     .map(|input| ir::Expr::from_proto(input).map(|e| e.into()))
                //     .collect::<ProtoResult<Vec<_>>>()?;
                // Self::MapGroups { func, inputs }
            }
        };
        Ok(agg)
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        let variant = match self {
            Self::Count(expr, count_mode) => {
                let name = match count_mode {
                    ir::CountMode::Valid => "count",
                    ir::CountMode::All => "count_star", // !! not to be confused with COUNT(ALL x) !!
                    ir::CountMode::Null => "count_nulls",
                };
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: name.to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: true,
                })
            }
            Self::CountDistinct(expr) => {
                // COUNT(DISTINCT <expr>)
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: "count".to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: false,
                })
            }
            Self::Sum(expr) => {
                // SUM([ALL] <expr>)
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: "sum".to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: true,
                })
            }
            Self::Mean(expr) => {
                // MEAN([ALL] <expr>)
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: "mean".to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: true,
                })
            }
            Self::Stddev(expr) => {
                // STDDEV([ALL] <expr>)
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: "stddev".to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: true,
                })
            }
            Self::Min(expr) => {
                // MIN([ALL] <expr>)
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: "min".to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: true,
                })
            }
            Self::Max(expr) => {
                // MAX([ALL] <expr>)
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: "max".to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: true,
                })
            }
            Self::BoolAnd(expr) => {
                // BOOL_AND([ALL] <expr>)
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: "bool_and".to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: true,
                })
            }
            Self::BoolOr(expr) => {
                // BOOL_OR([ALL] <expr>)
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: "bool_or".to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: true,
                })
            }
            Self::AnyValue(expr, _) => {
                // ANY_VALUE([ALL] <expr>)
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: "any_value".to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: true,
                })
            }
            Self::List(expr) => {
                // AGG_LIST([ALL] <expr>)
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: "agg_list".to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: true,
                })
            }
            Self::Set(expr) => {
                // AGG_SET([ALL] <expr>)
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: "agg_set".to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: true,
                })
            }
            Self::Concat(expr) => {
                // AGG_CONCAT([ALL] <expr>)
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: "agg_concat".to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: true,
                })
            }
            Self::Skew(expr) => {
                // SKEW([ALL] <expr>)
                proto::AggVariant::SetFunction(proto::agg::SetFunction {
                    name: "skew".to_string(),
                    args: vec![expr.to_proto()?],
                    is_all: true,
                })
            }
            Self::MapGroups { .. } => not_implemented_err!("map_groups"),
            Self::ApproxPercentile(_) => not_implemented_err!("approx_percentile"),
            Self::ApproxCountDistinct(_) => not_implemented_err!("approx_count_distinct"),
            Self::ApproxSketch(..) => not_implemented_err!("approx_sketch"),
            Self::MergeSketch(..) => not_implemented_err!("merge_sketch"),
        };
        Ok(Self::Message {
            variant: Some(variant),
        })
    }
}

impl ToFromProto for ir::expr::WindowExpr {
    type Message = proto::WindowFunction;

    fn from_proto(_message: Self::Message) -> ProtoResult<Self> {
        not_implemented_err!("expr_window_function")
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        not_implemented_err!("expr_window_function")
    }
}

impl ToFromProto for ir::expr::Subquery {
    type Message = proto::Subquery;

    fn from_proto(_message: Self::Message) -> ProtoResult<Self> {
        not_implemented_err!("expr_subquery")
    }

    fn to_proto(&self) -> ProtoResult<Self::Message> {
        not_implemented_err!("expr_subquery")
    }
}

impl ToFromProto for ir::Operator {
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

impl ToFromProto for ir::LiteralValue {
    type Message = proto::Literal;

    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        let literal_value = match non_null!(message.variant) {
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
                not_implemented_err!("literal_timestamp")
            }
            proto::LiteralVariant::Date(days) => Self::Date(days),
            proto::LiteralVariant::Time(_) => {
                // let unit = daft_ir::schema::TimeUnit::from_proto(time.unit)?;
                // LiteralValue::Time(time.value, unit)
                not_implemented_err!("literal_time")
            }
            proto::LiteralVariant::Duration(_duration) => {
                // let unit = daft_ir::schema::TimeUnit::from_proto(duration.unit)?;
                // LiteralValue::Duration(duration.value, unit)
                not_implemented_err!("literal_duration")
            }
            proto::LiteralVariant::Interval(_interval) => {
                // LiteralValue::Interval(daft_ir::IntervalValue {
                //     months: interval.months,
                //     days: interval.days,
                //     nanoseconds: interval.nanoseconds,
                // })
                not_implemented_err!("literal_interval")
            }
            proto::LiteralVariant::Float64(f) => Self::Float64(f),
            proto::LiteralVariant::Decimal(_decimal) => {
                // TODO: Implement decimal parsing from string
                not_implemented_err!("literal_decimal")
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
            Self::Series(_) => not_implemented_err!("series literal"),
            #[cfg(feature = "python")]
            Self::Python(_) => todo!("python literal"),
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
