//! Direct expression and aggregate binding for `PgDaftPlan`.

use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::prelude::{DataType, Field as DaftField, Literal, Schema, Series, TimeUnit};
use daft_dsl::{
    bound_col,
    expr::{
        bound_expr::{BoundAggExpr, BoundExpr},
        AggExpr, ExprRef,
    },
    functions::{AggFn, AggFnHandle, State},
    lit, null_lit,
};
use serde::{Deserialize, Serialize};

use crate::{
    error::{DpgError, DpgErrorCode},
    ir::{AggregateExpr, BoolOp, Expr, Field, LiteralEncoding, NamedExpr, PgType},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum PgCheckedI64Kind {
    CountAll,
    CountValid,
    SumInt2,
    SumInt4,
}

/// Bridge-owned checked accumulator for PostgreSQL COUNT and SUM(int2/int4).
///
/// Daft may combine partial aggregate states in any order. PostgreSQL's int8
/// transition/combine semantics require an error at every addition, so both
/// block aggregation and state combination use `checked_add`; native wrapping
/// integer sum and final-only narrowing are intentionally not used.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PgCheckedI64Agg {
    kind: PgCheckedI64Kind,
}

impl PgCheckedI64Agg {
    fn overflow() -> DaftError {
        DaftError::ComputeError(
            "PGDAFT_AGG_INT8_OVERFLOW: PostgreSQL bigint aggregate state is out of range"
                .to_string(),
        )
    }

    fn add(total: i64, value: i64) -> DaftResult<i64> {
        total.checked_add(value).ok_or_else(Self::overflow)
    }

    fn require_one_series<'a>(values: &'a [Series], owner: &str) -> DaftResult<&'a Series> {
        if values.len() != 1 {
            return Err(DaftError::InternalError(format!(
                "PGDAFT_AGG_STATE_INVALID: {owner} expected one series, got {}",
                values.len()
            )));
        }
        Ok(&values[0])
    }

    fn literal_i64(value: Literal, owner: &str) -> DaftResult<i64> {
        match value {
            Literal::Int64(value) => Ok(value),
            other => Err(DaftError::InternalError(format!(
                "PGDAFT_AGG_STATE_INVALID: {owner} expected Int64, got {:?}",
                other.get_type()
            ))),
        }
    }
}

#[typetag::serde(name = "PgDaftCheckedI64AggV1")]
impl AggFn for PgCheckedI64Agg {
    fn name(&self) -> &str {
        match self.kind {
            PgCheckedI64Kind::CountAll => "pg_count_all_i64_v1",
            PgCheckedI64Kind::CountValid => "pg_count_valid_i64_v1",
            PgCheckedI64Kind::SumInt2 => "pg_sum_int2_checked_i64_v1",
            PgCheckedI64Kind::SumInt4 => "pg_sum_int4_checked_i64_v1",
        }
    }

    fn return_dtype(&self, input_types: &[DataType]) -> DaftResult<DataType> {
        if input_types.len() != 1 {
            return Err(DaftError::TypeError(
                "PGDAFT_AGG_ARGUMENT_COUNT: checked aggregate requires one execution input"
                    .to_string(),
            ));
        }
        let valid = match self.kind {
            PgCheckedI64Kind::CountAll => input_types[0] == DataType::Int64,
            PgCheckedI64Kind::CountValid => {
                matches!(input_types[0], DataType::Int16 | DataType::Int32)
            }
            PgCheckedI64Kind::SumInt2 => input_types[0] == DataType::Int16,
            PgCheckedI64Kind::SumInt4 => input_types[0] == DataType::Int32,
        };
        if !valid {
            return Err(DaftError::TypeError(format!(
                "PGDAFT_AGG_ARGUMENT_TYPE: {} received {:?}",
                self.name(),
                input_types[0]
            )));
        }
        Ok(DataType::Int64)
    }

    fn state_fields(&self, _inputs: &[DaftField]) -> DaftResult<Vec<DaftField>> {
        let mut fields = vec![DaftField::new("pg_i64_state", DataType::Int64)];
        if matches!(
            self.kind,
            PgCheckedI64Kind::SumInt2 | PgCheckedI64Kind::SumInt4
        ) {
            fields.push(DaftField::new("pg_i64_seen", DataType::Boolean));
        }
        Ok(fields)
    }

    fn call_agg_block(&self, inputs: Vec<Series>) -> DaftResult<Vec<State>> {
        let input = Self::require_one_series(&inputs, "aggregate block")?;
        match self.kind {
            PgCheckedI64Kind::CountAll => {
                let count = i64::try_from(input.len()).map_err(|_| Self::overflow())?;
                Ok(vec![Literal::Int64(count)])
            }
            PgCheckedI64Kind::CountValid => {
                let mut count = 0_i64;
                for value in input.to_literals() {
                    if value != Literal::Null {
                        count = Self::add(count, 1)?;
                    }
                }
                Ok(vec![Literal::Int64(count)])
            }
            PgCheckedI64Kind::SumInt2 | PgCheckedI64Kind::SumInt4 => {
                let mut total = 0_i64;
                let mut seen = false;
                for value in input.to_literals() {
                    let value = match value {
                        Literal::Null => continue,
                        Literal::Int16(value) => i64::from(value),
                        Literal::Int32(value) => i64::from(value),
                        other => {
                            return Err(DaftError::InternalError(format!(
                                "PGDAFT_AGG_ARGUMENT_TYPE: {} received {:?}",
                                self.name(),
                                other.get_type()
                            )));
                        }
                    };
                    total = Self::add(total, value)?;
                    seen = true;
                }
                Ok(vec![Literal::Int64(total), Literal::Boolean(seen)])
            }
        }
    }

    fn call_agg_combine(&self, states: Vec<Series>) -> DaftResult<Vec<State>> {
        match self.kind {
            PgCheckedI64Kind::CountAll | PgCheckedI64Kind::CountValid => {
                let counts = Self::require_one_series(&states, "count combine")?;
                let mut total = 0_i64;
                for count in counts.to_literals() {
                    total = Self::add(total, Self::literal_i64(count, "count combine")?)?;
                }
                Ok(vec![Literal::Int64(total)])
            }
            PgCheckedI64Kind::SumInt2 | PgCheckedI64Kind::SumInt4 => {
                if states.len() != 2 || states[0].len() != states[1].len() {
                    return Err(DaftError::InternalError(
                        "PGDAFT_AGG_STATE_INVALID: sum combine requires aligned total/seen states"
                            .to_string(),
                    ));
                }
                let mut total = 0_i64;
                let mut seen_any = false;
                for index in 0..states[0].len() {
                    let seen = match states[1].get_lit(index) {
                        Literal::Boolean(seen) => seen,
                        other => {
                            return Err(DaftError::InternalError(format!(
                                "PGDAFT_AGG_STATE_INVALID: sum seen state is {:?}",
                                other.get_type()
                            )));
                        }
                    };
                    if seen {
                        total = Self::add(
                            total,
                            Self::literal_i64(states[0].get_lit(index), "sum combine")?,
                        )?;
                        seen_any = true;
                    }
                }
                Ok(vec![Literal::Int64(total), Literal::Boolean(seen_any)])
            }
        }
    }

    fn call_agg_finalize(&self, state: Vec<State>) -> DaftResult<State> {
        match self.kind {
            PgCheckedI64Kind::CountAll | PgCheckedI64Kind::CountValid => {
                if state.len() != 1 {
                    return Err(DaftError::InternalError(
                        "PGDAFT_AGG_STATE_INVALID: count finalize requires one state".to_string(),
                    ));
                }
                Ok(Literal::Int64(Self::literal_i64(
                    state[0].clone(),
                    "count finalize",
                )?))
            }
            PgCheckedI64Kind::SumInt2 | PgCheckedI64Kind::SumInt4 => {
                if state.len() != 2 {
                    return Err(DaftError::InternalError(
                        "PGDAFT_AGG_STATE_INVALID: sum finalize requires total/seen states"
                            .to_string(),
                    ));
                }
                match state[1] {
                    Literal::Boolean(false) => Ok(Literal::Null),
                    Literal::Boolean(true) => Ok(Literal::Int64(Self::literal_i64(
                        state[0].clone(),
                        "sum finalize",
                    )?)),
                    ref other => Err(DaftError::InternalError(format!(
                        "PGDAFT_AGG_STATE_INVALID: sum seen state is {:?}",
                        other.get_type()
                    ))),
                }
            }
        }
    }
}

/// Convert and validate one frozen P0 PostgreSQL type mapping.
pub(crate) fn to_daft_type(pg_type: &PgType) -> Result<DataType, Box<DpgError>> {
    let expected = match pg_type.pg_oid {
        16 => ("b", "Boolean", DataType::Boolean),
        17 => ("z", "Binary", DataType::Binary),
        20 => ("l", "Int64", DataType::Int64),
        21 => ("s", "Int16", DataType::Int16),
        23 => ("i", "Int32", DataType::Int32),
        25 => ("u", "Utf8", DataType::Utf8),
        1043 => ("u", "Utf8", DataType::Utf8),
        1082 => ("tdD", "Date", DataType::Date),
        1114 => (
            "tsu:",
            "TimestampMicroseconds",
            DataType::Timestamp(TimeUnit::Microseconds, None),
        ),
        1184 => (
            "tsu:UTC",
            "TimestampMicrosecondsUTC",
            DataType::Timestamp(TimeUnit::Microseconds, Some("UTC".to_string())),
        ),
        oid => {
            return Err(invalid(format!(
                "PGDAFT_TYPE_UNSUPPORTED: PostgreSQL type OID {oid} is not enabled in P0"
            )));
        }
    };

    if pg_type.arrow_format != expected.0 || pg_type.daft_dtype != expected.1 {
        return Err(invalid(format!(
            "PGDAFT_TYPE_MISMATCH: OID {} requires Arrow '{}' and Daft '{}', got '{}' and '{}'",
            pg_type.pg_oid, expected.0, expected.1, pg_type.arrow_format, pg_type.daft_dtype
        )));
    }
    if !matches!(pg_type.pg_oid, 25 | 1043) && pg_type.collation_oid != 0 {
        return Err(invalid(format!(
            "PGDAFT_COLLATION_INVALID: type OID {} cannot carry collation {}",
            pg_type.pg_oid, pg_type.collation_oid
        )));
    }
    Ok(expected.2)
}

/// Bind the P0 expression family (an input reference) by stable column ID.
pub(crate) fn bind_project_expr(
    expr: &Expr,
    input_fields: &[Field],
    output: &Field,
) -> Result<BoundExpr, Box<DpgError>> {
    let Expr::InputRef { column_id, pg_type } = expr else {
        return Err(invalid(
            "PGDAFT_EXPR_CAPABILITY_DISABLED: P0 Project only enables input_ref".to_string(),
        ));
    };

    let mut matches = input_fields
        .iter()
        .enumerate()
        .filter(|(_, field)| field.column_id == *column_id);
    let Some((index, input)) = matches.next() else {
        return Err(invalid(format!(
            "PGDAFT_COLUMN_DANGLING: input column_id {column_id} does not exist"
        )));
    };
    if matches.next().is_some() {
        return Err(invalid(format!(
            "PGDAFT_COLUMN_DUPLICATE: input column_id {column_id} is ambiguous"
        )));
    }
    if pg_type != &input.pg_type {
        return Err(invalid(format!(
            "PGDAFT_INPUT_TYPE_MISMATCH: column_id {column_id} type differs from input schema"
        )));
    }
    if &output.pg_type != pg_type {
        return Err(invalid(format!(
            "PGDAFT_PROJECT_TYPE_MISMATCH: output column_id {} changes the input_ref type",
            output.column_id
        )));
    }

    let dtype = to_daft_type(&input.pg_type)?;
    let input_field = DaftField::new(input.name.as_str(), dtype);
    let aliased = bound_col(index, input_field).alias(output.name.as_str());
    Ok(BoundExpr::new_unchecked(aliased))
}

/// Bind the immutable P1-001-A predicate subset to Daft's native expression
/// tree. Unsupported wire variants fail closed before a LocalPhysicalPlan is
/// created; there is no Python UDF or second optimizer fallback here.
pub(crate) fn bind_filter_expr(
    expr: &Expr,
    input_fields: &[Field],
) -> Result<BoundExpr, Box<DpgError>> {
    let bound = bind_filter_expr_inner(expr, input_fields)?;
    if expr_pg_type(expr).pg_oid != 16 {
        return Err(invalid(
            "PGDAFT_FILTER_RESULT_TYPE: WHERE predicate must return bool".to_string(),
        ));
    }
    Ok(BoundExpr::new_unchecked(bound))
}

fn bind_filter_expr_inner(expr: &Expr, input_fields: &[Field]) -> Result<ExprRef, Box<DpgError>> {
    match expr {
        Expr::InputRef { column_id, pg_type } => {
            let mut matches = input_fields
                .iter()
                .enumerate()
                .filter(|(_, field)| field.column_id == *column_id);
            let Some((index, input)) = matches.next() else {
                return Err(invalid(format!(
                    "PGDAFT_COLUMN_DANGLING: input column_id {column_id} does not exist"
                )));
            };
            if matches.next().is_some() || &input.pg_type != pg_type {
                return Err(invalid(format!(
                    "PGDAFT_INPUT_TYPE_MISMATCH: column_id {column_id} is ambiguous or has a different type"
                )));
            }
            let field = DaftField::new(input.name.as_str(), to_daft_type(pg_type)?);
            Ok(bound_col(index, field))
        }
        Expr::Literal {
            encoding,
            value,
            pg_type,
        } => {
            let dtype = to_daft_type(pg_type)?;
            let literal = match encoding {
                LiteralEncoding::Null if value.is_null() => null_lit().cast(&dtype),
                LiteralEncoding::Bool if pg_type.pg_oid == 16 => {
                    let value = value.as_bool().ok_or_else(|| {
                        invalid("PGDAFT_LITERAL_INVALID: bool literal is not JSON boolean".to_string())
                    })?;
                    lit(value)
                }
                LiteralEncoding::I64 if matches!(pg_type.pg_oid, 20 | 21 | 23) => {
                    let value = value.as_i64().ok_or_else(|| {
                        invalid("PGDAFT_LITERAL_INVALID: i64 literal is out of range".to_string())
                    })?;
                    lit(value).cast(&dtype)
                }
                _ => {
                    return Err(invalid(
                        "PGDAFT_LITERAL_CAPABILITY_DISABLED: Filter admits only NULL, bool and int2/int4/int8 literals"
                            .to_string(),
                    ));
                }
            };
            Ok(literal)
        }
        Expr::Bool { op, args, pg_type } => {
            require_bool_type(pg_type, "Boolean expression")?;
            let mut bound = args
                .iter()
                .map(|arg| bind_filter_expr_inner(arg, input_fields))
                .collect::<Result<Vec<_>, _>>()?;
            match op {
                BoolOp::Not if bound.len() == 1 => Ok(bound.remove(0).not()),
                BoolOp::And if bound.len() >= 2 => Ok(bound
                    .into_iter()
                    .reduce(|left, right| left.and(right))
                    .expect("arity checked")),
                BoolOp::Or if bound.len() >= 2 => Ok(bound
                    .into_iter()
                    .reduce(|left, right| left.or(right))
                    .expect("arity checked")),
                _ => Err(invalid(
                    "PGDAFT_BOOL_ARITY: NOT needs one argument; AND/OR need at least two"
                        .to_string(),
                )),
            }
        }
        Expr::Compare {
            semantic_id,
            pg_oid,
            args,
            pg_type,
        } => {
            require_bool_type(pg_type, "Comparison")?;
            if args.len() != 2 {
                return Err(invalid(
                    "PGDAFT_COMPARE_ARITY: comparison requires two operands".to_string(),
                ));
            }
            let left_type = expr_pg_type(&args[0]);
            let right_type = expr_pg_type(&args[1]);
            if left_type != right_type || !comparison_oid_allowed(left_type.pg_oid, *pg_oid) {
                return Err(invalid(format!(
                    "PGDAFT_COMPARE_IDENTITY: operator {pg_oid} is not a whitelisted same-type comparison"
                )));
            }
            let left = bind_filter_expr_inner(&args[0], input_fields)?;
            let right = bind_filter_expr_inner(&args[1], input_fields)?;
            match semantic_id.as_str() {
                "eq" => Ok(left.eq(right)),
                "ne" => Ok(left.not_eq(right)),
                "lt" => Ok(left.lt(right)),
                "le" => Ok(left.lt_eq(right)),
                "gt" => Ok(left.gt(right)),
                "ge" => Ok(left.gt_eq(right)),
                _ => Err(invalid(format!(
                    "PGDAFT_COMPARE_SEMANTIC_ID: unsupported semantic_id {semantic_id}"
                ))),
            }
        }
        Expr::NullTest {
            is_not,
            input,
            pg_type,
        } => {
            require_bool_type(pg_type, "NULL test")?;
            let input = bind_filter_expr_inner(input, input_fields)?;
            Ok(if *is_not { input.not_null() } else { input.is_null() })
        }
        Expr::Arithmetic { .. } | Expr::Cast { .. } => Err(invalid(
            "PGDAFT_EXPR_CAPABILITY_DISABLED: arithmetic and cast are not admitted for P1-001-A Filter"
                .to_string(),
        )),
    }
}

fn expr_pg_type(expr: &Expr) -> &PgType {
    match expr {
        Expr::InputRef { pg_type, .. }
        | Expr::Literal { pg_type, .. }
        | Expr::Bool { pg_type, .. }
        | Expr::Compare { pg_type, .. }
        | Expr::Arithmetic { pg_type, .. }
        | Expr::NullTest { pg_type, .. }
        | Expr::Cast { pg_type, .. } => pg_type,
    }
}

fn require_bool_type(pg_type: &PgType, owner: &str) -> Result<(), Box<DpgError>> {
    if pg_type.pg_oid == 16 && to_daft_type(pg_type)? == DataType::Boolean {
        Ok(())
    } else {
        Err(invalid(format!(
            "PGDAFT_FILTER_RESULT_TYPE: {owner} must return PostgreSQL bool"
        )))
    }
}

fn comparison_oid_allowed(type_oid: u32, op_oid: u32) -> bool {
    match type_oid {
        16 => matches!(op_oid, 58 | 59 | 85 | 91 | 1694 | 1695),
        21 => matches!(op_oid, 94 | 95 | 519 | 520 | 522 | 524),
        23 => matches!(op_oid, 96 | 97 | 518 | 521 | 523 | 525),
        20 => (410..=415).contains(&op_oid),
        _ => false,
    }
}

/// Bind one frozen Aggregate group key by stable column ID. P1 only admits
/// int2/int4 input references and does not let lowering infer a new type.
pub(crate) fn bind_aggregate_group_key(
    key: &NamedExpr,
    input_fields: &[Field],
) -> Result<BoundExpr, Box<DpgError>> {
    let (index, input) = aggregate_input_ref(&key.expr, input_fields, "group key")?;
    if !matches!(input.pg_type.pg_oid, 21 | 23) {
        return Err(invalid(format!(
            "PGDAFT_AGG_GROUP_TYPE_UNSUPPORTED: PostgreSQL type OID {} is not admitted",
            input.pg_type.pg_oid
        )));
    }
    if key.output.pg_type != input.pg_type {
        return Err(invalid(
            "PGDAFT_AGG_GROUP_OUTPUT_TYPE: group key output changes the input type".to_string(),
        ));
    }
    let field = DaftField::new(input.name.as_str(), to_daft_type(&input.pg_type)?);
    Ok(BoundExpr::new_unchecked(
        bound_col(index, field).alias(key.output.name.as_str()),
    ))
}

/// Bind one admitted PostgreSQL aggregate OID to its exact Daft physical
/// implementation. The OID, semantic ID, argument type, result type and
/// nullability must all match the frozen matrix before a plan is created.
pub(crate) fn bind_aggregate_expr(
    aggregate: &AggregateExpr,
    input_fields: &[Field],
    input_schema: &Schema,
) -> Result<BoundAggExpr, Box<DpgError>> {
    if aggregate.distinct || !aggregate.order_by.is_empty() || aggregate.filter.is_some() {
        return Err(invalid(
            "PGDAFT_AGG_MODIFIER_UNSUPPORTED: DISTINCT, aggregate ORDER BY and FILTER are disabled"
                .to_string(),
        ));
    }

    let output_type = &aggregate.output.pg_type;
    if output_type.typmod != -1 || output_type.collation_oid != 0 {
        return Err(invalid(
            "PGDAFT_AGG_OUTPUT_TYPE: aggregate output typmod/collation is not admitted".to_string(),
        ));
    }
    let expected_i64 = |nullable| PgType {
        pg_oid: 20,
        typmod: -1,
        collation_oid: 0,
        nullable,
        arrow_format: "l".to_string(),
        daft_dtype: "Int64".to_string(),
    };

    let aggregate_expr = match (aggregate.semantic_id.as_str(), aggregate.pg_aggregate_oid) {
        ("count_star", 2803) if aggregate.args.is_empty() => {
            if output_type != &expected_i64(false) {
                return Err(invalid(
                    "PGDAFT_AGG_OUTPUT_TYPE: count(*) must return non-null int8".to_string(),
                ));
            }
            // AggFn requires one execution input to determine block row count.
            // A bound literal is evaluated once per input row and is not part
            // of SQL aggregate semantics or the wire argument list.
            AggExpr::AggFn {
                handle: checked_handle(PgCheckedI64Kind::CountAll),
                inputs: vec![lit(1_i64).alias(aggregate.output.name.as_str())],
            }
        }
        ("count_value", 2147) if aggregate.args.len() == 1 => {
            if output_type != &expected_i64(false) {
                return Err(invalid(
                    "PGDAFT_AGG_OUTPUT_TYPE: count(value) must return non-null int8".to_string(),
                ));
            }
            let (_, input) = aggregate_input_ref(&aggregate.args[0], input_fields, "count")?;
            if !matches!(input.pg_type.pg_oid, 21 | 23) {
                return Err(invalid(
                    "PGDAFT_AGG_ARGUMENT_TYPE: count(value) admits int2/int4 in P1".to_string(),
                ));
            }
            AggExpr::AggFn {
                handle: checked_handle(PgCheckedI64Kind::CountValid),
                inputs: vec![aggregate_bound_input(
                    &aggregate.args[0],
                    input_fields,
                    aggregate.output.name.as_str(),
                    "count",
                )?],
            }
        }
        ("sum_int2", 2109) if aggregate.args.len() == 1 => {
            require_aggregate_arg_oid(&aggregate.args[0], input_fields, 21, "sum_int2")?;
            if output_type != &expected_i64(true) {
                return Err(invalid(
                    "PGDAFT_AGG_OUTPUT_TYPE: sum(int2) must return nullable int8".to_string(),
                ));
            }
            AggExpr::AggFn {
                handle: checked_handle(PgCheckedI64Kind::SumInt2),
                inputs: vec![aggregate_bound_input(
                    &aggregate.args[0],
                    input_fields,
                    aggregate.output.name.as_str(),
                    "sum_int2",
                )?],
            }
        }
        ("sum_int4", 2108) if aggregate.args.len() == 1 => {
            require_aggregate_arg_oid(&aggregate.args[0], input_fields, 23, "sum_int4")?;
            if output_type != &expected_i64(true) {
                return Err(invalid(
                    "PGDAFT_AGG_OUTPUT_TYPE: sum(int4) must return nullable int8".to_string(),
                ));
            }
            AggExpr::AggFn {
                handle: checked_handle(PgCheckedI64Kind::SumInt4),
                inputs: vec![aggregate_bound_input(
                    &aggregate.args[0],
                    input_fields,
                    aggregate.output.name.as_str(),
                    "sum_int4",
                )?],
            }
        }
        (semantic @ ("min_int2" | "max_int2"), oid @ (2133 | 2117))
            if aggregate.args.len() == 1
                && ((semantic == "min_int2" && oid == 2133)
                    || (semantic == "max_int2" && oid == 2117)) =>
        {
            require_aggregate_arg_oid(&aggregate.args[0], input_fields, 21, semantic)?;
            let (_, input) = aggregate_input_ref(&aggregate.args[0], input_fields, semantic)?;
            if output_type != &input.pg_type || !output_type.nullable {
                return Err(invalid(format!(
                    "PGDAFT_AGG_OUTPUT_TYPE: {semantic} must return nullable int2"
                )));
            }
            let input = aggregate_bound_input(
                &aggregate.args[0],
                input_fields,
                aggregate.output.name.as_str(),
                semantic,
            )?;
            if semantic.starts_with("min") {
                AggExpr::Min(input)
            } else {
                AggExpr::Max(input)
            }
        }
        (semantic @ ("min_int4" | "max_int4"), oid @ (2132 | 2116))
            if aggregate.args.len() == 1
                && ((semantic == "min_int4" && oid == 2132)
                    || (semantic == "max_int4" && oid == 2116)) =>
        {
            require_aggregate_arg_oid(&aggregate.args[0], input_fields, 23, semantic)?;
            let (_, input) = aggregate_input_ref(&aggregate.args[0], input_fields, semantic)?;
            if output_type != &input.pg_type || !output_type.nullable {
                return Err(invalid(format!(
                    "PGDAFT_AGG_OUTPUT_TYPE: {semantic} must return nullable int4"
                )));
            }
            let input = aggregate_bound_input(
                &aggregate.args[0],
                input_fields,
                aggregate.output.name.as_str(),
                semantic,
            )?;
            if semantic.starts_with("min") {
                AggExpr::Min(input)
            } else {
                AggExpr::Max(input)
            }
        }
        _ => {
            return Err(invalid(format!(
                "PGDAFT_AGG_IDENTITY_UNSUPPORTED: semantic_id '{}' OID {} or arity does not match the frozen matrix",
                aggregate.semantic_id, aggregate.pg_aggregate_oid
            )));
        }
    };

    to_daft_type(output_type)?;
    let bound = BoundAggExpr::try_new(aggregate_expr, input_schema)
        .map_err(|error| invalid(format!("PGDAFT_AGG_BIND_FAILED: {}", error)))?;
    let actual = bound
        .as_ref()
        .to_field(input_schema)
        .map_err(|error| invalid(format!("PGDAFT_AGG_BIND_FAILED: {error}")))?;
    if actual.name.as_ref() != aggregate.output.name || actual.dtype != to_daft_type(output_type)? {
        return Err(invalid(
            "PGDAFT_AGG_OUTPUT_INFERENCE: Daft aggregate field differs from the frozen PostgreSQL output"
                .to_string(),
        ));
    }
    Ok(bound)
}

fn checked_handle(kind: PgCheckedI64Kind) -> AggFnHandle {
    AggFnHandle::new(Arc::new(PgCheckedI64Agg { kind }))
}

fn aggregate_input_ref<'a>(
    expr: &Expr,
    input_fields: &'a [Field],
    owner: &str,
) -> Result<(usize, &'a Field), Box<DpgError>> {
    let Expr::InputRef { column_id, pg_type } = expr else {
        return Err(invalid(format!(
            "PGDAFT_AGG_ARGUMENT_EXPR: {owner} requires input_ref"
        )));
    };
    let mut matches = input_fields
        .iter()
        .enumerate()
        .filter(|(_, field)| field.column_id == *column_id);
    let Some((index, input)) = matches.next() else {
        return Err(invalid(format!(
            "PGDAFT_COLUMN_DANGLING: {owner} column_id {column_id} does not exist"
        )));
    };
    if matches.next().is_some() || &input.pg_type != pg_type {
        return Err(invalid(format!(
            "PGDAFT_INPUT_TYPE_MISMATCH: {owner} column_id {column_id} is ambiguous or has a different type"
        )));
    }
    Ok((index, input))
}

fn aggregate_bound_input(
    expr: &Expr,
    input_fields: &[Field],
    output_name: &str,
    owner: &str,
) -> Result<ExprRef, Box<DpgError>> {
    let (index, input) = aggregate_input_ref(expr, input_fields, owner)?;
    let field = DaftField::new(input.name.as_str(), to_daft_type(&input.pg_type)?);
    Ok(bound_col(index, field).alias(output_name))
}

fn require_aggregate_arg_oid(
    expr: &Expr,
    input_fields: &[Field],
    expected_oid: u32,
    owner: &str,
) -> Result<(), Box<DpgError>> {
    let (_, input) = aggregate_input_ref(expr, input_fields, owner)?;
    if input.pg_type.pg_oid != expected_oid {
        return Err(invalid(format!(
            "PGDAFT_AGG_ARGUMENT_TYPE: {owner} requires PostgreSQL type OID {expected_oid}, got {}",
            input.pg_type.pg_oid
        )));
    }
    Ok(())
}

fn invalid(message: String) -> Box<DpgError> {
    DpgError::new(DpgErrorCode::InvalidArg, message)
}
