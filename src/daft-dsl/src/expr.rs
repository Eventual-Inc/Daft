use daft_core::{
    count_mode::CountMode,
    datatypes::{try_mean_supertype, try_sum_supertype, DataType, Field, FieldID},
    schema::Schema,
    utils::supertype::try_get_supertype,
};

use crate::{
    functions::{
        function_display, function_semantic_id, scalar_function_semantic_id,
        sketch::{HashableVecPercentiles, SketchExpr},
        struct_::StructExpr,
        FunctionEvaluator, ScalarFunction,
    },
    lit,
    optimization::{get_required_columns, requires_computation},
};

use common_error::{DaftError, DaftResult};

use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    fmt::{Debug, Display, Formatter, Result},
    io::{self, Write},
    sync::Arc,
};

use super::functions::FunctionExpr;

pub type ExprRef = Arc<Expr>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Expr {
    Alias(ExprRef, Arc<str>),
    Agg(AggExpr),
    BinaryOp {
        op: Operator,
        left: ExprRef,
        right: ExprRef,
    },
    Cast(ExprRef, DataType),
    Column(Arc<str>),
    Function {
        func: FunctionExpr,
        inputs: Vec<ExprRef>,
    },
    Not(ExprRef),
    IsNull(ExprRef),
    NotNull(ExprRef),
    FillNull(ExprRef, ExprRef),
    IsIn(ExprRef, ExprRef),
    Between(ExprRef, ExprRef, ExprRef),
    Literal(lit::LiteralValue),
    IfElse {
        if_true: ExprRef,
        if_false: ExprRef,
        predicate: ExprRef,
    },
    ScalarFunction(ScalarFunction),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash, Eq)]
pub struct ApproxPercentileParams {
    pub child: ExprRef,
    pub percentiles: Vec<daft_core::utils::hashable_float_wrapper::FloatWrapper<f64>>,
    pub force_list_output: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AggExpr {
    Count(ExprRef, CountMode),
    Sum(ExprRef),
    ApproxSketch(ExprRef),
    ApproxPercentile(ApproxPercentileParams),
    MergeSketch(ExprRef),
    Mean(ExprRef),
    Min(ExprRef),
    Max(ExprRef),
    AnyValue(ExprRef, bool),
    List(ExprRef),
    Concat(ExprRef),
    MapGroups {
        func: FunctionExpr,
        inputs: Vec<ExprRef>,
    },
}

pub fn col<S: Into<Arc<str>>>(name: S) -> ExprRef {
    Expr::Column(name.into()).into()
}

pub fn binary_op(op: Operator, left: ExprRef, right: ExprRef) -> ExprRef {
    Expr::BinaryOp { op, left, right }.into()
}

impl AggExpr {
    pub fn name(&self) -> &str {
        use AggExpr::*;
        match self {
            Count(expr, ..)
            | Sum(expr)
            | ApproxSketch(expr)
            | ApproxPercentile(ApproxPercentileParams { child: expr, .. })
            | MergeSketch(expr)
            | Mean(expr)
            | Min(expr)
            | Max(expr)
            | AnyValue(expr, _)
            | List(expr)
            | Concat(expr) => expr.name(),
            MapGroups { func: _, inputs } => inputs.first().unwrap().name(),
        }
    }

    pub fn semantic_id(&self, schema: &Schema) -> FieldID {
        use AggExpr::*;
        match self {
            Count(expr, mode) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_count({mode})"))
            }
            Sum(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_sum()"))
            }
            ApproxSketch(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_approx_sketch()"))
            }
            ApproxPercentile(ApproxPercentileParams {
                child: expr,
                percentiles,
                force_list_output,
            }) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!(
                    "{child_id}.local_approx_percentiles(percentiles={:?},force_list_output={force_list_output})",
                    percentiles,
                ))
            }
            MergeSketch(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_merge_sketch()"))
            }
            Mean(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_mean()"))
            }
            Min(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_min()"))
            }
            Max(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_max()"))
            }
            AnyValue(expr, ignore_nulls) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!(
                    "{child_id}.local_any_value(ignore_nulls={ignore_nulls})"
                ))
            }
            List(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_list()"))
            }
            Concat(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_concat()"))
            }
            MapGroups { func, inputs } => function_semantic_id(func, inputs, schema),
        }
    }

    pub fn children(&self) -> Vec<ExprRef> {
        use AggExpr::*;
        match self {
            Count(expr, ..)
            | Sum(expr)
            | ApproxSketch(expr)
            | ApproxPercentile(ApproxPercentileParams { child: expr, .. })
            | MergeSketch(expr)
            | Mean(expr)
            | Min(expr)
            | Max(expr)
            | AnyValue(expr, _)
            | List(expr)
            | Concat(expr) => vec![expr.clone()],
            MapGroups { func: _, inputs } => inputs.clone(),
        }
    }

    pub fn with_new_children(&self, children: Vec<ExprRef>) -> Self {
        use AggExpr::*;

        if let MapGroups { func: _, inputs } = &self {
            assert_eq!(children.len(), inputs.len());
        } else {
            assert_eq!(children.len(), 1);
        }
        match self {
            Count(_, count_mode) => Count(children[0].clone(), *count_mode),
            Sum(_) => Sum(children[0].clone()),
            Mean(_) => Mean(children[0].clone()),
            Min(_) => Min(children[0].clone()),
            Max(_) => Max(children[0].clone()),
            AnyValue(_, ignore_nulls) => AnyValue(children[0].clone(), *ignore_nulls),
            List(_) => List(children[0].clone()),
            Concat(_) => Concat(children[0].clone()),
            MapGroups { func, inputs: _ } => MapGroups {
                func: func.clone(),
                inputs: children,
            },
            ApproxPercentile(ApproxPercentileParams {
                percentiles,
                force_list_output,
                ..
            }) => ApproxPercentile(ApproxPercentileParams {
                child: children[0].clone(),
                percentiles: percentiles.clone(),
                force_list_output: *force_list_output,
            }),
            ApproxSketch(_) => ApproxSketch(children[0].clone()),
            MergeSketch(_) => MergeSketch(children[0].clone()),
        }
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        use AggExpr::*;
        match self {
            Count(expr, ..) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(field.name.as_str(), DataType::UInt64))
            }
            Sum(expr) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(
                    field.name.as_str(),
                    try_sum_supertype(&field.dtype)?,
                ))
            }
            ApproxSketch(expr) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(
                    field.name.as_str(),
                    match &field.dtype {
                        DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                        | DataType::Float32
                        | DataType::Float64 => DataType::from(&*daft_sketch::ARROW2_DDSKETCH_DTYPE),
                        other => {
                            return Err(DaftError::TypeError(format!(
                                "Expected input to approx_sketch() to be numeric but received dtype {} for column \"{}\"",
                                other, field.name,
                            )))
                        }
                    },
                ))
            }
            ApproxPercentile(ApproxPercentileParams {
                child: expr,
                percentiles,
                force_list_output,
            }) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(
                    field.name.as_str(),
                    match &field.dtype {
                        dt if dt.is_numeric() => if percentiles.len() > 1 || *force_list_output {
                            DataType::FixedSizeList(Box::new(DataType::Float64), percentiles.len())
                        } else {
                            DataType::Float64
                        },
                        other => {
                            return Err(DaftError::TypeError(format!(
                                "Expected input to approx_percentiles() to be numeric but received dtype {} for column \"{}\"",
                                other, field.name,
                            )))
                        }
                    },
                ))
            }
            MergeSketch(expr) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(
                  field.name.as_str(),
                  match &field.dtype {
                      DataType::Struct(fields) => DataType::Struct(fields.clone()),
                      other => {
                          return Err(DaftError::TypeError(format!(
                              "Expected input to merge_sketch() to be struct but received dtype {} for column \"{}\"",
                              other, field.name,
                          )))
                        }
                    },
                ))
            }
            Mean(expr) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(
                    field.name.as_str(),
                    try_mean_supertype(&field.dtype)?,
                ))
            }
            Min(expr) | Max(expr) | AnyValue(expr, _) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(field.name.as_str(), field.dtype))
            }
            List(expr) => expr.to_field(schema)?.to_list_field(),
            Concat(expr) => {
                let field = expr.to_field(schema)?;
                match field.dtype {
                    DataType::List(..) => Ok(field),
                    #[cfg(feature = "python")]
                    DataType::Python => Ok(field),
                    _ => Err(DaftError::TypeError(format!(
                        "We can only perform List Concat Agg on List or Python Types, got dtype {} for column \"{}\"",
                        field.dtype, field.name
                    ))),
                }
            }
            MapGroups { func, inputs } => func.to_field(inputs.as_slice(), schema, func),
        }
    }

    pub fn from_name_and_child_expr(name: &str, child: ExprRef) -> DaftResult<AggExpr> {
        use AggExpr::*;
        match name {
            "count" => Ok(Count(child, CountMode::Valid)),
            "sum" => Ok(Sum(child)),
            "mean" => Ok(Mean(child)),
            "min" => Ok(Min(child)),
            "max" => Ok(Max(child)),
            "list" => Ok(List(child)),
            _ => Err(DaftError::ValueError(format!(
                "{} not a valid aggregation name",
                name
            ))),
        }
    }
}

impl From<&AggExpr> for ExprRef {
    fn from(agg_expr: &AggExpr) -> Self {
        Arc::new(Expr::Agg(agg_expr.clone()))
    }
}

impl AsRef<Expr> for Expr {
    fn as_ref(&self) -> &Expr {
        self
    }
}

impl Expr {
    pub fn arced(self) -> ExprRef {
        Arc::new(self)
    }

    pub fn alias<S: Into<Arc<str>>>(self: &ExprRef, name: S) -> ExprRef {
        Expr::Alias(self.clone(), name.into()).into()
    }

    pub fn if_else(self: ExprRef, if_true: ExprRef, if_false: ExprRef) -> ExprRef {
        Expr::IfElse {
            if_true,
            if_false,
            predicate: self,
        }
        .into()
    }

    pub fn cast(self: ExprRef, dtype: &DataType) -> ExprRef {
        Expr::Cast(self, dtype.clone()).into()
    }

    pub fn count(self: ExprRef, mode: CountMode) -> ExprRef {
        Expr::Agg(AggExpr::Count(self, mode)).into()
    }

    pub fn sum(self: ExprRef) -> ExprRef {
        Expr::Agg(AggExpr::Sum(self)).into()
    }

    pub fn approx_sketch(self: ExprRef) -> ExprRef {
        Expr::Agg(AggExpr::ApproxSketch(self)).into()
    }

    pub fn approx_percentiles(
        self: ExprRef,
        percentiles: &[f64],
        force_list_output: bool,
    ) -> ExprRef {
        Expr::Agg(AggExpr::ApproxPercentile(ApproxPercentileParams {
            child: self,
            percentiles: percentiles
                .iter()
                .map(|f| daft_core::utils::hashable_float_wrapper::FloatWrapper(*f))
                .collect(),
            force_list_output,
        }))
        .into()
    }

    pub fn sketch_percentile(
        self: ExprRef,
        percentiles: &[f64],
        force_list_output: bool,
    ) -> ExprRef {
        Expr::Function {
            func: FunctionExpr::Sketch(SketchExpr::Percentile {
                percentiles: HashableVecPercentiles(percentiles.to_vec()),
                force_list_output,
            }),
            inputs: vec![self],
        }
        .into()
    }

    pub fn merge_sketch(self: ExprRef) -> ExprRef {
        Expr::Agg(AggExpr::MergeSketch(self)).into()
    }

    pub fn mean(self: ExprRef) -> ExprRef {
        Expr::Agg(AggExpr::Mean(self)).into()
    }

    pub fn min(self: ExprRef) -> ExprRef {
        Expr::Agg(AggExpr::Min(self)).into()
    }

    pub fn max(self: ExprRef) -> ExprRef {
        Expr::Agg(AggExpr::Max(self)).into()
    }

    pub fn any_value(self: ExprRef, ignore_nulls: bool) -> ExprRef {
        Expr::Agg(AggExpr::AnyValue(self, ignore_nulls)).into()
    }

    pub fn agg_list(self: ExprRef) -> ExprRef {
        Expr::Agg(AggExpr::List(self)).into()
    }

    pub fn agg_concat(self: ExprRef) -> ExprRef {
        Expr::Agg(AggExpr::Concat(self)).into()
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(self: ExprRef) -> ExprRef {
        Expr::Not(self).into()
    }

    pub fn is_null(self: ExprRef) -> ExprRef {
        Expr::IsNull(self).into()
    }

    pub fn not_null(self: ExprRef) -> ExprRef {
        Expr::NotNull(self).into()
    }

    pub fn fill_null(self: ExprRef, fill_value: ExprRef) -> ExprRef {
        Expr::FillNull(self, fill_value).into()
    }

    pub fn is_in(self: ExprRef, items: ExprRef) -> ExprRef {
        Expr::IsIn(self, items).into()
    }

    pub fn between(self: ExprRef, lower: ExprRef, upper: ExprRef) -> ExprRef {
        Expr::Between(self, lower, upper).into()
    }

    pub fn eq(self: ExprRef, other: ExprRef) -> ExprRef {
        binary_op(Operator::Eq, self, other)
    }

    pub fn not_eq(self: ExprRef, other: ExprRef) -> ExprRef {
        binary_op(Operator::NotEq, self, other)
    }

    pub fn and(self: ExprRef, other: ExprRef) -> ExprRef {
        binary_op(Operator::And, self, other)
    }

    pub fn or(self: ExprRef, other: ExprRef) -> ExprRef {
        binary_op(Operator::Or, self, other)
    }

    pub fn lt(self: ExprRef, other: ExprRef) -> ExprRef {
        binary_op(Operator::Lt, self, other)
    }

    pub fn lt_eq(self: ExprRef, other: ExprRef) -> ExprRef {
        binary_op(Operator::LtEq, self, other)
    }

    pub fn gt(self: ExprRef, other: ExprRef) -> ExprRef {
        binary_op(Operator::Gt, self, other)
    }

    pub fn gt_eq(self: ExprRef, other: ExprRef) -> ExprRef {
        binary_op(Operator::GtEq, self, other)
    }

    pub fn semantic_id(&self, schema: &Schema) -> FieldID {
        use Expr::*;
        match self {
            // Base case - anonymous column reference.
            // Look up the column name in the provided schema and get its field ID.
            Column(name) => FieldID::new(&**name),

            // Base case - literal.
            Literal(value) => FieldID::new(format!("Literal({value:?})")),

            // Recursive cases.
            Cast(expr, dtype) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.cast({dtype})"))
            }
            Not(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.not()"))
            }
            IsNull(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.is_null()"))
            }
            NotNull(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.not_null()"))
            }
            FillNull(expr, fill_value) => {
                let child_id = expr.semantic_id(schema);
                let fill_value_id = fill_value.semantic_id(schema);
                FieldID::new(format!("{child_id}.fill_null({fill_value_id})"))
            }
            IsIn(expr, items) => {
                let child_id = expr.semantic_id(schema);
                let items_id = items.semantic_id(schema);
                FieldID::new(format!("{child_id}.is_in({items_id})"))
            }
            Between(expr, lower, upper) => {
                let child_id = expr.semantic_id(schema);
                let lower_id = lower.semantic_id(schema);
                let upper_id = upper.semantic_id(schema);
                FieldID::new(format!("{child_id}.between({lower_id},{upper_id})"))
            }
            Function { func, inputs } => function_semantic_id(func, inputs, schema),
            BinaryOp { op, left, right } => {
                let left_id = left.semantic_id(schema);
                let right_id = right.semantic_id(schema);
                // TODO: check for symmetry here.
                FieldID::new(format!("({left_id} {op} {right_id})"))
            }

            IfElse {
                if_true,
                if_false,
                predicate,
            } => {
                let if_true = if_true.semantic_id(schema);
                let if_false = if_false.semantic_id(schema);
                let predicate = predicate.semantic_id(schema);
                FieldID::new(format!("({if_true} if {predicate} else {if_false})"))
            }

            // Alias: ID does not change.
            Alias(expr, ..) => expr.semantic_id(schema),

            // Agg: Separate path.
            Agg(agg_expr) => agg_expr.semantic_id(schema),
            ScalarFunction(sf) => scalar_function_semantic_id(sf, schema),
        }
    }

    pub fn children(&self) -> Vec<ExprRef> {
        use Expr::*;
        match self {
            // No children.
            Column(..) => vec![],
            Literal(..) => vec![],

            // One child.
            Not(expr) | IsNull(expr) | NotNull(expr) | Cast(expr, ..) | Alias(expr, ..) => {
                vec![expr.clone()]
            }
            Agg(agg_expr) => agg_expr.children(),

            // Multiple children.
            Function { inputs, .. } => inputs.clone(),
            BinaryOp { left, right, .. } => {
                vec![left.clone(), right.clone()]
            }
            IsIn(expr, items) => vec![expr.clone(), items.clone()],
            Between(expr, lower, upper) => vec![expr.clone(), lower.clone(), upper.clone()],
            IfElse {
                if_true,
                if_false,
                predicate,
            } => {
                vec![if_true.clone(), if_false.clone(), predicate.clone()]
            }
            FillNull(expr, fill_value) => vec![expr.clone(), fill_value.clone()],
            ScalarFunction(sf) => sf.inputs.clone(),
        }
    }

    pub(super) fn with_new_children(&self, children: Vec<ExprRef>) -> Expr {
        use Expr::*;
        match self {
            // no children
            Column(..) | Literal(..) => self.clone(),
            // 1 child
            Not(..) => Not(children.first().expect("Should have 1 child").clone()),
            Alias(.., name) => Alias(
                children.first().expect("Should have 1 child").clone(),
                name.clone(),
            ),
            IsNull(..) => IsNull(children.first().expect("Should have 1 child").clone()),
            NotNull(..) => NotNull(children.first().expect("Should have 1 child").clone()),
            Cast(.., dtype) => Cast(
                children.first().expect("Should have 1 child").clone(),
                dtype.clone(),
            ),
            // 2 children
            BinaryOp { op, .. } => BinaryOp {
                op: *op,
                left: children.first().expect("Should have 1 child").clone(),
                right: children.get(1).expect("Should have 2 child").clone(),
            },
            IsIn(..) => IsIn(
                children.first().expect("Should have 1 child").clone(),
                children.get(1).expect("Should have 2 child").clone(),
            ),
            Between(..) => Between(
                children.first().expect("Should have 1 child").clone(),
                children.get(1).expect("Should have 2 child").clone(),
                children.get(2).expect("Should have 3 child").clone(),
            ),
            FillNull(..) => FillNull(
                children.first().expect("Should have 1 child").clone(),
                children.get(1).expect("Should have 2 child").clone(),
            ),
            // ternary
            IfElse { .. } => IfElse {
                if_true: children.first().expect("Should have 1 child").clone(),
                if_false: children.get(1).expect("Should have 2 child").clone(),
                predicate: children.get(2).expect("Should have 3 child").clone(),
            },
            // N-ary
            Agg(agg_expr) => Agg(agg_expr.with_new_children(children)),
            Function { func, .. } => Function {
                func: func.clone(),
                inputs: children,
            },
            ScalarFunction(sf) => ScalarFunction(sf.clone()),
        }
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        use Expr::*;
        match self {
            Alias(expr, name) => Ok(Field::new(name.as_ref(), expr.get_type(schema)?)),
            Agg(agg_expr) => agg_expr.to_field(schema),
            Cast(expr, dtype) => Ok(Field::new(expr.name(), dtype.clone())),
            Column(name) => Ok(schema.get_field(name).cloned()?),
            Not(expr) => {
                let child_field = expr.to_field(schema)?;
                match child_field.dtype {
                    DataType::Boolean => Ok(Field::new(expr.name(), DataType::Boolean)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expected argument to be a Boolean expression, but received {child_field}",
                    ))),
                }
            }
            IsNull(expr) => Ok(Field::new(expr.name(), DataType::Boolean)),
            NotNull(expr) => Ok(Field::new(expr.name(), DataType::Boolean)),
            FillNull(expr, fill_value) => {
                let expr_field = expr.to_field(schema)?;
                let fill_value_field = fill_value.to_field(schema)?;
                match try_get_supertype(&expr_field.dtype, &fill_value_field.dtype) {
                    Ok(supertype) => Ok(Field::new(expr_field.name.as_str(), supertype)),
                    Err(_) => Err(DaftError::TypeError(format!(
                        "Expected expr and fill_value arguments for fill_null to be castable to the same supertype, but received {expr_field} and {fill_value_field}",
                    )))
                }
            }
            IsIn(left, right) => {
                let left_field = left.to_field(schema)?;
                let right_field = right.to_field(schema)?;
                let (result_type, _intermediate, _comp_type) =
                    left_field.dtype.membership_op(&right_field.dtype)?;
                Ok(Field::new(left_field.name.as_str(), result_type))
            }
            Between(value, lower, upper) => {
                let value_field = value.to_field(schema)?;
                let lower_field = lower.to_field(schema)?;
                let upper_field = upper.to_field(schema)?;
                let (lower_result_type, _intermediate, _comp_type) =
                    value_field.dtype.membership_op(&lower_field.dtype)?;
                let (upper_result_type, _intermediate, _comp_type) =
                    value_field.dtype.membership_op(&upper_field.dtype)?;
                let (result_type, _intermediate, _comp_type) =
                    lower_result_type.membership_op(&upper_result_type)?;
                Ok(Field::new(value_field.name.as_str(), result_type))
            }
            Literal(value) => Ok(Field::new("literal", value.get_type())),
            Function { func, inputs } => func.to_field(inputs.as_slice(), schema, func),
            ScalarFunction(sf) => sf.to_field(schema),

            BinaryOp { op, left, right } => {
                let left_field = left.to_field(schema)?;
                let right_field = right.to_field(schema)?;

                match op {
                    // Logical operations
                    Operator::And | Operator::Or | Operator::Xor => {
                        let result_type = left_field.dtype.logical_op(&right_field.dtype)?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }

                    // Comparison operations
                    Operator::Lt
                    | Operator::Gt
                    | Operator::Eq
                    | Operator::NotEq
                    | Operator::LtEq
                    | Operator::GtEq => {
                        let (result_type, _intermediate, _comp_type) =
                            left_field.dtype.comparison_op(&right_field.dtype)?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }

                    // Arithmetic operations
                    Operator::Plus => {
                        let result_type = (&left_field.dtype + &right_field.dtype)?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::Minus => {
                        let result_type = (&left_field.dtype - &right_field.dtype)?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::Multiply => {
                        let result_type = (&left_field.dtype * &right_field.dtype)?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::TrueDivide => {
                        let result_type = (&left_field.dtype / &right_field.dtype)?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::Modulus => {
                        let result_type = (&left_field.dtype % &right_field.dtype)?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::ShiftLeft => {
                        let result_type = (&left_field.dtype << &right_field.dtype)?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::ShiftRight => {
                        let result_type = (&left_field.dtype >> &right_field.dtype)?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::FloorDivide => {
                        unimplemented!()
                    }
                }
            }
            IfElse {
                if_true,
                if_false,
                predicate,
            } => {
                let predicate_field = predicate.to_field(schema)?;
                if predicate_field.dtype != DataType::Boolean {
                    return Err(DaftError::TypeError(format!(
                        "Expected predicate for if_else to be boolean but received {predicate_field}",
                    )));
                }
                match predicate.as_ref() {
                    Expr::Literal(lit::LiteralValue::Boolean(true)) => if_true.to_field(schema),
                    Expr::Literal(lit::LiteralValue::Boolean(false)) => {
                        Ok(if_false.to_field(schema)?.rename(if_true.name()))
                    }
                    _ => {
                        let if_true_field = if_true.to_field(schema)?;
                        let if_false_field = if_false.to_field(schema)?;
                        match try_get_supertype(&if_true_field.dtype, &if_false_field.dtype) {
                            Ok(supertype) => Ok(Field::new(if_true_field.name, supertype)),
                            Err(_) => Err(DaftError::TypeError(format!("Expected if_true and if_false arguments for if_else to be castable to the same supertype, but received {if_true_field} and {if_false_field}")))
                        }
                    }
                }
            }
        }
    }

    pub fn name(&self) -> &str {
        use Expr::*;
        match self {
            Alias(.., name) => name.as_ref(),
            Agg(agg_expr) => agg_expr.name(),
            Cast(expr, ..) => expr.name(),
            Column(name) => name.as_ref(),
            Not(expr) => expr.name(),
            IsNull(expr) => expr.name(),
            NotNull(expr) => expr.name(),
            FillNull(expr, ..) => expr.name(),
            IsIn(expr, ..) => expr.name(),
            Between(expr, ..) => expr.name(),
            Literal(..) => "literal",
            Function { func, inputs } => match func {
                FunctionExpr::Struct(StructExpr::Get(name)) => name,
                _ => inputs.first().unwrap().name(),
            },
            ScalarFunction(func) => func.inputs.first().unwrap().name(),
            BinaryOp {
                op: _,
                left,
                right: _,
            } => left.name(),
            IfElse { if_true, .. } => if_true.name(),
        }
    }

    pub fn get_type(&self, schema: &Schema) -> DaftResult<DataType> {
        Ok(self.to_field(schema)?.dtype)
    }

    pub fn input_mapping(self: &Arc<Self>) -> Option<String> {
        let required_columns = get_required_columns(self);
        let requires_computation = requires_computation(self);

        // Return the required column only if:
        //   1. There is only one required column
        //   2. No computation is run on this required column
        match (&required_columns[..], requires_computation) {
            ([required_col], false) => Some(required_col.to_string()),
            _ => None,
        }
    }

    pub fn to_sql(&self) -> Option<String> {
        fn to_sql_inner<W: Write>(expr: &Expr, buffer: &mut W) -> io::Result<()> {
            match expr {
                Expr::Column(name) => write!(buffer, "{}", name),
                Expr::Literal(lit) => lit.display_sql(buffer),
                Expr::Alias(inner, ..) => to_sql_inner(inner, buffer),
                Expr::BinaryOp { op, left, right } => {
                    to_sql_inner(left, buffer)?;
                    let op = match op {
                        Operator::Eq => "=",
                        Operator::NotEq => "!=",
                        Operator::Lt => "<",
                        Operator::LtEq => "<=",
                        Operator::Gt => ">",
                        Operator::GtEq => ">=",
                        Operator::And => "AND",
                        Operator::Or => "OR",
                        Operator::ShiftLeft => "<<",
                        Operator::ShiftRight => ">>",
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                "Unsupported operator for SQL translation",
                            ))
                        }
                    };
                    write!(buffer, " {} ", op)?;
                    to_sql_inner(right, buffer)
                }
                Expr::Not(inner) => {
                    write!(buffer, "NOT (")?;
                    to_sql_inner(inner, buffer)?;
                    write!(buffer, ")")
                }
                Expr::IsNull(inner) => {
                    write!(buffer, "(")?;
                    to_sql_inner(inner, buffer)?;
                    write!(buffer, ") IS NULL")
                }
                Expr::NotNull(inner) => {
                    write!(buffer, "(")?;
                    to_sql_inner(inner, buffer)?;
                    write!(buffer, ") IS NOT NULL")
                }
                Expr::IfElse {
                    if_true,
                    if_false,
                    predicate,
                } => {
                    write!(buffer, "CASE WHEN ")?;
                    to_sql_inner(predicate, buffer)?;
                    write!(buffer, " THEN ")?;
                    to_sql_inner(if_true, buffer)?;
                    write!(buffer, " ELSE ")?;
                    to_sql_inner(if_false, buffer)?;
                    write!(buffer, " END")
                }
                // TODO: Implement SQL translations for these expressions if possible
                Expr::Agg(..)
                | Expr::Cast(..)
                | Expr::IsIn(..)
                | Expr::Between(..)
                | Expr::Function { .. }
                | Expr::FillNull(..)
                | Expr::ScalarFunction { .. } => Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unsupported expression for SQL translation",
                )),
            }
        }

        let mut buffer = Vec::new();
        to_sql_inner(self, &mut buffer)
            .ok()
            .and_then(|_| String::from_utf8(buffer).ok())
    }
}

impl Display for Expr {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        use Expr::*;
        match self {
            Alias(expr, name) => write!(f, "{expr} AS {name}"),
            Agg(agg_expr) => write!(f, "{agg_expr}"),
            BinaryOp { op, left, right } => {
                let write_out_expr = |f: &mut Formatter, input: &Expr| match input {
                    Alias(e, _) => write!(f, "{e}"),
                    BinaryOp { .. } => write!(f, "[{input}]"),
                    _ => write!(f, "{input}"),
                };

                write_out_expr(f, left)?;
                write!(f, " {op} ")?;
                write_out_expr(f, right)?;
                Ok(())
            }
            Cast(expr, dtype) => write!(f, "cast({expr} AS {dtype})"),
            Column(name) => write!(f, "col({name})"),
            Not(expr) => write!(f, "not({expr})"),
            IsNull(expr) => write!(f, "is_null({expr})"),
            NotNull(expr) => write!(f, "not_null({expr})"),
            FillNull(expr, fill_value) => write!(f, "fill_null({expr}, {fill_value})"),
            IsIn(expr, items) => write!(f, "{expr} in {items}"),
            Between(expr, lower, upper) => write!(f, "{expr} in [{lower},{upper}]"),
            Literal(val) => write!(f, "lit({val})"),
            Function { func, inputs } => function_display(f, func, inputs),
            ScalarFunction(func) => write!(f, "{func}"),

            IfElse {
                if_true,
                if_false,
                predicate,
            } => {
                write!(f, "if [{predicate}] then [{if_true}] else [{if_false}]")
            }
        }
    }
}

impl Display for AggExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        use AggExpr::*;
        match self {
            Count(expr, mode) => write!(f, "count({expr}, {mode})"),
            Sum(expr) => write!(f, "sum({expr})"),
            ApproxSketch(expr) => write!(f, "approx_sketch({expr})"),
            ApproxPercentile(ApproxPercentileParams { child, percentiles, force_list_output }) => write!(
                f,
                "approx_percentiles({child}, percentiles={percentiles:?}, force_list_output={force_list_output})"
            ),
            MergeSketch(expr) => write!(f, "merge_sketch({expr})"),
            Mean(expr) => write!(f, "mean({expr})"),
            Min(expr) => write!(f, "min({expr})"),
            Max(expr) => write!(f, "max({expr})"),
            AnyValue(expr, ignore_nulls) => {
                write!(f, "any_value({expr}, ignore_nulls={ignore_nulls})")
            }
            List(expr) => write!(f, "list({expr})"),
            Concat(expr) => write!(f, "list({expr})"),
            MapGroups { func, inputs } => function_display(f, func, inputs),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    TrueDivide,
    FloorDivide,
    Modulus,
    And,
    Or,
    Xor,
    ShiftLeft,
    ShiftRight,
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Operator::*;
        let tkn = match self {
            Eq => "==",
            NotEq => "!=",
            Lt => "<",
            LtEq => "<=",
            Gt => ">",
            GtEq => ">=",
            Plus => "+",
            Minus => "-",
            Multiply => "*",
            TrueDivide => "/",
            FloorDivide => "//",
            Modulus => "%",
            And => "&",
            Or => "|",
            Xor => "^",
            ShiftLeft => "<<",
            ShiftRight => ">>",
        };
        write!(f, "{tkn}")
    }
}

impl Operator {
    #![allow(dead_code)]
    pub(crate) fn is_comparison(&self) -> bool {
        matches!(
            self,
            Self::Eq
                | Self::NotEq
                | Self::Lt
                | Self::LtEq
                | Self::Gt
                | Self::GtEq
                | Self::And
                | Self::Or
                | Self::Xor
        )
    }

    pub(crate) fn is_arithmetic(&self) -> bool {
        !(self.is_comparison())
    }
}

/// Converts an expression with syntactic sugar into struct gets.
/// Does left-associative parsing to to resolve ambiguity.
///
/// For example, if col("a.b.c") could be interpreted as either col("a.b").struct.get("c")
/// or col("a").struct.get("b.c"), this function will resolve it to col("a.b").struct.get("c").
fn substitute_expr_getter_sugar(expr: ExprRef, schema: &Schema) -> DaftResult<ExprRef> {
    use common_treenode::{Transformed, TransformedResult, TreeNode};

    #[derive(PartialEq, Eq)]
    struct BfsState<'a> {
        name: String,
        expr: ExprRef,
        field: &'a Field,
    }

    impl Ord for BfsState<'_> {
        fn cmp(&self, other: &Self) -> Ordering {
            self.name.cmp(&other.name)
        }
    }

    impl PartialOrd for BfsState<'_> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut pq: BinaryHeap<BfsState> = BinaryHeap::new();

    for field in schema.fields.values() {
        pq.push(BfsState {
            name: field.name.clone(),
            expr: Arc::new(Expr::Column(field.name.clone().into())),
            field,
        });
    }

    let mut str_to_get_expr: HashMap<String, ExprRef> = HashMap::new();

    while let Some(BfsState { name, expr, field }) = pq.pop() {
        if !str_to_get_expr.contains_key(&name) {
            str_to_get_expr.insert(name.clone(), expr.clone());
        }

        if let DataType::Struct(children) = &field.dtype {
            for child in children {
                pq.push(BfsState {
                    name: format!("{}.{}", name, child.name),
                    expr: crate::functions::struct_::get(expr.clone(), &child.name),
                    field: child,
                });
            }
        }
    }

    expr.transform(|e| match e.as_ref() {
        Expr::Column(name) => str_to_get_expr
            .get(name.as_ref())
            .ok_or(DaftError::ValueError(format!(
                "Column not found in schema: {name}"
            )))
            .map(|get_expr| match get_expr.as_ref() {
                Expr::Column(_) => Transformed::no(e),
                _ => Transformed::yes(get_expr.clone()),
            }),
        _ => Ok(Transformed::no(e)),
    })
    .data()
}

fn expr_has_agg(expr: &ExprRef) -> bool {
    use Expr::*;

    match expr.as_ref() {
        Agg(_) => true,
        Column(_) | Literal(_) => false,
        Alias(e, _) | Cast(e, _) | Not(e) | IsNull(e) | NotNull(e) => expr_has_agg(e),
        BinaryOp { left, right, .. } => expr_has_agg(left) || expr_has_agg(right),
        Function { inputs, .. } => inputs.iter().any(expr_has_agg),
        ScalarFunction(func) => func.inputs.iter().any(expr_has_agg),
        IsIn(l, r) | FillNull(l, r) => expr_has_agg(l) || expr_has_agg(r),
        Between(v, l, u) => expr_has_agg(v) || expr_has_agg(l) || expr_has_agg(u),
        IfElse {
            if_true,
            if_false,
            predicate,
        } => expr_has_agg(if_true) || expr_has_agg(if_false) || expr_has_agg(predicate),
    }
}

fn extract_agg_expr(expr: &Expr) -> DaftResult<AggExpr> {
    use Expr::*;

    match expr {
        Agg(agg_expr) => Ok(agg_expr.clone()),
        Function { func, inputs } => Ok(AggExpr::MapGroups {
            func: func.clone(),
            inputs: inputs.clone(),
        }),
        Alias(e, name) => extract_agg_expr(e).map(|agg_expr| {
            use AggExpr::*;

            // reorder expressions so that alias goes before agg
            match agg_expr {
                Count(e, count_mode) => Count(Alias(e, name.clone()).into(), count_mode),
                Sum(e) => Sum(Alias(e, name.clone()).into()),
                ApproxSketch(e) => ApproxSketch(Alias(e, name.clone()).into()),
                ApproxPercentile(ApproxPercentileParams {
                    child: e,
                    percentiles,
                    force_list_output,
                }) => ApproxPercentile(ApproxPercentileParams {
                    child: Alias(e, name.clone()).into(),
                    percentiles,
                    force_list_output,
                }),
                MergeSketch(e) => MergeSketch(Alias(e, name.clone()).into()),
                Mean(e) => Mean(Alias(e, name.clone()).into()),
                Min(e) => Min(Alias(e, name.clone()).into()),
                Max(e) => Max(Alias(e, name.clone()).into()),
                AnyValue(e, ignore_nulls) => AnyValue(Alias(e, name.clone()).into(), ignore_nulls),
                List(e) => List(Alias(e, name.clone()).into()),
                Concat(e) => Concat(Alias(e, name.clone()).into()),
                MapGroups { func, inputs } => MapGroups {
                    func,
                    inputs: inputs
                        .into_iter()
                        .map(|input| input.alias(name.clone()))
                        .collect(),
                },
            }
        }),
        // TODO(Kevin): Support a mix of aggregation and non-aggregation expressions
        // as long as the final value always has a cardinality of 1.
        _ => Err(DaftError::ValueError(format!(
            "Expected aggregation expression, but got: {expr}"
        ))),
    }
}

/// Resolves and validates the expression with a schema, returning the new expression and its field.
pub fn resolve_expr(expr: ExprRef, schema: &Schema) -> DaftResult<(ExprRef, Field)> {
    // TODO(Kevin): Support aggregation expressions everywhere
    if expr_has_agg(&expr) {
        return Err(DaftError::ValueError(format!(
            "Aggregation expressions are currently only allowed in agg and pivot: {expr}\nIf you would like to have this feature, please see https://github.com/Eventual-Inc/Daft/issues/1979#issue-2170913383",
        )));
    }
    let resolved_expr = substitute_expr_getter_sugar(expr, schema)?;
    let resolved_field = resolved_expr.to_field(schema)?;
    Ok((resolved_expr, resolved_field))
}

pub fn resolve_exprs(
    exprs: Vec<ExprRef>,
    schema: &Schema,
) -> DaftResult<(Vec<ExprRef>, Vec<Field>)> {
    let resolved_iter = exprs.into_iter().map(|e| resolve_expr(e, schema));
    itertools::process_results(resolved_iter, |res| res.unzip())
}

/// Resolves and validates the expression with a schema, returning the extracted aggregation expression and its field.
pub fn resolve_aggexpr(expr: ExprRef, schema: &Schema) -> DaftResult<(AggExpr, Field)> {
    let agg_expr = extract_agg_expr(&expr)?;

    let has_nested_agg = agg_expr.children().iter().any(expr_has_agg);

    if has_nested_agg {
        return Err(DaftError::ValueError(format!(
            "Nested aggregation expressions are not supported: {expr}\nIf you would like to have this feature, please see https://github.com/Eventual-Inc/Daft/issues/1979#issue-2170913383"
        )));
    }

    let resolved_children = agg_expr
        .children()
        .into_iter()
        .map(|e| substitute_expr_getter_sugar(e, schema))
        .collect::<DaftResult<Vec<_>>>()?;
    let resolved_agg = agg_expr.with_new_children(resolved_children);
    let resolved_field = resolved_agg.to_field(schema)?;
    Ok((resolved_agg, resolved_field))
}

pub fn resolve_aggexprs(
    exprs: Vec<ExprRef>,
    schema: &Schema,
) -> DaftResult<(Vec<AggExpr>, Vec<Field>)> {
    let resolved_iter = exprs.into_iter().map(|e| resolve_aggexpr(e, schema));
    itertools::process_results(resolved_iter, |res| res.unzip())
}

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn check_comparison_type() -> DaftResult<()> {
        let x = lit(10.);
        let y = lit(12);
        let schema = Schema::empty();

        let z = Expr::BinaryOp {
            left: x,
            right: y,
            op: Operator::Lt,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Boolean);
        Ok(())
    }

    #[test]
    fn check_alias_type() -> DaftResult<()> {
        let a = col("a");
        let b = a.alias("b");
        match b.as_ref() {
            Expr::Alias(..) => Ok(()),
            other => Err(common_error::DaftError::ValueError(format!(
                "expected expression to be a alias, got {other:?}"
            ))),
        }
    }

    #[test]
    fn check_arithmetic_type() -> DaftResult<()> {
        let x = lit(10.);
        let y = lit(12);
        let schema = Schema::empty();

        let z = Expr::BinaryOp {
            left: x,
            right: y,
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        let x = lit(10.);
        let y = lit(12);

        let z = Expr::BinaryOp {
            left: y,
            right: x,
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        Ok(())
    }

    #[test]
    fn check_arithmetic_type_with_columns() -> DaftResult<()> {
        let x = col("x");
        let y = col("y");
        let schema = Schema::new(vec![
            Field::new("x", DataType::Float64),
            Field::new("y", DataType::Int64),
        ])?;

        let z = Expr::BinaryOp {
            left: x,
            right: y,
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        let x = col("x");
        let y = col("y");

        let z = Expr::BinaryOp {
            left: y,
            right: x,
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        Ok(())
    }

    #[test]
    fn test_substitute_expr_getter_sugar() -> DaftResult<()> {
        use crate::functions::struct_::get as struct_get;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)])?);

        assert_eq!(substitute_expr_getter_sugar(col("a"), &schema)?, col("a"));
        assert!(substitute_expr_getter_sugar(col("a.b"), &schema).is_err());
        assert!(matches!(
            substitute_expr_getter_sugar(col("a.b"), &schema).unwrap_err(),
            DaftError::ValueError(..)
        ));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Struct(vec![Field::new("b", DataType::Int64)]),
        )])?);

        assert_eq!(substitute_expr_getter_sugar(col("a"), &schema)?, col("a"));
        assert_eq!(
            substitute_expr_getter_sugar(col("a.b"), &schema)?,
            struct_get(col("a"), "b")
        );
        assert_eq!(
            substitute_expr_getter_sugar(col("a.b").alias("c"), &schema)?,
            struct_get(col("a"), "b").alias("c")
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Struct(vec![Field::new(
                "b",
                DataType::Struct(vec![Field::new("c", DataType::Int64)]),
            )]),
        )])?);

        assert_eq!(
            substitute_expr_getter_sugar(col("a.b"), &schema)?,
            struct_get(col("a"), "b")
        );
        assert_eq!(
            substitute_expr_getter_sugar(col("a.b.c"), &schema)?,
            struct_get(struct_get(col("a"), "b"), "c")
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "a",
                DataType::Struct(vec![Field::new(
                    "b",
                    DataType::Struct(vec![Field::new("c", DataType::Int64)]),
                )]),
            ),
            Field::new("a.b", DataType::Int64),
        ])?);

        assert_eq!(
            substitute_expr_getter_sugar(col("a.b"), &schema)?,
            col("a.b")
        );
        assert_eq!(
            substitute_expr_getter_sugar(col("a.b.c"), &schema)?,
            struct_get(struct_get(col("a"), "b"), "c")
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "a",
                DataType::Struct(vec![Field::new("b.c", DataType::Int64)]),
            ),
            Field::new(
                "a.b",
                DataType::Struct(vec![Field::new("c", DataType::Int64)]),
            ),
        ])?);

        assert_eq!(
            substitute_expr_getter_sugar(col("a.b.c"), &schema)?,
            struct_get(col("a.b"), "c")
        );

        Ok(())
    }
}
