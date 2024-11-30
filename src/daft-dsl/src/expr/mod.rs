#[cfg(test)]
mod tests;

use std::{
    io::{self, Write},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_hashable_float_wrapper::FloatWrapper;
use common_treenode::TreeNode;
use daft_core::{
    datatypes::{
        try_mean_aggregation_supertype, try_stddev_aggregation_supertype, try_sum_supertype,
        InferDataType,
    },
    prelude::*,
    utils::supertype::try_get_supertype,
};
use derive_more::Display;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::functions::FunctionExpr;
use crate::{
    functions::{
        binary_op_display_without_formatter, function_display_without_formatter,
        function_semantic_id, is_in_display_without_formatter,
        python::PythonUDF,
        scalar_function_semantic_id,
        sketch::{HashableVecPercentiles, SketchExpr},
        struct_::StructExpr,
        FunctionEvaluator, ScalarFunction,
    },
    lit,
    optimization::{get_required_columns, requires_computation},
};

pub trait SubqueryPlan: std::fmt::Debug + std::fmt::Display + Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
    fn name(&self) -> &'static str;
    fn schema(&self) -> SchemaRef;
}

#[derive(Display, Debug, Clone)]
pub struct Subquery {
    pub plan: Arc<dyn SubqueryPlan>,
}

impl Subquery {
    pub fn new<T: SubqueryPlan + 'static>(plan: T) -> Self {
        Self {
            plan: Arc::new(plan),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }
    pub fn name(&self) -> &'static str {
        self.plan.name()
    }
}

impl Serialize for Subquery {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("Subquery cannot be serialized"))
    }
}

impl<'de> Deserialize<'de> for Subquery {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom("Subquery cannot be deserialized"))
    }
}

impl PartialEq for Subquery {
    fn eq(&self, other: &Self) -> bool {
        self.plan.name() == other.plan.name() && self.plan.schema() == other.plan.schema()
    }
}

impl Eq for Subquery {}

impl std::hash::Hash for Subquery {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.plan.name().hash(state);
        self.plan.schema().hash(state);
    }
}

pub type ExprRef = Arc<Expr>;

#[derive(Display, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Expr {
    #[display("{_0} as {_1}")]
    Alias(ExprRef, Arc<str>),

    #[display("{_0}")]
    Agg(AggExpr),

    #[display("{}", binary_op_display_without_formatter(op, left, right)?)]
    BinaryOp {
        op: Operator,
        left: ExprRef,
        right: ExprRef,
    },

    #[display("cast({_0} as {_1})")]
    Cast(ExprRef, DataType),

    #[display("col({_0})")]
    Column(Arc<str>),

    #[display("{}", function_display_without_formatter(func, inputs)?)]
    Function {
        func: FunctionExpr,
        inputs: Vec<ExprRef>,
    },

    #[display("not({_0})")]
    Not(ExprRef),

    #[display("is_null({_0})")]
    IsNull(ExprRef),

    #[display("not_null({_0})")]
    NotNull(ExprRef),

    #[display("fill_null({_0}, {_1})")]
    FillNull(ExprRef, ExprRef),

    #[display("{}", is_in_display_without_formatter(_0, _1)?)]
    IsIn(ExprRef, Vec<ExprRef>),

    #[display("{_0} in [{_1},{_2}]")]
    Between(ExprRef, ExprRef, ExprRef),

    #[display("lit({_0})")]
    Literal(lit::LiteralValue),

    #[display("if [{predicate}] then [{if_true}] else [{if_false}]")]
    IfElse {
        if_true: ExprRef,
        if_false: ExprRef,
        predicate: ExprRef,
    },

    #[display("{_0}")]
    ScalarFunction(ScalarFunction),

    #[display("subquery {_0}")]
    Subquery(Subquery),
    #[display("{_0} in {_1}")]
    InSubquery(ExprRef, Subquery),
    #[display("exists {_0}")]
    Exists(Subquery),

    #[display("{_0}")]
    OuterReferenceColumn(OuterReferenceColumn),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash, Eq)]
pub struct ApproxPercentileParams {
    pub child: ExprRef,
    pub percentiles: Vec<FloatWrapper<f64>>,
    pub force_list_output: bool,
}

/// Reference to a qualified field in a parent query, used for correlated subqueries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash, Eq)]
pub struct OuterReferenceColumn {
    pub field: Field,
    /// The parent query that the column refers to, with depth=1 denoting the direct parent.
    pub depth: u64,
}

impl Display for OuterReferenceColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "outer_col({}, {})", self.field.name, self.depth)
    }
}

#[derive(Display, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AggExpr {
    #[display("count({_0}, {_1})")]
    Count(ExprRef, CountMode),

    #[display("sum({_0})")]
    Sum(ExprRef),

    #[display("approx_percentile({}, percentiles={:?}, force_list_output={})", _0.child, _0.percentiles, _0.force_list_output)]
    ApproxPercentile(ApproxPercentileParams),

    #[display("approx_count_distinct({_0})")]
    ApproxCountDistinct(ExprRef),

    #[display("approx_sketch({_0}, sketch_type={_1:?})")]
    ApproxSketch(ExprRef, SketchType),

    #[display("merge_sketch({_0}, sketch_type={_1:?})")]
    MergeSketch(ExprRef, SketchType),

    #[display("mean({_0})")]
    Mean(ExprRef),

    #[display("stddev({_0})")]
    Stddev(ExprRef),

    #[display("min({_0})")]
    Min(ExprRef),

    #[display("max({_0})")]
    Max(ExprRef),

    #[display("any_value({_0}, ignore_nulls={_1})")]
    AnyValue(ExprRef, bool),

    #[display("list({_0})")]
    List(ExprRef),

    #[display("list({_0})")]
    Concat(ExprRef),

    #[display("{}", function_display_without_formatter(func, inputs)?)]
    MapGroups {
        func: FunctionExpr,
        inputs: Vec<ExprRef>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SketchType {
    DDSketch,
    HyperLogLog,
}

pub fn col<S: Into<Arc<str>>>(name: S) -> ExprRef {
    Expr::Column(name.into()).into()
}

pub fn binary_op(op: Operator, left: ExprRef, right: ExprRef) -> ExprRef {
    Expr::BinaryOp { op, left, right }.into()
}

impl AggExpr {
    pub fn name(&self) -> &str {
        match self {
            Self::Count(expr, ..)
            | Self::Sum(expr)
            | Self::ApproxPercentile(ApproxPercentileParams { child: expr, .. })
            | Self::ApproxCountDistinct(expr)
            | Self::ApproxSketch(expr, _)
            | Self::MergeSketch(expr, _)
            | Self::Mean(expr)
            | Self::Stddev(expr)
            | Self::Min(expr)
            | Self::Max(expr)
            | Self::AnyValue(expr, _)
            | Self::List(expr)
            | Self::Concat(expr) => expr.name(),
            Self::MapGroups { func: _, inputs } => inputs.first().unwrap().name(),
        }
    }

    pub fn semantic_id(&self, schema: &Schema) -> FieldID {
        match self {
            Self::Count(expr, mode) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_count({mode})"))
            }
            Self::Sum(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_sum()"))
            }
            Self::ApproxPercentile(ApproxPercentileParams {
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
            Self::ApproxCountDistinct(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_approx_count_distinct()"))
            }
            Self::ApproxSketch(expr, sketch_type) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!(
                    "{child_id}.local_approx_sketch(sketch_type={sketch_type:?})"
                ))
            }
            Self::MergeSketch(expr, sketch_type) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!(
                    "{child_id}.local_merge_sketch(sketch_type={sketch_type:?})"
                ))
            }
            Self::Mean(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_mean()"))
            }
            Self::Stddev(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_stddev()"))
            }
            Self::Min(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_min()"))
            }
            Self::Max(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_max()"))
            }
            Self::AnyValue(expr, ignore_nulls) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!(
                    "{child_id}.local_any_value(ignore_nulls={ignore_nulls})"
                ))
            }
            Self::List(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_list()"))
            }
            Self::Concat(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_concat()"))
            }
            Self::MapGroups { func, inputs } => function_semantic_id(func, inputs, schema),
        }
    }

    pub fn children(&self) -> Vec<ExprRef> {
        match self {
            Self::Count(expr, ..)
            | Self::Sum(expr)
            | Self::ApproxPercentile(ApproxPercentileParams { child: expr, .. })
            | Self::ApproxCountDistinct(expr)
            | Self::ApproxSketch(expr, _)
            | Self::MergeSketch(expr, _)
            | Self::Mean(expr)
            | Self::Stddev(expr)
            | Self::Min(expr)
            | Self::Max(expr)
            | Self::AnyValue(expr, _)
            | Self::List(expr)
            | Self::Concat(expr) => vec![expr.clone()],
            Self::MapGroups { func: _, inputs } => inputs.clone(),
        }
    }

    pub fn with_new_children(&self, mut children: Vec<ExprRef>) -> Self {
        if let Self::MapGroups { func: _, inputs } = &self {
            assert_eq!(children.len(), inputs.len());
        } else {
            assert_eq!(children.len(), 1);
        }
        let mut first_child = || children.pop().unwrap();
        match self {
            Self::Count(_, count_mode) => Self::Count(first_child(), *count_mode),
            Self::Sum(_) => Self::Sum(first_child()),
            Self::Mean(_) => Self::Mean(first_child()),
            Self::Stddev(_) => Self::Stddev(first_child()),
            Self::Min(_) => Self::Min(first_child()),
            Self::Max(_) => Self::Max(first_child()),
            Self::AnyValue(_, ignore_nulls) => Self::AnyValue(first_child(), *ignore_nulls),
            Self::List(_) => Self::List(first_child()),
            Self::Concat(_) => Self::Concat(first_child()),
            Self::MapGroups { func, inputs: _ } => Self::MapGroups {
                func: func.clone(),
                inputs: children,
            },
            Self::ApproxPercentile(ApproxPercentileParams {
                percentiles,
                force_list_output,
                ..
            }) => Self::ApproxPercentile(ApproxPercentileParams {
                child: first_child(),
                percentiles: percentiles.clone(),
                force_list_output: *force_list_output,
            }),
            Self::ApproxCountDistinct(_) => Self::ApproxCountDistinct(first_child()),
            &Self::ApproxSketch(_, sketch_type) => Self::ApproxSketch(first_child(), sketch_type),
            &Self::MergeSketch(_, sketch_type) => Self::MergeSketch(first_child(), sketch_type),
        }
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        match self {
            Self::Count(expr, ..) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(field.name.as_str(), DataType::UInt64))
            }
            Self::Sum(expr) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(
                    field.name.as_str(),
                    try_sum_supertype(&field.dtype)?,
                ))
            }

            Self::ApproxPercentile(ApproxPercentileParams {
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
            Self::ApproxCountDistinct(expr) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(field.name.as_str(), DataType::UInt64))
            }
            Self::ApproxSketch(expr, sketch_type) => {
                let field = expr.to_field(schema)?;
                let dtype = match sketch_type {
                    SketchType::DDSketch => {
                        if !field.dtype.is_numeric() {
                            return Err(DaftError::TypeError(format!(
                                r#"Expected input to approx_sketch() to be numeric but received dtype {} for column "{}""#,
                                field.dtype, field.name,
                            )));
                        };
                        DataType::from(&*daft_sketch::ARROW2_DDSKETCH_DTYPE)
                    }
                    SketchType::HyperLogLog => daft_core::array::ops::HLL_SKETCH_DTYPE,
                };
                Ok(Field::new(field.name, dtype))
            }
            Self::MergeSketch(expr, sketch_type) => {
                let field = expr.to_field(schema)?;
                let dtype = match sketch_type {
                    SketchType::DDSketch => {
                        if let DataType::Struct(..) = field.dtype {
                            field.dtype
                        } else {
                            return Err(DaftError::TypeError(format!(
                                "Expected input to merge_sketch() to be struct but received dtype {} for column \"{}\"",
                                field.dtype, field.name,
                            )));
                        }
                    }
                    SketchType::HyperLogLog => DataType::UInt64,
                };
                Ok(Field::new(field.name, dtype))
            }
            Self::Mean(expr) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(
                    field.name.as_str(),
                    try_mean_aggregation_supertype(&field.dtype)?,
                ))
            }
            Self::Stddev(expr) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(
                    field.name.as_str(),
                    try_stddev_aggregation_supertype(&field.dtype)?,
                ))
            }

            Self::Min(expr) | Self::Max(expr) | Self::AnyValue(expr, _) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(field.name.as_str(), field.dtype))
            }
            Self::List(expr) => expr.to_field(schema)?.to_list_field(),
            Self::Concat(expr) => {
                let field = expr.to_field(schema)?;
                match field.dtype {
                    DataType::List(..) => Ok(field),
                    DataType::Utf8 => Ok(field),
                    #[cfg(feature = "python")]
                    DataType::Python => Ok(field),
                    _ => Err(DaftError::TypeError(format!(
                        "We can only perform List Concat Agg on List or Python Types, got dtype {} for column \"{}\"",
                        field.dtype, field.name
                    ))),
                }
            }
            Self::MapGroups { func, inputs } => func.to_field(inputs.as_slice(), schema, func),
        }
    }
}

impl From<&AggExpr> for ExprRef {
    fn from(agg_expr: &AggExpr) -> Self {
        Self::new(Expr::Agg(agg_expr.clone()))
    }
}

impl AsRef<Self> for Expr {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl Expr {
    pub fn arced(self) -> ExprRef {
        Arc::new(self)
    }

    pub fn alias<S: Into<Arc<str>>>(self: &ExprRef, name: S) -> ExprRef {
        Self::Alias(self.clone(), name.into()).into()
    }

    pub fn if_else(self: ExprRef, if_true: ExprRef, if_false: ExprRef) -> ExprRef {
        Self::IfElse {
            if_true,
            if_false,
            predicate: self,
        }
        .into()
    }

    pub fn cast(self: ExprRef, dtype: &DataType) -> ExprRef {
        Self::Cast(self, dtype.clone()).into()
    }

    pub fn count(self: ExprRef, mode: CountMode) -> ExprRef {
        Self::Agg(AggExpr::Count(self, mode)).into()
    }

    pub fn sum(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::Sum(self)).into()
    }

    pub fn approx_count_distinct(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::ApproxCountDistinct(self)).into()
    }

    pub fn approx_percentiles(
        self: ExprRef,
        percentiles: &[f64],
        force_list_output: bool,
    ) -> ExprRef {
        Self::Agg(AggExpr::ApproxPercentile(ApproxPercentileParams {
            child: self,
            percentiles: percentiles.iter().map(|f| FloatWrapper(*f)).collect(),
            force_list_output,
        }))
        .into()
    }

    pub fn sketch_percentile(
        self: ExprRef,
        percentiles: &[f64],
        force_list_output: bool,
    ) -> ExprRef {
        Self::Function {
            func: FunctionExpr::Sketch(SketchExpr::Percentile {
                percentiles: HashableVecPercentiles(percentiles.to_vec()),
                force_list_output,
            }),
            inputs: vec![self],
        }
        .into()
    }

    pub fn mean(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::Mean(self)).into()
    }

    pub fn stddev(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::Stddev(self)).into()
    }

    pub fn min(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::Min(self)).into()
    }

    pub fn max(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::Max(self)).into()
    }

    pub fn any_value(self: ExprRef, ignore_nulls: bool) -> ExprRef {
        Self::Agg(AggExpr::AnyValue(self, ignore_nulls)).into()
    }

    pub fn agg_list(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::List(self)).into()
    }

    pub fn agg_concat(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::Concat(self)).into()
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(self: ExprRef) -> ExprRef {
        Self::Not(self).into()
    }

    pub fn is_null(self: ExprRef) -> ExprRef {
        Self::IsNull(self).into()
    }

    pub fn not_null(self: ExprRef) -> ExprRef {
        Self::NotNull(self).into()
    }

    pub fn fill_null(self: ExprRef, fill_value: ExprRef) -> ExprRef {
        Self::FillNull(self, fill_value).into()
    }

    pub fn is_in(self: ExprRef, items: Vec<ExprRef>) -> ExprRef {
        Self::IsIn(self, items).into()
    }

    pub fn between(self: ExprRef, lower: ExprRef, upper: ExprRef) -> ExprRef {
        Self::Between(self, lower, upper).into()
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
    pub fn in_subquery(self: ExprRef, subquery: Subquery) -> ExprRef {
        Self::InSubquery(self, subquery).into()
    }

    pub fn semantic_id(&self, schema: &Schema) -> FieldID {
        match self {
            // Base case - anonymous column reference.
            // Look up the column name in the provided schema and get its field ID.
            Self::Column(name) => FieldID::new(&**name),

            // Base case - literal.
            Self::Literal(value) => FieldID::new(format!("Literal({value:?})")),

            // Recursive cases.
            Self::Cast(expr, dtype) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.cast({dtype})"))
            }
            Self::Not(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.not()"))
            }
            Self::IsNull(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.is_null()"))
            }
            Self::NotNull(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.not_null()"))
            }
            Self::FillNull(expr, fill_value) => {
                let child_id = expr.semantic_id(schema);
                let fill_value_id = fill_value.semantic_id(schema);
                FieldID::new(format!("{child_id}.fill_null({fill_value_id})"))
            }
            Self::IsIn(expr, items) => {
                let child_id = expr.semantic_id(schema);
                let items_id = items.iter().fold(String::new(), |acc, item| {
                    format!("{},{}", acc, item.semantic_id(schema))
                });

                FieldID::new(format!("{child_id}.is_in({items_id})"))
            }
            Self::Between(expr, lower, upper) => {
                let child_id = expr.semantic_id(schema);
                let lower_id = lower.semantic_id(schema);
                let upper_id = upper.semantic_id(schema);
                FieldID::new(format!("{child_id}.between({lower_id},{upper_id})"))
            }
            Self::Function { func, inputs } => function_semantic_id(func, inputs, schema),
            Self::BinaryOp { op, left, right } => {
                let left_id = left.semantic_id(schema);
                let right_id = right.semantic_id(schema);
                // TODO: check for symmetry here.
                FieldID::new(format!("({left_id} {op} {right_id})"))
            }
            Self::IfElse {
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
            Self::Alias(expr, ..) => expr.semantic_id(schema),
            // Agg: Separate path.
            Self::Agg(agg_expr) => agg_expr.semantic_id(schema),
            Self::ScalarFunction(sf) => scalar_function_semantic_id(sf, schema),

            Self::Subquery(..) | Self::InSubquery(..) | Self::Exists(..) => {
                FieldID::new("__subquery__")
            } // todo: better/unique id
            Self::OuterReferenceColumn(c) => {
                let name = &c.field.name;
                let depth = c.depth;
                FieldID::new(format!("outer_col({name}, {depth})"))
            }
        }
    }

    pub fn children(&self) -> Vec<ExprRef> {
        match self {
            // No children.
            Self::Column(..) => vec![],
            Self::Literal(..) => vec![],
            Self::Subquery(..) => vec![],
            Self::Exists(..) => vec![],
            Self::OuterReferenceColumn(..) => vec![],

            // One child.
            Self::Not(expr)
            | Self::IsNull(expr)
            | Self::NotNull(expr)
            | Self::Cast(expr, ..)
            | Self::Alias(expr, ..)
            | Self::InSubquery(expr, _) => {
                vec![expr.clone()]
            }
            Self::Agg(agg_expr) => agg_expr.children(),

            // Multiple children.
            Self::Function { inputs, .. } => inputs.clone(),
            Self::BinaryOp { left, right, .. } => {
                vec![left.clone(), right.clone()]
            }
            Self::IsIn(expr, items) => std::iter::once(expr.clone())
                .chain(items.iter().cloned())
                .collect::<Vec<_>>(),
            Self::Between(expr, lower, upper) => vec![expr.clone(), lower.clone(), upper.clone()],
            Self::IfElse {
                if_true,
                if_false,
                predicate,
            } => {
                vec![if_true.clone(), if_false.clone(), predicate.clone()]
            }
            Self::FillNull(expr, fill_value) => vec![expr.clone(), fill_value.clone()],
            Self::ScalarFunction(sf) => sf.inputs.clone(),
        }
    }

    pub fn with_new_children(&self, children: Vec<ExprRef>) -> Self {
        match self {
            // no children
            Self::Column(..)
            | Self::Literal(..)
            | Self::Subquery(..)
            | Self::Exists(..)
            | Self::OuterReferenceColumn(..) => {
                assert!(children.is_empty(), "Should have no children");
                self.clone()
            }
            // 1 child
            Self::Not(..) => Self::Not(children.first().expect("Should have 1 child").clone()),
            Self::Alias(.., name) => Self::Alias(
                children.first().expect("Should have 1 child").clone(),
                name.clone(),
            ),
            Self::IsNull(..) => {
                Self::IsNull(children.first().expect("Should have 1 child").clone())
            }
            Self::NotNull(..) => {
                Self::NotNull(children.first().expect("Should have 1 child").clone())
            }
            Self::Cast(.., dtype) => Self::Cast(
                children.first().expect("Should have 1 child").clone(),
                dtype.clone(),
            ),
            Self::InSubquery(_, subquery) => Self::InSubquery(
                children.first().expect("Should have 1 child").clone(),
                subquery.clone(),
            ),
            // 2 children
            Self::BinaryOp { op, .. } => Self::BinaryOp {
                op: *op,
                left: children.first().expect("Should have 1 child").clone(),
                right: children.get(1).expect("Should have 2 child").clone(),
            },
            Self::IsIn(_, old_children) => {
                assert_eq!(
                    children.len(),
                    old_children.len() + 1,
                    "Should have same number of children"
                );
                let mut children_iter = children.into_iter();
                let expr = children_iter.next().expect("Should have 1 child");
                let items = children_iter.collect();

                Self::IsIn(expr, items)
            }
            Self::Between(..) => Self::Between(
                children.first().expect("Should have 1 child").clone(),
                children.get(1).expect("Should have 2 child").clone(),
                children.get(2).expect("Should have 3 child").clone(),
            ),
            Self::FillNull(..) => Self::FillNull(
                children.first().expect("Should have 1 child").clone(),
                children.get(1).expect("Should have 2 child").clone(),
            ),
            // ternary
            Self::IfElse { .. } => Self::IfElse {
                if_true: children.first().expect("Should have 1 child").clone(),
                if_false: children.get(1).expect("Should have 2 child").clone(),
                predicate: children.get(2).expect("Should have 3 child").clone(),
            },
            // N-ary
            Self::Agg(agg_expr) => Self::Agg(agg_expr.with_new_children(children)),
            Self::Function {
                func,
                inputs: old_children,
            } => {
                assert!(
                    children.len() == old_children.len(),
                    "Should have same number of children"
                );
                Self::Function {
                    func: func.clone(),
                    inputs: children,
                }
            }
            Self::ScalarFunction(sf) => {
                assert!(
                    children.len() == sf.inputs.len(),
                    "Should have same number of children"
                );

                Self::ScalarFunction(crate::functions::ScalarFunction {
                    udf: sf.udf.clone(),
                    inputs: children,
                })
            }
        }
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        match self {
            Self::Alias(expr, name) => Ok(Field::new(name.as_ref(), expr.get_type(schema)?)),
            Self::Agg(agg_expr) => agg_expr.to_field(schema),
            Self::Cast(expr, dtype) => Ok(Field::new(expr.name(), dtype.clone())),
            Self::Column(name) => Ok(schema.get_field(name).cloned()?),
            Self::Not(expr) => {
                let child_field = expr.to_field(schema)?;
                match child_field.dtype {
                    DataType::Boolean => Ok(Field::new(expr.name(), DataType::Boolean)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expected argument to be a Boolean expression, but received {child_field}",
                    ))),
                }
            }
            Self::IsNull(expr) => Ok(Field::new(expr.name(), DataType::Boolean)),
            Self::NotNull(expr) => Ok(Field::new(expr.name(), DataType::Boolean)),
            Self::FillNull(expr, fill_value) => {
                let expr_field = expr.to_field(schema)?;
                let fill_value_field = fill_value.to_field(schema)?;
                match try_get_supertype(&expr_field.dtype, &fill_value_field.dtype) {
                    Ok(supertype) => Ok(Field::new(expr_field.name.as_str(), supertype)),
                    Err(_) => Err(DaftError::TypeError(format!(
                        "Expected expr and fill_value arguments for fill_null to be castable to the same supertype, but received {expr_field} and {fill_value_field}",
                    )))
                }
            }
            Self::IsIn(left, right) => {
                let left_field = left.to_field(schema)?;

                let first_right_field = right
                    .first()
                    .expect("Should have at least 1 child")
                    .to_field(schema)?;
                let all_same_type = right.iter().all(|expr| {
                    let field = expr.to_field(schema).unwrap();
                    // allow nulls to be compared with anything
                    if field.dtype == DataType::Null || first_right_field.dtype == DataType::Null {
                        return true;
                    }
                    field.dtype == first_right_field.dtype
                });
                if !all_same_type {
                    return Err(DaftError::TypeError(format!(
                        "Expected all arguments to be of the same type, but received {first_right_field} and others",
                    )));
                }

                let (result_type, _intermediate, _comp_type) =
                    InferDataType::from(&left_field.dtype)
                        .membership_op(&InferDataType::from(&first_right_field.dtype))?;
                Ok(Field::new(left_field.name.as_str(), result_type))
            }
            Self::Between(value, lower, upper) => {
                let value_field = value.to_field(schema)?;
                let lower_field = lower.to_field(schema)?;
                let upper_field = upper.to_field(schema)?;
                let (lower_result_type, _intermediate, _comp_type) =
                    InferDataType::from(&value_field.dtype)
                        .membership_op(&InferDataType::from(&lower_field.dtype))?;
                let (upper_result_type, _intermediate, _comp_type) =
                    InferDataType::from(&value_field.dtype)
                        .membership_op(&InferDataType::from(&upper_field.dtype))?;
                let (result_type, _intermediate, _comp_type) =
                    InferDataType::from(&lower_result_type)
                        .membership_op(&InferDataType::from(&upper_result_type))?;
                Ok(Field::new(value_field.name.as_str(), result_type))
            }
            Self::Literal(value) => Ok(Field::new("literal", value.get_type())),
            Self::Function { func, inputs } => func.to_field(inputs.as_slice(), schema, func),
            Self::ScalarFunction(sf) => sf.to_field(schema),
            Self::BinaryOp { op, left, right } => {
                let left_field = left.to_field(schema)?;
                let right_field = right.to_field(schema)?;

                match op {
                    // Logical operations
                    Operator::And | Operator::Or | Operator::Xor => {
                        let result_type = InferDataType::from(&left_field.dtype)
                            .logical_op(&InferDataType::from(&right_field.dtype))?;
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
                            InferDataType::from(&left_field.dtype)
                                .comparison_op(&InferDataType::from(&right_field.dtype))?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }

                    // Arithmetic operations
                    Operator::Plus => {
                        let result_type = (InferDataType::from(&left_field.dtype)
                            + InferDataType::from(&right_field.dtype))?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::Minus => {
                        let result_type = (InferDataType::from(&left_field.dtype)
                            - InferDataType::from(&right_field.dtype))?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::Multiply => {
                        let result_type = (InferDataType::from(&left_field.dtype)
                            * InferDataType::from(&right_field.dtype))?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::TrueDivide => {
                        let result_type = (InferDataType::from(&left_field.dtype)
                            / InferDataType::from(&right_field.dtype))?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::Modulus => {
                        let result_type = (InferDataType::from(&left_field.dtype)
                            % InferDataType::from(&right_field.dtype))?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::ShiftLeft => {
                        let result_type = (InferDataType::from(&left_field.dtype)
                            << InferDataType::from(&right_field.dtype))?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::ShiftRight => {
                        let result_type = (InferDataType::from(&left_field.dtype)
                            >> InferDataType::from(&right_field.dtype))?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                    Operator::FloorDivide => {
                        let result_type = (InferDataType::from(&left_field.dtype)
                            .floor_div(&InferDataType::from(&right_field.dtype)))?;
                        Ok(Field::new(left_field.name.as_str(), result_type))
                    }
                }
            }
            Self::IfElse {
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
                    Self::Literal(lit::LiteralValue::Boolean(true)) => if_true.to_field(schema),
                    Self::Literal(lit::LiteralValue::Boolean(false)) => {
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
            Self::Subquery(subquery) => {
                let subquery_schema = subquery.schema();
                if subquery_schema.len() != 1 {
                    return Err(DaftError::TypeError(format!(
                        "Expected subquery to return a single column but received {subquery_schema}",
                    )));
                }
                let (_, first_field) = subquery_schema.fields.first().unwrap();

                Ok(first_field.clone())
            }
            Self::InSubquery(expr, _) => Ok(Field::new(expr.name(), DataType::Boolean)),
            Self::Exists(_) => Ok(Field::new("exists", DataType::Boolean)),
            Self::OuterReferenceColumn(c) => Ok(c.field.clone()),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Alias(.., name) => name.as_ref(),
            Self::Agg(agg_expr) => agg_expr.name(),
            Self::Cast(expr, ..) => expr.name(),
            Self::Column(name) => name.as_ref(),
            Self::Not(expr) => expr.name(),
            Self::IsNull(expr) => expr.name(),
            Self::NotNull(expr) => expr.name(),
            Self::FillNull(expr, ..) => expr.name(),
            Self::IsIn(expr, ..) => expr.name(),
            Self::Between(expr, ..) => expr.name(),
            Self::Literal(..) => "literal",
            Self::Function { func, inputs } => match func {
                FunctionExpr::Struct(StructExpr::Get(name)) => name,
                _ => inputs.first().unwrap().name(),
            },
            Self::ScalarFunction(func) => match func.name() {
                "to_struct" => "struct", // FIXME: make .name() use output name from schema
                _ => func.inputs.first().unwrap().name(),
            },
            Self::BinaryOp {
                op: _,
                left,
                right: _,
            } => left.name(),
            Self::IfElse { if_true, .. } => if_true.name(),
            Self::Subquery(subquery) => subquery.name(),
            Self::InSubquery(expr, _) => expr.name(),
            Self::Exists(subquery) => subquery.name(),
            Self::OuterReferenceColumn(c) => &c.field.name,
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
                // TODO: Implement SQL translations for these expressions if possible
                Expr::IfElse { .. }
                | Expr::Agg(..)
                | Expr::Cast(..)
                | Expr::IsIn(..)
                | Expr::Between(..)
                | Expr::Function { .. }
                | Expr::FillNull(..)
                | Expr::ScalarFunction { .. }
                | Expr::Subquery(..)
                | Expr::InSubquery(..)
                | Expr::Exists(..)
                | Expr::OuterReferenceColumn(..) => Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unsupported expression for SQL translation",
                )),
            }
        }

        let mut buffer = Vec::new();
        to_sql_inner(self, &mut buffer)
            .ok()
            .and_then(|()| String::from_utf8(buffer).ok())
    }

    /// If the expression is a literal, return it. Otherwise, return None.
    pub fn as_literal(&self) -> Option<&lit::LiteralValue> {
        match self {
            Self::Literal(lit) => Some(lit),
            _ => None,
        }
    }
}

#[derive(Display, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Operator {
    #[display("==")]
    Eq,
    #[display("!=")]
    NotEq,
    #[display("<")]
    Lt,
    #[display("<=")]
    LtEq,
    #[display(">")]
    Gt,
    #[display(">=")]
    GtEq,
    #[display("+")]
    Plus,
    #[display("-")]
    Minus,
    #[display("*")]
    Multiply,
    #[display("/")]
    TrueDivide,
    #[display("//")]
    FloorDivide,
    #[display("%")]
    Modulus,
    #[display("&")]
    And,
    #[display("|")]
    Or,
    #[display("^")]
    Xor,
    #[display("<<")]
    ShiftLeft,
    #[display(">>")]
    ShiftRight,
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

// Check if one set of columns is a reordering of the other
pub fn is_partition_compatible(a: &[ExprRef], b: &[ExprRef]) -> bool {
    // sort a and b by name
    let a: Vec<&str> = a.iter().map(|a| a.name()).sorted().collect();
    let b: Vec<&str> = b.iter().map(|a| a.name()).sorted().collect();
    a == b
}

pub fn has_agg(expr: &ExprRef) -> bool {
    expr.exists(|e| matches!(e.as_ref(), Expr::Agg(_)))
}

pub fn has_stateful_udf(expr: &ExprRef) -> bool {
    expr.exists(|e| {
        matches!(
            e.as_ref(),
            Expr::Function {
                func: FunctionExpr::Python(PythonUDF::Stateful(_)),
                ..
            }
        )
    })
}
