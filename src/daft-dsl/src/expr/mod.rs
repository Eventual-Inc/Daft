pub mod bound_expr;
pub mod window;

mod display;
#[cfg(test)]
mod tests;

use std::{
    any::Any,
    collections::HashSet,
    fmt::Formatter,
    hash::{DefaultHasher, Hash, Hasher},
    io::{self, Write},
    str::FromStr,
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_hashable_float_wrapper::FloatWrapper;
use common_treenode::{Transformed, TreeNode};
use daft_core::{
    datatypes::{
        try_mean_aggregation_supertype, try_skew_aggregation_supertype,
        try_stddev_aggregation_supertype, try_sum_supertype, InferDataType,
    },
    join::JoinSide,
    prelude::*,
    utils::supertype::{try_get_collection_supertype, try_get_supertype},
};
use derive_more::Display;
use serde::{Deserialize, Serialize};

use super::functions::FunctionExpr;
use crate::{
    expr::bound_expr::BoundExpr,
    functions::{
        function_display_without_formatter, function_semantic_id,
        python::PythonUDF,
        scalar::scalar_function_semantic_id,
        sketch::{HashableVecPercentiles, SketchExpr},
        struct_::StructExpr,
        FunctionArg, FunctionArgs, FunctionEvaluator, ScalarFunction, FUNCTION_REGISTRY,
    },
    lit,
    optimization::{get_required_columns, requires_computation},
};

pub trait SubqueryPlan: std::fmt::Debug + std::fmt::Display + Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn name(&self) -> &'static str;
    fn schema(&self) -> SchemaRef;
    fn dyn_eq(&self, other: &dyn SubqueryPlan) -> bool;
    fn dyn_hash(&self, state: &mut dyn Hasher);
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

    pub fn semantic_id(&self) -> FieldID {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        let hash = s.finish();

        FieldID::new(format!("subquery({}-{})", self.name(), hash))
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
        self.plan.dyn_eq(other.plan.as_ref())
    }
}

impl Eq for Subquery {}

impl std::hash::Hash for Subquery {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.plan.dyn_hash(state);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Column {
    Unresolved(UnresolvedColumn),
    Resolved(ResolvedColumn),
    Bound(BoundColumn),
}

/// Information about the logical plan node that a column comes from.
///
/// Used for resolving columns in the logical plan builder and subquery unnesting rule.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PlanRef {
    /// Corresponds to a SubqueryAlias
    Alias(Arc<str>),

    /// No specified source.
    ///
    /// Can either be from the immediate input or an outer scope.
    Unqualified,
    Id(usize),
}

/// Column that is not yet resolved to a scope.
///
/// Unresolved columns should only be used before logical plan construction
/// (e.g. in the DataFrame and SQL frontends and the logical plan builder).
///
/// Expressions are assumed to contain no unresolved columns by the time
/// they are used in a logical plan op, as well as subsequent steps such as
/// physical plan translation and execution.
///
/// The logical plan builder is responsible for resolving all Column::Unresolved
/// into Column::Resolved
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UnresolvedColumn {
    pub name: Arc<str>,
    pub plan_ref: PlanRef,
    pub plan_schema: Option<SchemaRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ResolvedColumn {
    /// Column resolved to the scope of a singular input.
    Basic(Arc<str>),

    /// Column resolved to the scope of either the left or right input of a join.
    ///
    /// This variant should only exist in join predicates.
    ///
    /// TODO: Once we support identifying columns by ordinals, join-side columns
    /// should just be normal resolved columns, where ordinal < (# left schema fields) means
    /// it's from the left side, and otherwise right side. This is similar to Substrait.
    JoinSide(Field, JoinSide),

    /// Column resolved to the scope of an outer plan of a subquery.
    ///
    /// This variant should only exist in subquery plans.
    OuterRef(Field, PlanRef),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BoundColumn {
    pub index: usize,

    #[deprecated(since = "TBD", note = "name-referenced columns")]
    /// Should only be used for display and debugging purposes
    pub field: Field,
}

impl Column {
    #[deprecated(since = "TBD", note = "name-referenced columns")]
    pub fn name(&self) -> String {
        match self {
            Self::Unresolved(UnresolvedColumn {
                name,
                plan_ref: PlanRef::Alias(plan_alias),
                ..
            }) => format!("{plan_alias}.{name}"),
            Self::Unresolved(UnresolvedColumn { name, .. }) => name.to_string(),
            Self::Resolved(ResolvedColumn::Basic(name)) => name.to_string(),
            Self::Resolved(ResolvedColumn::JoinSide(name, side)) => format!("{side}.{name}"),
            Self::Resolved(ResolvedColumn::OuterRef(
                Field { name, .. },
                PlanRef::Alias(plan_alias),
            )) => format!("{plan_alias}.{name}"),
            Self::Resolved(ResolvedColumn::OuterRef(Field { name, .. }, _)) => name.to_string(),
            Self::Bound(BoundColumn {
                field: Field { name, .. },
                ..
            }) => name.to_string(),
        }
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Self::Bound(BoundColumn { index, field, .. }) = self {
            write!(f, "col({}: {})", index, field.name)
        } else {
            write!(f, "col({})", self.name())
        }
    }
}

pub type ExprRef = Arc<Expr>;

#[derive(Display, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Expr {
    #[display("{_0}")]
    Column(Column),

    #[display("{_0} as {_1}")]
    Alias(ExprRef, Arc<str>),

    #[display("{_0}")]
    Agg(AggExpr),

    #[display("{}", display::expr_binary_op_display_without_formatter(op, left, right)?)]
    BinaryOp {
        op: Operator,
        left: ExprRef,
        right: ExprRef,
    },

    #[display("cast({_0} as {_1})")]
    Cast(ExprRef, DataType),

    #[display("{}", function_display_without_formatter(func, inputs)?)]
    Function {
        func: FunctionExpr,
        inputs: Vec<ExprRef>,
    },

    // Over represents a window function as it is actually evaluated (since it requires a window spec)
    #[display("{_0} over {_1}")]
    Over(WindowExpr, window::WindowSpec),

    // WindowFunction represents a window function as an expression, this alone cannot be evaluated since
    // it requires a window spec. This variant only exists for constructing window functions in the
    // DataFrame API and should not appear in logical or physical plans. It must be converted to an Over
    // expression with a window spec before evaluation.
    #[display("window({_0})")]
    WindowFunction(WindowExpr),

    #[display("not({_0})")]
    Not(ExprRef),

    #[display("is_null({_0})")]
    IsNull(ExprRef),

    #[display("not_null({_0})")]
    NotNull(ExprRef),

    #[display("fill_null({_0}, {_1})")]
    FillNull(ExprRef, ExprRef),

    #[display("{}", display::expr_is_in_display_without_formatter(_0, _1)?)]
    IsIn(ExprRef, Vec<ExprRef>),

    #[display("{_0} in [{_1},{_2}]")]
    Between(ExprRef, ExprRef, ExprRef),

    #[display("{}", display::expr_list_display_without_formatter(_0)?)]
    List(Vec<ExprRef>),

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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash, Eq)]
pub struct ApproxPercentileParams {
    pub child: ExprRef,
    pub percentiles: Vec<FloatWrapper<f64>>,
    pub force_list_output: bool,
}

#[derive(Display, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AggExpr {
    #[display("count({_0}, {_1})")]
    Count(ExprRef, CountMode),

    #[display("count_distinct({_0})")]
    CountDistinct(ExprRef),

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

    #[display("bool_and({_0})")]
    BoolAnd(ExprRef),

    #[display("bool_or({_0})")]
    BoolOr(ExprRef),

    #[display("any_value({_0}, ignore_nulls={_1})")]
    AnyValue(ExprRef, bool),

    #[display("list({_0})")]
    List(ExprRef),

    #[display("set({_0})")]
    Set(ExprRef),

    #[display("list({_0})")]
    Concat(ExprRef),

    #[display("skew({_0}")]
    Skew(ExprRef),

    #[display("{}", function_display_without_formatter(func, inputs)?)]
    MapGroups {
        func: FunctionExpr,
        inputs: Vec<ExprRef>,
    },
}

#[derive(Display, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum WindowExpr {
    #[display("agg({_0})")]
    Agg(AggExpr),

    #[display("row_number")]
    RowNumber,

    #[display("rank")]
    Rank,

    #[display("dense_rank")]
    DenseRank,

    // input: the column to offset
    // offset > 0: LEAD (shift values ahead by offset)
    // offset < 0: LAG (shift values behind by offset)
    // default: the value to fill before / after the offset
    #[display("offset({input}, {offset}, {default:?})")]
    Offset {
        input: ExprRef,
        offset: isize,
        default: Option<ExprRef>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SketchType {
    DDSketch,
    HyperLogLog,
}

/// Unresolved column with no associated plan ID or schema.
pub fn unresolved_col(name: impl Into<Arc<str>>) -> ExprRef {
    UnresolvedColumn {
        name: name.into(),
        plan_ref: PlanRef::Unqualified,
        plan_schema: None,
    }
    .into()
}

pub fn bound_col(index: usize, field: Field) -> ExprRef {
    BoundColumn { index, field }.into()
}

/// Basic resolved column, refers to a singular input scope
pub fn resolved_col<S: Into<Arc<str>>>(name: S) -> ExprRef {
    ResolvedColumn::Basic(name.into()).into()
}

/// Resolved column referring to the left side of a join
pub fn left_col(field: Field) -> ExprRef {
    ResolvedColumn::JoinSide(field, JoinSide::Left).into()
}

/// Resolved column referring to the right side of a join
pub fn right_col(field: Field) -> ExprRef {
    ResolvedColumn::JoinSide(field, JoinSide::Right).into()
}

pub fn binary_op(op: Operator, left: ExprRef, right: ExprRef) -> ExprRef {
    Expr::BinaryOp { op, left, right }.into()
}

impl AggExpr {
    pub fn name(&self) -> &str {
        match self {
            Self::Count(expr, ..)
            | Self::CountDistinct(expr)
            | Self::Sum(expr)
            | Self::ApproxPercentile(ApproxPercentileParams { child: expr, .. })
            | Self::ApproxCountDistinct(expr)
            | Self::ApproxSketch(expr, _)
            | Self::MergeSketch(expr, _)
            | Self::Mean(expr)
            | Self::Stddev(expr)
            | Self::Min(expr)
            | Self::Max(expr)
            | Self::BoolAnd(expr)
            | Self::BoolOr(expr)
            | Self::AnyValue(expr, _)
            | Self::List(expr)
            | Self::Set(expr)
            | Self::Concat(expr)
            | Self::Skew(expr) => expr.name(),
            Self::MapGroups { func: _, inputs } => inputs.first().unwrap().name(),
        }
    }

    pub fn semantic_id(&self, schema: &Schema) -> FieldID {
        match self {
            Self::Count(expr, mode) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_count({mode})"))
            }
            Self::CountDistinct(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_count_distinct()"))
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
            Self::BoolAnd(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_bool_and()"))
            }
            Self::BoolOr(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_bool_or()"))
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
            Self::Set(_expr) => {
                let child_id = _expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_set()"))
            }
            Self::Concat(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_concat()"))
            }
            Self::Skew(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_skew()"))
            }
            Self::MapGroups { func, inputs } => function_semantic_id(func, inputs, schema),
        }
    }

    pub fn children(&self) -> Vec<ExprRef> {
        match self {
            Self::Count(expr, ..)
            | Self::CountDistinct(expr)
            | Self::Sum(expr)
            | Self::ApproxPercentile(ApproxPercentileParams { child: expr, .. })
            | Self::ApproxCountDistinct(expr)
            | Self::ApproxSketch(expr, _)
            | Self::MergeSketch(expr, _)
            | Self::Mean(expr)
            | Self::Stddev(expr)
            | Self::Min(expr)
            | Self::Max(expr)
            | Self::BoolAnd(expr)
            | Self::BoolOr(expr)
            | Self::AnyValue(expr, _)
            | Self::List(expr)
            | Self::Set(expr)
            | Self::Concat(expr)
            | Self::Skew(expr) => vec![expr.clone()],
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
            &Self::Count(_, count_mode) => Self::Count(first_child(), count_mode),
            Self::CountDistinct(_) => Self::CountDistinct(first_child()),
            Self::Sum(_) => Self::Sum(first_child()),
            Self::Mean(_) => Self::Mean(first_child()),
            Self::Stddev(_) => Self::Stddev(first_child()),
            Self::Min(_) => Self::Min(first_child()),
            Self::Max(_) => Self::Max(first_child()),
            Self::BoolAnd(_) => Self::BoolAnd(first_child()),
            Self::BoolOr(_) => Self::BoolOr(first_child()),
            Self::AnyValue(_, ignore_nulls) => Self::AnyValue(first_child(), *ignore_nulls),
            Self::List(_) => Self::List(first_child()),
            Self::Set(_expr) => Self::Set(first_child()),
            Self::Concat(_) => Self::Concat(first_child()),
            Self::Skew(_) => Self::Skew(first_child()),
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
            Self::Count(expr, ..) | Self::CountDistinct(expr) => {
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
                        }
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

            Self::List(expr) | Self::Set(expr) => expr.to_field(schema)?.to_list_field(),

            Self::BoolAnd(expr) | Self::BoolOr(expr) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(field.name.as_str(), DataType::Boolean))
            }

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

            Self::Skew(expr) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(
                    field.name.as_str(),
                    try_skew_aggregation_supertype(&field.dtype)?,
                ))
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

impl WindowExpr {
    pub fn name(&self) -> &str {
        match self {
            Self::Agg(agg_expr) => agg_expr.name(),
            Self::RowNumber => "row_number",
            Self::Rank => "rank",
            Self::DenseRank => "dense_rank",
            Self::Offset {
                input,
                offset: _,
                default: _,
            } => input.name(),
        }
    }

    pub fn semantic_id(&self, schema: &Schema) -> FieldID {
        match self {
            Self::Agg(agg_expr) => agg_expr.semantic_id(schema),
            Self::RowNumber => FieldID::new("row_number"),
            Self::Rank => FieldID::new("rank"),
            Self::DenseRank => FieldID::new("dense_rank"),
            Self::Offset {
                input,
                offset,
                default,
            } => {
                let child_id = input.semantic_id(schema);
                let default_part = if let Some(default_expr) = default {
                    let default_id = default_expr.semantic_id(schema);
                    format!(",default={default_id}")
                } else {
                    String::new()
                };
                FieldID::new(format!("{child_id}.offset(offset={offset}{default_part})"))
            }
        }
    }

    pub fn children(&self) -> Vec<ExprRef> {
        match self {
            Self::Agg(agg_expr) => agg_expr.children(),
            Self::RowNumber => vec![],
            Self::Rank => vec![],
            Self::DenseRank => vec![],
            Self::Offset {
                input,
                offset: _,
                default,
            } => {
                let mut children = vec![input.clone()];
                if let Some(default_expr) = default {
                    children.push(default_expr.clone());
                }
                children
            }
        }
    }

    pub fn with_new_children(&self, children: Vec<ExprRef>) -> Self {
        match self {
            Self::Agg(agg_expr) => Self::Agg(agg_expr.with_new_children(children)),
            Self::RowNumber => Self::RowNumber,
            Self::Rank => Self::Rank,
            Self::DenseRank => Self::DenseRank,
            // Offset can have either one or two children:
            // 1. The first child is always the expression to offset
            // 2. The second child is the optional default value (if provided)
            Self::Offset {
                input: _,
                offset,
                default: _,
            } => {
                let input = children
                    .first()
                    .expect("Should have at least 1 child")
                    .clone();
                let default = if children.len() > 1 {
                    Some(children.get(1).unwrap().clone())
                } else {
                    None
                };
                Self::Offset {
                    input,
                    offset: *offset,
                    default,
                }
            }
        }
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        match self {
            Self::Agg(agg_expr) => agg_expr.to_field(schema),
            Self::RowNumber => Ok(Field::new("row_number", DataType::UInt64)),
            Self::Rank => Ok(Field::new("rank", DataType::UInt64)),
            Self::DenseRank => Ok(Field::new("dense_rank", DataType::UInt64)),
            Self::Offset {
                input,
                offset: _,
                default: _,
            } => input.to_field(schema),
        }
    }
}

impl From<&WindowExpr> for ExprRef {
    fn from(window_expr: &WindowExpr) -> Self {
        Self::new(Expr::WindowFunction(window_expr.clone()))
    }
}

impl From<AggExpr> for WindowExpr {
    fn from(agg_expr: AggExpr) -> Self {
        Self::Agg(agg_expr)
    }
}

impl TryFrom<ExprRef> for WindowExpr {
    type Error = DaftError;

    fn try_from(expr: ExprRef) -> Result<Self, Self::Error> {
        match expr.as_ref() {
            Expr::Agg(agg_expr) => Ok(Self::Agg(agg_expr.clone())),
            Expr::WindowFunction(window_expr) => Ok(window_expr.clone()),
            _ => Err(DaftError::ValueError(format!(
                "Expected an AggExpr or WindowFunction, got {:?}",
                expr
            ))),
        }
    }
}

impl From<UnresolvedColumn> for ExprRef {
    fn from(col: UnresolvedColumn) -> Self {
        Self::new(Expr::Column(Column::Unresolved(col)))
    }
}
impl From<ResolvedColumn> for ExprRef {
    fn from(col: ResolvedColumn) -> Self {
        Self::new(Expr::Column(Column::Resolved(col)))
    }
}

impl From<BoundColumn> for ExprRef {
    fn from(col: BoundColumn) -> Self {
        Self::new(Expr::Column(Column::Bound(col)))
    }
}

impl From<Column> for ExprRef {
    fn from(col: Column) -> Self {
        Self::new(Expr::Column(col))
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

    pub fn count_distinct(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::CountDistinct(self)).into()
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

    pub fn bool_and(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::BoolAnd(self)).into()
    }

    pub fn bool_or(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::BoolOr(self)).into()
    }

    pub fn any_value(self: ExprRef, ignore_nulls: bool) -> ExprRef {
        Self::Agg(AggExpr::AnyValue(self, ignore_nulls)).into()
    }

    pub fn skew(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::Skew(self)).into()
    }

    pub fn agg_list(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::List(self)).into()
    }

    pub fn agg_set(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::Set(self)).into()
    }

    pub fn agg_concat(self: ExprRef) -> ExprRef {
        Self::Agg(AggExpr::Concat(self)).into()
    }

    pub fn row_number() -> ExprRef {
        Self::WindowFunction(WindowExpr::RowNumber).into()
    }

    pub fn rank() -> ExprRef {
        Self::WindowFunction(WindowExpr::Rank).into()
    }

    pub fn dense_rank() -> ExprRef {
        Self::WindowFunction(WindowExpr::DenseRank).into()
    }

    pub fn offset(self: ExprRef, offset: isize, default: Option<ExprRef>) -> ExprRef {
        Self::WindowFunction(WindowExpr::Offset {
            input: self,
            offset,
            default,
        })
        .into()
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

    #[deprecated(since = "TBD", note = "name-referenced columns")]
    pub fn semantic_id(&self, schema: &Schema) -> FieldID {
        match self {
            // Base case - anonymous column reference.
            // Look up the column name in the provided schema and get its field ID.
            Self::Column(Column::Unresolved(UnresolvedColumn {
                name,
                plan_ref: PlanRef::Alias(alias),
                ..
            })) => FieldID::new(format!("{alias}.{name}")),

            Self::Column(Column::Unresolved(UnresolvedColumn {
                name,
                plan_ref: PlanRef::Id(id),
                ..
            })) => FieldID::new(format!("{id}.{name}")),

            Self::Column(Column::Unresolved(UnresolvedColumn {
                name,
                plan_ref: PlanRef::Unqualified,
                ..
            })) => FieldID::new(&**name),

            Self::Column(Column::Resolved(ResolvedColumn::Basic(name))) => FieldID::new(&**name),

            Self::Column(Column::Resolved(ResolvedColumn::JoinSide(name, side))) => {
                FieldID::new(format!("{side}.{name}"))
            }

            Self::Column(Column::Bound(BoundColumn {
                index,
                field: Field { name, .. },
            })) => FieldID::new(format!("{name}#{index}")),

            Self::Column(Column::Resolved(ResolvedColumn::OuterRef(
                Field { name, .. },
                PlanRef::Alias(alias),
            ))) => FieldID::new(format!("outer.{alias}.{name}")),
            Self::Column(Column::Resolved(ResolvedColumn::OuterRef(
                Field { name, .. },
                PlanRef::Id(id),
            ))) => FieldID::new(format!("outer.{id}.{name}")),
            Self::Column(Column::Resolved(ResolvedColumn::OuterRef(
                Field { name, .. },
                PlanRef::Unqualified,
            ))) => FieldID::new(format!("outer.{name}")),

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
            Self::List(items) => {
                let items_id = items.iter().fold(String::new(), |acc, item| {
                    format!("{},{}", acc, item.semantic_id(schema))
                });
                FieldID::new(format!("List({items_id})"))
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
            Self::Subquery(subquery) => subquery.semantic_id(),
            Self::InSubquery(expr, subquery) => {
                let child_id = expr.semantic_id(schema);
                let subquery_id = subquery.semantic_id();

                FieldID::new(format!("({child_id} IN {subquery_id})"))
            }
            Self::Exists(subquery) => {
                let subquery_id = subquery.semantic_id();

                FieldID::new(format!("(EXISTS {subquery_id})"))
            }
            Self::Over(expr, window_spec) => {
                let child_id = expr.semantic_id(schema);

                let partition_by_ids = window_spec
                    .partition_by
                    .iter()
                    .map(|e| e.semantic_id(schema).id.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let order_by_ids = window_spec
                    .order_by
                    .iter()
                    .zip(window_spec.descending.iter())
                    .map(|(e, desc)| {
                        format!(
                            "{}:{}",
                            e.semantic_id(schema),
                            if *desc { "desc" } else { "asc" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                let frame_details = if let Some(frame) = &window_spec.frame {
                    format!(
                        ",start={:?},end={:?},min_periods={}",
                        frame.start, frame.end, window_spec.min_periods
                    )
                } else {
                    String::new()
                };

                FieldID::new(format!("{child_id}.window(partition_by=[{partition_by_ids}],order_by=[{order_by_ids}]{frame_details})"))
            }
            Self::WindowFunction(window_expr) => {
                let child_id = window_expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.window_function()"))
            }
        }
    }

    pub fn children(&self) -> Vec<ExprRef> {
        match self {
            // No children.
            Self::Column(..) | Self::Literal(..) | Self::Subquery(..) | Self::Exists(..) => vec![],

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
            Self::Over(window_expr, _) => window_expr.children(),
            Self::WindowFunction(window_expr) => window_expr.children(),

            // Multiple children.
            Self::Function { inputs, .. } => inputs.clone(),
            Self::BinaryOp { left, right, .. } => {
                vec![left.clone(), right.clone()]
            }
            Self::IsIn(expr, items) => std::iter::once(expr.clone())
                .chain(items.iter().cloned())
                .collect::<Vec<_>>(),
            Self::List(items) => items.clone(),
            Self::Between(expr, lower, upper) => vec![expr.clone(), lower.clone(), upper.clone()],
            Self::IfElse {
                if_true,
                if_false,
                predicate,
            } => {
                vec![if_true.clone(), if_false.clone(), predicate.clone()]
            }
            Self::FillNull(expr, fill_value) => vec![expr.clone(), fill_value.clone()],
            Self::ScalarFunction(sf) => sf.inputs.clone().into_inner(),
        }
    }

    pub fn with_new_children(&self, children: Vec<ExprRef>) -> Self {
        match self {
            // no children
            Self::Column(..) | Self::Literal(..) | Self::Subquery(..) | Self::Exists(..) => {
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
            Self::List(children_old) => {
                let c_len = children.len();
                let c_len_old = children_old.len();
                assert_eq!(
                    c_len, c_len_old,
                    "Should have same number of children ({c_len_old}), found ({c_len})"
                );
                Self::List(children)
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
            Self::Over(window_expr, window_spec) => {
                Self::Over(window_expr.with_new_children(children), window_spec.clone())
            }
            Self::WindowFunction(window_expr) => {
                Self::WindowFunction(window_expr.with_new_children(children))
            }
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
                let new_children = sf
                    .inputs
                    .iter()
                    .zip(children.into_iter())
                    .map(|(fn_arg, child)| match fn_arg {
                        FunctionArg::Named { name, .. } => FunctionArg::Named {
                            name: name.clone(),
                            arg: child,
                        },
                        FunctionArg::Unnamed(_) => FunctionArg::Unnamed(child),
                    })
                    .collect();

                Self::ScalarFunction(crate::functions::ScalarFunction {
                    udf: sf.udf.clone(),
                    inputs: FunctionArgs::new_unchecked(new_children),
                })
            }
        }
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        match self {
            Self::Alias(expr, name) => Ok(Field::new(name.as_ref(), expr.get_type(schema)?)),
            Self::Agg(agg_expr) => agg_expr.to_field(schema),
            Self::Cast(expr, dtype) => Ok(Field::new(expr.name(), dtype.clone())),
            Self::Column(Column::Unresolved(UnresolvedColumn {
                name,
                plan_schema: Some(plan_schema),
                ..
            })) => plan_schema.get_field(name).cloned(),
            Self::Column(Column::Unresolved(UnresolvedColumn {
                name,
                plan_schema: None,
                ..
            })) => schema.get_field(name).cloned(),

            Self::Column(Column::Resolved(ResolvedColumn::Basic(name))) => {
                schema.get_field(name).cloned()
            }
            Self::Column(Column::Resolved(ResolvedColumn::JoinSide(field, ..))) => {
                Ok(field.clone())
            }

            Self::Column(Column::Bound(BoundColumn { index, .. })) => Ok(schema[*index].clone()),

            Self::Column(Column::Resolved(ResolvedColumn::OuterRef(field, _))) => Ok(field.clone()),
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
            Self::IsIn(expr, items) => {
                // Use the expr's field name, and infer membership op type.
                let list_dtype = try_compute_is_in_type(items, schema)?.unwrap_or(DataType::Null);
                let expr_field = expr.to_field(schema)?;
                let expr_type = &expr_field.dtype;
                let field_name = &expr_field.name;
                let field_type = InferDataType::from(expr_type)
                    .membership_op(&(&list_dtype).into())?
                    .0;
                Ok(Field::new(field_name, field_type))
            }
            Self::List(items) => {
                // Use "list" as the field name, and infer list type from items.
                let field_name = "list";
                let field_types = items
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<DaftResult<Vec<_>>>()?;
                let field_type = try_get_collection_supertype(&field_types)?;
                Ok(Field::new(field_name, DataType::new_list(field_type)))
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
                    | Operator::GtEq
                    | Operator::EqNullSafe => {
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
                let first_field = &subquery_schema[0];

                Ok(first_field.clone())
            }
            Self::InSubquery(expr, _) => Ok(Field::new(expr.name(), DataType::Boolean)),
            Self::Exists(_) => Ok(Field::new("exists", DataType::Boolean)),
            Self::Over(expr, _) => expr.to_field(schema),
            Self::WindowFunction(expr) => expr.to_field(schema),
        }
    }

    #[deprecated(since = "TBD", note = "name-referenced columns")]
    pub fn name(&self) -> &str {
        match self {
            Self::Alias(.., name) => name.as_ref(),
            // unlike alias, we only use the expr name here for functions,
            Self::Agg(agg_expr) => agg_expr.name(),
            Self::Cast(expr, ..) => expr.name(),
            Self::Column(Column::Unresolved(UnresolvedColumn { name, .. })) => name.as_ref(),
            Self::Column(Column::Resolved(ResolvedColumn::Basic(name))) => name.as_ref(),
            Self::Column(Column::Resolved(ResolvedColumn::JoinSide(Field { name, .. }, ..))) => {
                name.as_ref()
            }
            Self::Column(Column::Resolved(ResolvedColumn::OuterRef(Field { name, .. }, _))) => {
                name.as_ref()
            }
            Self::Column(Column::Bound(BoundColumn {
                field: Field { name, .. },
                ..
            })) => name.as_ref(),
            Self::Not(expr) => expr.name(),
            Self::IsNull(expr) => expr.name(),
            Self::NotNull(expr) => expr.name(),
            Self::FillNull(expr, ..) => expr.name(),
            Self::IsIn(expr, ..) => expr.name(),
            Self::Between(expr, ..) => expr.name(),
            Self::Literal(..) => "literal",
            Self::List(..) => "list",
            Self::Function { func, inputs } => match func {
                FunctionExpr::Struct(StructExpr::Get(name)) => name,
                _ => inputs.first().unwrap().name(),
            },
            Self::ScalarFunction(func) => match func.name() {
                "struct" => "struct", // FIXME: make struct its own expr variant
                "monotonically_increasing_id" => "monotonically_increasing_id", // Special case for functions with no inputs
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
            Self::Over(expr, ..) => expr.name(),
            Self::WindowFunction(expr) => expr.name(),
        }
    }

    pub fn get_type(&self, schema: &Schema) -> DaftResult<DataType> {
        Ok(self.to_field(schema)?.dtype)
    }

    pub fn get_name(&self, schema: &Schema) -> DaftResult<String> {
        Ok(self.to_field(schema)?.name)
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

    /// Returns the expression as SQL using PostgreSQL's dialect.
    pub fn to_sql(&self) -> Option<String> {
        fn to_sql_inner<W: Write>(expr: &Expr, buffer: &mut W) -> io::Result<()> {
            match expr {
                Expr::Column(Column::Resolved(ResolvedColumn::Basic(name))) => {
                    write!(buffer, "{}", name)
                }
                Expr::Literal(lit) => lit.display_sql(buffer),
                Expr::Alias(expr, ..) => to_sql_inner(expr, buffer),
                Expr::BinaryOp { op, left, right } => {
                    to_sql_inner(left, buffer)?;
                    let op = match op {
                        Operator::Eq => "=",
                        Operator::EqNullSafe => "<=>",
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
                            return Err(io::Error::other(
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
                | Expr::List(..)
                | Expr::Between(..)
                | Expr::Function { .. }
                | Expr::FillNull(..)
                | Expr::ScalarFunction { .. }
                | Expr::Subquery(..)
                | Expr::InSubquery(..)
                | Expr::Exists(..)
                | Expr::Over(..)
                | Expr::WindowFunction(..)
                | Expr::Column(_) => Err(io::Error::other(
                    "Unsupported expression for SQL translation",
                )),
            }
        }

        let mut buffer = Vec::new();
        to_sql_inner(self, &mut buffer)
            .ok()
            .and_then(|()| String::from_utf8(buffer).ok())
    }

    /// Returns the literal value if this is a literal expression, otherwise none.
    pub fn as_literal(&self) -> Option<&lit::LiteralValue> {
        match self {
            Self::Literal(lit) => Some(lit),
            _ => None,
        }
    }

    /// Returns the list vector if this is a list expression, otherwise none.
    pub fn as_list(&self) -> Option<&Vec<ExprRef>> {
        match self {
            Self::List(items) => Some(items),
            _ => None,
        }
    }

    pub fn has_compute(&self) -> bool {
        match self {
            Self::Column(..) => false,
            Self::Literal(..) => false,
            Self::Subquery(..) => false,
            Self::Exists(..) => false,
            Self::Function { .. } => true,
            Self::ScalarFunction(..) => true,
            Self::Agg(_) => true,
            Self::Over(..) => true,
            Self::WindowFunction(..) => true,
            Self::IsIn(..) => true,
            Self::Between(..) => true,
            Self::BinaryOp { .. } => true,
            Self::Alias(expr, ..) => expr.has_compute(),
            Self::Cast(expr, ..) => expr.has_compute(),
            Self::Not(expr) => expr.has_compute(),
            Self::IsNull(expr) => expr.has_compute(),
            Self::NotNull(expr) => expr.has_compute(),
            Self::FillNull(expr, fill_value) => expr.has_compute() || fill_value.has_compute(),
            Self::IfElse {
                if_true,
                if_false,
                predicate,
            } => if_true.has_compute() || if_false.has_compute() || predicate.has_compute(),
            Self::InSubquery(expr, _) => expr.has_compute(),
            Self::List(..) => true,
        }
    }

    pub fn eq_null_safe(self: ExprRef, other: ExprRef) -> ExprRef {
        binary_op(Operator::EqNullSafe, self, other)
    }

    /// Convert all basic resolved columns to left join side columns
    pub fn to_left_cols(self: ExprRef, schema: SchemaRef) -> DaftResult<ExprRef> {
        Ok(self
            .transform(|e| match e.as_ref() {
                Self::Column(Column::Resolved(ResolvedColumn::Basic(name)))
                | Self::Column(Column::Unresolved(UnresolvedColumn { name, .. })) => {
                    Ok(Transformed::yes(left_col(schema.get_field(name)?.clone())))
                }
                _ => Ok(Transformed::no(e)),
            })?
            .data)
    }

    /// Convert all basic resolved columns to right join side columns
    pub fn to_right_cols(self: ExprRef, schema: SchemaRef) -> DaftResult<ExprRef> {
        Ok(self
            .transform(|e| match e.as_ref() {
                Self::Column(Column::Resolved(ResolvedColumn::Basic(name)))
                | Self::Column(Column::Unresolved(UnresolvedColumn { name, .. })) => {
                    Ok(Transformed::yes(right_col(schema.get_field(name)?.clone())))
                }
                _ => Ok(Transformed::no(e)),
            })?
            .data)
    }

    pub fn explode(self: Arc<Self>) -> DaftResult<ExprRef> {
        let explode_fn = FUNCTION_REGISTRY.read().unwrap().get("explode").unwrap();
        let f = explode_fn.get_function(FunctionArgs::empty(), &Schema::empty())?;

        Ok(Self::ScalarFunction(ScalarFunction {
            udf: f,
            inputs: FunctionArgs::new_unchecked(vec![FunctionArg::Unnamed(self)]),
        })
        .arced())
    }
}

#[derive(Display, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Operator {
    #[display("==")]
    Eq,
    #[display("<=>")]
    EqNullSafe,
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
                | Self::EqNullSafe
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

impl FromStr for Operator {
    type Err = DaftError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "==" => Ok(Self::Eq),
            "!=" => Ok(Self::NotEq),
            "<" => Ok(Self::Lt),
            "<=" => Ok(Self::LtEq),
            ">" => Ok(Self::Gt),
            ">=" => Ok(Self::GtEq),
            "+" => Ok(Self::Plus),
            "-" => Ok(Self::Minus),
            "*" => Ok(Self::Multiply),
            "/" => Ok(Self::TrueDivide),
            "//" => Ok(Self::FloorDivide),
            "%" => Ok(Self::Modulus),
            "&" => Ok(Self::And),
            "|" => Ok(Self::Or),
            "^" => Ok(Self::Xor),
            "<<" => Ok(Self::ShiftLeft),
            ">>" => Ok(Self::ShiftRight),
            _ => Err(DaftError::ComputeError(format!("Invalid operator: {}", s))),
        }
    }
}

// Check if one set of columns is a reordering of the other
pub fn is_partition_compatible(a: &[ExprRef], b: &[ExprRef]) -> bool {
    // sort a and b by name
    let a_set: HashSet<&ExprRef> = HashSet::from_iter(a);
    let b_set: HashSet<&ExprRef> = HashSet::from_iter(b);
    a_set == b_set
}

pub fn has_agg(expr: &ExprRef) -> bool {
    use common_treenode::{TreeNode, TreeNodeRecursion};

    let mut found_agg = false;

    let _ = expr.apply(|e| match e.as_ref() {
        Expr::Agg(_) => {
            found_agg = true;
            Ok(TreeNodeRecursion::Stop)
        }
        Expr::Over(_, _) => Ok(TreeNodeRecursion::Jump),
        _ => Ok(TreeNodeRecursion::Continue),
    });

    found_agg
}

#[inline]
pub fn is_actor_pool_udf(expr: &ExprRef) -> bool {
    matches!(
        expr.as_ref(),
        Expr::Function {
            func: FunctionExpr::Python(PythonUDF {
                concurrency: Some(_),
                ..
            }),
            ..
        }
    )
}

pub fn count_actor_pool_udfs(exprs: &[ExprRef]) -> usize {
    exprs
        .iter()
        .map(|expr| {
            let mut count = 0;
            expr.apply(|e| {
                if is_actor_pool_udf(e) {
                    count += 1;
                }

                Ok(common_treenode::TreeNodeRecursion::Continue)
            })
            .unwrap();

            count
        })
        .sum()
}

pub fn estimated_selectivity(expr: &Expr, schema: &Schema) -> f64 {
    let estimate = match expr {
        // Boolean operations that filter rows
        Expr::BinaryOp { op, left, right } => {
            let left_selectivity = estimated_selectivity(left, schema);
            let right_selectivity = estimated_selectivity(right, schema);
            match op {
                // Fixed selectivity for all common comparisons
                Operator::Eq => 0.2,
                Operator::EqNullSafe => 0.2,
                Operator::NotEq => 0.8,
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => 0.3,

                // Logical operators with fixed estimates
                // Use the minimum selectivity of the two operands for AND
                // This is a more conservative estimate than the product of the two selectivities,
                // because we cannot assume independence between the two operands.
                Operator::And => left_selectivity.min(right_selectivity),
                // P(A or B) = P(A) + P(B) - P(A and B)
                Operator::Or => left_selectivity
                    .mul_add(-right_selectivity, left_selectivity + right_selectivity),
                // P(A xor B) = P(A) + P(B) - 2 * P(A and B)
                Operator::Xor => 2.0f64.mul_add(
                    -(left_selectivity * right_selectivity),
                    left_selectivity + right_selectivity,
                ),

                // Non-boolean operators don't filter
                Operator::Plus
                | Operator::Minus
                | Operator::Multiply
                | Operator::TrueDivide
                | Operator::FloorDivide
                | Operator::Modulus
                | Operator::ShiftLeft
                | Operator::ShiftRight => 1.0,
            }
        }

        // Revert selectivity for NOT
        Expr::Not(expr) => 1.0 - estimated_selectivity(expr, schema),

        // Fixed selectivity for IS NULL and IS NOT NULL, assume not many nulls
        Expr::IsNull(_) => 0.05,
        Expr::NotNull(_) => 0.95,

        // All membership operations use same selectivity
        Expr::IsIn(_, _) | Expr::Between(_, _, _) | Expr::InSubquery(_, _) | Expr::Exists(_) => 0.2,

        // Pass through for expressions that wrap other expressions
        Expr::Cast(expr, _) | Expr::Alias(expr, _) => estimated_selectivity(expr, schema),

        // Boolean literals
        Expr::Literal(lit) => match lit {
            lit::LiteralValue::Boolean(true) => 1.0,
            lit::LiteralValue::Boolean(false) => 0.0,
            _ => 1.0,
        },

        // String contains
        Expr::ScalarFunction(ScalarFunction { udf, .. }) if udf.name() == "contains" => 0.1,

        // Everything else that could be boolean gets 0.2, non-boolean gets 1.0
        Expr::ScalarFunction(_)
        | Expr::Function { .. }
        | Expr::Column(_)
        | Expr::IfElse { .. }
        | Expr::FillNull(_, _) => match expr.to_field(schema) {
            Ok(field) if field.dtype == DataType::Boolean => 0.2,
            _ => 1.0,
        },

        // Everything else doesn't filter
        Expr::Over(..) | Expr::WindowFunction(_) | Expr::Subquery(_) | Expr::List(_) => 1.0,
        Expr::Agg(_) => panic!("Aggregates are not allowed in WHERE clauses"),
    };

    // Lower bound to 1% to prevent overly selective estimate
    estimate.max(0.01)
}

pub fn exprs_to_schema(exprs: &[ExprRef], input_schema: SchemaRef) -> DaftResult<SchemaRef> {
    let fields = exprs
        .iter()
        .map(|e| e.to_field(&input_schema))
        .collect::<DaftResult<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

pub fn bound_exprs_to_schema(
    exprs: &[BoundExpr],
    input_schema: SchemaRef,
) -> DaftResult<SchemaRef> {
    let fields = exprs
        .iter()
        .map(|e| e.inner().to_field(&input_schema))
        .collect::<DaftResult<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Adds aliases as appropriate to ensure that all expressions have unique names.
pub fn deduplicate_expr_names(exprs: &[ExprRef]) -> Vec<ExprRef> {
    let mut names_so_far = HashSet::new();

    exprs
        .iter()
        .map(|e| {
            let curr_name = e.name();

            let mut i = 0;
            let mut new_name = curr_name.to_string();

            while names_so_far.contains(&new_name) {
                i += 1;
                new_name = format!("{}_{}", curr_name, i);
            }

            names_so_far.insert(new_name.clone());

            if i == 0 {
                e.clone()
            } else {
                e.alias(new_name)
            }
        })
        .collect()
}

/// Asserts an expr slice is homogeneous and returns the type, or None if empty or all nulls.
/// None allows for context-dependent handling such as erroring or defaulting to Null.
fn try_compute_is_in_type(exprs: &[ExprRef], schema: &Schema) -> DaftResult<Option<DataType>> {
    let mut dtype: Option<DataType> = None;
    for expr in exprs {
        let other_dtype = expr.get_type(schema)?;
        // other is null, continue
        if other_dtype == DataType::Null {
            continue;
        }
        // other != null and dtype is unset -> set dtype
        if dtype.is_none() {
            dtype = Some(other_dtype);
            continue;
        }
        // other != null and dtype is set -> compare or err!
        if dtype.as_ref() != Some(&other_dtype) {
            return Err(DaftError::TypeError(format!("Expected all arguments to be of the same type {}, but found element with type {other_dtype}", dtype.unwrap())));
        }
    }
    Ok(dtype)
}
