use daft_core::{
    count_mode::CountMode,
    datatypes::DataType,
    datatypes::{Field, FieldID},
    schema::Schema,
    utils::supertype::try_get_supertype,
};

use crate::{
    functions::FunctionEvaluator,
    lit,
    optimization::{get_required_columns, requires_computation},
};

use common_error::{DaftError, DaftResult};

use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display, Formatter, Result},
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
        inputs: Vec<Expr>,
    },
    Not(ExprRef),
    IsNull(ExprRef),
    Literal(lit::LiteralValue),
    IfElse {
        if_true: ExprRef,
        if_false: ExprRef,
        predicate: ExprRef,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AggExpr {
    Count(ExprRef, CountMode),
    Sum(ExprRef),
    Mean(ExprRef),
    Min(ExprRef),
    Max(ExprRef),
    List(ExprRef),
    Concat(ExprRef),
}

pub fn col<S: Into<Arc<str>>>(name: S) -> Expr {
    Expr::Column(name.into())
}

pub fn binary_op(op: Operator, left: &Expr, right: &Expr) -> Expr {
    Expr::BinaryOp {
        op,
        left: left.clone().into(),
        right: right.clone().into(),
    }
}

impl AggExpr {
    pub fn name(&self) -> DaftResult<&str> {
        use AggExpr::*;
        match self {
            Count(expr, ..)
            | Sum(expr)
            | Mean(expr)
            | Min(expr)
            | Max(expr)
            | List(expr)
            | Concat(expr) => expr.name(),
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
            List(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_list()"))
            }
            Concat(expr) => {
                let child_id = expr.semantic_id(schema);
                FieldID::new(format!("{child_id}.local_concat()"))
            }
        }
    }

    pub fn child(&self) -> ExprRef {
        use AggExpr::*;
        match self {
            Count(expr, ..)
            | Sum(expr)
            | Mean(expr)
            | Min(expr)
            | Max(expr)
            | List(expr)
            | Concat(expr) => expr.clone(),
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
                    match &field.dtype {
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                            DataType::Int64
                        }
                        DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64 => DataType::UInt64,
                        DataType::Float32 => DataType::Float32,
                        DataType::Float64 => DataType::Float64,
                        other => {
                            return Err(DaftError::TypeError(format!(
                                "Expected input to sum() to be numeric but received dtype {} for column \"{}\"",
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
                        | DataType::Float64 => DataType::Float64,
                        other => {
                            return Err(DaftError::TypeError(format!(
                                "Numeric mean is not implemented for column \"{}\" of type {}",
                                field.name, other,
                            )))
                        }
                    },
                ))
            }
            Min(expr) | Max(expr) => {
                let field = expr.to_field(schema)?;
                // TODO: [ISSUE-688] Make Binary type comparable
                if field.dtype == DataType::Binary {
                    return Err(DaftError::TypeError(format!(
                        "Cannot get min/max of Binary type in column \"{}\"",
                        field.name,
                    )));
                }
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
        }
    }

    pub fn from_name_and_child_expr(name: &str, child: &Expr) -> DaftResult<AggExpr> {
        use AggExpr::*;
        match name {
            "count" => Ok(Count(child.clone().into(), CountMode::Valid)),
            "sum" => Ok(Sum(child.clone().into())),
            "mean" => Ok(Mean(child.clone().into())),
            "min" => Ok(Min(child.clone().into())),
            "max" => Ok(Max(child.clone().into())),
            "list" => Ok(List(child.clone().into())),
            _ => Err(DaftError::ValueError(format!(
                "{} not a valid aggregation name",
                name
            ))),
        }
    }
}

impl AsRef<Expr> for Expr {
    fn as_ref(&self) -> &Expr {
        self
    }
}

impl Expr {
    pub fn alias<S: Into<Arc<str>>>(&self, name: S) -> Self {
        Expr::Alias(self.clone().into(), name.into())
    }

    pub fn if_else(&self, if_true: &Self, if_false: &Self) -> Self {
        Expr::IfElse {
            if_true: if_true.clone().into(),
            if_false: if_false.clone().into(),
            predicate: self.clone().into(),
        }
    }

    pub fn cast(&self, dtype: &DataType) -> Self {
        Expr::Cast(self.clone().into(), dtype.clone())
    }

    pub fn count(&self, mode: CountMode) -> Self {
        Expr::Agg(AggExpr::Count(self.clone().into(), mode))
    }

    pub fn sum(&self) -> Self {
        Expr::Agg(AggExpr::Sum(self.clone().into()))
    }

    pub fn mean(&self) -> Self {
        Expr::Agg(AggExpr::Mean(self.clone().into()))
    }

    pub fn min(&self) -> Self {
        Expr::Agg(AggExpr::Min(self.clone().into()))
    }

    pub fn max(&self) -> Self {
        Expr::Agg(AggExpr::Max(self.clone().into()))
    }

    pub fn agg_list(&self) -> Self {
        Expr::Agg(AggExpr::List(self.clone().into()))
    }

    pub fn agg_concat(&self) -> Self {
        Expr::Agg(AggExpr::Concat(self.clone().into()))
    }

    pub fn not(&self) -> Self {
        Expr::Not(self.clone().into())
    }

    pub fn is_null(&self) -> Self {
        Expr::IsNull(self.clone().into())
    }

    pub fn eq(&self, other: &Self) -> Self {
        binary_op(Operator::Eq, self, other)
    }

    pub fn not_eq(&self, other: &Self) -> Self {
        binary_op(Operator::NotEq, self, other)
    }

    pub fn and(&self, other: &Self) -> Self {
        binary_op(Operator::And, self, other)
    }

    pub fn or(&self, other: &Self) -> Self {
        binary_op(Operator::Or, self, other)
    }

    pub fn lt(&self, other: &Self) -> Self {
        binary_op(Operator::Lt, self, other)
    }

    pub fn lt_eq(&self, other: &Self) -> Self {
        binary_op(Operator::LtEq, self, other)
    }

    pub fn gt(&self, other: &Self) -> Self {
        binary_op(Operator::Gt, self, other)
    }

    pub fn gt_eq(&self, other: &Self) -> Self {
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
            Function { func, inputs } => {
                let inputs = inputs
                    .iter()
                    .map(|expr| expr.semantic_id(schema).id.to_string())
                    .collect::<Vec<String>>()
                    .join(", ");
                // TODO: check for function idempotency here.
                FieldID::new(format!("Function_{func:?}({inputs})"))
            }
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
        }
    }

    pub fn children(&self) -> Vec<ExprRef> {
        use Expr::*;
        match self {
            // No children.
            Column(..) => vec![],
            Literal(..) => vec![],

            // One child.
            Not(expr) | IsNull(expr) | Cast(expr, ..) | Alias(expr, ..) => vec![expr.clone()],
            Agg(agg_expr) => vec![agg_expr.child()],

            // Multiple children.
            Function { inputs, .. } => inputs.iter().map(|e| e.clone().into()).collect(),
            BinaryOp { left, right, .. } => {
                vec![left.clone(), right.clone()]
            }
            IfElse {
                if_true,
                if_false,
                predicate,
            } => {
                vec![predicate.clone(), if_true.clone(), if_false.clone()]
            }
        }
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        use Expr::*;
        match self {
            Alias(expr, name) => Ok(Field::new(name.as_ref(), expr.get_type(schema)?)),
            Agg(agg_expr) => agg_expr.to_field(schema),
            Cast(expr, dtype) => Ok(Field::new(expr.name()?, dtype.clone())),
            Column(name) => Ok(schema.get_field(name).cloned()?),
            Not(expr) => {
                let child_field = expr.to_field(schema)?;
                match child_field.dtype {
                    DataType::Boolean => Ok(Field::new(expr.name()?, DataType::Boolean)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expected argument to be a Boolean expression, but received {child_field}",
                    ))),
                }
            }
            IsNull(expr) => Ok(Field::new(expr.name()?, DataType::Boolean)),
            Literal(value) => Ok(Field::new("literal", value.get_type())),
            Function { func, inputs } => func.to_field(inputs.as_slice(), schema, self),
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
                        let (result_type, _comp_type) =
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
                let if_true_field = if_true.to_field(schema)?;
                let if_false_field = if_false.to_field(schema)?;
                let predicate_field = predicate.to_field(schema)?;
                if predicate_field.dtype != DataType::Boolean {
                    return Err(DaftError::TypeError(format!(
                        "Expected predicate for if_else to be boolean but received {predicate_field}",
                    )));
                }
                match try_get_supertype(&if_true_field.dtype, &if_false_field.dtype) {
                    Ok(supertype) => Ok(Field::new(if_true_field.name, supertype)),
                    Err(_) => Err(DaftError::TypeError(format!("Expected if_true and if_false arguments for if_else to be castable to the same supertype, but received {if_true_field} and {if_false_field}")))
                }
            }
        }
    }

    pub fn name(&self) -> DaftResult<&str> {
        use Expr::*;
        match self {
            Alias(.., name) => Ok(name.as_ref()),
            Agg(agg_expr) => agg_expr.name(),
            Cast(expr, ..) => expr.name(),
            Column(name) => Ok(name.as_ref()),
            Not(expr) => expr.name(),
            IsNull(expr) => expr.name(),
            Literal(..) => Ok("literal"),
            Function { func: _, inputs } => inputs.first().unwrap().name(),
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

    pub fn input_mapping(&self) -> Option<String> {
        let required_columns = get_required_columns(self);
        let requires_computation = requires_computation(self);

        // Return the required column only if:
        //   1. There is only one required column
        //   2. No computation is run on this required column
        match (&required_columns[..], requires_computation) {
            ([required_col], false) => Some(required_col.clone()),
            _ => None,
        }
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
            Literal(val) => write!(f, "lit({val})"),
            Function { func, inputs } => {
                write!(f, "{}(", func.fn_name())?;
                for (i, input) in inputs.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{input}")?;
                }
                write!(f, ")")?;
                Ok(())
            }
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
            Mean(expr) => write!(f, "mean({expr})"),
            Min(expr) => write!(f, "min({expr})"),
            Max(expr) => write!(f, "max({expr})"),
            List(expr) => write!(f, "list({expr})"),
            Concat(expr) => write!(f, "list({expr})"),
        }
    }
}

/// Based on Polars first class operators: https://github.com/pola-rs/polars/blob/master/polars/polars-lazy/polars-plan/src/dsl/expr.rs
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::lit;
    #[test]
    fn check_comparison_type() -> DaftResult<()> {
        let x = lit(10.);
        let y = lit(12);
        let schema = Schema::empty();

        let z = Expr::BinaryOp {
            left: x.into(),
            right: y.into(),
            op: Operator::Lt,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Boolean);
        Ok(())
    }

    #[test]
    fn check_alias_type() -> DaftResult<()> {
        let a = col("a");
        let b = a.alias("b");
        match b {
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
            left: x.into(),
            right: y.into(),
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        let x = lit(10.);
        let y = lit(12);

        let z = Expr::BinaryOp {
            left: y.into(),
            right: x.into(),
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
            left: x.into(),
            right: y.into(),
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        let x = col("x");
        let y = col("y");

        let z = Expr::BinaryOp {
            left: y.into(),
            right: x.into(),
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        Ok(())
    }
}
