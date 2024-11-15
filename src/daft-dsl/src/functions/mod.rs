pub mod agg;
pub mod map;
pub mod partitioning;
pub mod python;
pub mod scalar;
pub mod sketch;
pub mod struct_;

use std::{
    fmt::{Display, Formatter, Result, Write},
    hash::Hash,
};

use common_error::DaftResult;
use daft_core::prelude::*;
use python::PythonUDF;
pub use scalar::*;
use serde::{Deserialize, Serialize};

use self::{map::MapExpr, partitioning::PartitioningExpr, sketch::SketchExpr, struct_::StructExpr};
use crate::{Expr, ExprRef, Operator};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FunctionExpr {
    Map(MapExpr),
    Sketch(SketchExpr),
    Struct(StructExpr),
    Python(PythonUDF),
    Partitioning(PartitioningExpr),
}

pub trait FunctionEvaluator {
    fn fn_name(&self) -> &'static str;
    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        expr: &FunctionExpr,
    ) -> DaftResult<Field>;
    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series>;
}

impl FunctionExpr {
    #[inline]
    fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        match self {
            Self::Map(expr) => expr.get_evaluator(),
            Self::Sketch(expr) => expr.get_evaluator(),
            Self::Struct(expr) => expr.get_evaluator(),
            Self::Python(expr) => expr.get_evaluator(),
            Self::Partitioning(expr) => expr.get_evaluator(),
        }
    }
}

impl Display for FunctionExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.fn_name())
    }
}

impl FunctionEvaluator for FunctionExpr {
    fn fn_name(&self) -> &'static str {
        self.get_evaluator().fn_name()
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        expr: &FunctionExpr,
    ) -> DaftResult<Field> {
        self.get_evaluator().to_field(inputs, schema, expr)
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        self.get_evaluator().evaluate(inputs, expr)
    }
}

pub fn function_display(f: &mut Formatter, func: &FunctionExpr, inputs: &[ExprRef]) -> Result {
    write!(f, "{}(", func)?;
    for (i, input) in inputs.iter().enumerate() {
        if i != 0 {
            write!(f, ", ")?;
        }
        write!(f, "{input}")?;
    }
    write!(f, ")")?;
    Ok(())
}

pub fn function_display_without_formatter(
    func: &FunctionExpr,
    inputs: &[ExprRef],
) -> std::result::Result<String, std::fmt::Error> {
    let mut f = String::default();
    write!(&mut f, "{}(", func)?;
    for (i, input) in inputs.iter().enumerate() {
        if i != 0 {
            write!(&mut f, ", ")?;
        }
        write!(&mut f, "{input}")?;
    }
    write!(&mut f, ")")?;
    Ok(f)
}

pub fn is_in_display_without_formatter(
    expr: &ExprRef,
    inputs: &[ExprRef],
) -> std::result::Result<String, std::fmt::Error> {
    let mut f = String::default();
    write!(&mut f, "{expr} IN (")?;
    for (i, input) in inputs.iter().enumerate() {
        if i != 0 {
            write!(&mut f, ", ")?;
        }
        write!(&mut f, "{input}")?;
    }
    write!(&mut f, ")")?;
    Ok(f)
}

pub fn binary_op_display_without_formatter(
    op: &Operator,
    left: &ExprRef,
    right: &ExprRef,
) -> std::result::Result<String, std::fmt::Error> {
    let mut f = String::default();
    let write_out_expr = |f: &mut String, input: &Expr| match input {
        Expr::Alias(e, _) => write!(f, "{e}"),
        Expr::BinaryOp { .. } => write!(f, "[{input}]"),
        _ => write!(f, "{input}"),
    };
    write_out_expr(&mut f, left)?;
    write!(&mut f, " {op} ")?;
    write_out_expr(&mut f, right)?;
    Ok(f)
}

pub fn function_semantic_id(func: &FunctionExpr, inputs: &[ExprRef], schema: &Schema) -> FieldID {
    let inputs = inputs
        .iter()
        .map(|expr| expr.semantic_id(schema).id.to_string())
        .collect::<Vec<String>>()
        .join(", ");
    // TODO: check for function idempotency here.
    FieldID::new(format!("Function_{func:?}({inputs})"))
}
