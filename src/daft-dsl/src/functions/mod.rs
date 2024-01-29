pub mod float;
pub mod image;
pub mod list;
pub mod numeric;
pub mod partitioning;
pub mod struct_;
pub mod temporal;
pub mod uri;
pub mod utf8;

use std::fmt::{Formatter, Result};

use self::image::ImageExpr;
use self::list::ListExpr;
use self::numeric::NumericExpr;
use self::partitioning::PartitioningExpr;
use self::struct_::StructExpr;
use self::temporal::TemporalExpr;
use self::utf8::Utf8Expr;
use self::{float::FloatExpr, uri::UriExpr};
use common_error::DaftResult;
use daft_core::datatypes::FieldID;
use daft_core::{datatypes::Field, schema::Schema, series::Series};
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
pub mod python;
#[cfg(feature = "python")]
use python::PythonUDF;

use super::Expr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FunctionExpr {
    Numeric(NumericExpr),
    Float(FloatExpr),
    Utf8(Utf8Expr),
    Temporal(TemporalExpr),
    List(ListExpr),
    Struct(StructExpr),
    Image(ImageExpr),
    #[cfg(feature = "python")]
    Python(PythonUDF),
    Partitioning(PartitioningExpr),
    Uri(UriExpr),
}

pub trait FunctionEvaluator {
    fn fn_name(&self) -> &'static str;
    fn to_field(&self, inputs: &[Expr], schema: &Schema, expr: &Expr) -> DaftResult<Field>;
    fn evaluate(&self, inputs: &[Series], expr: &Expr) -> DaftResult<Series>;
}

impl FunctionExpr {
    #[inline]
    fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use FunctionExpr::*;
        match self {
            Numeric(expr) => expr.get_evaluator(),
            Float(expr) => expr.get_evaluator(),
            Utf8(expr) => expr.get_evaluator(),
            Temporal(expr) => expr.get_evaluator(),
            List(expr) => expr.get_evaluator(),
            Struct(expr) => expr.get_evaluator(),
            Image(expr) => expr.get_evaluator(),
            Uri(expr) => expr.get_evaluator(),
            #[cfg(feature = "python")]
            Python(expr) => expr,
            Partitioning(expr) => expr.get_evaluator(),
        }
    }
}

impl FunctionEvaluator for FunctionExpr {
    fn fn_name(&self) -> &'static str {
        self.get_evaluator().fn_name()
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, expr: &Expr) -> DaftResult<Field> {
        self.get_evaluator().to_field(inputs, schema, expr)
    }

    fn evaluate(&self, inputs: &[Series], expr: &Expr) -> DaftResult<Series> {
        self.get_evaluator().evaluate(inputs, expr)
    }
}

pub fn function_display(f: &mut Formatter, func: &FunctionExpr, inputs: &[Expr]) -> Result {
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

pub fn function_semantic_id(func: &FunctionExpr, inputs: &[Expr], schema: &Schema) -> FieldID {
    let inputs = inputs
        .iter()
        .map(|expr| expr.semantic_id(schema).id.to_string())
        .collect::<Vec<String>>()
        .join(", ");
    // TODO: check for function idempotency here.
    FieldID::new(format!("Function_{func:?}({inputs})"))
}
