use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

use common_error::DaftResult;
use daft_core::prelude::*;
use serde::{Deserialize, Serialize};

use super::function_args::{FunctionArg, FunctionArgs};
use crate::{Expr, ExprRef};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalarFunction {
    pub udf: Arc<dyn ScalarUDF>,
    pub inputs: Vec<ExprRef>,
}

impl ScalarFunction {
    pub fn new<UDF: ScalarUDF + 'static>(udf: UDF, inputs: Vec<ExprRef>) -> Self {
        Self {
            udf: Arc::new(udf),
            inputs,
        }
    }
    pub fn name(&self) -> &str {
        self.udf.name()
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        let inputs = self
            .inputs
            .iter()
            .map(|expr| {
                if let Expr::NamedExpr { name, expr } = &**expr {
                    FunctionArg::named(name.clone(), expr.clone())
                } else {
                    FunctionArg::unnamed(expr.clone())
                }
            })
            .collect::<Vec<_>>();

        let inputs = FunctionArgs::try_new(inputs)?;

        self.udf.function_args_to_field(inputs, schema)
    }
}

impl From<ScalarFunction> for ExprRef {
    fn from(func: ScalarFunction) -> Self {
        Expr::ScalarFunction(func).into()
    }
}

#[typetag::serde(tag = "type")]
pub trait ScalarUDF: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &'static str;
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }
    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        let inputs = FunctionArgs::try_new(
            inputs
                .iter()
                .map(|s| FunctionArg::unnamed(s.clone()))
                .collect(),
        )?;

        self.evaluate(inputs)
    }

    fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series>;

    #[allow(deprecated)]
    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        // for backwards compatibility we add a default implementation.
        // TODO: move all existing implementations of `to_field` over to `function_args_to_field`
        // Once that is done, we can name it back to `to_field`.
        self.to_field(inputs.into_inner().as_slice(), schema)
    }

    #[deprecated = "use `function_args_to_field` instead"]
    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        let inputs = inputs
            .iter()
            .map(|expr| {
                if let Expr::NamedExpr { name, expr } = &**expr {
                    FunctionArg::named(name.clone(), expr.clone())
                } else {
                    FunctionArg::unnamed(expr.clone())
                }
            })
            .collect::<Vec<_>>();

        let inputs = FunctionArgs::try_new(inputs)?;
        self.function_args_to_field(inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "No documentation available"
    }
}

pub fn scalar_function_semantic_id(func: &ScalarFunction, schema: &Schema) -> FieldID {
    let inputs = func
        .inputs
        .iter()
        .map(|expr| expr.semantic_id(schema).id.to_string())
        .collect::<Vec<String>>()
        .join(", ");
    // TODO: check for function idempotency here.
    FieldID::new(format!("Function_{func:?}({inputs})"))
}

impl PartialEq for ScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name() && self.inputs == other.inputs
    }
}

impl Eq for ScalarFunction {}
impl std::hash::Hash for ScalarFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.inputs.hash(state);
    }
}

impl Display for ScalarFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}(", self.name())?;
        for (i, input) in self.inputs.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{input}")?;
        }
        write!(f, ")")?;
        Ok(())
    }
}
