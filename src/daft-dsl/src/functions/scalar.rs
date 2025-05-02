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
        self.udf.to_field(&self.inputs, schema)
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
    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field>;
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

#[cfg(test)]
mod tests {
    use crate::functions::function_args::{FunctionArg, FunctionArgs};
    #[test]
    fn test_function_args_ordering() {
        let res = FunctionArgs::try_new(vec![
            FunctionArg::unnamed(1),
            FunctionArg::unnamed(2),
            FunctionArg::named("arg1", 3),
        ]);

        assert!(res.is_err());
    }
    #[test]
    fn test_function_args_ordering_invalid() {
        let res = FunctionArgs::try_new(vec![
            FunctionArg::unnamed(1),
            FunctionArg::named("arg1", 2),
            FunctionArg::unnamed(3),
        ]);

        assert!(res.is_err());
    }
}
