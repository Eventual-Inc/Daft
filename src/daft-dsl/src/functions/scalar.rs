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
    pub inputs: FunctionArgs<ExprRef>,
}

impl ScalarFunction {
    // TODO(cory): use FunctionArgs instead of `Vec<ExprRef>`
    pub fn new<UDF: ScalarUDF + 'static>(udf: UDF, inputs: Vec<ExprRef>) -> Self {
        let inputs = inputs.into_iter().map(FunctionArg::unnamed).collect();
        Self {
            udf: Arc::new(udf),
            inputs: FunctionArgs::new_unchecked(inputs),
        }
    }
    pub fn name(&self) -> &str {
        self.udf.name()
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        self.udf.function_args_to_field(self.inputs.clone(), schema)
    }
}

impl From<ScalarFunction> for ExprRef {
    fn from(func: ScalarFunction) -> Self {
        Expr::ScalarFunction(func).into()
    }
}

#[typetag::serde(tag = "type")]
pub trait ScalarUDF: Send + Sync + std::fmt::Debug {
    /// The name of the function.
    fn name(&self) -> &'static str;
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    #[deprecated = "use evaluate instead"]
    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        let inputs = FunctionArgs::try_new(
            inputs
                .iter()
                .map(|s| FunctionArg::unnamed(s.clone()))
                .collect(),
        )?;

        self.evaluate(inputs)
    }
    /// This is where the actual logic of the function is implemented.
    /// A simple example would be a string function such as `to_uppercase` that simply takes in a utf8 array and uppercases all values
    /// ```rs, no_run
    /// impl ScalarUDF for MyToUppercase {
    ///     fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series>
    ///         let s = inputs.required(0)?;
    ///
    ///         let arr = s
    ///             .utf8()
    ///             .expect("type should have been validated already during `function_args_to_field`")
    ///             .into_iter()
    ///             .map(|s_opt| s_opt.map(|s| s.to_uppercase()))
    ///             .collect::<Utf8Array>();
    ///
    ///         Ok(arr.into_series())
    ///     }
    /// }
    /// ```
    fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series>;

    /// `function_args_to_field` is used during planning to ensure that args and datatypes are compatible.
    /// A simple example would be a string function such as `to_uppercase` that expects a single string input, and a single string output.
    /// ```rs, no_run
    /// impl ScalarUDF for MyToUppercase {
    ///     fn function_args_to_field(
    ///         &self,
    ///         inputs: FunctionArgs<ExprRef>,
    ///         schema: &Schema,
    ///     ) -> DaftResult<Field> {
    ///         ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
    ///         /// grab the first positional value from `inputs`
    ///         let input = inputs.required(0)?.to_field(schema)?;
    ///         // make sure the input is a string datatype
    ///         ensure!(input.dtype.is_string(), "expected string");
    ///         Ok(input)
    ///     }
    /// }
    /// ```
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
            .map(|e| FunctionArg::unnamed(e.clone()))
            .collect::<Vec<_>>();

        let inputs = FunctionArgs::new_unchecked(inputs);
        self.function_args_to_field(inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "No documentation available"
    }
}

pub fn scalar_function_semantic_id(func: &ScalarFunction, schema: &Schema) -> FieldID {
    let inputs = func
        .inputs
        .clone()
        .into_inner()
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
        for (i, input) in self.inputs.clone().into_inner().into_iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{input}")?;
        }
        write!(f, ")")?;
        Ok(())
    }
}
