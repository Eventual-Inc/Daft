use std::{
    any::TypeId,
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
        self.udf.get_return_field(self.inputs.clone(), schema)
    }

    /// Returns true if `self.udf` is of type `F`
    pub fn is_function_type<F: ScalarUDF>(&self) -> bool {
        self.udf.as_ref().type_id() == TypeId::of::<F>()
    }
}

impl From<ScalarFunction> for ExprRef {
    fn from(func: ScalarFunction) -> Self {
        Expr::ScalarFunction(func).into()
    }
}

/// This is a factory for scalar function implementations.
///
/// TODO:
///   Rename to ScalarFunction (or similar) once ScalarFunction is migrated
///   to ScalarUDF, and update ScalarUDF to ScalarFunctionImpl (or similar).
///   Then update Expr::Function(ScalarUDF) to Expr::Function(ScalarFunctionFactory)
///   which will enable *name* resolution within the DSL, but then we now have
///   the ability for *type* resolution during planning via get_function. We can
///   build rule-based type resolution at a later time.
///
pub trait ScalarFunctionFactory: Send + Sync {
    /// The name of this function.
    fn name(&self) -> &'static str;

    /// Any additional aliases for this function.
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    /// Returns a ScalarUDF for the given fields.
    ///
    /// Note:
    ///   When the time comes, we should replace FunctionArgs<ExprRef> with bound ExprRef.
    ///   This way we have the pair (Expr, Field) so we don't have to keep re-computing
    ///   expression types each time we resolve a function. At present, I wanted to keep
    ///   this signature the exact same as get_return_field.
    ///
    fn get_function(
        &self,
        args: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Arc<dyn ScalarUDF>>;
}

/// This is a concrete implementation of a ScalarFunction.
#[typetag::serde(tag = "type")]
pub trait ScalarUDF: Send + Sync + std::fmt::Debug + std::any::Any {
    /// The name of the function.
    fn name(&self) -> &'static str;

    /// Any additional aliases for this function.
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    /// This is where the actual logic of the function is implemented.
    /// A simple example would be a string function such as `to_uppercase` that simply takes in a utf8 array and uppercases all values
    /// ```rs, no_run
    /// impl ScalarUDF for MyToUppercase {
    ///     fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series>
    ///         let s = inputs.required(0)?;
    ///
    ///         let arr = s
    ///             .utf8()
    ///             .expect("type should have been validated already during `get_return_field`")
    ///             .into_iter()
    ///             .map(|s_opt| s_opt.map(|s| s.to_uppercase()))
    ///             .collect::<Utf8Array>();
    ///
    ///         Ok(arr.into_series())
    ///     }
    /// }
    /// ```
    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series>;

    /// `get_return_field` is used during planning to ensure that args and datatypes are compatible.
    /// A simple example would be a string function such as `to_uppercase` that expects a single string input, and a single string output.
    /// ```rs, no_run
    /// impl ScalarUDF for MyToUppercase {
    ///     fn get_return_field(
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
    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field>;

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

/// Function factory which is backed by a single dynamic ScalarUDF.
pub struct DynamicScalarFunction(Arc<dyn ScalarUDF>);

impl From<Arc<dyn ScalarUDF>> for DynamicScalarFunction {
    fn from(value: Arc<dyn ScalarUDF>) -> Self {
        Self(value)
    }
}

impl ScalarFunctionFactory for DynamicScalarFunction {
    /// Delegate to inner.
    fn name(&self) -> &'static str {
        self.0.name()
    }

    /// Delegate to inner.
    fn aliases(&self) -> &'static [&'static str] {
        self.0.aliases()
    }

    /// All typing for implementation variants is done during evaluation, hence dynamic.
    fn get_function(&self, _: FunctionArgs<ExprRef>, _: &Schema) -> DaftResult<Arc<dyn ScalarUDF>> {
        Ok(self.0.clone())
    }
}
