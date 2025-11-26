use std::{
    any::TypeId,
    fmt::{Display, Formatter},
    sync::Arc,
};

use common_error::DaftResult;
use daft_core::prelude::*;
use derive_more::Display;
use serde::{Deserialize, Serialize};

use super::function_args::{FunctionArg, FunctionArgs};
use crate::{Expr, ExprRef, python_udf::PyScalarFn};

#[derive(Display, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ScalarFn {
    Builtin(BuiltinScalarFn),
    Python(PyScalarFn),
}

impl ScalarFn {
    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        match self {
            Self::Builtin(builtin_scalar_func) => builtin_scalar_func.to_field(schema),
            Self::Python(python_scalar_func) => python_scalar_func.to_field(schema),
        }
    }

    pub fn builtin<UDF: ScalarUDF + 'static>(udf: UDF, inputs: Vec<ExprRef>) -> Self {
        Self::Builtin(BuiltinScalarFn::new(udf, inputs))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum BuiltinScalarFnVariant {
    Sync(Arc<dyn ScalarUDF>),
    Async(Arc<dyn AsyncScalarUDF>),
}
impl std::fmt::Debug for BuiltinScalarFnVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sync(udf) => write!(f, "{}", udf.name()),
            Self::Async(udf) => write!(f, "{}", udf.name()),
        }
    }
}

impl BuiltinScalarFnVariant {
    pub fn sync(udf: Arc<dyn ScalarUDF>) -> Self {
        Self::Sync(udf)
    }

    pub fn async_(udf: Arc<dyn AsyncScalarUDF>) -> Self {
        Self::Async(udf)
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Sync(udf) => udf.name(),
            Self::Async(udf) => udf.name(),
        }
    }

    pub fn aliases(&self) -> &'static [&'static str] {
        match self {
            Self::Sync(udf) => udf.aliases(),
            Self::Async(udf) => udf.aliases(),
        }
    }

    pub fn type_id(&self) -> TypeId {
        match self {
            Self::Sync(udf) => udf.as_ref().type_id(),
            Self::Async(udf) => udf.as_ref().type_id(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuiltinScalarFn {
    pub func: BuiltinScalarFnVariant,
    pub inputs: FunctionArgs<ExprRef>,
}

impl BuiltinScalarFn {
    // TODO(cory): use FunctionArgs instead of `Vec<ExprRef>`
    pub fn new<UDF: ScalarUDF + 'static>(udf: UDF, inputs: Vec<ExprRef>) -> Self {
        let inputs = inputs.into_iter().map(FunctionArg::unnamed).collect();
        Self {
            func: BuiltinScalarFnVariant::Sync(Arc::new(udf)),
            inputs: FunctionArgs::new_unchecked(inputs),
        }
    }
    pub fn new_async<UDF: AsyncScalarUDF + 'static>(udf: UDF, inputs: Vec<ExprRef>) -> Self {
        let inputs = inputs.into_iter().map(FunctionArg::unnamed).collect();
        Self {
            func: BuiltinScalarFnVariant::Async(Arc::new(udf)),
            inputs: FunctionArgs::new_unchecked(inputs),
        }
    }
    pub fn name(&self) -> &str {
        self.func.name()
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        match &self.func {
            BuiltinScalarFnVariant::Sync(scalar_udf) => {
                scalar_udf.get_return_field(self.inputs.clone(), schema)
            }
            BuiltinScalarFnVariant::Async(async_scalar_udf) => {
                async_scalar_udf.get_return_field(self.inputs.clone(), schema)
            }
        }
    }

    /// Returns true if `self.udf` is of type `F`
    pub fn is_function_type<F: ScalarUDF>(&self) -> bool {
        match &self.func {
            BuiltinScalarFnVariant::Sync(scalar_udf) => {
                scalar_udf.as_ref().type_id() == TypeId::of::<F>()
            }
            BuiltinScalarFnVariant::Async(async_scalar_udf) => {
                async_scalar_udf.as_ref().type_id() == TypeId::of::<F>()
            }
        }
    }
}

impl From<ScalarFn> for ExprRef {
    fn from(func: ScalarFn) -> Self {
        Self::new(Expr::ScalarFn(func))
    }
}

impl From<Arc<dyn ScalarUDF>> for BuiltinScalarFnVariant {
    fn from(func: Arc<dyn ScalarUDF>) -> Self {
        Self::Sync(func)
    }
}

impl From<Arc<dyn AsyncScalarUDF>> for BuiltinScalarFnVariant {
    fn from(func: Arc<dyn AsyncScalarUDF>) -> Self {
        Self::Async(func)
    }
}

impl From<BuiltinScalarFn> for ExprRef {
    fn from(func: BuiltinScalarFn) -> Self {
        Self::new(Expr::ScalarFn(ScalarFn::Builtin(func)))
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
    ) -> DaftResult<BuiltinScalarFnVariant>;
}

/// This is a concrete implementation of a ScalarFunction.
#[typetag::serde(tag = "type")]
pub trait ScalarUDF: Send + Sync + std::any::Any {
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

/// This is a concrete implementation of a ScalarFunction.
#[typetag::serde(tag = "type")]
#[async_trait::async_trait]
pub trait AsyncScalarUDF: Send + Sync + std::any::Any {
    fn preferred_batch_size(&self, _inputs: FunctionArgs<ExprRef>) -> DaftResult<Option<usize>> {
        Ok(None)
    }

    /// The name of the function.
    fn name(&self) -> &'static str;

    /// Any additional aliases for this function.
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    async fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series>;

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field>;

    fn docstring(&self) -> &'static str {
        "No documentation available"
    }
}

pub fn scalar_function_semantic_id(func: &BuiltinScalarFn, schema: &Schema) -> FieldID {
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

impl PartialEq for BuiltinScalarFn {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name() && self.inputs == other.inputs
    }
}

impl Eq for BuiltinScalarFn {}
impl std::hash::Hash for BuiltinScalarFn {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.inputs.hash(state);
    }
}

impl Display for BuiltinScalarFn {
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

/// Function factory
/// Function factory which is backed by a single dynamic ScalarUDF.
pub struct DynamicScalarFunction(BuiltinScalarFnVariant);

impl From<BuiltinScalarFnVariant> for DynamicScalarFunction {
    fn from(value: BuiltinScalarFnVariant) -> Self {
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
    fn get_function(
        &self,
        _: FunctionArgs<ExprRef>,
        _: &Schema,
    ) -> DaftResult<BuiltinScalarFnVariant> {
        Ok(self.0.clone())
    }
}
