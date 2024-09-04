use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use common_error::DaftResult;
use daft_core::datatypes::FieldID;
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use crate::{Expr, ExprRef};

use serde::{Deserialize, Serialize};

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
    fn as_any(&self) -> &dyn Any;
    fn name(&self) -> &'static str;
    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series>;
    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field>;
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

#[macro_export]
macro_rules! impl_scalar_udf {
    (
        name: $func_name:expr,
        to_field: ($inputs:ident, $schema:ident) $to_field:block,
        evaluate: ($eval_inputs:ident) $evaluate:block
    ) => {
        paste::paste! {
            #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
            pub struct [<$func_name:camel Function>] {}

            #[typetag::serde]
            impl $crate::functions::ScalarUDF for [<$func_name:camel Function>] {
                fn as_any(&self) -> &dyn std::any::Any { self }
                fn name(&self) -> &'static str { $func_name }
                fn to_field(&self, $inputs: &[$crate::ExprRef], $schema: &daft_core::schema::Schema) -> common_error::DaftResult<daft_core::datatypes::Field> $to_field
                fn evaluate(&self, $eval_inputs: &[daft_core::series::Series]) -> common_error::DaftResult<daft_core::series::Series> $evaluate
            }

        }

    };
}

/// generates a scalar UDF implementation
/// # Example
/// ```rust,ignore
///
/// make_unary_udf_function!{
///     name: "my_udf",
///     to_field: (inputs, schema) {
///       // implementation
///     },
///     evaluate: (inputs) {
///       // implementation
///     }
/// }
/// ```
/// generates the following code
/// ```rust,ignore
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
/// pub struct MyUDF {}
///
/// #[typetag::serde]
/// impl ScalarUDF for MyUDF {
///    fn as_any(&self) -> &dyn std::any::Any { self }
///    fn name(&self) -> &'static str { "my_udf" }
///
///    fn to_field(&self, inputs: &[ExprRef], schema: &daft_core::schema::Schema) -> common_error::DaftResult<daft_core::datatypes::Field> {
///      // implementation
///    }
///
///    fn evaluate(&self, inputs: &[daft_core::series::Series]) -> common_error::DaftResult<daft_core::series::Series> {
///      // implementation
///    }
/// }
/// fn my_udf(input: ExprRef) -> ExprRef {
///    ScalarFunction::new(MyUDF {}, vec![input]).into()
/// }
///
/// #[cfg(feature = "python")]
/// #[pyo3::pyfunction]
/// #[pyo3(name = "my_udf")]
/// pub fn py_my_udf(expr: PyExpr) -> PyResult<PyExpr> {
///   my_udf(expr.into()).into()
/// }
/// ```
/// Alternatively, if you need to provide custom implementations for `my_udf` and `py_my_udf` functions,
/// you can use the `impl_scalar_udf` macro instead
#[macro_export]
macro_rules! make_unary_udf_function {
    (
        name: $func_name:expr,
        to_field: ($inputs:ident, $schema:ident) $to_field:block,
        evaluate: ($eval_inputs:ident) $evaluate:block
    ) => {
        $crate::impl_scalar_udf!{
            name: $func_name,
            to_field: ($inputs, $schema) $to_field,
            evaluate: ($eval_inputs) $evaluate
        }
        paste::paste! {
            pub fn [<$func_name:lower>](input: $crate::ExprRef) -> $crate::ExprRef {
                $crate::functions::ScalarFunction::new([<$func_name:camel Function>] {}, vec![input]).into()
            }

            #[cfg(feature = "python")]
            #[pyo3::pyfunction]
            #[pyo3(name = $func_name)]
            pub fn [<py_ $func_name:lower>](expr: daft_dsl::python::PyExpr) -> pyo3::prelude::PyResult<daft_dsl::python::PyExpr> {
                Ok([<$func_name:lower>](expr.into()).into())
            }
        }

    };

}

/// generates a scalar UDF implementation
/// Similar to `impl_scalar_udf0`, but with one additional argument
/// # Example
/// ```rust,ignore
///
/// make_binary_udf_function!{
///     name: "my_udf",
///     to_field: (inputs, schema) {
///       // implementation
///     },
///     evaluate: (inputs) {
///       // implementation
///     }
/// }
/// ```
/// generates the following code
/// ```rust,ignore
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
/// pub struct MyUDF {}
///
/// #[typetag::serde]
/// impl ScalarUDF for MyUDF {
///    fn as_any(&self) -> &dyn std::any::Any { self }
///    fn name(&self) -> &'static str { "my_udf" }
///
///    fn to_field(&self, inputs: &[ExprRef], schema: &daft_core::schema::Schema) -> common_error::DaftResult<daft_core::datatypes::Field> {
///      // implementation
///    }
///
///    fn evaluate(&self, inputs: &[daft_core::series::Series]) -> common_error::DaftResult<daft_core::series::Series> {
///      // implementation
///    }
/// }
/// fn my_udf(input: ExprRef, arg: ExprRef) -> ExprRef {
///    ScalarFunction::new(MyUDF {}, vec![input, arg]).into()
/// }
///
/// #[cfg(feature = "python")]
/// #[pyo3::pyfunction]
/// #[pyo3(name = "my_udf")]
/// pub fn py_my_udf(expr: PyExpr, arg: PyExpr) -> PyResult<PyExpr> {
///   my_udf(expr.into(), arg.into()).into()
/// }
/// ```
#[macro_export]
macro_rules! make_binary_udf_function {
    (
        name: $func_name:expr,
        to_field: ($inputs:ident, $schema:ident) $to_field:block,
        evaluate: ($eval_inputs:ident) $evaluate:block
    ) => {
        $crate::impl_scalar_udf!{
            name: $func_name,
            to_field: ($inputs, $schema) $to_field,
            evaluate: ($eval_inputs) $evaluate
        }
        paste::paste! {
            pub fn [<$func_name:lower>](input: $crate::ExprRef, arg: $crate::ExprRef) -> $crate::ExprRef {
                $crate::functions::ScalarFunction::new([<$func_name:camel Function>] {}, vec![input, arg]).into()
            }

            #[cfg(feature = "python")]
            #[pyo3::pyfunction]
            #[pyo3(name = $func_name)]
            pub fn [<py_ $func_name:lower>](expr: daft_dsl::python::PyExpr, arg: daft_dsl::python::PyExpr) -> pyo3::prelude::PyResult<daft_dsl::python::PyExpr> {
                Ok([<$func_name:lower>](expr.into(), arg.into()).into())
            }
        }

    };
}
