#![feature(async_closure)]

/// generates a scalar function implementation
/// # Example
/// ```rust,ignore
///
/// make_unary_udf_function!{
///     name: "my_udf",
///     to_field: (input0, schema) {
///       // implementation
///     },
///     evaluate: (input0) {
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
///      match inputs {
///        [input0] => { // implementation }
///        _ => Err(common_error::DaftError::InvalidInput("my_udf".to_string()))
///      }
///    }
///
///    fn evaluate(&self, inputs: &[daft_core::series::Series]) -> common_error::DaftResult<daft_core::series::Series> {
///      match inputs {
///        [input0] => { // implementation }
///        _ => Err(common_error::DaftError::InvalidInput("my_udf".to_string()))
///      }
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
/// These are not meant to cover all cases, but to provide a starting point for implementing most of the basic functions
macro_rules! make_udf_function {
    // unary
    (
        name: $func_name:expr,
        to_field: ($to_field_input0:ident, $schema:ident) $to_field:block,
        evaluate: ($input0:ident) $evaluate:block  $(,)?
    ) => {
        paste::paste! {
            #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
            pub struct [<$func_name:camel Function>] {}

            #[typetag::serde]
            impl daft_dsl::functions::ScalarUDF for [<$func_name:camel Function>] {
                fn as_any(&self) -> &dyn std::any::Any { self }
                fn name(&self) -> &'static str { $func_name }
                fn to_field(&self, inputs: &[daft_dsl::ExprRef], $schema: &daft_core::schema::Schema) -> common_error::DaftResult<daft_core::datatypes::Field> {
                    match inputs {
                        [$to_field_input0] => $to_field,
                        _ => Err(common_error::DaftError::ValueError(format!("function '{}' expects no arguments, instead received {}", $func_name , inputs.len())))
                    }
                }
                fn evaluate(&self, inputs: &[daft_core::series::Series]) -> common_error::DaftResult<daft_core::series::Series> {
                    match inputs {
                        [$input0] => $evaluate,
                        _ => Err(common_error::DaftError::ValueError(format!("function '{}' expects no arguments, instead received {}", $func_name , inputs.len())))
                    }
                }
            }
            pub fn [<$func_name:lower>](input: daft_dsl::ExprRef) -> daft_dsl::ExprRef {
                daft_dsl::functions::ScalarFunction::new([<$func_name:camel Function>] {}, vec![input]).into()
            }

            #[cfg(feature = "python")]
            #[pyo3::pyfunction]
            #[pyo3(name = $func_name)]
            pub fn [<py_ $func_name:lower>](expr: daft_dsl::python::PyExpr) -> pyo3::prelude::PyResult<daft_dsl::python::PyExpr> {
                Ok([<$func_name:lower>](expr.into()).into())
            }
        }
    };
    // binary
    (
        name: $func_name:expr,
        to_field: ($to_field_input0:ident, $to_field_input1:ident, $schema:ident) $to_field:block,
        evaluate: ($input0:ident, $input1:ident) $evaluate:block  $(,)?
    ) => {
        paste::paste! {
            #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
            pub struct [<$func_name:camel Function>] {}

            #[typetag::serde]
            impl daft_dsl::functions::ScalarUDF for [<$func_name:camel Function>] {
                fn as_any(&self) -> &dyn std::any::Any { self }
                fn name(&self) -> &'static str { $func_name }
                fn to_field(&self, inputs: &[daft_dsl::ExprRef], $schema: &daft_core::schema::Schema) -> common_error::DaftResult<daft_core::datatypes::Field> {
                    match inputs {
                        [$to_field_input0, $to_field_input1] => $to_field,
                        _ => Err(common_error::DaftError::ValueError(format!("function '{}' expects 1 argument, instead received {}", $func_name , inputs.len())))
                    }
                }
                fn evaluate(&self, inputs: &[daft_core::series::Series]) -> common_error::DaftResult<daft_core::series::Series> {
                    match inputs {
                        [$input0, $input1] => $evaluate,
                        _ => Err(common_error::DaftError::ValueError(format!("function '{}' expects 1 argument, instead received {}", $func_name , inputs.len())))
                    }
                }
            }


            pub fn [<$func_name:lower>](input: daft_dsl::ExprRef, input2: daft_dsl::ExprRef) -> daft_dsl::ExprRef {
                daft_dsl::functions::ScalarFunction::new([<$func_name:camel Function>] {}, vec![input, input2]).into()
            }

            #[cfg(feature = "python")]
            #[pyo3::pyfunction]
            #[pyo3(name = $func_name)]
            pub fn [<py_ $func_name:lower>](expr: daft_dsl::python::PyExpr, expr2: daft_dsl::python::PyExpr) -> pyo3::prelude::PyResult<daft_dsl::python::PyExpr> {
                Ok([<$func_name:lower>](expr.into(), expr2.into()).into())
            }
        }
    };
    // ternary
    (
        name: $func_name:expr,
        to_field: ($to_field_input0:ident, $to_field_input1:ident, $to_field_input2:ident,  $schema:ident) $to_field:block,
        evaluate: ($input0:ident, $input1:ident, $input2:ident) $evaluate:block  $(,)?
    ) => {
        paste::paste! {
            #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
            pub struct [<$func_name:camel Function>] {}

            #[typetag::serde]
            impl daft_dsl::functions::ScalarUDF for [<$func_name:camel Function>] {
                fn as_any(&self) -> &dyn std::any::Any { self }
                fn name(&self) -> &'static str { $func_name }
                fn to_field(&self, inputs: &[daft_dsl::ExprRef], $schema: &daft_core::schema::Schema) -> common_error::DaftResult<daft_core::datatypes::Field> {
                    match inputs {
                        [$to_field_input0, $to_field_input1, $to_field_input2] => $to_field,
                        _ => Err(common_error::DaftError::ValueError(format!("function '{}' expects 2 argument, instead received {}", $func_name , inputs.len())))
                    }
                }
                fn evaluate(&self, inputs: &[daft_core::series::Series]) -> common_error::DaftResult<daft_core::series::Series> {
                    match inputs {
                        [$input0, $input1, $input2] => $evaluate,
                        _ => Err(common_error::DaftError::ValueError(format!("function '{}' expects 2 argument, instead received {}", $func_name , inputs.len())))
                    }
                }
            }

            pub fn [<$func_name:lower>](input: daft_dsl::ExprRef, arg0: daft_dsl::ExprRef, arg1: daft_dsl::ExprRef) -> daft_dsl::ExprRef {
                daft_dsl::functions::ScalarFunction::new([<$func_name:camel Function>] {}, vec![input,arg0, arg1]).into()
            }

            #[cfg(feature = "python")]
            #[pyo3::pyfunction]
            #[pyo3(name = $func_name)]
            pub fn [<py_ $func_name:lower>](expr: daft_dsl::python::PyExpr, arg0: daft_dsl::python::PyExpr, arg1: daft_dsl::python::PyExpr) -> pyo3::prelude::PyResult<daft_dsl::python::PyExpr> {
                Ok([<$func_name:lower>](expr.into(), arg0.into(), arg1.into()).into())
            }
        }
    };
    (
        name: $func_name:expr,
        params: ($($param:ident: $param_type:ident) *$(,)?),
        to_field: ($self0:ident, $to_field_input0:ident, $schema:ident) $to_field:block,
        evaluate: ($self1:ident, $input0:ident) $evaluate:block  $(,)?
    ) => {
        paste::paste! {
            #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
            pub struct [<$func_name:camel Function>] {
                $(
                    pub $param: $param_type,
                )*
            }

            #[typetag::serde]
            impl daft_dsl::functions::ScalarUDF for [<$func_name:camel Function>] {
                fn as_any(&self) -> &dyn std::any::Any { self }
                fn name(&self) -> &'static str { $func_name }
                fn to_field(&$self0, inputs: &[daft_dsl::ExprRef], $schema: &daft_core::schema::Schema) -> common_error::DaftResult<daft_core::datatypes::Field> {
                    match inputs {
                        [$to_field_input0] => $to_field,
                        _ => Err(common_error::DaftError::ValueError(format!("function '{}' expects no arguments, instead received {}", $func_name , inputs.len())))
                    }
                }
                fn evaluate(&$self1, inputs: &[daft_core::series::Series]) -> common_error::DaftResult<daft_core::series::Series> {

                    match inputs {
                        [$input0] => $evaluate,
                        _ => Err(common_error::DaftError::ValueError(format!("function '{}' expects no arguments, instead received {}", $func_name , inputs.len())))
                    }
                }
            }

            pub fn [<$func_name:lower>](input: daft_dsl::ExprRef, $($param: $param_type),*) -> daft_dsl::ExprRef {
                daft_dsl::functions::ScalarFunction::new([<$func_name:camel Function>] {
                    $(
                        $param,
                    )*
                }, vec![input]).into()

            }

            #[cfg(feature = "python")]
            #[pyo3::pyfunction]
            #[pyo3(name = $func_name)]
            pub fn [<py_ $func_name:lower>](expr: daft_dsl::python::PyExpr, $($param: $param_type),*) -> pyo3::prelude::PyResult<daft_dsl::python::PyExpr> {
                Ok([<$func_name:lower>](expr.into(), $($param),*).into())
            }
        }
    };
    (
        name: $func_name:expr,
        params: ($($param:ident: $param_type:ident)*$(,)?),
        to_field: ($to_field_input0:ident, $schema:ident) $to_field:block,
        evaluate: ($self:ident, $input0:ident) $evaluate:block  $(,)?
    ) => {
        paste::paste! {
            #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
            pub struct [<$func_name:camel Function>] {
                $(
                    pub $param: $param_type,
                )*
            }

            #[typetag::serde]
            impl daft_dsl::functions::ScalarUDF for [<$func_name:camel Function>] {
                fn as_any(&self) -> &dyn std::any::Any { self }
                fn name(&self) -> &'static str { $func_name }
                fn to_field(&self, inputs: &[daft_dsl::ExprRef], $schema: &daft_core::schema::Schema) -> common_error::DaftResult<daft_core::datatypes::Field> {
                    match inputs {
                        [$to_field_input0] => $to_field,
                        _ => Err(common_error::DaftError::ValueError(format!("function '{}' expects no arguments, instead received {}", $func_name , inputs.len())))
                    }
                }
                fn evaluate(&$self, inputs: &[daft_core::series::Series]) -> common_error::DaftResult<daft_core::series::Series> {

                    match inputs {
                        [$input0] => $evaluate,
                        _ => Err(common_error::DaftError::ValueError(format!("function '{}' expects no arguments, instead received {}", $func_name , inputs.len())))
                    }
                }
            }

            pub fn [<$func_name:lower>](input: daft_dsl::ExprRef, $($param: $param_type),*) -> daft_dsl::ExprRef {
                daft_dsl::functions::ScalarFunction::new([<$func_name:camel Function>] {
                    $(
                        $param,
                    )*
                }, vec![input]).into()

            }

            #[cfg(feature = "python")]
            #[pyo3::pyfunction]
            #[pyo3(name = $func_name)]
            pub fn [<py_ $func_name:lower>](expr: daft_dsl::python::PyExpr, $($param: $param_type),*) -> pyo3::prelude::PyResult<daft_dsl::python::PyExpr> {
                Ok([<$func_name:lower>](expr.into(), $($param),*).into())
            }
        }
    };
}

pub mod count_matches;
pub mod distance;
pub mod float;

pub mod json;

pub mod hash;
pub mod image;
pub mod list;
pub mod list_sort;
pub mod minhash;
pub mod numeric;
pub mod temporal;
pub mod to_struct;
pub mod tokenize;
pub mod uri;

use common_error::DaftError;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use snafu::Snafu;

#[cfg(feature = "python")]
pub fn register_modules(py: Python, parent: &PyModule) -> PyResult<()> {
    // keep in sorted order
    parent.add_wrapped(wrap_pyfunction!(count_matches::python::utf8_count_matches))?;
    parent.add_wrapped(wrap_pyfunction!(distance::cosine::python::cosine_distance))?;
    parent.add_wrapped(wrap_pyfunction!(hash::python::hash))?;
    parent.add_wrapped(wrap_pyfunction!(list_sort::python::list_sort))?;
    parent.add_wrapped(wrap_pyfunction!(minhash::python::minhash))?;
    parent.add_wrapped(wrap_pyfunction!(numeric::cbrt::python::cbrt))?;
    parent.add_wrapped(wrap_pyfunction!(to_struct::python::to_struct))?;
    parent.add_wrapped(wrap_pyfunction!(tokenize::python::tokenize_decode))?;
    parent.add_wrapped(wrap_pyfunction!(tokenize::python::tokenize_encode))?;
    parent.add_wrapped(wrap_pyfunction!(uri::python::url_download))?;
    parent.add_wrapped(wrap_pyfunction!(uri::python::url_upload))?;
    parent.add_wrapped(wrap_pyfunction!(json::py_json_query))?;
    float::register_modules(py, parent)?;
    temporal::register_modules(py, parent)?;
    image::register_modules(py, parent)?;
    list::register_modules(py, parent)?;

    Ok(())
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid Argument: {:?}", msg))]
    InvalidArgument { msg: String },
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, err)
    }
}

impl From<Error> for DaftError {
    fn from(err: Error) -> DaftError {
        DaftError::External(err.into())
    }
}
