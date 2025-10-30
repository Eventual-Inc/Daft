mod batch;
mod row_wise;

pub use batch::{BatchPyFn, batch_udf};
use common_error::DaftResult;
use daft_core::prelude::*;
pub use row_wise::{RowWisePyFn, row_wise_udf};
use serde::{Deserialize, Serialize};

use crate::ExprRef;

#[derive(derive_more::Display, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[display("{_0}")]
pub enum PyScalarFn {
    RowWise(RowWisePyFn),
    Batch(BatchPyFn),
}

impl PyScalarFn {
    pub fn name(&self) -> &str {
        match self {
            Self::RowWise(RowWisePyFn { function_name, .. })
            | Self::Batch(BatchPyFn { function_name, .. }) => function_name,
        }
    }
    pub fn call(&self, args: &[Series]) -> DaftResult<Series> {
        match self {
            Self::RowWise(func) => func.call(args),
            Self::Batch(func) => func.call(args),
        }
    }

    pub async fn call_async(&self, args: &[Series]) -> DaftResult<Series> {
        match self {
            Self::RowWise(func) => func.call_async(args).await,
            Self::Batch(func) => func.call_async(args).await,
        }
    }

    pub fn args(&self) -> Vec<ExprRef> {
        match self {
            Self::RowWise(RowWisePyFn { args, .. }) | Self::Batch(BatchPyFn { args, .. }) => {
                args.clone()
            }
        }
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        match self {
            #[cfg(feature = "python")]
            Self::RowWise(RowWisePyFn {
                function_name,
                args,
                return_dtype,
                input_dtypes: expected_inputs,
                ..
            }) => {
                let expected_inputs: Vec<&DataType> = expected_inputs.iter().collect();

                let actual_inputs = args
                    .iter()
                    .map(|arg| arg.to_field(schema).map(|f| f.dtype))
                    .collect::<DaftResult<Vec<DataType>>>()?;
                let actual_inputs: Vec<&DataType> = actual_inputs.iter().collect();

                common_error::ensure!(
                   expected_inputs.len() == actual_inputs.len(),
                   TypeError: "Expected {} inputs, but got {}",
                   expected_inputs.len(),
                   actual_inputs.len()
                );

                for (expected, actual) in expected_inputs.iter().zip(actual_inputs.iter()) {
                    // We use `Python` to represent on `Any` types, so we don't validate those
                    // as it means that the user did not provide a type signature, or it's `Any`
                    if !matches!(expected, DataType::Python) {
                        common_error::ensure!(
                            expected == actual,
                            TypeError: "Expects input to '{function_name}' to be {expected}, but received {actual}",
                        );
                    }
                }
                let field_name = if let Some(first_child) = args.first() {
                    first_child.get_name(schema)?
                } else {
                    function_name.to_string()
                };

                Ok(Field::new(field_name, return_dtype.clone()))
            }
            #[cfg(not(feature = "python"))]
            Self::RowWise(RowWisePyFn {
                function_name,
                args,
                return_dtype,
                ..
            }) => {
                let field_name = if let Some(first_child) = args.first() {
                    first_child.get_name(schema)?
                } else {
                    function_name.to_string()
                };

                Ok(Field::new(field_name, return_dtype.clone()))
            }
            Self::Batch(BatchPyFn {
                function_name,
                args,
                return_dtype,
                ..
            }) => {
                let field_name = if let Some(first_child) = args.first() {
                    first_child.get_name(schema)?
                } else {
                    function_name.to_string()
                };

                Ok(Field::new(field_name, return_dtype.clone()))
            }
        }
    }

    pub fn with_new_children(&self, children: Vec<ExprRef>) -> Self {
        match self {
            Self::RowWise(row_wise_py_fn) => {
                Self::RowWise(row_wise_py_fn.with_new_children(children))
            }
            Self::Batch(batch_py_fn) => Self::Batch(batch_py_fn.with_new_children(children)),
        }
    }

    pub fn dtype(&self) -> DataType {
        match self {
            Self::RowWise(RowWisePyFn { return_dtype, .. })
            | Self::Batch(BatchPyFn { return_dtype, .. }) => return_dtype.clone(),
        }
    }

    pub fn is_async(&self) -> bool {
        match self {
            Self::RowWise(RowWisePyFn { is_async, .. }) => *is_async,
            Self::Batch(BatchPyFn { is_async, .. }) => *is_async,
        }
    }
}
