mod batch;
mod row_wise;

pub use batch::{BatchPyFn, batch_udf};
use common_error::DaftResult;
use daft_core::prelude::*;
pub use row_wise::{RowWisePyFn, row_wise_udf};
use serde::{Deserialize, Serialize};

use crate::{ExprRef, operator_metrics::MetricsCollector};

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
    pub fn call(&self, args: &[Series], metrics: &mut dyn MetricsCollector) -> DaftResult<Series> {
        match self {
            Self::RowWise(func) => func.call(args, metrics),
            Self::Batch(func) => func.call(args, metrics),
        }
    }

    pub async fn call_async(
        &self,
        args: &[Series],
        metrics: &mut dyn MetricsCollector,
    ) -> DaftResult<Series> {
        match self {
            Self::RowWise(func) => func.call_async(args, metrics).await,
            Self::Batch(func) => func.call_async(args, metrics).await,
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
            Self::RowWise(RowWisePyFn {
                function_name,
                args,
                return_dtype,
                ..
            })
            | Self::Batch(BatchPyFn {
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
