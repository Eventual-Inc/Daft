#![allow(
    deprecated,
    reason = "moving over all scalarUDFs to new pattern. Remove once completed!"
)]
pub mod coalesce;
pub mod distance;
pub mod float;
pub mod hash;
pub mod minhash;
pub mod monotonically_increasing_id;
pub mod numeric;
#[cfg(feature = "python")]
pub mod python;
pub mod to_struct;

use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::logical::FileArray,
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, FunctionModule, ScalarUDF},
    ExprRef,
};
use hash::HashFunction;
use minhash::MinHashFunction;
#[cfg(feature = "python")]
pub use python::register as register_modules;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use to_struct::ToStructFunction;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid Argument: {:?}", msg))]
    InvalidArgument { msg: String },
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        Self::other(err)
    }
}

impl From<Error> for DaftError {
    fn from(err: Error) -> Self {
        Self::External(err.into())
    }
}

/// TODO chore: cleanup function implementations using error macros
#[macro_export]
macro_rules! invalid_argument_err {
    ($($arg:tt)*)  => {{
        let msg = format!($($arg)*);
        return Err(common_error::DaftError::TypeError(msg).into());
    }};
}

pub struct HashFunctions;

impl FunctionModule for HashFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(HashFunction);
        parent.add_fn(MinHashFunction);
    }
}

pub struct ConversionFunctions;

impl FunctionModule for ConversionFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(ToStructFunction);
        parent.add_fn(File);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct File;
#[derive(FunctionArgs)]
struct FileArgs<T> {
    input: T,
    runner_name: String,
}

#[typetag::serde]
impl ScalarUDF for File {
    fn name(&self) -> &'static str {
        "file"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let FileArgs { input, .. } = args.try_into()?;
        Ok(match input.data_type() {
            DataType::Binary => {
                FileArray::new_from_data_array(input.name(), input.binary()?).into_series()
            }
            DataType::Utf8 => {
                FileArray::new_from_reference_array(input.name(), input.utf8()?).into_series()
            }
            _ => {
                return Err(DaftError::ValueError(format!(
                "Unsupported data type for 'file' function: {}. Expected either String | Binary",
                input.data_type()
            )))
            }
        })
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let FileArgs { input, runner_name } = args.try_into()?;
        let input = input.to_field(schema)?;
        if input.dtype == DataType::Utf8 && runner_name.as_str() == "ray" {
            return Err(DaftError::ValueError(
                "Cannot reference local files within this context".to_string(),
            ));
        }

        Ok(Field::new(input.name, DataType::File))
    }
}
