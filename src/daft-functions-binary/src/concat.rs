use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{BinaryArray, DataType, Field, FixedSizeBinaryArray},
    prelude::Schema,
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::kernels::BinaryArrayExtension;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinaryConcat;

#[derive(common_macros::FunctionArgs)]
pub struct BinaryConcatArgs<T> {
    input: T,
    other: T,
}

#[typetag::serde]
impl ScalarUDF for BinaryConcat {
    fn name(&self) -> &'static str {
        "binary_concat"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let BinaryConcatArgs { input, other } = inputs.try_into()?;
        let name = input.name();
        use DataType::{Binary, FixedSizeBinary, Null};

        match (input.data_type(), other.data_type()) {
            (Binary, Binary) => {
                let left_array = input.downcast::<BinaryArray>()?;
                let right_array = other.downcast::<BinaryArray>()?;
                let result = left_array.binary_concat(right_array)?;
                Ok(result.into_series())
            }
            (FixedSizeBinary(_), FixedSizeBinary(_)) => {
                let left_array = input.downcast::<FixedSizeBinaryArray>()?;
                let right_array = other.downcast::<FixedSizeBinaryArray>()?;
                let result = left_array.binary_concat(right_array)?;
                Ok(result.into_series())
            }
            (FixedSizeBinary(_), Binary) | (Binary, FixedSizeBinary(_)) => {
                let left_array = match input.data_type() {
                    FixedSizeBinary(_) => {
                        input.downcast::<FixedSizeBinaryArray>()?.cast(&Binary)?
                    }
                    _ => input.downcast::<BinaryArray>()?.clone().into_series(),
                };
                let right_array = match other.data_type() {
                    FixedSizeBinary(_) => {
                        other.downcast::<FixedSizeBinaryArray>()?.cast(&Binary)?
                    }
                    _ => other.downcast::<BinaryArray>()?.clone().into_series(),
                };
                let result = left_array.binary()?.binary_concat(right_array.binary()?)?;
                Ok(result.into_series())
            }
            (_, Null) | (Null, _) => {
                let len = input.len().max(other.len());
                Ok(Series::full_null(name, &DataType::Binary, len))
            }
            _ => unreachable!("Type checking done in to_field"),
        }
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let BinaryConcatArgs {
            input: left,
            other: right,
        } = inputs.try_into()?;
        let left = left.to_field(schema)?;
        let right = right.to_field(schema)?;
        use DataType::{Binary, FixedSizeBinary, Null};
        let name = &left.name;

        Ok(match (&left.dtype, &right.dtype) {
            (Binary, Binary)
            | (Binary, Null)
            | (Null, Binary)
            | (FixedSizeBinary(_), Binary)
            | (Binary, FixedSizeBinary(_))
            | (FixedSizeBinary(_), Null)
            | (Null, FixedSizeBinary(_)) => Field::new(name, Binary),
            (FixedSizeBinary(size1), FixedSizeBinary(size2)) => {
                Field::new(name, FixedSizeBinary(size1 + size2))
            }
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Expects inputs to concat to be binary, but received {} and {}",
                    format_field_type_for_error(&left),
                    format_field_type_for_error(&right),
                )))
            }
        })
    }
}

fn format_field_type_for_error(field: &Field) -> String {
    match field.dtype {
        DataType::FixedSizeBinary(_) => format!("{}#Binary", field.name),
        _ => format!("{}#{}", field.name, field.dtype),
    }
}
