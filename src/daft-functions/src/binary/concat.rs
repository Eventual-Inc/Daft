use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::as_arrow::AsArrow,
    datatypes::{BinaryArray, DataType, Field, FixedSizeBinaryArray},
    prelude::Schema,
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinaryConcat {}

#[typetag::serde]
impl ScalarUDF for BinaryConcat {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inner = inputs.into_inner();
        self.evaluate_from_series(&inner)
    }

    fn name(&self) -> &'static str {
        "concat"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [left, right] => {
                let left_field = left.to_field(schema)?;
                let right_field = right.to_field(schema)?;
                match (&left_field.dtype, &right_field.dtype) {
                    (DataType::Binary, DataType::Binary) => {
                        Ok(Field::new(left_field.name, DataType::Binary))
                    }
                    (DataType::Binary, DataType::Null) | (DataType::Null, DataType::Binary) => {
                        Ok(Field::new(left_field.name, DataType::Binary))
                    }
                    (DataType::FixedSizeBinary(size1), DataType::FixedSizeBinary(size2)) => Ok(
                        Field::new(left_field.name, DataType::FixedSizeBinary(size1 + size2)),
                    ),
                    (DataType::FixedSizeBinary(_), DataType::Binary)
                    | (DataType::Binary, DataType::FixedSizeBinary(_)) => {
                        Ok(Field::new(left_field.name, DataType::Binary))
                    }
                    (DataType::FixedSizeBinary(_), DataType::Null)
                    | (DataType::Null, DataType::FixedSizeBinary(_)) => {
                        Ok(Field::new(left_field.name, DataType::Binary))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expects inputs to concat to be binary, but received {} and {}",
                        format_field_type_for_error(&left_field),
                        format_field_type_for_error(&right_field),
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        let result_name = inputs[0].name();
        match (inputs[0].data_type(), inputs[1].data_type()) {
            (DataType::Binary, DataType::Binary) => {
                let left_array = inputs[0].downcast::<BinaryArray>()?;
                let right_array = inputs[1].downcast::<BinaryArray>()?;
                let result = left_array.binary_concat(right_array)?;
                Ok(
                    BinaryArray::from((result_name, Box::new(result.as_arrow().clone())))
                        .into_series(),
                )
            }
            (DataType::FixedSizeBinary(_), DataType::FixedSizeBinary(_)) => {
                let left_array = inputs[0].downcast::<FixedSizeBinaryArray>()?;
                let right_array = inputs[1].downcast::<FixedSizeBinaryArray>()?;
                let result = left_array.binary_concat(right_array)?;
                Ok(
                    FixedSizeBinaryArray::from((result_name, Box::new(result.as_arrow().clone())))
                        .into_series(),
                )
            }
            (DataType::FixedSizeBinary(_), DataType::Binary)
            | (DataType::Binary, DataType::FixedSizeBinary(_)) => {
                let left_array = match inputs[0].data_type() {
                    DataType::FixedSizeBinary(_) => inputs[0]
                        .downcast::<FixedSizeBinaryArray>()?
                        .cast(&DataType::Binary)?,
                    _ => inputs[0].downcast::<BinaryArray>()?.clone().into_series(),
                };
                let right_array = match inputs[1].data_type() {
                    DataType::FixedSizeBinary(_) => inputs[1]
                        .downcast::<FixedSizeBinaryArray>()?
                        .cast(&DataType::Binary)?,
                    _ => inputs[1].downcast::<BinaryArray>()?.clone().into_series(),
                };
                let result = left_array.binary()?.binary_concat(right_array.binary()?)?;
                Ok(
                    BinaryArray::from((result_name, Box::new(result.as_arrow().clone())))
                        .into_series(),
                )
            }
            (_, DataType::Null) | (DataType::Null, _) => {
                let len = inputs[0].len().max(inputs[1].len());
                Ok(Series::full_null(result_name, &DataType::Binary, len))
            }
            _ => unreachable!("Type checking done in to_field"),
        }
    }
}

pub fn binary_concat(left: ExprRef, right: ExprRef) -> ExprRef {
    ScalarFunction::new(BinaryConcat {}, vec![left, right]).into()
}

fn format_field_type_for_error(field: &Field) -> String {
    match field.dtype {
        DataType::FixedSizeBinary(_) => format!("{}#Binary", field.name),
        _ => format!("{}#{}", field.name, field.dtype),
    }
}
