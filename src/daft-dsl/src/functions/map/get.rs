use crate::ExprRef;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct GetEvaluator {}

impl FunctionEvaluator for GetEvaluator {
    fn fn_name(&self) -> &'static str {
        "map_get"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [input, key] => match (input.to_field(schema), key.to_field(schema)) {
                (Ok(input_field), Ok(_)) => match input_field.dtype {
                    DataType::Map(inner) => match inner.as_ref() {
                        DataType::Struct(fields) if fields.len() == 2 => {
                            let value_dtype = &fields[1].dtype;
                            Ok(Field::new("value", value_dtype.clone()))
                        }
                        _ => Err(DaftError::TypeError(format!(
                            "Expected input map to have struct values with 2 fields, got {}",
                            inner
                        ))),
                    },
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to be a map, got {}",
                        input_field.dtype
                    ))),
                },
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [input, key] => input.map_get(key),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
