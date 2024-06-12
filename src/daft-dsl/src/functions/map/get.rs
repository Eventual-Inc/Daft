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
                    DataType::Map(_, value_dtype) => {
                        // TODO: Turn this on for v0.3
                        // let field_name = match key.as_ref() {
                        //     Expr::Literal(value)
                        //         if matches!(
                        //             value,
                        //             LiteralValue::Null
                        //                 | LiteralValue::Boolean(..)
                        //                 | LiteralValue::Int32(..)
                        //                 | LiteralValue::UInt32(..)
                        //                 | LiteralValue::Int64(..)
                        //                 | LiteralValue::UInt64(..)
                        //         ) =>
                        //     {
                        //         format!("{}", value)
                        //     }
                        //     _ => "value".to_string(),
                        // };

                        let field_name = "value";

                        Ok(Field::new(field_name, *value_dtype))
                    }
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
