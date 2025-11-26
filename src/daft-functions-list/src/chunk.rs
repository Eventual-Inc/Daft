use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
    lit,
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListChunk;

#[typetag::serde]
impl ScalarUDF for ListChunk {
    fn name(&self) -> &'static str {
        "list_chunk"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 2, SchemaMismatch: "Expected 2 input args, got {}", inputs.len());

        let input = inputs.required((0, "input"))?;
        let size = inputs.required((1, "size"))?;
        ensure!(size.len() == 1 && size.data_type().is_numeric(), ValueError: "expected numeric literal for 'size'");

        let size = size.cast(&DataType::UInt64)?.u64()?.get(0).unwrap();

        input.list_chunk(size as _)
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 2, SchemaMismatch: "Expected 2 input args, got {}", inputs.len());
        let input_field = inputs.required((0, "input"))?.to_field(schema)?;

        let size = inputs.required((1, "size"))?;
        let size = size
            .as_literal()
            .and_then(|size| size.try_as_usize().transpose())
            .ok_or_else(|| DaftError::TypeError("Expected numeric literal for 'size'".to_string()))?
            .map_err(|_| {
                DaftError::ValueError("Expected positive integer for 'size'".to_string())
            })?;

        ensure!(size > 0, ValueError: "Expected non zero integer for 'size'");

        Ok(input_field
            .to_exploded_field()?
            .to_fixed_size_list_field(size)?
            .to_list_field())
    }
}

#[must_use]
pub fn list_chunk(expr: ExprRef, size: usize) -> ExprRef {
    ScalarFn::builtin(ListChunk, vec![expr, lit(size as u64)]).into()
}
