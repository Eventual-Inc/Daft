use std::str::FromStr;

use arrow2::{
    array::{MutableArray, MutablePrimitiveArray, PrimitiveArray},
    offset::OffsetsBuffer,
};
use common_error::{DaftError, DaftResult};

use daft_core::{
    array::{ops::as_arrow::AsArrow, ListArray},
    datatypes::{Field, Utf8Array},
    schema::Schema,
    DataType, IntoSeries, Series,
};
use daft_dsl::{functions::ScalarUDF, ExprRef};
use serde::Serialize;

use crate::tokenize::bpe::DaftBPE;

pub fn tokenize_encode_array(arr: &Utf8Array, tokens_path: &str) -> DaftResult<ListArray> {
    let bpe = DaftBPE::from_str(tokens_path)?;

    let mut flat_child = MutablePrimitiveArray::<u32>::new();
    let mut offsets: Vec<i64> = Vec::with_capacity(arr.len() + 1);
    offsets.push(0);
    let self_arrow = arr.as_arrow();
    for s_opt in self_arrow.iter() {
        if let Some(s) = s_opt {
            let tokens = bpe.encode(s);
            let tokens_iter = tokens.iter().map(|t| Some(*t));
            flat_child.extend(tokens_iter);
        }
        offsets.push(flat_child.len() as i64);
    }
    let flat_child: PrimitiveArray<u32> = flat_child.into();
    let child_series = Series::from_arrow(
        Field::new("flat_child", DataType::UInt32).into(),
        Box::new(flat_child),
    )?;
    let offsets = OffsetsBuffer::try_from(offsets)?;
    Ok(ListArray::new(
        Field::new(arr.name(), DataType::List(Box::new(DataType::UInt32))),
        child_series,
        offsets,
        arr.validity().cloned(),
    ))
}

pub fn tokenize_encode_series(series: &Series, tokens_path: &str) -> DaftResult<Series> {
    series.with_utf8_array(|arr| Ok(tokenize_encode_array(arr, tokens_path)?.into_series()))
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub(super) struct TokenizeEncodeFunction {
    pub(super) tokens_path: String,
}

#[typetag::serde]
impl ScalarUDF for TokenizeEncodeFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "tokenize_encode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Utf8 => Ok(Field::new(
                        data_field.name,
                        DataType::List(Box::new(DataType::UInt32)),
                    )),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to tokenize_encode to be utf8, but received {data_field}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data] => tokenize_encode_series(data, &self.tokens_path),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
