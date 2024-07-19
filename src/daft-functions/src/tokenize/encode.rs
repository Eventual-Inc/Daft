use std::sync::Arc;

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
use daft_io::IOConfig;
use serde::{Deserialize, Serialize};

use crate::tokenize::bpe::DaftBPE;

fn tokenize_encode_array(
    arr: &Utf8Array,
    tokens_path: &str,
    io_config: Option<Arc<IOConfig>>,
    pattern: Option<&str>,
    special_tokens: Option<&str>,
    use_special_tokens: bool,
) -> DaftResult<ListArray> {
    let bpe = DaftBPE::new(tokens_path, io_config, pattern, special_tokens)?;

    let mut flat_child = MutablePrimitiveArray::<u32>::new();
    let mut offsets: Vec<i64> = Vec::with_capacity(arr.len() + 1);
    offsets.push(0);
    let self_arrow = arr.as_arrow();
    for s_opt in self_arrow.iter() {
        if let Some(s) = s_opt {
            let tokens = bpe.encode(s, use_special_tokens);
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

fn tokenize_encode_series(
    series: &Series,
    tokens_path: &str,
    io_config: Option<Arc<IOConfig>>,
    pattern: Option<&str>,
    special_tokens: Option<&str>,
    use_special_tokens: bool,
) -> DaftResult<Series> {
    series.with_utf8_array(|arr| {
        Ok(tokenize_encode_array(
            arr,
            tokens_path,
            io_config.clone(),
            pattern,
            special_tokens,
            use_special_tokens,
        )?
        .into_series())
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct TokenizeEncodeFunction {
    pub(super) tokens_path: String,
    pub(super) io_config: Option<Arc<IOConfig>>,
    pub(super) pattern: Option<String>,
    pub(super) special_tokens: Option<String>,
    pub(super) use_special_tokens: bool,
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
                Ok(field) => match &field.dtype {
                    DataType::Utf8 => Ok(Field::new(
                        field.name,
                        DataType::List(Box::new(DataType::UInt32)),
                    )),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to tokenize_encode to be utf8, but received {field}",
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
            [data] => tokenize_encode_series(
                data,
                &self.tokens_path,
                self.io_config.clone(),
                self.pattern.as_deref(),
                self.special_tokens.as_deref(),
                self.use_special_tokens,
            ),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
