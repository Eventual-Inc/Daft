use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use daft_core::{
    array::{ops::as_arrow::AsArrow, ListArray},
    datatypes::{Field, Utf8Array},
    schema::Schema,
    DataType, IntoSeries, Series,
};
use daft_dsl::{functions::ScalarUDF, ExprRef};
use daft_io::IOConfig;
use serde::Serialize;

use crate::tokenize::bpe::DaftBPE;

fn decode_list(series: &Series, bpe: &DaftBPE) -> DaftResult<String> {
    if !series.data_type().is_integer() {
        return Err(DaftError::TypeError(format!(
            "expected integer list inner type, got {}",
            series.data_type()
        )));
    }
    let series = series.cast(&DataType::UInt32)?;
    let tokens: Vec<u32> = series.u32()?.as_arrow().values_iter().copied().collect();
    bpe.decode(&tokens)
}

fn tokenize_decode_array(
    arr: &ListArray,
    tokens_path: &str,
    io_config: Arc<IOConfig>,
) -> DaftResult<Utf8Array> {
    let bpe = DaftBPE::new(tokens_path, io_config)?;
    let offsets = arr.offsets();
    let strs = (0..offsets.len() - 1)
        .map(|i| {
            let start = offsets[i] as usize;
            let end = offsets[i + 1] as usize;
            let sub_series = arr.flat_child.slice(start, end)?;
            decode_list(&sub_series, &bpe)
        })
        .collect::<DaftResult<Vec<String>>>()?;
    Utf8Array::from_iter(arr.name(), strs.iter().map(Some)).with_validity(arr.validity().cloned())
}

fn tokenize_decode_series(
    series: &Series,
    tokens_path: &str,
    io_config: Arc<IOConfig>,
) -> DaftResult<Series> {
    match series.data_type() {
        DataType::List(_) => {
            Ok(tokenize_decode_array(series.list()?, tokens_path, io_config)?.into_series())
        }
        dt => Err(DaftError::TypeError(format!(
            "Tokenize decode not implemented for type {}",
            dt
        ))),
    }
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub(super) struct TokenizeDecodeFunction {
    pub(super) tokens_path: String,
    pub(super) io_config: Arc<IOConfig>,
}

#[typetag::serde]
impl ScalarUDF for TokenizeDecodeFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "tokenize_decode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::List(inner) if inner.is_integer() => {
                        Ok(Field::new(data_field.name, DataType::Utf8))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to tokenize_decode to be list[integer], but received {data_field}",
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
            [data] => tokenize_decode_series(data, &self.tokens_path, self.io_config.clone()),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
