#![allow(deprecated, reason = "arrow2 migration")]
use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::functions::prelude::*;
use daft_io::IOConfig;
use serde::{Deserialize, Serialize};

use crate::bpe::DaftBPE;

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TokenizeDecodeFunction;

#[derive(FunctionArgs)]
struct DecodeArgs<T> {
    pub input: T,
    pub tokens_path: String,

    // force all othes to be kwargs
    // similar to python `input, tokens_path, *, ...`
    #[arg(variadic)]
    pub _varargs: Vec<T>,
    #[arg(optional)]
    pub io_config: Option<IOConfig>,
    #[arg(optional)]
    pub pattern: Option<String>,
    #[arg(optional)]
    pub special_tokens: Option<String>,
}

#[typetag::serde]
impl ScalarUDF for TokenizeDecodeFunction {
    fn call(&self, args: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let DecodeArgs {
            input,
            _varargs,
            tokens_path,
            io_config,
            pattern,
            special_tokens,
        } = args.try_into()?;

        tokenize_decode_series(
            &input,
            &tokens_path,
            io_config.map(Arc::new),
            pattern.as_deref(),
            special_tokens.as_deref(),
        )
    }

    fn name(&self) -> &'static str {
        "tokenize_decode"
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let input = args.required((0, "input"))?.to_field(schema)?;
        ensure!(
            matches!(&input.dtype, DataType::List(inner) if inner.is_integer()),
            TypeError: "Expects input to tokenize_encode to be utf8, but received {input}",
        );

        Ok(Field::new(input.name, DataType::Utf8))
    }
}

// Helper function that operates on a single Utf8 series
fn decode_list(series: &Series, bpe: &DaftBPE) -> DaftResult<String> {
    if !series.data_type().is_integer() {
        return Err(DaftError::TypeError(format!(
            "expected integer list inner type, got {}",
            series.data_type()
        )));
    }
    let series = series.cast(&DataType::UInt32)?;
    let tokens: &[u32] = series.u32()?.as_slice();
    bpe.decode(tokens)
}

fn tokenize_decode_array(
    arr: &ListArray,
    tokens_path: &str,
    io_config: Option<Arc<IOConfig>>,
    pattern: Option<&str>,
    special_tokens: Option<&str>,
) -> DaftResult<Utf8Array> {
    let bpe = DaftBPE::new(tokens_path, io_config, pattern, special_tokens)?;
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
    io_config: Option<Arc<IOConfig>>,
    pattern: Option<&str>,
    special_tokens: Option<&str>,
) -> DaftResult<Series> {
    match series.data_type() {
        DataType::List(_) => Ok(tokenize_decode_array(
            series.list()?,
            tokens_path,
            io_config,
            pattern,
            special_tokens,
        )?
        .into_series()),
        dt => Err(DaftError::TypeError(format!(
            "Tokenize decode not implemented for type {dt}"
        ))),
    }
}
