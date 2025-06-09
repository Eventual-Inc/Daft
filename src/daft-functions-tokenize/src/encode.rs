use std::sync::Arc;

use arrow2::{
    array::{MutableArray, MutablePrimitiveArray, PrimitiveArray},
    offset::OffsetsBuffer,
};
use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::functions::prelude::*;
use daft_io::IOConfig;
use serde::{Deserialize, Serialize};

use crate::bpe::DaftBPE;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TokenizeEncodeFunction;

#[derive(FunctionArgs)]
struct EncodeArgs<T> {
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
    #[arg(optional)]
    pub use_special_tokens: Option<bool>,
}

#[typetag::serde]
impl ScalarUDF for TokenizeEncodeFunction {
    fn name(&self) -> &'static str {
        "tokenize_encode"
    }
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let EncodeArgs {
            input,
            _varargs: _,
            tokens_path,
            io_config,
            pattern,
            special_tokens,
            use_special_tokens,
        } = inputs.try_into()?;

        // if special tokens are passed in, enable using special tokens
        let use_special_tokens = use_special_tokens.unwrap_or_else(|| special_tokens.is_some());
        tokenize_encode_series(
            &input,
            &tokens_path,
            io_config.map(Arc::new),
            pattern.as_deref(),
            special_tokens.as_deref(),
            use_special_tokens,
        )
    }
    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        ensure!(
            input.dtype.is_string(),
            TypeError: "Expects input to tokenize_encode to be utf8, but received {input}",
        );
        Ok(Field::new(
            input.name,
            DataType::List(Box::new(DataType::UInt32)),
        ))
    }
}

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
    for s_opt in self_arrow {
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
