use arrow2::{
    array::{MutableArray, MutablePrimitiveArray, PrimitiveArray},
    offset::OffsetsBuffer,
};
use common_error::{DaftError, DaftResult};
use tiktoken_rs::CoreBPE;

use crate::{
    array::ListArray,
    datatypes::{Field, Utf8Array},
    DataType, Series,
};

use super::as_arrow::AsArrow;

fn get_bpe(tokens_path: &str) -> DaftResult<CoreBPE> {
    match tokens_path {
        "cl100k_base" => Ok(tiktoken_rs::cl100k_base().unwrap()),
        "o200k_base" => Ok(tiktoken_rs::o200k_base().unwrap()),
        "p50k_base" => Ok(tiktoken_rs::p50k_base().unwrap()),
        "p50k_edit" => Ok(tiktoken_rs::p50k_edit().unwrap()),
        "r50k_base" => Ok(tiktoken_rs::r50k_base().unwrap()),
        url if url.starts_with("http://") || url.starts_with("https://") => {
            unimplemented!("URL download for tokens unimplemented")
        }
        _ => Err(DaftError::ValueError(format!(
            "Failed to load tokens from token set: {}",
            tokens_path
        ))),
    }
}

fn decode_list(series: &Series, bpe: &CoreBPE) -> DaftResult<String> {
    if !series.data_type().is_numeric() {
        return Err(DaftError::TypeError(format!(
            "expected numeric list inner type, got {}",
            series.data_type()
        )));
    }
    let series = series.cast(&DataType::Int32)?;
    let tokens: Vec<usize> = series
        .i32()?
        .as_arrow()
        .values_iter()
        .map(|v| *v as usize)
        .collect();
    match bpe.decode(tokens) {
        Ok(str) => Ok(str),
        Err(err) => Err(DaftError::ValueError(format!(
            "Error while decoding tokens: {}",
            err
        ))),
    }
}

impl Utf8Array {
    pub fn tokenize_encode(&self, tokens_path: &str) -> DaftResult<ListArray> {
        let bpe = get_bpe(tokens_path)?;

        let mut flat_child = MutablePrimitiveArray::<i32>::new();
        let mut offsets: Vec<i64> = Vec::with_capacity(self.len() + 1);
        offsets.push(0);
        let self_arrow = self.as_arrow();
        for s_opt in self_arrow.iter() {
            if let Some(s) = s_opt {
                let tokens = bpe.encode_ordinary(s);
                let tokens_iter = tokens.iter().map(|t| Some(*t as i32));
                flat_child.extend(tokens_iter);
            }
            offsets.push(flat_child.len() as i64);
        }
        let flat_child: PrimitiveArray<i32> = flat_child.into();
        let child_series = Series::from_arrow(
            Field::new("flat_child", DataType::Int32).into(),
            Box::new(flat_child),
        )?;
        let offsets = OffsetsBuffer::try_from(offsets)?;
        Ok(ListArray::new(
            Field::new(self.name(), DataType::List(Box::new(DataType::Int32))),
            child_series,
            offsets,
            self.validity().cloned(),
        ))
    }
}

impl ListArray {
    pub fn tokenize_decode(&self, tokens_path: &str) -> DaftResult<Utf8Array> {
        let bpe = get_bpe(tokens_path)?;
        let offsets = self.offsets();
        let strs = (0..offsets.len() - 1)
            .map(|i| {
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                let sub_series = self.flat_child.slice(start, end)?;
                decode_list(&sub_series, &bpe)
            })
            .collect::<DaftResult<Vec<String>>>()?;
        Ok(Utf8Array::from_iter(self.name(), strs.iter().map(Some)))
    }
}
