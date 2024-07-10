use std::str::FromStr;

use arrow2::{
    array::{MutableArray, MutablePrimitiveArray, PrimitiveArray},
    offset::OffsetsBuffer,
};
use common_error::{DaftError, DaftResult};

use crate::{
    array::ListArray,
    datatypes::{Field, Utf8Array},
    kernels::tokenize::DaftBPE,
    DataType, Series,
};

use super::as_arrow::AsArrow;

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

impl Utf8Array {
    pub fn tokenize_encode(&self, tokens_path: &str) -> DaftResult<ListArray> {
        let bpe = DaftBPE::from_str(tokens_path)?;

        let mut flat_child = MutablePrimitiveArray::<i32>::new();
        let mut offsets: Vec<i64> = Vec::with_capacity(self.len() + 1);
        offsets.push(0);
        let self_arrow = self.as_arrow();
        for s_opt in self_arrow.iter() {
            if let Some(s) = s_opt {
                let tokens = bpe.encode(s);
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
        let bpe = DaftBPE::from_str(tokens_path)?;
        let offsets = self.offsets();
        let strs = (0..offsets.len() - 1)
            .map(|i| {
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                let sub_series = self.flat_child.slice(start, end)?;
                decode_list(&sub_series, &bpe)
            })
            .collect::<DaftResult<Vec<String>>>()?;
        Utf8Array::from_iter(self.name(), strs.iter().map(Some))
            .with_validity(self.validity().cloned())
    }
}
