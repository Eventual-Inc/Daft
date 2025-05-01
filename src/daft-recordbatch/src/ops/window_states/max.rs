use std::collections::BinaryHeap;

use arrow2::bitmap::MutableBitmap;
use common_error::DaftResult;
use daft_core::prelude::*;

use super::{IndexedValue, WindowAggStateOps};

pub struct MaxWindowState<'a> {
    source: &'a Series,
    max_heap: BinaryHeap<IndexedValue<'a>>,
    cur_idx: usize,
    max_idxs: Vec<u64>,
    validity: MutableBitmap,
}

impl<'a> MaxWindowState<'a> {
    pub fn new(source: &'a Series, total_length: usize) -> Self {
        Self {
            source,
            max_heap: BinaryHeap::new(),
            cur_idx: 0,
            max_idxs: Vec::with_capacity(total_length),
            validity: MutableBitmap::with_capacity(total_length),
        }
    }
}

impl<'a> WindowAggStateOps<'a> for MaxWindowState<'a> {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        // if end_idx <= start_idx {
        //     return Err(DaftError::ValueError(
        //         "end_idx must be greater than start_idx".into(),
        //     ));
        // }
        assert!(
            end_idx > start_idx,
            "end_idx must be greater than start_idx"
        );

        for i in start_idx..end_idx {
            if self.source.is_valid(i) {
                self.max_heap.push(IndexedValue {
                    source: self.source,
                    idx: i as u64,
                });
            }
        }
        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        // if end_idx <= start_idx {
        //     return Err(DaftError::ValueError(
        //         "end_idx must be greater than start_idx".into(),
        //     ));
        // }
        assert!(
            end_idx > start_idx,
            "end_idx must be greater than start_idx"
        );

        self.cur_idx = end_idx;
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        while !self.max_heap.is_empty() && self.max_heap.peek().unwrap().idx < self.cur_idx as u64 {
            self.max_heap.pop();
        }
        if self.max_heap.is_empty() {
            self.validity.push(false);
            self.max_idxs.push(0);
        } else {
            self.validity.push(true);
            self.max_idxs.push(self.max_heap.peek().unwrap().idx);
        }
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        let result = self
            .source
            .take(&DataArray::<UInt64Type>::from(("", self.max_idxs.clone())).into_series())?;
        result.with_validity(Some(self.validity.clone().into()))
    }
}
