use std::collections::BinaryHeap;

use arrow2::bitmap::MutableBitmap;
use common_error::DaftResult;
use daft_core::prelude::*;

use super::{IndexedValue, WindowAggStateOps};

pub struct MaxWindowState {
    source: Series,
    max_heap: BinaryHeap<IndexedValue>,
    cur_idx: usize,
    max_idxs: Vec<u64>,
    validity: MutableBitmap,
}

#[allow(dead_code)]
impl MaxWindowState {
    pub fn new(source: &Series, total_length: usize) -> Self {
        Self {
            source: source.clone(),
            max_heap: BinaryHeap::new(),
            cur_idx: 0,
            max_idxs: Vec::with_capacity(total_length),
            validity: MutableBitmap::from_len_zeroed(total_length),
        }
    }
}

impl WindowAggStateOps for MaxWindowState {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        assert!(
            end_idx > start_idx,
            "end_idx must be greater than start_idx"
        );

        for i in start_idx..end_idx {
            if self.source.is_valid(i) {
                self.max_heap.push(IndexedValue {
                    value: self.source.slice(i, i + 1).unwrap(),
                    idx: i as u64,
                });
            }
        }
        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
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
            .take(&DataArray::<UInt64Type>::from(("", self.max_idxs.clone())).into_series())
            .unwrap();
        result.with_validity(Some(self.validity.clone().into()))
    }
}
