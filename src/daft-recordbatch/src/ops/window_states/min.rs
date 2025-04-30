use std::{cmp::Reverse, collections::BinaryHeap};

use arrow2::bitmap::MutableBitmap;
use common_error::DaftResult;
use daft_core::prelude::*;

use super::{IndexedValue, WindowAggStateOps};

pub struct MinWindowState {
    source: Series,
    min_heap: BinaryHeap<Reverse<IndexedValue>>,
    cur_idx: usize,
    validity: MutableBitmap,
    min_idxs: Vec<u64>,
}

impl MinWindowState {
    pub fn new(source: &Series, total_length: usize) -> Self {
        Self {
            source: source.clone(),
            min_heap: BinaryHeap::new(),
            cur_idx: 0,
            validity: MutableBitmap::with_capacity(total_length),
            min_idxs: Vec::with_capacity(total_length),
        }
    }
}

impl WindowAggStateOps for MinWindowState {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if self.source.is_valid(i) {
                self.min_heap.push(Reverse(IndexedValue {
                    value: self.source.slice(i, i + 1).unwrap(),
                    idx: i as u64,
                }));
            }
        }
        Ok(())
    }

    fn remove(&mut self, _start_idx: usize, end_idx: usize) -> DaftResult<()> {
        self.cur_idx = end_idx;
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        while !self.min_heap.is_empty() && self.min_heap.peek().unwrap().0.idx < self.cur_idx as u64
        {
            self.min_heap.pop();
        }
        if self.min_heap.is_empty() {
            self.validity.push(false);
            self.min_idxs.push(0);
        } else {
            self.validity.push(true);
            self.min_idxs.push(self.min_heap.peek().unwrap().0.idx);
        }
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        let result = self
            .source
            .take(&DataArray::<UInt64Type>::from(("", self.min_idxs.clone())).into_series())
            .unwrap();
        result.with_validity(Some(self.validity.clone().into()))
    }
}
