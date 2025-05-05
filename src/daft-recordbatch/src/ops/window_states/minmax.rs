use std::collections::VecDeque;

use common_error::DaftResult;
use daft_core::prelude::*;

use super::WindowAggStateOps;

pub struct MinMaxWindowState {
    source: Series,
    deque: VecDeque<usize>,
    cur_idx: usize,
    result_idxs: Vec<u64>,
    validity: arrow2::bitmap::MutableBitmap,
    is_min: bool,
}

impl MinMaxWindowState {
    #[allow(dead_code)]
    pub fn new(source: &Series, total_length: usize, is_min: bool) -> Self {
        Self {
            source: source.clone(),
            deque: VecDeque::new(),
            cur_idx: 0,
            result_idxs: Vec::with_capacity(total_length),
            validity: arrow2::bitmap::MutableBitmap::with_capacity(total_length),
            is_min,
        }
    }

    fn should_remove(&self, a_idx: usize, b_idx: usize) -> bool {
        if !self.source.is_valid(a_idx) {
            return false;
        }
        if !self.source.is_valid(b_idx) {
            return true;
        }

        let a_value = self.source.slice(a_idx, a_idx + 1).unwrap();
        let b_value = self.source.slice(b_idx, b_idx + 1).unwrap();

        if self.is_min {
            match a_value.lt(&b_value) {
                Ok(result) => result.get(0).unwrap_or(false),
                Err(_) => false,
            }
        } else {
            match a_value.gt(&b_value) {
                Ok(result) => result.get(0).unwrap_or(false),
                Err(_) => false,
            }
        }
    }
}

impl WindowAggStateOps for MinMaxWindowState {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if !self.source.is_valid(i) {
                continue;
            }

            while !self.deque.is_empty() {
                let back = *self.deque.back().unwrap();
                if self.should_remove(i, back) {
                    self.deque.pop_back();
                } else {
                    break;
                }
            }

            self.deque.push_back(i);
        }
        Ok(())
    }

    fn remove(&mut self, _start_idx: usize, end_idx: usize) -> DaftResult<()> {
        self.cur_idx = end_idx;

        while !self.deque.is_empty() && *self.deque.front().unwrap() < self.cur_idx {
            self.deque.pop_front();
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        if self.deque.is_empty() {
            self.validity.push(false);
            self.result_idxs.push(0);
        } else {
            self.validity.push(true);
            self.result_idxs.push(*self.deque.front().unwrap() as u64);
        }
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        let result = self
            .source
            .take(&DataArray::<UInt64Type>::from(("", self.result_idxs.clone())).into_series())
            .unwrap();
        result.with_validity(Some(self.validity.clone().into()))
    }
}
