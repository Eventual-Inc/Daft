mod count;
mod count_distinct;
mod max;
mod mean;
mod min;
mod sum;

use std::cmp::{Eq, Ordering};

use common_error::DaftResult;
use count::CountWindowState;
use count_distinct::CountDistinctWindowState;
use daft_core::prelude::*;
use daft_dsl::AggExpr;
use max::MaxWindowState;
use min::MinWindowState;
use sum::SumWindowState;

/// Trait for window aggregation state implementations
pub trait WindowAggStateOps<'a> {
    /// Add a value to the state with index information
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()>;

    /// Remove a value from the state with index information
    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()>;

    /// Evaluate the current state and push the result to internal buffer
    fn evaluate(&mut self) -> DaftResult<()>;

    /// Build the final result series containing all accumulated results
    fn build(&self) -> DaftResult<Series>;
}

/// Wrapper struct holding a reference to a Series and an original index.
/// Used in Min/Max window states with BinaryHeap to keep track of minimum/maximum values.
///
/// Note: The `Ord` implementation relies on `partial_cmp`, which computes and compares values on demand.
/// This means comparisons might yield unexpected results for non-totally-ordered values like NaN
/// or Null. However, the Min/Max window implementations specifically handle Nulls by ignoring them.
/// NaN values will follow standard floating-point comparison behavior (NaN is not greater than, less than,
/// or equal to any other number, including itself). When multiple NaNs are encountered, their relative
/// order is determined by their original index `idx`.
#[derive(Debug, Clone)]
pub struct IndexedValue<'a> {
    pub source: &'a Series,
    pub idx: u64,
}

impl Eq for IndexedValue<'_> {}

impl PartialEq for IndexedValue<'_> {
    fn eq(&self, other: &Self) -> bool {
        if self.idx != other.idx {
            return false;
        }

        let self_value = match self
            .source
            .slice(self.idx as usize, (self.idx as usize) + 1)
        {
            Ok(v) => v,
            Err(_) => return false,
        };

        let other_value = match other
            .source
            .slice(other.idx as usize, (other.idx as usize) + 1)
        {
            Ok(v) => v,
            Err(_) => return false,
        };

        match self_value.equal(&other_value) {
            Ok(result) => result.into_iter().all(|x| x.unwrap_or(false)),
            Err(_) => false,
        }
    }
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for IndexedValue<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_value = match self
            .source
            .slice(self.idx as usize, (self.idx as usize) + 1)
        {
            Ok(v) => v,
            Err(_) => return Some(Ordering::Less),
        };

        let other_value = match other
            .source
            .slice(other.idx as usize, (other.idx as usize) + 1)
        {
            Ok(v) => v,
            Err(_) => return Some(Ordering::Greater),
        };

        if self_value.data_type() == &DataType::Float64
            && other_value.data_type() == &DataType::Float64
        {
            let self_val_opt = self_value
                .downcast::<Float64Array>()
                .ok()
                .and_then(|arr| arr.get(0));
            let other_val_opt = other_value
                .downcast::<Float64Array>()
                .ok()
                .and_then(|arr| arr.get(0));

            match (self_val_opt, other_val_opt) {
                (Some(self_val), Some(other_val)) => {
                    if self_val.is_nan() || other_val.is_nan() {
                        return if self_val.is_nan() && other_val.is_nan() {
                            Some(self.idx.cmp(&other.idx))
                        } else if self_val.is_nan() {
                            Some(Ordering::Greater)
                        } else {
                            Some(Ordering::Less)
                        };
                    }
                }
                (None, None) => return Some(self.idx.cmp(&other.idx)),
                (None, Some(_)) => return Some(Ordering::Less),
                (Some(_), None) => return Some(Ordering::Greater),
            }
        }

        match self_value.lt(&other_value) {
            Ok(result) => {
                if result.get(0).unwrap_or(false) {
                    return Some(Ordering::Less);
                }
            }
            Err(_) => return None,
        }

        match self_value.equal(&other_value) {
            Ok(result) => {
                if result.get(0).unwrap_or(false) {
                    return Some(self.idx.cmp(&other.idx));
                }
            }
            Err(_) => return None,
        }

        Some(Ordering::Greater)
    }
}

impl Ord for IndexedValue<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

pub fn create_window_agg_state<'a>(
    source: &'a Series,
    agg_expr: &AggExpr,
    total_length: usize,
) -> DaftResult<Option<Box<dyn WindowAggStateOps<'a> + 'a>>> {
    match agg_expr {
        AggExpr::Sum(_) => sum::create_for_type(source, total_length),
        AggExpr::Count(_, _) => Ok(Some(Box::new(CountWindowState::new(source, total_length)))),
        AggExpr::Min(_) => Ok(Some(Box::new(MinWindowState::new(source, total_length)))),
        AggExpr::Max(_) => Ok(Some(Box::new(MaxWindowState::new(source, total_length)))),
        AggExpr::CountDistinct(_) => Ok(Some(Box::new(CountDistinctWindowState::new(
            source,
            total_length,
        )))),
        AggExpr::Mean(_) => mean::create_for_type(source, total_length),
        _ => Ok(None),
    }
}
