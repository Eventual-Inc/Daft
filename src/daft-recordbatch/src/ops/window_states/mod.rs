mod count;
mod count_distinct;
mod max;
mod mean;
mod min;
mod sum;

use std::cmp::{Eq, Ordering};

use common_error::{DaftError, DaftResult};
pub use count::CountWindowStateInner;
pub use count_distinct::CountDistinctWindowStateInner;
use daft_core::prelude::*;
use daft_dsl::AggExpr;
pub use max::MaxWindowStateInner;
pub use mean::MeanWindowStateInner;
pub use min::MinWindowStateInner;
pub use sum::SumWindowStateInner;

/// Trait for window aggregation state inner implementations
pub trait WindowAggStateOps {
    /// Add a value to the state with index information
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()>;

    /// Remove a value from the state with index information
    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()>;

    /// Evaluate the current state and push the result to internal buffer
    fn evaluate(&mut self) -> DaftResult<()>;

    /// Build the final result series containing all accumulated results
    fn build(&self) -> DaftResult<Series>;
}

#[derive(Debug, Clone)]
pub struct IndexedValue {
    pub value: Series,
    pub idx: u64,
}

impl Eq for IndexedValue {}

impl PartialEq for IndexedValue {
    fn eq(&self, other: &Self) -> bool {
        if self.idx != other.idx {
            return false;
        }

        match self.value.equal(&other.value) {
            Ok(result) => result.into_iter().all(|x| x.unwrap_or(false)),
            Err(_) => false,
        }
    }
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for IndexedValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.value.lt(&other.value) {
            Ok(result) => {
                if result.into_iter().any(|x| x.unwrap_or(false)) {
                    return Some(Ordering::Less);
                }
            }
            Err(_) => return None,
        }

        match self.value.equal(&other.value) {
            Ok(result) => {
                if result.into_iter().all(|x| x.unwrap_or(false)) {
                    return Some(self.idx.cmp(&other.idx));
                }
            }
            Err(_) => return None,
        }

        Some(Ordering::Greater)
    }
}

impl Ord for IndexedValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

pub fn create_window_agg_state(
    source: &Series,
    agg_expr: &AggExpr,
    total_length: usize,
) -> Option<DaftResult<Box<dyn WindowAggStateOps>>> {
    match agg_expr {
        AggExpr::Sum(_) => {
            let result: DaftResult<Box<dyn WindowAggStateOps>> =
                (|| -> DaftResult<Box<dyn WindowAggStateOps>> {
                    match source.data_type() {
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                            let casted = source.cast(&DataType::Int64)?;
                            Ok(Box::new(SumWindowStateInner::<Int64Type>::new(
                                &casted,
                                total_length,
                            )))
                        }
                        DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64 => {
                            let casted = source.cast(&DataType::UInt64)?;
                            Ok(Box::new(SumWindowStateInner::<UInt64Type>::new(
                                &casted,
                                total_length,
                            )))
                        }
                        DataType::Float32 => Ok(Box::new(SumWindowStateInner::<Float32Type>::new(
                            source,
                            total_length,
                        ))),
                        DataType::Float64 => Ok(Box::new(SumWindowStateInner::<Float64Type>::new(
                            source,
                            total_length,
                        ))),
                        dt => Err(DaftError::TypeError(format!(
                            "Cannot run Sum over type {}",
                            dt
                        ))),
                    }
                })();
            Some(result)
        }
        AggExpr::Count(_, _) => Some(Ok(Box::new(CountWindowStateInner::new(
            source,
            total_length,
        )))),
        AggExpr::Min(_) => Some(Ok(Box::new(MinWindowStateInner::new(source, total_length)))),
        AggExpr::Max(_) => Some(Ok(Box::new(MaxWindowStateInner::new(source, total_length)))),
        AggExpr::CountDistinct(_) => Some(Ok(Box::new(CountDistinctWindowStateInner::new(
            source,
            total_length,
        )))),
        AggExpr::Mean(_) => {
            let result: DaftResult<Box<dyn WindowAggStateOps>> =
                (|| -> DaftResult<Box<dyn WindowAggStateOps>> {
                    match source.data_type() {
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                            let casted = source.cast(&DataType::Int64)?;
                            Ok(Box::new(MeanWindowStateInner::<Int64Type>::new(
                                &casted,
                                total_length,
                            )))
                        }
                        DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64 => {
                            let casted = source.cast(&DataType::UInt64)?;
                            Ok(Box::new(MeanWindowStateInner::<UInt64Type>::new(
                                &casted,
                                total_length,
                            )))
                        }
                        DataType::Float32 => Ok(Box::new(
                            MeanWindowStateInner::<Float32Type>::new(source, total_length),
                        )),
                        DataType::Float64 => Ok(Box::new(
                            MeanWindowStateInner::<Float64Type>::new(source, total_length),
                        )),
                        dt => Err(DaftError::TypeError(format!(
                            "Cannot run Mean over type {}",
                            dt
                        ))),
                    }
                })();
            Some(result)
        }
        _ => None,
    }
}
