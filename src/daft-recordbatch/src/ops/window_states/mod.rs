mod count;
mod count_distinct;
mod mean;
mod minmax;
mod sum;

use common_error::DaftResult;
use count::CountWindowState;
use count_distinct::CountDistinctWindowState;
use daft_core::prelude::*;
use daft_dsl::{AggExpr, expr::bound_expr::BoundAggExpr};
use sum::SumWindowState;

use crate::RecordBatch;

/// Trait for window aggregation state implementations
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

pub fn create_window_agg_state(
    sources: &RecordBatch,
    agg_expr: &BoundAggExpr,
    total_length: usize,
) -> DaftResult<Option<Box<dyn WindowAggStateOps>>> {
    match agg_expr.as_ref() {
        AggExpr::Sum(_) => sum::create_for_type(sources, total_length),
        AggExpr::Count(_, mode) => {
            let [source] = sources.columns() else {
                unreachable!("count should only have one input")
            };

            Ok(Some(Box::new(CountWindowState::new(
                source,
                total_length,
                *mode,
            ))))
        }
        // TODO: Implement once proper behavior regarding NaNs is decided
        // AggExpr::Min(_) => Ok(Some(Box::new(MinMaxWindowState::new(source, total_length, true)))),
        // AggExpr::Max(_) => Ok(Some(Box::new(MinMaxWindowState::new(source, total_length, false)))),
        AggExpr::CountDistinct(_) => {
            let [source] = sources.columns() else {
                unreachable!("count distinct should only have one input")
            };

            Ok(Some(Box::new(CountDistinctWindowState::new(
                source,
                total_length,
            ))))
        }
        AggExpr::Mean(_) => mean::create_for_type(sources, total_length),
        _ => Ok(None),
    }
}
