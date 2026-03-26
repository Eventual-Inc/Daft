use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::count_mode::CountMode;
use daft_dsl::{
    Expr,
    expr::{Column, bound_expr::BoundExpr},
};
use daft_functions_list::{SeriesListExtension, explode};
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::intermediate_op::{IntermediateOpExecuteResult, IntermediateOperator};
use crate::{
    ExecutionTaskSpawner,
    buffer::RowBasedBuffer,
    dynamic_batching::BatchingStrategy,
    pipeline::{MorselSizeRequirement, NodeName},
};

pub struct ExplodeOperator {
    to_explode: Arc<Vec<BoundExpr>>,
    explode_col_indices: Vec<usize>,
    ignore_empty_and_null: bool,
    index_column: Option<String>,
}

impl ExplodeOperator {
    pub fn new(
        to_explode: Vec<BoundExpr>,
        ignore_empty_and_null: bool,
        index_column: Option<String>,
    ) -> Self {
        let explode_col_indices = to_explode
            .iter()
            .filter_map(|expr| match expr.inner().as_ref() {
                Expr::Column(Column::Bound(bound)) => Some(bound.index),
                _ => None,
            })
            .collect();
        Self {
            to_explode: Arc::new(
                to_explode
                    .into_iter()
                    .map(|expr| {
                        BoundExpr::new_unchecked(explode(
                            expr.inner().clone(),
                            daft_dsl::lit(ignore_empty_and_null),
                        ))
                    })
                    .collect(),
            ),
            explode_col_indices,
            ignore_empty_and_null,
            index_column,
        }
    }
}

impl IntermediateOperator for ExplodeOperator {
    type State = ();
    type BatchingStrategy = crate::dynamic_batching::DynBatchingStrategy;
    #[instrument(skip_all, name = "ExplodeOperator::execute")]
    fn execute(
        &self,
        input: MicroPartition,
        state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let to_explode = self.to_explode.clone();
        let index_column = self.index_column.clone();
        task_spawner
            .spawn(
                async move {
                    let out = input.explode(&to_explode, index_column.as_deref())?;
                    Ok((state, out))
                },
                Span::current(),
            )
            .into()
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!(
            "Explode: {}",
            self.to_explode.iter().map(|e| e.to_string()).join(", ")
        )];
        if let Some(ref idx_col) = self.index_column {
            res.push(format!("Index column = {}", idx_col));
        }
        res
    }

    fn name(&self) -> NodeName {
        "Explode".into()
    }

    fn make_state(&self) -> Self::State {}

    fn op_type(&self) -> NodeType {
        NodeType::Explode
    }

    fn batching_strategy(
        &self,
        morsel_size_requirement: MorselSizeRequirement,
    ) -> DaftResult<Self::BatchingStrategy> {
        let cfg = daft_context::get_context().execution_config();
        Ok(
            if cfg.enable_dynamic_batching && !self.explode_col_indices.is_empty() {
                ExpansionAwareBatchingStrategy::new(
                    morsel_size_requirement,
                    self.explode_col_indices.clone(),
                    self.ignore_empty_and_null,
                )
                .into()
            } else {
                crate::dynamic_batching::StaticBatchingStrategy::new(morsel_size_requirement).into()
            },
        )
    }
}

/// A batching strategy that proactively determines optimal batch sizes for explode
/// by scanning list column lengths in buffered data before forming batches.
///
/// Instead of reactively adjusting after observing expansion, this strategy reads
/// list lengths directly from the buffered columns to determine exactly how many
/// input rows will produce the desired output size.
///
/// # Example
/// - Downstream operator requires 100 output rows
/// - Buffered rows have list lengths: [5, 20, 30, 50, 10, ...]
/// - Strategy scans and determines: first 4 rows produce 5+20+30+50 = 105 rows (>= 100)
/// - Takes exactly 4 rows from buffer → explode → ~105 rows → downstream is satisfied
#[derive(Debug, Clone)]
struct ExpansionAwareBatchingStrategy {
    downstream_requirement: MorselSizeRequirement,
    col_indices: Vec<usize>,
    ignore_empty_and_null: bool,
}

impl ExpansionAwareBatchingStrategy {
    pub fn new(
        downstream_requirement: MorselSizeRequirement,
        col_indices: Vec<usize>,
        ignore_empty_and_null: bool,
    ) -> Self {
        Self {
            downstream_requirement,
            col_indices,
            ignore_empty_and_null,
        }
    }

    fn output_rows_for_length(&self, length: Option<u64>) -> usize {
        if self.ignore_empty_and_null {
            length.unwrap_or(0) as usize
        } else {
            std::cmp::max(length.unwrap_or(1), 1) as usize
        }
    }
}

impl BatchingStrategy for ExpansionAwareBatchingStrategy {
    type State = ();

    fn make_state(&self) -> Self::State {}

    fn calculate_new_requirements(&self, _state: &mut Self::State) -> MorselSizeRequirement {
        self.downstream_requirement
    }

    fn initial_requirements(&self) -> MorselSizeRequirement {
        self.downstream_requirement
    }

    fn next_batch(
        &self,
        _state: &mut Self::State,
        buffer: &mut RowBasedBuffer,
    ) -> DaftResult<Option<MicroPartition>> {
        if buffer.total_rows() == 0 {
            return Ok(None);
        }

        let (lower, upper) = self.downstream_requirement.values();
        let target = upper.get();

        // we are just assuming that all columns expand to the same number of rows.
        // This is actually validated during execution.
        // so its pretty safe to just get the first column and assume all others expand the same.
        let col_idx = self.col_indices[0];

        let mut cumulative_output = 0usize;
        let mut cumulative_input = 0usize;

        for partition in buffer.peek() {
            for batch in partition.record_batches() {
                let series = batch.get_column(col_idx);
                let lengths = series.list_count(CountMode::All)?;

                for length in &lengths {
                    cumulative_output += self.output_rows_for_length(length);
                    cumulative_input += 1;

                    if cumulative_output >= target {
                        return buffer.take_rows(cumulative_input);
                    }
                }
            }
        }

        if cumulative_output >= lower {
            buffer.pop_all()
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use arrow_buffer::OffsetBuffer;
    use daft_core::{
        array::ListArray,
        datatypes::{DataType, Field, Int32Array},
        prelude::Schema,
        series::IntoSeries,
    };
    use daft_recordbatch::RecordBatch;

    use super::*;
    use crate::buffer::RowBasedBuffer;

    fn make_list_mp(list_lengths: &[usize]) -> MicroPartition {
        let total_elements: usize = list_lengths.iter().sum();
        let flat_child = Int32Array::from_field_and_values(
            Field::new("item", DataType::Int32),
            0..total_elements as i32,
        )
        .into_series();

        let mut offsets = Vec::with_capacity(list_lengths.len() + 1);
        offsets.push(0i64);
        let mut running = 0i64;
        for &len in list_lengths {
            running += len as i64;
            offsets.push(running);
        }

        let list_field = Field::new("my_list", DataType::List(Box::new(DataType::Int32)));
        let list_array = ListArray::new(
            list_field.clone(),
            flat_child,
            OffsetBuffer::new(offsets.into()),
            None,
        );
        let list_series = list_array.into_series();

        let schema = Arc::new(Schema::new(vec![list_field]));
        let rb = RecordBatch::new_unchecked(
            schema.clone(),
            vec![list_series.into()],
            list_lengths.len(),
        );
        MicroPartition::new_loaded(schema.into(), vec![rb].into(), None)
    }

    fn make_strategy(requirement: MorselSizeRequirement) -> ExpansionAwareBatchingStrategy {
        // "my_list" is at column index 0 in make_list_mp
        ExpansionAwareBatchingStrategy::new(requirement, vec![0], false)
    }

    #[test]
    fn test_empty_buffer_returns_none() -> DaftResult<()> {
        let strategy = make_strategy(MorselSizeRequirement::Strict(
            NonZeroUsize::new(100).unwrap(),
        ));
        let mut buffer = RowBasedBuffer::new(0, NonZeroUsize::new(1000).unwrap());

        assert!(strategy.next_batch(&mut (), &mut buffer)?.is_none());
        Ok(())
    }

    #[test]
    fn test_takes_exact_rows_to_meet_target() -> DaftResult<()> {
        let strategy = make_strategy(MorselSizeRequirement::Strict(
            NonZeroUsize::new(100).unwrap(),
        ));

        let mut buffer = RowBasedBuffer::new(0, NonZeroUsize::new(1000).unwrap());

        // 5 rows with list lengths [20, 30, 25, 35, 10] → cumulative: 20, 50, 75, 110
        // Target = 100, so should take 4 rows (cumulative 110 >= 100)
        buffer.push(make_list_mp(&[20, 30, 25, 35, 10]));

        let batch = strategy.next_batch(&mut (), &mut buffer)?.unwrap();
        assert_eq!(batch.len(), 4);
        assert_eq!(buffer.total_rows(), 1);
        Ok(())
    }

    #[test]
    fn test_single_large_row_exceeds_target() -> DaftResult<()> {
        let strategy = make_strategy(MorselSizeRequirement::Strict(
            NonZeroUsize::new(10).unwrap(),
        ));

        let mut buffer = RowBasedBuffer::new(0, NonZeroUsize::new(1000).unwrap());

        // Single row with list length 1000 — must still take it
        buffer.push(make_list_mp(&[1000, 5]));

        let batch = strategy.next_batch(&mut (), &mut buffer)?.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(buffer.total_rows(), 1);
        Ok(())
    }

    #[test]
    fn test_waits_for_lower_bound() -> DaftResult<()> {
        let strategy = make_strategy(MorselSizeRequirement::Flexible(
            50,
            NonZeroUsize::new(100).unwrap(),
        ));

        let mut buffer = RowBasedBuffer::new(0, NonZeroUsize::new(1000).unwrap());

        // 3 rows with list lengths [5, 5, 5] → total output = 15 < lower bound 50
        buffer.push(make_list_mp(&[5, 5, 5]));

        assert!(strategy.next_batch(&mut (), &mut buffer)?.is_none());
        assert_eq!(buffer.total_rows(), 3);
        Ok(())
    }

    #[test]
    fn test_emits_when_above_lower_bound() -> DaftResult<()> {
        let strategy = make_strategy(MorselSizeRequirement::Flexible(
            50,
            NonZeroUsize::new(100).unwrap(),
        ));

        let mut buffer = RowBasedBuffer::new(0, NonZeroUsize::new(1000).unwrap());

        // 3 rows with list lengths [20, 20, 20] → total output = 60 >= lower bound 50, < upper 100
        buffer.push(make_list_mp(&[20, 20, 20]));

        let batch = strategy.next_batch(&mut (), &mut buffer)?.unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(buffer.total_rows(), 0);
        Ok(())
    }

    #[test]
    fn test_multiple_partitions_in_buffer() -> DaftResult<()> {
        let strategy = make_strategy(MorselSizeRequirement::Strict(
            NonZeroUsize::new(100).unwrap(),
        ));

        let mut buffer = RowBasedBuffer::new(0, NonZeroUsize::new(1000).unwrap());

        // Push two partitions
        buffer.push(make_list_mp(&[30, 30])); // partition 1: 60 total output
        buffer.push(make_list_mp(&[25, 25, 10])); // partition 2: 60 total output

        // Target = 100. Scan: 30, 60, 85, 110 → take 4 rows (spans both partitions)
        let batch = strategy.next_batch(&mut (), &mut buffer)?.unwrap();
        assert_eq!(batch.len(), 4);
        assert_eq!(buffer.total_rows(), 1);
        Ok(())
    }

    #[test]
    fn test_all_single_element_lists() -> DaftResult<()> {
        let strategy = make_strategy(MorselSizeRequirement::Strict(NonZeroUsize::new(5).unwrap()));

        let mut buffer = RowBasedBuffer::new(0, NonZeroUsize::new(1000).unwrap());

        // 10 rows each with list length 1 → 1:1 expansion
        buffer.push(make_list_mp(&[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]));

        let batch = strategy.next_batch(&mut (), &mut buffer)?.unwrap();
        assert_eq!(batch.len(), 5);
        assert_eq!(buffer.total_rows(), 5);
        Ok(())
    }

    #[test]
    fn test_first_batch_is_correctly_sized() -> DaftResult<()> {
        let strategy = make_strategy(MorselSizeRequirement::Strict(
            NonZeroUsize::new(50).unwrap(),
        ));

        let mut buffer = RowBasedBuffer::new(0, NonZeroUsize::new(1000).unwrap());

        // High expansion: each row expands to 100 elements
        // Target = 50, so should take just 1 row on the very first batch
        buffer.push(make_list_mp(&[100, 100, 100]));

        let batch = strategy.next_batch(&mut (), &mut buffer)?.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(buffer.total_rows(), 2);
        Ok(())
    }
}
