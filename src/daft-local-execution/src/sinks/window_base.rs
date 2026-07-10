use common_error::DaftResult;
use daft_core::{array::ops::build_multi_array_compare, datatypes::UInt64Array, prelude::*};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_groupby::IntoGroups;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

/// Shared state for a single partition in window operations
#[derive(Default)]
pub struct SinglePartitionWindowState {
    pub partitions: Vec<RecordBatch>,
}

/// Base state for window operations
pub enum WindowBaseState {
    Accumulating {
        inner_states: Vec<Option<SinglePartitionWindowState>>,
    },
    Done,
}

impl WindowBaseState {
    pub fn new(num_partitions: usize) -> Self {
        let inner_states = (0..num_partitions).map(|_| None).collect::<Vec<_>>();
        Self::Accumulating { inner_states }
    }

    pub fn make_base_state(num_partitions: usize) -> DaftResult<Self> {
        Ok(Self::new(num_partitions))
    }

    pub fn push(
        &mut self,
        input: MicroPartition,
        partition_by: &[BoundExpr],
        sink_name: &str,
    ) -> DaftResult<()> {
        if let Self::Accumulating { inner_states } = self {
            let partitioned = input.partition_by_hash(partition_by, inner_states.len())?;
            for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
                let state = state.get_or_insert_with(SinglePartitionWindowState::default);
                for table in p.record_batches() {
                    state.partitions.push(table.clone());
                }
            }
        } else {
            panic!("{} should be in Accumulating state", sink_name);
        }
        Ok(())
    }

    pub fn finalize(&mut self, sink_name: &str) -> Vec<Option<SinglePartitionWindowState>> {
        let res = if let Self::Accumulating { inner_states } = self {
            std::mem::take(inner_states)
        } else {
            panic!("{} should be in Accumulating state", sink_name);
        };
        *self = Self::Done;
        res
    }
}

/// Base trait for window sink params
#[allow(dead_code)]
pub trait WindowSinkParams: Send + Sync {
    fn original_schema(&self) -> &SchemaRef;
    fn partition_by(&self) -> &[BoundExpr];
    fn name(&self) -> &'static str;
}

/// Groups rows by the partition_by columns, returning one Vec<u64> of row indices per group.
pub(super) fn partition_into_groups(
    all_partitions: &[RecordBatch],
    partition_by: &[BoundExpr],
) -> DaftResult<Vec<Vec<u64>>> {
    let partition_key_batches: Vec<RecordBatch> = all_partitions
        .iter()
        .map(|p| p.eval_expression_list(partition_by))
        .collect::<DaftResult<_>>()?;
    let partition_keys = RecordBatch::concat(&partition_key_batches)?;
    let (_, groups) = partition_keys.make_groups()?;
    Ok(groups
        .into_iter()
        .map(|g| g.into_iter().collect())
        .collect())
}

/// Sorts each group by the order_by columns and materializes them as RecordBatches,
/// skipping empty groups.
pub(super) fn sort_and_materialize_groups(
    groups: Vec<Vec<u64>>,
    full_data: RecordBatch,
    order_by: &[BoundExpr],
    descending: &[bool],
    nulls_first: &[bool],
) -> DaftResult<Vec<RecordBatch>> {
    let order_keys = full_data.eval_expression_list(order_by)?;
    let order_key_series: Vec<Series> = order_keys
        .columns()
        .iter()
        .map(|c| c.as_materialized_series().clone())
        .collect();
    let cmp = build_multi_array_compare(&order_key_series, descending, nulls_first)?;

    groups
        .into_iter()
        .filter(|g| !g.is_empty())
        .map(|mut group_idxs| {
            group_idxs.sort_by(|&a, &b| cmp(a as usize, b as usize));
            full_data.take(&UInt64Array::from_vec("idx", group_idxs))
        })
        .collect()
}
