use std::sync::{Arc, Mutex};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::NodeName,
    resource_manager::get_or_init_spill_manager,
    sorter::{ExternalSorter, SortParams},
    spill::SpillManager,
};

type SortResultIter = Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>;

pub(crate) enum SortState {
    Building(ExternalSorter),
    Yielding { results: Arc<Mutex<SortResultIter>> },
}

pub struct SortSink {
    params: Arc<SortParams>,
    spill_manager: Arc<SpillManager>,
    schema: daft_core::prelude::SchemaRef,
    memory_limit_bytes: Option<usize>,
    spill_batch_size: usize,
}

impl SortSink {
    pub fn new(
        sort_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        schema: daft_core::prelude::SchemaRef,
        memory_limit_bytes: Option<usize>,
        spill_batch_size: usize,
    ) -> Self {
        let spill_manager = get_or_init_spill_manager(None).clone();
        Self {
            params: Arc::new(SortParams {
                sort_by,
                descending,
                nulls_first,
            }),
            spill_manager,
            schema,
            memory_limit_bytes,
            spill_batch_size,
        }
    }
}

impl BlockingSink for SortSink {
    type State = SortState;

    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        if let SortState::Building(ref mut sorter) = state {
            let res = sorter.push(input);
            if let Err(e) = res {
                return Err(e).into();
            }
            Ok(state).into()
        } else {
            panic!("SortSink should be in Building state");
        }
    }

    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(
        &self,
        mut states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        spawner
            .spawn(
                async move {
                    let state = states.pop().unwrap();
                    if let SortState::Building(sorter) = state {
                        let mut results = sorter.finish()?;
                        if let Some(first) = results.next() {
                            Ok(BlockingSinkFinalizeOutput::HasMoreOutput {
                                states: vec![SortState::Yielding {
                                    results: Arc::new(Mutex::new(results)),
                                }],
                                output: vec![first?],
                            })
                        } else {
                            Ok(BlockingSinkFinalizeOutput::Finished(vec![]))
                        }
                    } else if let SortState::Yielding { results } = state {
                        let mut guard = results.lock().unwrap();
                        if let Some(next) = guard.next() {
                            drop(guard);
                            Ok(BlockingSinkFinalizeOutput::HasMoreOutput {
                                states: vec![SortState::Yielding { results }],
                                output: vec![next?],
                            })
                        } else {
                            Ok(BlockingSinkFinalizeOutput::Finished(vec![]))
                        }
                    } else {
                        panic!("Invalid state in SortSink::finalize");
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Sort".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Sort
    }

    fn multiline_display(&self) -> Vec<String> {
        let pairs = self
            .params
            .sort_by
            .iter()
            .zip(self.params.descending.iter())
            .zip(self.params.nulls_first.iter())
            .map(|((sb, d), nf)| {
                format!(
                    "({}, {}, {})",
                    sb,
                    if *d { "descending" } else { "ascending" },
                    if *nf { "nulls first" } else { "nulls last" }
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        vec![format!("Sort (External): Sort by = {}", pairs)]
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        let mem_limit = crate::resource_manager::resolve_memory_limit(self.memory_limit_bytes);
        Ok(SortState::Building(ExternalSorter::new(
            self.params.clone(),
            self.spill_manager.clone(),
            mem_limit,
            self.schema.clone(),
            self.spill_batch_size,
        )))
    }

    fn max_concurrency(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_metrics::ops::NodeType;
    use daft_core::{
        datatypes::{Field, Int64Array},
        prelude::{DataType, Schema},
        series::IntoSeries,
    };
    use daft_dsl::{expr::bound_expr::BoundExpr, resolved_col};
    use daft_micropartition::MicroPartition;
    use daft_recordbatch::RecordBatch;

    use super::{BlockingSink, SortSink, SortState};

    fn make_sort_sink() -> SortSink {
        let schema = Arc::new(Schema::new(vec![daft_core::datatypes::Field::new(
            "a",
            DataType::Int64,
        )]));
        let expr = BoundExpr::try_new(resolved_col("a"), &schema).unwrap();
        SortSink::new(vec![expr], vec![false], vec![false], schema, None, 8192)
    }

    #[test]
    fn test_sort_sink_make_state() {
        let sink = make_sort_sink();
        let state = sink.make_state().unwrap();
        assert!(matches!(state, SortState::Building(_)));
    }

    #[test]
    fn test_sort_sink_name() {
        let sink = make_sort_sink();
        assert_eq!(sink.name().as_ref(), "Sort");
    }

    #[test]
    fn test_sort_sink_op_type() {
        let sink = make_sort_sink();
        assert!(matches!(sink.op_type(), NodeType::Sort));
    }

    #[test]
    fn test_sort_sink_max_concurrency() {
        let sink = make_sort_sink();
        assert_eq!(sink.max_concurrency(), 1);
    }

    #[test]
    fn test_sort_sink_multiline_display() {
        let sink = make_sort_sink();
        assert_eq!(
            sink.multiline_display(),
            vec!["Sort (External): Sort by = (col(0: a), ascending, nulls last)".to_string()]
        );
    }

    #[test]
    fn test_sort_state_make_and_push() {
        let sink = make_sort_sink();
        let state = sink.make_state().unwrap();

        // Extract the sorter from Building state and push data directly
        if let SortState::Building(mut sorter) = state {
            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
            let arr = Int64Array::from_vec("a", vec![5, 3, 1, 4, 2]);
            let rb = RecordBatch::from_nonempty_columns(vec![arr.into_series()]).unwrap();
            let mp = Arc::new(MicroPartition::new_loaded(schema, Arc::new(vec![rb]), None));

            sorter.push(mp).unwrap();
            let iter = sorter.finish().unwrap();
            let mut all_values = Vec::new();
            for result in iter {
                let mp = result.unwrap();
                let rb = mp.concat_or_get().unwrap();
                if let Some(rb) = rb.as_ref() {
                    let col = rb.get_column(0);
                    for i in 0..col.len() {
                        if let daft_core::lit::Literal::Int64(v) = col.get_lit(i) {
                            all_values.push(v);
                        }
                    }
                }
            }
            assert_eq!(all_values, vec![1, 2, 3, 4, 5]);
        } else {
            panic!("Expected Building state");
        }
    }
}
