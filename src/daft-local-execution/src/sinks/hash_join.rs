use std::sync::Arc;

use crate::{
    channel::{create_channel, MultiSender},
    intermediate_ops::intermediate_op::{IntermediateNode, IntermediateOperator},
    pipeline::PipelineNode,
    runtime_stats::RuntimeStatsContext,
    ExecutionRuntimeHandle, JoinSnafu, PipelineExecutionSnafu, NUM_CPUS,
};
use async_trait::async_trait;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use daft_core::{
    datatypes::Field,
    schema::{Schema, SchemaRef},
    utils::supertype,
};
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::JoinType;
use snafu::{futures::TryFutureExt, ResultExt};
use tracing::info_span;

use super::blocking_sink::{BlockingSink, BlockingSinkStatus};
use daft_table::{GrowableTable, ProbeTable, ProbeTableBuilder, Table};

enum HashJoinState {
    Building {
        probe_table_builder: Option<ProbeTableBuilder>,
        projection: Vec<ExprRef>,
        tables: Vec<Table>,
    },
    Probing {
        probe_table: Arc<ProbeTable>,
        tables: Arc<Vec<Table>>,
    },
}

impl HashJoinState {
    fn new(key_schema: &SchemaRef, projection: Vec<ExprRef>) -> DaftResult<Self> {
        Ok(Self::Building {
            probe_table_builder: Some(ProbeTableBuilder::new(key_schema.clone())?),
            projection,
            tables: vec![],
        })
    }

    fn add_tables(&mut self, input: &Arc<MicroPartition>) -> DaftResult<()> {
        if let Self::Building {
            ref mut probe_table_builder,
            projection,
            tables,
        } = self
        {
            let probe_table_builder = probe_table_builder.as_mut().unwrap();
            for table in input.get_tables()?.iter() {
                tables.push(table.clone());
                let join_keys = table.eval_expression_list(projection)?;

                probe_table_builder.add_table(&join_keys)?;
            }
            Ok(())
        } else {
            panic!("add_tables can only be used during the Building Phase")
        }
    }
    fn finalize(&mut self) -> DaftResult<()> {
        if let Self::Building {
            probe_table_builder,
            tables,
            ..
        } = self
        {
            let ptb = std::mem::take(probe_table_builder).expect("should be set in building mode");
            let pt = ptb.build();

            *self = Self::Probing {
                probe_table: Arc::new(pt),
                tables: Arc::new(tables.clone()),
            };
            Ok(())
        } else {
            panic!("finalize can only be used during the Building Phase")
        }
    }
}

pub(crate) struct HashJoinOperator {
    right_on: Vec<ExprRef>,
    pruned_right_side_columns: Vec<String>,
    _join_type: JoinType,
    join_state: HashJoinState,
}

impl HashJoinOperator {
    pub(crate) fn new(
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        join_type: JoinType,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
    ) -> DaftResult<Self> {
        let left_key_fields = left_on
            .iter()
            .map(|e| e.to_field(left_schema))
            .collect::<DaftResult<Vec<_>>>()?;
        let right_key_fields = right_on
            .iter()
            .map(|e| e.to_field(right_schema))
            .collect::<DaftResult<Vec<_>>>()?;
        let key_schema: SchemaRef = Schema::new(
            left_key_fields
                .into_iter()
                .zip(right_key_fields.into_iter())
                .map(|(l, r)| {
                    // TODO we should be using the comparison_op function here instead but i'm just using existing behavior for now
                    let dtype = supertype::try_get_supertype(&l.dtype, &r.dtype)?;
                    Ok(Field::new(l.name, dtype))
                })
                .collect::<DaftResult<Vec<_>>>()?,
        )?
        .into();

        let left_on = left_on
            .into_iter()
            .zip(key_schema.fields.values())
            .map(|(e, f)| e.cast(&f.dtype))
            .collect::<Vec<_>>();
        let right_on = right_on
            .into_iter()
            .zip(key_schema.fields.values())
            .map(|(e, f)| e.cast(&f.dtype))
            .collect::<Vec<_>>();
        let common_join_keys = left_on
            .iter()
            .zip(right_on.iter())
            .filter_map(|(l, r)| {
                if l.name() == r.name() {
                    Some(l.name())
                } else {
                    None
                }
            })
            .collect::<std::collections::HashSet<_>>();
        let pruned_right_side_columns = right_schema
            .fields
            .keys()
            .filter(|k| !common_join_keys.contains(k.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(join_type, JoinType::Inner);
        Ok(Self {
            right_on,
            pruned_right_side_columns,
            _join_type: join_type,
            join_state: HashJoinState::new(&key_schema, left_on)?,
        })
    }

    fn as_sink(&mut self) -> &mut dyn BlockingSink {
        self
    }

    fn as_intermediate_op(&self) -> Arc<dyn IntermediateOperator> {
        if let HashJoinState::Probing {
            probe_table,
            tables,
        } = &self.join_state
        {
            Arc::new(HashJoinProber {
                probe_table: probe_table.clone(),
                tables: tables.clone(),
                right_on: self.right_on.clone(),
                pruned_right_side_columns: self.pruned_right_side_columns.clone(),
            })
        } else {
            panic!("can't call as_intermediate_op when not in probing state")
        }
    }
}

struct HashJoinProber {
    probe_table: Arc<ProbeTable>,
    tables: Arc<Vec<Table>>,
    right_on: Vec<ExprRef>,
    pruned_right_side_columns: Vec<String>,
}

impl IntermediateOperator for HashJoinProber {
    fn name(&self) -> &'static str {
        "HashJoinProber"
    }
    fn execute(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        let _span = info_span!("HashJoinOperator::execute").entered();
        let _growables = info_span!("HashJoinOperator::build_growables").entered();

        // Left should only be created once per probe table
        let mut left_growable =
            GrowableTable::new(&self.tables.iter().collect::<Vec<_>>(), false, 20)?;
        // right should only be created morsel

        let right_input_tables = input.get_tables()?;

        let mut right_growable =
            GrowableTable::new(&right_input_tables.iter().collect::<Vec<_>>(), false, 20)?;

        drop(_growables);
        {
            let _loop = info_span!("HashJoinOperator::eval_and_probe").entered();
            for (r_table_idx, table) in right_input_tables.iter().enumerate() {
                // we should emit one table at a time when this is streaming
                let join_keys = table.eval_expression_list(&self.right_on)?;
                let iter = self.probe_table.probe(&join_keys)?;

                for (l_table_idx, l_row_idx, right_idx) in iter {
                    left_growable.extend(l_table_idx as usize, l_row_idx as usize, 1);
                    // we can perform run length compression for this to make this more efficient
                    right_growable.extend(r_table_idx, right_idx as usize, 1);
                }
            }
        }
        let left_table = left_growable.build()?;
        let right_table = right_growable.build()?;

        let pruned_right_table = right_table.get_columns(&self.pruned_right_side_columns)?;

        let final_table = left_table.union(&pruned_right_table)?;
        Ok(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        )))
    }
}

impl BlockingSink for HashJoinOperator {
    fn name(&self) -> &'static str {
        "HashJoin"
    }

    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus> {
        self.join_state.add_tables(input)?;
        Ok(BlockingSinkStatus::NeedMoreInput)
    }
    fn finalize(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        self.join_state.finalize()?;
        Ok(None)
    }
}

pub(crate) struct HashJoinNode {
    // use a RW lock
    hash_join: Arc<tokio::sync::Mutex<HashJoinOperator>>,
    left: Box<dyn PipelineNode>,
    right: Box<dyn PipelineNode>,
    build_runtime_stats: Arc<RuntimeStatsContext>,
    probe_runtime_stats: Arc<RuntimeStatsContext>,
}

impl HashJoinNode {
    pub(crate) fn new(
        op: HashJoinOperator,
        left: Box<dyn PipelineNode>,
        right: Box<dyn PipelineNode>,
    ) -> Self {
        HashJoinNode {
            hash_join: Arc::new(tokio::sync::Mutex::new(op)),
            left,
            right,
            build_runtime_stats: RuntimeStatsContext::new(),
            probe_runtime_stats: RuntimeStatsContext::new(),
        }
    }
    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

impl TreeDisplay for HashJoinNode {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", self.name()).unwrap();
        use common_display::DisplayLevel::*;
        match level {
            Compact => {}
            _ => {
                let build_rt_result = self.build_runtime_stats.result();
                writeln!(display, "Probe Table Build:").unwrap();

                build_rt_result
                    .display(&mut display, true, false, true)
                    .unwrap();

                let probe_rt_result = self.probe_runtime_stats.result();
                writeln!(display, "\nProbe Phase:").unwrap();
                probe_rt_result
                    .display(&mut display, true, true, true)
                    .unwrap();
            }
        }
        display
    }
    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.left.as_tree_display(), self.right.as_tree_display()]
    }
}

#[async_trait]
impl PipelineNode for HashJoinNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }

    fn name(&self) -> &'static str {
        "HashJoin"
    }

    async fn start(
        &mut self,
        mut destination: MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<()> {
        let (sender, mut pt_receiver) = create_channel(*NUM_CPUS, false);
        self.left.start(sender, runtime_handle).await?;
        let hash_join = self.hash_join.clone();
        let build_runtime_stats = self.build_runtime_stats.clone();
        let name = self.name();
        let probe_table_build = tokio::spawn(
            async move {
                let span = info_span!("ProbeTable::sink");
                let mut guard = hash_join.lock().await;
                let sink = guard.as_sink();
                while let Some(val) = pt_receiver.recv().await {
                    build_runtime_stats.mark_rows_received(val.len() as u64);
                    if let BlockingSinkStatus::Finished =
                        build_runtime_stats.in_span(&span, || sink.sink(&val))?
                    {
                        break;
                    }
                }
                build_runtime_stats
                    .in_span(&info_span!("ProbeTable::finalize"), || sink.finalize())?;
                DaftResult::Ok(())
            }
            .with_context(move |_| PipelineExecutionSnafu { node_name: name }),
        );
        // should wrap in context join handle

        let (right_sender, streaming_receiver) = create_channel(*NUM_CPUS, destination.in_order());
        // now we can start building the right side
        self.right.start(right_sender, runtime_handle).await?;

        probe_table_build.await.context(JoinSnafu {})??;

        let hash_join = self.hash_join.clone();
        let probing_op = {
            let guard = hash_join.lock().await;
            guard.as_intermediate_op()
        };
        let probing_node = IntermediateNode::new_with_runtime_stats(
            probing_op,
            vec![],
            self.probe_runtime_stats.clone(),
        );
        let worker_senders = probing_node
            .spawn_workers(&mut destination, runtime_handle)
            .await;
        runtime_handle.spawn(
            IntermediateNode::send_to_workers(
                streaming_receiver,
                worker_senders,
                runtime_handle.default_morsel_size(),
            ),
            self.name(),
        );
        Ok(())
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
