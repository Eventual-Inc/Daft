use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::JoinType;
use daft_table::{make_probeable_builder, ProbeState, ProbeableBuilder, Table};

use super::blocking_sink::{BlockingSink, BlockingSinkState, BlockingSinkStatus};
use crate::pipeline::PipelineResultType;

enum ProbeTableState {
    Building {
        probe_table_builder: Option<Box<dyn ProbeableBuilder>>,
        projection: Vec<ExprRef>,
        tables: Vec<Table>,
    },
    Done {
        probe_state: Arc<ProbeState>,
    },
}

impl ProbeTableState {
    fn new(
        key_schema: &SchemaRef,
        projection: Vec<ExprRef>,
        nulls_equal_aware: Option<&Vec<bool>>,
        join_type: &JoinType,
    ) -> DaftResult<Self> {
        let track_indices = !matches!(join_type, JoinType::Anti | JoinType::Semi);
        Ok(Self::Building {
            probe_table_builder: Some(make_probeable_builder(
                key_schema.clone(),
                nulls_equal_aware,
                track_indices,
            )?),
            projection,
            tables: Vec::new(),
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

            *self = Self::Done {
                probe_state: Arc::new(ProbeState::new(pt, Arc::new(tables.clone()))),
            };
            Ok(())
        } else {
            panic!("finalize can only be used during the Building Phase")
        }
    }
}

impl BlockingSinkState for ProbeTableState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct HashJoinBuildSink {
    key_schema: SchemaRef,
    projection: Vec<ExprRef>,
    nulls_equal_aware: Option<Vec<bool>>,
    join_type: JoinType,
}

impl HashJoinBuildSink {
    pub(crate) fn new(
        key_schema: SchemaRef,
        projection: Vec<ExprRef>,
        nulls_equal_aware: Option<Vec<bool>>,
        join_type: &JoinType,
    ) -> DaftResult<Self> {
        Ok(Self {
            key_schema,
            projection,
            nulls_equal_aware,
            join_type: *join_type,
        })
    }
}

impl BlockingSink for HashJoinBuildSink {
    fn name(&self) -> &'static str {
        "HashJoinBuildSink"
    }

    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
    ) -> DaftResult<BlockingSinkStatus> {
        state
            .as_any_mut()
            .downcast_mut::<ProbeTableState>()
            .expect("HashJoinBuildSink should have ProbeTableState")
            .add_tables(input)?;
        Ok(BlockingSinkStatus::NeedMoreInput(state))
    }

    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
    ) -> DaftResult<Option<PipelineResultType>> {
        assert_eq!(states.len(), 1);
        let mut state = states.into_iter().next().unwrap();
        let probe_table_state = state
            .as_any_mut()
            .downcast_mut::<ProbeTableState>()
            .expect("State type mismatch");
        probe_table_state.finalize()?;
        if let ProbeTableState::Done { probe_state } = probe_table_state {
            Ok(Some(probe_state.clone().into()))
        } else {
            panic!("finalize should only be called after the probe table is built")
        }
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(ProbeTableState::new(
            &self.key_schema,
            self.projection.clone(),
            self.nulls_equal_aware.as_ref(),
            &self.join_type,
        )?))
    }
}
