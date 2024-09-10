use std::sync::Arc;

use crate::pipeline::PipelineResultType;
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;

use super::blocking_sink::{BlockingSink, BlockingSinkState, BlockingSinkStatus};
use daft_table::{ProbeTable, ProbeTableBuilder, Table};

enum ProbeTableState {
    Building {
        probe_table_builder: Option<ProbeTableBuilder>,
        projection: Vec<ExprRef>,
        tables: Vec<Table>,
    },
    Done {
        probe_table: Arc<ProbeTable>,
        tables: Arc<Vec<Table>>,
    },
}

impl ProbeTableState {
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
}

impl BlockingSinkState for ProbeTableState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn finalize(&mut self) {
        if let Self::Building {
            probe_table_builder,
            tables,
            ..
        } = self
        {
            let ptb = std::mem::take(probe_table_builder).expect("should be set in building mode");
            let pt = ptb.build();

            *self = Self::Done {
                probe_table: Arc::new(pt),
                tables: Arc::new(tables.clone()),
            };
        } else {
            panic!("finalize can only be used during the Building Phase")
        }
    }
}

pub(crate) struct HashJoinBuildSink {
    key_schema: SchemaRef,
    projection: Vec<ExprRef>,
}

impl HashJoinBuildSink {
    pub(crate) fn new(key_schema: SchemaRef, projection: Vec<ExprRef>) -> Self {
        Self {
            key_schema,
            projection,
        }
    }

    pub(crate) fn arced(self) -> Arc<dyn BlockingSink> {
        Arc::new(self)
    }
}

impl BlockingSink for HashJoinBuildSink {
    fn name(&self) -> &'static str {
        "HashJoinBuildSink"
    }

    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut dyn BlockingSinkState,
    ) -> DaftResult<BlockingSinkStatus> {
        let probe_table_state = state
            .as_any_mut()
            .downcast_mut::<ProbeTableState>()
            .unwrap();
        probe_table_state.add_tables(input)?;
        Ok(BlockingSinkStatus::NeedMoreInput)
    }

    fn finalize(
        &self,
        states: &[&dyn BlockingSinkState],
    ) -> DaftResult<Option<PipelineResultType>> {
        assert_eq!(states.len(), 1);
        let probe_table_state = states[0]
            .as_any()
            .downcast_ref::<ProbeTableState>()
            .unwrap();
        if let ProbeTableState::Done {
            probe_table,
            tables,
        } = &probe_table_state
        {
            Ok(Some((probe_table.clone(), tables.clone()).into()))
        } else {
            panic!("finalize should only be called after the probe table is built")
        }
    }

    fn max_parallelism(&self) -> usize {
        1
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(ProbeTableState::new(
            &self.key_schema,
            self.projection.clone(),
        )?))
    }
}
