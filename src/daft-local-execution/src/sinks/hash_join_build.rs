use std::sync::Arc;

use crate::pipeline::PipelineResultType;
use common_error::DaftResult;
use daft_core::schema::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;

use super::blocking_sink::{BlockingSink, BlockingSinkStatus};
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
                probe_table: Arc::new(pt),
                tables: Arc::new(tables.clone()),
            };
            Ok(())
        } else {
            panic!("finalize can only be used during the Building Phase")
        }
    }
}

pub(crate) struct HashJoinBuildSink {
    probe_table_state: ProbeTableState,
}

impl HashJoinBuildSink {
    pub(crate) fn new(key_schema: SchemaRef, projection: Vec<ExprRef>) -> DaftResult<Self> {
        Ok(Self {
            probe_table_state: ProbeTableState::new(&key_schema, projection)?,
        })
    }

    pub(crate) fn boxed(self) -> Box<dyn BlockingSink> {
        Box::new(self)
    }
}

impl BlockingSink for HashJoinBuildSink {
    fn name(&self) -> &'static str {
        "HashJoinBuildSink"
    }

    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus> {
        self.probe_table_state.add_tables(input)?;
        Ok(BlockingSinkStatus::NeedMoreInput)
    }

    fn finalize(&mut self) -> DaftResult<Option<PipelineResultType>> {
        self.probe_table_state.finalize()?;
        if let ProbeTableState::Done {
            probe_table,
            tables,
        } = &self.probe_table_state
        {
            Ok(Some((probe_table.clone(), tables.clone()).into()))
        } else {
            panic!("finalize should only be called after the probe table is built")
        }
    }
}
