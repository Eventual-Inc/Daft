use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_table::{GrowableTable, ProbeTable, Table};
use tracing::info_span;

use crate::{channel::PipelineOutput, DEFAULT_MORSEL_SIZE};

use super::{
    intermediate_op::{IntermediateOperator, IntermediateOperatorOutput},
    state::OperatorState,
};

pub enum ProbeTableState {
    Building,
    Probing(Arc<ProbeTable>, Arc<Vec<Table>>),
}

pub struct HashJoinProbeState {
    probe_state: ProbeTableState,
    buffer: Vec<Arc<MicroPartition>>,
    threshold: usize,
    curr_len: usize,
}

impl HashJoinProbeState {
    fn new() -> Self {
        Self {
            probe_state: ProbeTableState::Building,
            buffer: vec![],
            threshold: DEFAULT_MORSEL_SIZE,
            curr_len: 0,
        }
    }

    fn set_probe_table(&mut self, probe_table: Arc<ProbeTable>, tables: Arc<Vec<Table>>) {
        if let ProbeTableState::Building = self.probe_state {
            self.probe_state = ProbeTableState::Probing(probe_table, tables);
        } else {
            panic!("set_probe_table can only be called in Building state")
        }
    }

    fn get_probe_table(&self) -> (&Arc<ProbeTable>, &Arc<Vec<Table>>) {
        if let ProbeTableState::Probing(ref probe_table, ref tables) = self.probe_state {
            (probe_table, tables)
        } else {
            panic!("get_probe_table can only be called in Probing state")
        }
    }
}

impl OperatorState for HashJoinProbeState {
    fn add(&mut self, input: Arc<MicroPartition>) {
        self.curr_len += input.len();
        self.buffer.push(input);
    }

    // Try to clear the buffer if the threshold is reached.
    fn try_clear(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
        if self.curr_len >= self.threshold {
            self.clear()
        } else {
            None
        }
    }

    // Clear the buffer and return the concatenated MicroPartition.
    fn clear(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
        if self.buffer.is_empty() {
            return None;
        }

        let concated =
            MicroPartition::concat(&self.buffer.iter().map(|x| x.as_ref()).collect::<Vec<_>>())
                .map(Arc::new);
        self.buffer.clear();
        self.curr_len = 0;
        Some(concated)
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct HashJoinProber {
    right_on: Vec<ExprRef>,
}

impl HashJoinProber {
    pub fn new(right_on: Vec<ExprRef>) -> Self {
        Self { right_on }
    }

    fn probe(
        &self,
        input: &Arc<MicroPartition>,
        probe_table: &Arc<ProbeTable>,
        tables: &Arc<Vec<Table>>,
    ) -> DaftResult<Arc<MicroPartition>> {
        let _span = info_span!("HashJoinProber::execute").entered();
        let _growables = info_span!("HashJoinProber::build_growables").entered();

        // Left should only be created once per probe table
        let mut left_growable = GrowableTable::new(&tables.iter().collect::<Vec<_>>(), false, 20)?;
        // right should only be created morsel

        let right_input_tables = input.get_tables()?;

        let mut right_growable =
            GrowableTable::new(&right_input_tables.iter().collect::<Vec<_>>(), false, 20)?;

        drop(_growables);
        {
            let _loop = info_span!("HashJoinProber::eval_and_probe").entered();
            for (r_table_idx, table) in right_input_tables.iter().enumerate() {
                // we should emit one table at a time when this is streaming
                let join_keys = table.eval_expression_list(&self.right_on)?;
                let iter = probe_table.probe(&join_keys)?;

                for (l_table_idx, l_row_idx, right_idx) in iter {
                    left_growable.extend(l_table_idx as usize, l_row_idx as usize, 1);
                    // we can perform run length compression for this to make this more efficient
                    right_growable.extend(r_table_idx, right_idx as usize, 1);
                }
            }
        }
        let left_table = left_growable.build()?;
        let right_table = right_growable.build()?;

        let non_join_columns = right_table
            .schema
            .fields
            .iter()
            .filter(|c| !self.right_on.iter().any(|e| e.name() == c.0))
            .map(|c| c.0)
            .collect::<Vec<_>>();
        let right_table = right_table.get_columns(&non_join_columns)?;

        let final_table = left_table.union(&right_table)?;
        Ok(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        )))
    }
}

impl IntermediateOperator for HashJoinProber {
    fn make_state(&self) -> Box<dyn OperatorState> {
        Box::new(HashJoinProbeState::new())
    }

    fn execute(
        &self,
        index: usize,
        input: &PipelineOutput,
        state: &mut Box<dyn OperatorState>,
    ) -> DaftResult<IntermediateOperatorOutput> {
        match index {
            0 => {
                let state = state
                    .as_any_mut()
                    .downcast_mut::<HashJoinProbeState>()
                    .unwrap();
                let (probe_table, tables) = input.as_probe_table()?;
                state.set_probe_table(probe_table, tables);
                Ok(IntermediateOperatorOutput::NeedMoreInput(None))
            }
            _ => {
                let mp = input.as_micro_partition()?;
                state.add(mp);
                if let Some(part) = state.try_clear() {
                    let (probe_table, tables) = {
                        let state = state.as_any().downcast_ref::<HashJoinProbeState>().unwrap();
                        state.get_probe_table()
                    };
                    let out = self.probe(&part?, probe_table, tables)?;
                    Ok(IntermediateOperatorOutput::NeedMoreInput(out.into()))
                } else {
                    Ok(IntermediateOperatorOutput::NeedMoreInput(None))
                }
            }
        }
    }

    fn finalize(&self, state: &mut Box<dyn OperatorState>) -> DaftResult<Option<PipelineOutput>> {
        let state = state
            .as_any_mut()
            .downcast_mut::<HashJoinProbeState>()
            .unwrap();
        if let Some(part) = state.clear() {
            let (probe_table, tables) = state.get_probe_table();
            let out = self.probe(&part?, probe_table, tables)?;
            Ok(Some(out.into()))
        } else {
            Ok(None)
        }
    }
    fn name(&self) -> &'static str {
        "HashJoinProber"
    }
}
