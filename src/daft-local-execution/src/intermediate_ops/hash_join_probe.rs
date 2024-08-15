use std::sync::Arc;

use crate::{pipeline::PipelineOutput, DEFAULT_MORSEL_SIZE};
use common_error::DaftResult;
use daft_core::{schema::Schema, JoinType};
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_table::{GrowableTable, ProbeTable, Table};
use tracing::info_span;

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
    build_on: Vec<ExprRef>,
    probe_on: Vec<ExprRef>,
    join_type: JoinType,
}

impl HashJoinProber {
    pub fn new(build_on: Vec<ExprRef>, probe_on: Vec<ExprRef>, join_type: JoinType) -> Self {
        Self {
            build_on,
            probe_on,
            join_type,
        }
    }

    fn probe(
        &self,
        input: &Arc<MicroPartition>,
        probe_table: &Arc<ProbeTable>,
        tables: &Arc<Vec<Table>>,
    ) -> DaftResult<Arc<MicroPartition>> {
        let _span = info_span!("HashJoinProber::execute").entered();
        let _growables = info_span!("HashJoinProber::build_growables").entered();

        // build should only be created once per probe table
        let mut build_side_growable =
            GrowableTable::new(&tables.iter().collect::<Vec<_>>(), true, 20)?;
        // probe should only be created morsel

        let probe_side_input_tables = input.get_tables()?;

        let mut probe_side_growable = GrowableTable::new(
            &probe_side_input_tables.iter().collect::<Vec<_>>(),
            false,
            20,
        )?;

        drop(_growables);
        {
            let _loop = info_span!("HashJoinProber::eval_and_probe").entered();
            for (probe_side_table_idx, table) in probe_side_input_tables.iter().enumerate() {
                // we should emit one table at a time when this is streaming
                let join_keys = table.eval_expression_list(&self.probe_on)?;
                let iter = probe_table.probe(&join_keys)?;

                match self.join_type {
                    JoinType::Inner => {
                        for (probe_row_idx, inner_iter) in iter {
                            if let Some(inner_iter) = inner_iter {
                                for (build_side_table_idx, build_row_idx) in inner_iter {
                                    build_side_growable.extend(
                                        build_side_table_idx as usize,
                                        build_row_idx as usize,
                                        1,
                                    );
                                    // we can perform run length compression for this to make this more efficient
                                    probe_side_growable.extend(
                                        probe_side_table_idx,
                                        probe_row_idx as usize,
                                        1,
                                    );
                                }
                            }
                        }
                    }
                    JoinType::Left | JoinType::Right => {
                        for (probe_row_idx, inner_iter) in iter {
                            if let Some(inner_iter) = inner_iter {
                                for (build_side_table_idx, build_row_idx) in inner_iter {
                                    build_side_growable.extend(
                                        build_side_table_idx as usize,
                                        build_row_idx as usize,
                                        1,
                                    );
                                    probe_side_growable.extend(
                                        probe_side_table_idx,
                                        probe_row_idx as usize,
                                        1,
                                    );
                                }
                            } else {
                                // if there's no match, we should still emit the probe side and fill the build side with nulls
                                build_side_growable.add_nulls(1);
                                probe_side_growable.extend(
                                    probe_side_table_idx,
                                    probe_row_idx as usize,
                                    1,
                                );
                            }
                        }
                    }
                    JoinType::Semi => {
                        for (probe_row_idx, inner_iter) in iter {
                            if inner_iter.is_some() {
                                probe_side_growable.extend(
                                    probe_side_table_idx,
                                    probe_row_idx as usize,
                                    1,
                                );
                            }
                        }
                    }
                    JoinType::Anti => {
                        for (probe_row_idx, inner_iter) in iter {
                            if inner_iter.is_none() {
                                probe_side_growable.extend(
                                    probe_side_table_idx,
                                    probe_row_idx as usize,
                                    1,
                                );
                            }
                        }
                    }
                    _ => todo!(),
                }
            }
        }
        if self.join_type == JoinType::Semi || self.join_type == JoinType::Anti {
            let probe_side_table = probe_side_growable.build()?;
            return Ok(Arc::new(MicroPartition::new_loaded(
                probe_side_table.schema.clone(),
                Arc::new(vec![probe_side_table]),
                None,
            )));
        }
        let probe_side_table = probe_side_growable.build()?;
        let build_side_table = build_side_growable.build()?;
        let common_join_keys_iter =
            self.build_on
                .iter()
                .zip(self.probe_on.iter())
                .filter_map(|(l, r)| {
                    if l.name() == r.name() {
                        Some(l.name())
                    } else {
                        None
                    }
                });

        let final_table = match self.join_type {
            JoinType::Inner => {
                let common_join_keys =
                    common_join_keys_iter.collect::<std::collections::HashSet<_>>();
                let pruned_probe_side_columns = probe_side_table
                    .schema
                    .fields
                    .keys()
                    .filter(|c| !common_join_keys.contains(c.as_str()))
                    .collect::<Vec<_>>();
                let pruned_probe_side_table =
                    probe_side_table.get_columns(&pruned_probe_side_columns)?;
                build_side_table.union(&pruned_probe_side_table)?
            }
            JoinType::Left => {
                let common_join_keys =
                    common_join_keys_iter.collect::<std::collections::HashSet<_>>();
                let pruned_build_side_columns = build_side_table
                    .schema
                    .fields
                    .keys()
                    .filter(|c| !common_join_keys.contains(c.as_str()))
                    .collect::<Vec<_>>();
                let pruned_build_side_table =
                    build_side_table.get_columns(&pruned_build_side_columns)?;
                probe_side_table.union(&pruned_build_side_table)?
            }
            JoinType::Right => {
                let len = build_side_table.len();
                let common_join_keys_vec = common_join_keys_iter.collect::<Vec<_>>();
                let mut join_series = common_join_keys_vec
                    .iter()
                    .map(|name| {
                        let col = probe_side_table.get_column(name)?;
                        Ok(col)
                    })
                    .collect::<DaftResult<Vec<_>>>()?;
                let common_join_keys = common_join_keys_vec
                    .into_iter()
                    .collect::<std::collections::HashSet<_>>();
                for col_name in build_side_table.column_names() {
                    if !common_join_keys.contains(col_name.as_str()) {
                        join_series.push(build_side_table.get_column(col_name)?);
                    }
                }
                for col_name in probe_side_table.column_names() {
                    if !common_join_keys.contains(col_name.as_str()) {
                        join_series.push(probe_side_table.get_column(col_name)?);
                    }
                }
                let schema = Schema::new(join_series.iter().map(|s| s.field().clone()).collect())?;
                Table::new_with_size(schema, join_series.into_iter().cloned().collect(), len)?
            }
            _ => todo!(),
        };

        // let final_table = pruned_left_side_table.union(&pruned_right_side_table)?;
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
