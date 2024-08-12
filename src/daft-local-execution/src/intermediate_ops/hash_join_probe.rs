use std::sync::Arc;

use common_error::DaftResult;
use daft_core::JoinType;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_table::{GrowableTable, JoinOutputMapper, ProbeTable, Table};
use tracing::info_span;

use crate::channel::PipelineResult;

use super::{
    intermediate_op::{IntermediateOpSpec, IntermediateOperator, OperatorOutput},
    state::OperatorTaskState,
};

#[derive(Clone)]
pub struct HashJoinProbeSpec {
    probe_on: Vec<ExprRef>,
    join_mapper: Arc<JoinOutputMapper>,
    join_type: JoinType,
}

impl HashJoinProbeSpec {
    pub fn new(
        probe_on: Vec<ExprRef>,
        join_mapper: Arc<JoinOutputMapper>,
        join_type: JoinType,
    ) -> Self {
        Self {
            probe_on,
            join_mapper,
            join_type,
        }
    }
}

impl IntermediateOpSpec for HashJoinProbeSpec {
    fn to_operator(&self) -> Box<dyn IntermediateOperator> {
        Box::new(HashJoinProber {
            spec: self.clone(),
            state: OperatorTaskState::new(),
            probe_table: None,
            build_side_tables: None,
            join_type: self.join_type,
        })
    }
}

struct HashJoinProber {
    spec: HashJoinProbeSpec,
    state: OperatorTaskState,
    probe_table: Option<Arc<ProbeTable>>,
    build_side_tables: Option<Arc<Vec<Table>>>,
    join_type: JoinType,
}

impl HashJoinProber {
    fn probe(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        let (probe_table, build_side_tables) = (
            self.probe_table.as_ref().unwrap(),
            self.build_side_tables.as_ref().unwrap(),
        );

        let mut build_side_growable =
            GrowableTable::new(&build_side_tables.iter().collect::<Vec<_>>(), false, 20)?;
        let probe_side_tables = input.get_tables()?;

        let mapped_probe_side_tables = probe_side_tables
            .iter()
            .map(|t| self.spec.join_mapper.map_right(t))
            .collect::<DaftResult<Vec<_>>>()?;

        let mut probe_side_growable = GrowableTable::new(
            &mapped_probe_side_tables.iter().collect::<Vec<_>>(),
            false,
            20,
        )?;

        {
            let _loop = info_span!("HashJoinOperator::eval_and_probe").entered();
            for (probe_side_table_idx, table) in probe_side_tables.iter().enumerate() {
                // we should emit one table at a time when this is streaming
                let join_keys = table.eval_expression_list(&self.spec.probe_on)?;
                let iter = probe_table.probe(&join_keys)?;

                match self.join_type {
                    JoinType::Inner => {
                        for (build_side_table_idx, build_row_idx, probe_idx) in iter {
                            build_side_growable.extend(
                                build_side_table_idx as usize,
                                build_row_idx as usize,
                                1,
                            );
                            // we can perform run length compression for this to make this more efficient
                            probe_side_growable.extend(probe_side_table_idx, probe_idx as usize, 1);
                        }
                    }
                    JoinType::Left | JoinType::Right => {
                        let mut extended = false;
                        for (build_side_table_idx, build_row_idx, probe_idx) in iter {
                            build_side_growable.extend(
                                build_side_table_idx as usize,
                                build_row_idx as usize,
                                1,
                            );
                            // we can perform run length compression for this to make this more efficient
                            probe_side_growable.extend(probe_side_table_idx, probe_idx as usize, 1);
                            extended = true;
                        }
                        // if we didn't find any matches, emit the entire probe side table and extend build side with nulls
                        if !extended {
                            probe_side_growable.extend(probe_side_table_idx, 0, table.len());
                            build_side_growable.add_nulls(table.len());
                        }
                    }
                    _ => todo!("implement other join types"),
                }
            }
        }
        let build_side_table = build_side_growable.build()?;
        let probe_side_table = probe_side_growable.build()?;

        let final_table = build_side_table.union(&probe_side_table)?;
        let mp = Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        ));
        Ok(mp)
    }
}

impl IntermediateOperator for HashJoinProber {
    fn name(&self) -> &'static str {
        "HashJoinProber"
    }
    fn execute(&mut self, idx: usize, input: &PipelineResult) -> DaftResult<OperatorOutput> {
        match idx {
            0 => {
                let (probe_table, tables) = match input {
                    PipelineResult::ProbeTable(probe_table, tables) => (probe_table, tables),
                    _ => panic!("expected probe table and tables for idx 0"),
                };
                if self.probe_table.is_some() || self.build_side_tables.is_some() {
                    panic!("probe table and tables should only be set once");
                }
                self.probe_table = Some(probe_table.clone());
                self.build_side_tables = Some(tables.clone());
                Ok(OperatorOutput::NeedMoreInput)
            }
            _ => {
                let mp = match input {
                    PipelineResult::MicroPartition(mp) => mp,
                    _ => panic!("expected morsel for idx 1"),
                };
                let mp = self.probe(mp)?;
                self.state.add(mp);
                match self.state.try_clear() {
                    Some(part) => Ok(OperatorOutput::Ready(part?.into())),
                    None => Ok(OperatorOutput::NeedMoreInput),
                }
            }
        }
    }

    fn finalize(&mut self) -> DaftResult<Option<PipelineResult>> {
        match self.state.clear() {
            Some(part) => part.map(|p| Some(p.into())),
            None => Ok(None),
        }
    }
}
