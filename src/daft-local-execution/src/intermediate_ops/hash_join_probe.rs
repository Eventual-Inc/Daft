use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{schema::SchemaRef, JoinType};
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_table::{GrowableTable, ProbeTable, Table};
use indexmap::IndexSet;
use tracing::{info_span, instrument};

use crate::pipeline::PipelineResultType;

use super::intermediate_op::{
    IntermediateOperator, IntermediateOperatorResult, IntermediateOperatorState,
};

enum HashJoinProbeState {
    Building,
    ReadyToProbe(Arc<ProbeTable>, Arc<Vec<Table>>),
}

impl HashJoinProbeState {
    fn set_table(&mut self, table: &Arc<ProbeTable>, tables: &Arc<Vec<Table>>) {
        if let HashJoinProbeState::Building = self {
            *self = HashJoinProbeState::ReadyToProbe(table.clone(), tables.clone());
        } else {
            panic!("HashJoinProbeState should only be in Building state when setting table")
        }
    }

    fn probe_inner(
        &self,
        input: &Arc<MicroPartition>,
        probe_on: &[ExprRef],
        common_join_keys: &[String],
        left_side_non_join_columns: &[String],
        right_side_non_join_columns: &[String],
        build_on_left: bool,
    ) -> DaftResult<Arc<MicroPartition>> {
        if let HashJoinProbeState::ReadyToProbe(probe_table, tables) = self {
            let _growables = info_span!("HashJoinOperator::build_growables").entered();

            let mut build_side_growable =
                GrowableTable::new(&tables.iter().collect::<Vec<_>>(), false, 20)?;

            let input_tables = input.get_tables()?;

            let mut probe_side_growable =
                GrowableTable::new(&input_tables.iter().collect::<Vec<_>>(), false, 20)?;

            drop(_growables);
            {
                let _loop = info_span!("HashJoinOperator::eval_and_probe").entered();
                for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                    // we should emit one table at a time when this is streaming
                    let join_keys = table.eval_expression_list(probe_on)?;
                    let iter = probe_table.probe(&join_keys)?;

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
            }
            let build_side_table = build_side_growable.build()?;
            let probe_side_table = probe_side_growable.build()?;

            let (left_table, right_table) = if build_on_left {
                (build_side_table, probe_side_table)
            } else {
                (probe_side_table, build_side_table)
            };

            let join_keys_table = left_table.get_columns(common_join_keys)?;
            let left_non_join_columns = left_table.get_columns(left_side_non_join_columns)?;
            let right_non_join_columns = right_table.get_columns(right_side_non_join_columns)?;
            let final_table = join_keys_table
                .union(&left_non_join_columns)?
                .union(&right_non_join_columns)?;

            Ok(Arc::new(MicroPartition::new_loaded(
                final_table.schema.clone(),
                Arc::new(vec![final_table]),
                None,
            )))
        } else {
            panic!("probe can only be used during the ReadyToProbe Phase")
        }
    }

    fn probe_left_right(
        &self,
        input: &Arc<MicroPartition>,
        probe_on: &[ExprRef],
        common_join_keys: &[String],
        left_side_non_join_columns: &[String],
        right_side_non_join_columns: &[String],
        is_left: bool,
    ) -> DaftResult<Arc<MicroPartition>> {
        if let HashJoinProbeState::ReadyToProbe(probe_table, tables) = self {
            let _growables = info_span!("HashJoinOperator::build_growables").entered();

            // Need to set use_validity to true here because we add nulls to the build side
            let mut build_side_growable =
                GrowableTable::new(&tables.iter().collect::<Vec<_>>(), true, 20)?;

            let input_tables = input.get_tables()?;

            let mut probe_side_growable =
                GrowableTable::new(&input_tables.iter().collect::<Vec<_>>(), false, 20)?;

            drop(_growables);
            {
                let _loop = info_span!("HashJoinOperator::eval_and_probe").entered();
                for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                    let join_keys = table.eval_expression_list(probe_on)?;
                    let iter = probe_table.probe(&join_keys)?;

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
            }
            let build_side_table = build_side_growable.build()?;
            let probe_side_table = probe_side_growable.build()?;

            let final_table = if is_left {
                let join_table = probe_side_table.get_columns(common_join_keys)?;
                let left = probe_side_table.get_columns(left_side_non_join_columns)?;
                let right = build_side_table.get_columns(right_side_non_join_columns)?;
                join_table.union(&left)?.union(&right)?
            } else {
                let join_table = probe_side_table.get_columns(common_join_keys)?;
                let left = build_side_table.get_columns(left_side_non_join_columns)?;
                let right = probe_side_table.get_columns(right_side_non_join_columns)?;
                join_table.union(&left)?.union(&right)?
            };
            Ok(Arc::new(MicroPartition::new_loaded(
                final_table.schema.clone(),
                Arc::new(vec![final_table]),
                None,
            )))
        } else {
            panic!("probe can only be used during the ReadyToProbe Phase")
        }
    }

    fn probe_anti_semi(
        &self,
        input: &Arc<MicroPartition>,
        probe_on: &[ExprRef],
        is_semi: bool,
    ) -> DaftResult<Arc<MicroPartition>> {
        if let HashJoinProbeState::ReadyToProbe(probe_table, ..) = self {
            let _growables = info_span!("HashJoinOperator::build_growables").entered();

            let input_tables = input.get_tables()?;

            let mut probe_side_growable =
                GrowableTable::new(&input_tables.iter().collect::<Vec<_>>(), false, 20)?;

            drop(_growables);
            {
                let _loop = info_span!("HashJoinOperator::eval_and_probe").entered();
                for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                    let join_keys = table.eval_expression_list(probe_on)?;
                    let iter = probe_table.probe(&join_keys)?;

                    if is_semi {
                        for (probe_row_idx, inner_iter) in iter {
                            if inner_iter.is_some() {
                                probe_side_growable.extend(
                                    probe_side_table_idx,
                                    probe_row_idx as usize,
                                    1,
                                );
                            }
                        }
                    } else {
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
                }
            }
            let probe_side_table = probe_side_growable.build()?;
            Ok(Arc::new(MicroPartition::new_loaded(
                probe_side_table.schema.clone(),
                Arc::new(vec![probe_side_table]),
                None,
            )))
        } else {
            panic!("probe can only be used during the ReadyToProbe Phase")
        }
    }
}

impl IntermediateOperatorState for HashJoinProbeState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct HashJoinProbeOperator {
    probe_on: Vec<ExprRef>,
    common_join_keys: Option<Vec<String>>,
    left_non_join_columns: Option<Vec<String>>,
    right_non_join_columns: Option<Vec<String>>,
    join_type: JoinType,
    build_on_left: bool,
}

impl HashJoinProbeOperator {
    pub fn new(
        probe_on: Vec<ExprRef>,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        join_type: JoinType,
        build_on_left: bool,
        common_join_keys: IndexSet<String>,
    ) -> Self {
        let (common_join_keys, left_non_join_columns, right_non_join_columns) = match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right => {
                let left_non_join_columns = left_schema
                    .fields
                    .keys()
                    .filter_map(|c| {
                        if !common_join_keys.contains(c) {
                            Some(c.to_string())
                        } else {
                            None
                        }
                    })
                    .collect();
                let right_non_join_columns = right_schema
                    .fields
                    .keys()
                    .filter_map(|c| {
                        if !common_join_keys.contains(c) {
                            Some(c.to_string())
                        } else {
                            None
                        }
                    })
                    .collect();
                (
                    Some(common_join_keys.into_iter().collect()),
                    Some(left_non_join_columns),
                    Some(right_non_join_columns),
                )
            }
            JoinType::Anti | JoinType::Semi => (None, None, None),
            JoinType::Outer => unimplemented!("Outer join is not yet implemented"),
        };
        Self {
            probe_on,
            common_join_keys,
            left_non_join_columns,
            right_non_join_columns,
            join_type,
            build_on_left,
        }
    }
}

impl IntermediateOperator for HashJoinProbeOperator {
    #[instrument(skip_all, name = "HashJoinOperator::execute")]
    fn execute(
        &self,
        idx: usize,
        input: &PipelineResultType,
        state: Option<&mut Box<dyn IntermediateOperatorState>>,
    ) -> DaftResult<IntermediateOperatorResult> {
        match idx {
            0 => {
                let state = state
                    .expect("HashJoinProbeOperator should have state")
                    .as_any_mut()
                    .downcast_mut::<HashJoinProbeState>()
                    .expect("HashJoinProbeOperator state should be HashJoinProbeState");
                let (probe_table, tables) = input.as_probe_table();
                state.set_table(probe_table, tables);
                Ok(IntermediateOperatorResult::NeedMoreInput(None))
            }
            _ => {
                let state = state
                    .expect("HashJoinProbeOperator should have state")
                    .as_any_mut()
                    .downcast_mut::<HashJoinProbeState>()
                    .expect("HashJoinProbeOperator state should be HashJoinProbeState");
                let input = input.as_data();
                let out = match self.join_type {
                    JoinType::Inner => state.probe_inner(
                        input,
                        &self.probe_on,
                        self.common_join_keys
                            .as_ref()
                            .expect("Inner join should have common join keys"),
                        self.left_non_join_columns
                            .as_ref()
                            .expect("Inner join should have left non join columns"),
                        self.right_non_join_columns
                            .as_ref()
                            .expect("Inner join should have right non join columns"),
                        self.build_on_left,
                    ),
                    JoinType::Left | JoinType::Right => state.probe_left_right(
                        input,
                        &self.probe_on,
                        self.common_join_keys
                            .as_ref()
                            .expect("Left/right join should have common join keys"),
                        self.left_non_join_columns
                            .as_ref()
                            .expect("Left/right join should have left non join columns"),
                        self.right_non_join_columns
                            .as_ref()
                            .expect("Left/right join should have right non join columns"),
                        self.join_type == JoinType::Left,
                    ),
                    JoinType::Semi | JoinType::Anti => state.probe_anti_semi(
                        input,
                        &self.probe_on,
                        self.join_type == JoinType::Semi,
                    ),
                    JoinType::Outer => {
                        unimplemented!("Outer join is not yet implemented")
                    }
                }?;
                Ok(IntermediateOperatorResult::NeedMoreInput(Some(out)))
            }
        }
    }

    fn name(&self) -> &'static str {
        "HashJoinProbeOperator"
    }

    fn make_state(&self) -> Option<Box<dyn IntermediateOperatorState>> {
        Some(Box::new(HashJoinProbeState::Building))
    }
}
