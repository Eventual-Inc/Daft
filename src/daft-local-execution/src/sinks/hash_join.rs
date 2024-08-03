use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{
    datatypes::Field,
    schema::{Schema, SchemaRef},
    utils::supertype,
};
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::JoinType;
use futures::{stream, StreamExt};
use tracing::info_span;

use crate::{intermediate_ops::intermediate_op::IntermediateOperator, sources::source::Source};

use super::blocking_sink::{BlockingSink, BlockingSinkStatus};
use daft_table::{
    infer_join_schema_mapper, GrowableTable, JoinOutputMapper, ProbeTable, ProbeTableBuilder, Table,
};

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
    fn finalize(&mut self, join_mapper: &JoinOutputMapper) -> DaftResult<()> {
        if let Self::Building {
            probe_table_builder,
            tables,
            ..
        } = self
        {
            let ptb = std::mem::take(probe_table_builder).expect("should be set in building mode");
            let pt = ptb.build();
            let mapped_tables = tables
                .iter()
                .map(|t| join_mapper.map_left(t))
                .collect::<DaftResult<Vec<_>>>()?;

            *self = Self::Probing {
                probe_table: Arc::new(pt),
                tables: Arc::new(mapped_tables),
            };
            Ok(())
        } else {
            panic!("finalize can only be used during the Building Phase")
        }
    }
}

pub struct HashJoinOperator {
    right_on: Vec<ExprRef>,
    _join_type: JoinType,
    join_mapper: Arc<JoinOutputMapper>,
    join_state: HashJoinState,
}

impl HashJoinOperator {
    pub fn new(
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

        let join_mapper =
            infer_join_schema_mapper(left_schema, right_schema, &left_on, &right_on, join_type)?;

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
        assert_eq!(join_type, JoinType::Inner);
        Ok(Self {
            right_on,
            _join_type: join_type,
            join_mapper: Arc::new(join_mapper),
            join_state: HashJoinState::new(&key_schema, left_on)?,
        })
    }

    pub fn as_sink(&mut self) -> &mut dyn BlockingSink {
        self
    }

    pub fn as_intermediate_op(&self) -> Arc<dyn IntermediateOperator> {
        if let HashJoinState::Probing {
            probe_table,
            tables,
        } = &self.join_state
        {
            Arc::new(HashJoinProber {
                probe_table: probe_table.clone(),
                tables: tables.clone(),
                right_on: self.right_on.clone(),
                join_mapper: self.join_mapper.clone(),
            })
        } else {
            panic!("can't call as_intermediate_op when not in probing state")
        }
    }

    // #[instrument(skip_all, name = "HashJoin::probe-table")]
    // fn sink_left(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
    //     for table in input.get_tables()?.iter() {
    //         self.result_left.push(table.clone());
    //         let join_keys = table.eval_expression_list(&self.left_on)?;

    //         self.probe_table_builder.add_table(&join_keys)?;
    //     }
    //     Ok(SinkResultType::NeedMoreInput)
    // }

    // #[instrument(skip_all, name = "HashJoin::sink")]
    // fn sink_right(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
    //     for table in input.get_tables()?.iter() {
    //         self.result_right.push(table.clone());
    //     }
    //     Ok(SinkResultType::NeedMoreInput)
    // }
}

// impl Sink for HashJoinSink {
//     fn sink(&mut self, index: usize, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
//         match index {
//             0 => self.sink_left(input),
//             1 => self.sink_right(input),
//             _ => panic!("hash join only supports 2 inputs, got {index}")
//         }
//     }

//     fn in_order(&self) -> bool {
//         false
//     }

//     fn num_inputs(&self) -> usize {
//         2
//     }

//     #[instrument(skip_all, name = "HashJoin::finalize")]
//     fn finalize(self: Box<Self>) -> DaftResult<Vec<Arc<MicroPartition>>> {
//         let probe_table = self.probe_table_builder.build();

//         // Left should only be created once per probe table
//         let left_tables = self
//             .result_left
//             .iter()
//             .map(|t| self.join_mapper.map_left(t))
//             .collect::<DaftResult<Vec<_>>>()?;
//         let mut left_growable =
//             GrowableTable::new(&left_tables.iter().collect::<Vec<_>>(), false, 20);
//         // right should only be created morsel
//         let right_tables = self
//             .result_right
//             .iter()
//             .map(|t| self.join_mapper.map_right(t))
//             .collect::<DaftResult<Vec<_>>>()?;

//         let mut right_growable =
//             GrowableTable::new(&right_tables.iter().collect::<Vec<_>>(), false, 20);

//         for (r_table_idx, table) in self.result_right.iter().enumerate() {
//             // we should emit one table at a time when this is streaming
//             let join_keys = table.eval_expression_list(&self.right_on)?;
//             let iter = probe_table.probe(&join_keys)?;

//             for (l_table_idx, l_row_idx, right_idx) in iter {
//                 left_growable.extend(l_table_idx as usize, l_row_idx as usize, 1);
//                 // we can perform run length compression for this to make this more efficient
//                 right_growable.extend(r_table_idx, right_idx as usize, 1);
//             }
//         }
//         let left_table = left_growable.build()?;
//         let right_table = right_growable.build()?;

//         let final_table = left_table.union(&right_table)?;
//         Ok(vec![Arc::new(MicroPartition::new_loaded(
//             final_table.schema.clone(),
//             Arc::new(vec![final_table]),
//             None,
//         ))])
//     }

// }

struct HashJoinProber {
    probe_table: Arc<ProbeTable>,
    tables: Arc<Vec<Table>>,
    right_on: Vec<ExprRef>,
    join_mapper: Arc<JoinOutputMapper>,
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
            GrowableTable::new(&self.tables.iter().collect::<Vec<_>>(), false, 20);
        // right should only be created morsel

        let right_input_tables = input.get_tables()?;

        let right_tables = right_input_tables
            .iter()
            .map(|t| self.join_mapper.map_right(t))
            .collect::<DaftResult<Vec<_>>>()?;

        let mut right_growable =
            GrowableTable::new(&right_tables.iter().collect::<Vec<_>>(), false, 20);

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

        let final_table = left_table.union(&right_table)?;
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
    fn finalize(&mut self) -> DaftResult<()> {
        self.join_state.finalize(&self.join_mapper)?;
        Ok(())
    }
    fn as_source(&mut self) -> &mut dyn Source {
        self
    }
}

impl Source for HashJoinOperator {
    fn get_data(&self) -> crate::sources::source::SourceStream {
        stream::empty().boxed()
    }
}

impl IntermediateOperator for HashJoinOperator {
    fn execute(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        if let HashJoinState::Probing {
            probe_table,
            tables,
        } = &self.join_state
        {
            let _span = info_span!("HashJoinOperator::execute").entered();
            let _growables = info_span!("HashJoinOperator::build_growables").entered();

            // Left should only be created once per probe table
            let mut left_growable =
                GrowableTable::new(&tables.iter().collect::<Vec<_>>(), false, 20);
            // right should only be created morsel

            let right_input_tables = input.get_tables()?;

            let right_tables = right_input_tables
                .iter()
                .map(|t| self.join_mapper.map_right(t))
                .collect::<DaftResult<Vec<_>>>()?;

            let mut right_growable =
                GrowableTable::new(&right_tables.iter().collect::<Vec<_>>(), false, 20);

            drop(_growables);
            {
                let _loop = info_span!("HashJoinOperator::eval_and_probe").entered();
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

            let final_table = left_table.union(&right_table)?;
            Ok(Arc::new(MicroPartition::new_loaded(
                final_table.schema.clone(),
                Arc::new(vec![final_table]),
                None,
            )))
        } else {
            panic!("we should be in probing mode during execution");
        }
    }
    fn name(&self) -> &'static str {
        "HashJoin"
    }
}
