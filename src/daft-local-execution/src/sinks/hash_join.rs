use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{
    join,
    schema::{Schema, SchemaRef},
};
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::JoinType;
use tracing::instrument;

use super::sink::{DoubleInputSink, SinkResultType};
use daft_table::{
    infer_join_schema_mapper, GrowableTable, JoinOutputMapper, ProbeTableBuilder, Table,
};

pub struct HashJoinSink {
    probe_table_builder: ProbeTableBuilder,
    result_left: Vec<Table>,
    result_right: Vec<Table>,
    left_on: Vec<ExprRef>,
    right_on: Vec<ExprRef>,
    join_type: JoinType,
    join_mapper: JoinOutputMapper,
}

impl HashJoinSink {
    pub fn new(
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        join_type: JoinType,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
    ) -> DaftResult<Self> {
        let key_schema = Arc::new(Schema::new(
            left_on
                .iter()
                .map(|e| e.to_field(&left_schema))
                .collect::<DaftResult<Vec<_>>>()?,
        )?);
        let join_mapper =
            infer_join_schema_mapper(&left_schema, &right_schema, &left_on, &right_on, join_type)?;

        Ok(Self {
            probe_table_builder: ProbeTableBuilder::new(key_schema).unwrap(),
            result_left: Vec::new(),
            result_right: Vec::new(),
            left_on,
            right_on,
            join_type,
            join_mapper,
        })
    }
}

impl DoubleInputSink for HashJoinSink {
    #[instrument(skip_all, name = "HashJoin::probe-table")]
    fn sink_left(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        for table in input.get_tables()?.iter() {
            self.result_left.push(table.clone());
            let join_keys = table.eval_expression_list(&self.left_on)?;
            self.probe_table_builder.add_table(&join_keys)?;
        }
        Ok(SinkResultType::NeedMoreInput)
    }

    #[instrument(skip_all, name = "HashJoin::sink")]
    fn sink_right(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        for table in input.get_tables()?.iter() {
            self.result_right.push(table.clone());
        }
        Ok(SinkResultType::NeedMoreInput)
    }

    fn in_order(&self) -> bool {
        false
    }

    #[instrument(skip_all, name = "HashJoin::finalize")]
    fn finalize(self: Box<Self>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let probe_table = self.probe_table_builder.build();

        // Left should only be created once per probe table
        let left_tables = self
            .result_left
            .iter()
            .map(|t| self.join_mapper.map_left(t))
            .collect::<DaftResult<Vec<_>>>()?;
        let mut left_growable =
            GrowableTable::new(&left_tables.iter().collect::<Vec<_>>(), false, 20);
        // right should only be created morsel
        let right_tables = self
            .result_right
            .iter()
            .map(|t| self.join_mapper.map_right(t))
            .collect::<DaftResult<Vec<_>>>()?;

        let mut right_growable =
            GrowableTable::new(&right_tables.iter().collect::<Vec<_>>(), false, 20);

        for (r_table_idx, table) in self.result_right.iter().enumerate() {
            // we should emit one table at a time when this is streaming
            let join_keys = table.eval_expression_list(&self.right_on)?;
            let iter = probe_table.probe(&join_keys)?;

            for (l_table_idx, l_row_idx, right_idx) in iter {
                left_growable.extend(l_table_idx as usize, l_row_idx as usize, 1);
                // we can perform run length compression for this to make this more efficient
                right_growable.extend(r_table_idx, right_idx as usize, 1);
            }
        }
        let left_table = left_growable.build()?;
        let right_table = right_growable.build()?;

        let final_table = left_table.union(&right_table)?;
        Ok(vec![Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        ))])
        // Ok(vec![Arc::new(joined)])
    }

    fn name(&self) -> &'static str {
        "HashJoin"
    }
}
