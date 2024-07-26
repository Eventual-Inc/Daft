use std::sync::Arc;

use common_error::DaftResult;
use daft_core::schema::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::JoinType;
use tracing::instrument;

use super::sink::{DoubleInputSink, SinkResultType};
use daft_table::ProbeTableBuilder;

struct ProbeTable {
    result_left: Vec<Arc<MicroPartition>>,
}

pub struct HashJoinSink {
    probe_table_builder: ProbeTableBuilder,
    result_right: Vec<Arc<MicroPartition>>,
    left_on: Vec<ExprRef>,
    right_on: Vec<ExprRef>,
    join_type: JoinType,
}

impl HashJoinSink {
    pub fn new(
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        join_type: JoinType,
        key_schema: SchemaRef,
    ) -> Self {
        Self {
            probe_table_builder: ProbeTableBuilder::new(key_schema).unwrap(),
            result_right: Vec::new(),
            left_on,
            right_on,
            join_type,
        }
    }
}

impl DoubleInputSink for HashJoinSink {
    #[instrument(skip_all, name = "HashJoin::probe-table")]
    fn sink_left(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        for table in input.get_tables()?.iter() {
            let join_keys = table.eval_expression_list(&self.left_on)?;
            self.probe_table_builder.add_table(&join_keys)?;
        }
        Ok(SinkResultType::NeedMoreInput)
    }

    #[instrument(skip_all, name = "HashJoin::sink")]
    fn sink_right(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        self.result_right.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    fn in_order(&self) -> bool {
        false
    }

    #[instrument(skip_all, name = "HashJoin::finalize")]
    fn finalize(self: Box<Self>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let probe_table = self.probe_table_builder.build();
        for mp in self.result_right.iter() {
            for table in mp.get_tables()?.iter() {
                let join_keys = table.eval_expression_list(&self.right_on)?;
                let iter = probe_table.probe(&join_keys)?;
                for (l_table_idx, l_row_idx, right_idx) in iter {
                    println!("{l_table_idx} {l_row_idx} {right_idx}");
                }
            }
        }

        todo!("finalize");

        // let concated_left = MicroPartition::concat(
        //     &self
        //         .result_left
        //         .iter()
        //         .map(|x| x.as_ref())
        //         .collect::<Vec<_>>(),
        // )?;
        // let concated_right = MicroPartition::concat(
        //     &self
        //         .result_right
        //         .iter()
        //         .map(|x| x.as_ref())
        //         .collect::<Vec<_>>(),
        // )?;
        // let joined = concated_left.hash_join(
        //     &concated_right,
        //     &self.left_on,
        //     &self.right_on,
        //     self.join_type,
        // )?;
        // Ok(vec![Arc::new(joined)])
    }

    fn name(&self) -> &'static str {
        "HashJoin"
    }
}
