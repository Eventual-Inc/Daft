use std::sync::Arc;

use common_error::DaftResult;
use common_scan_info::ScanState;
use common_treenode::{Transformed, TreeNode};

use super::OptimizerRule;
use crate::{ops::Source, LogicalPlan, SourceInfo};

#[derive(Default, Debug)]
pub struct ShardScans {}

impl ShardScans {
    pub fn new() -> Self {
        Self {}
    }
}

// Fold sharding information into scan pushdowns for all physical scans.
impl OptimizerRule for ShardScans {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_up(Self::try_optimize_node)
    }
}

impl ShardScans {
    #[allow(clippy::only_used_in_recursion)]
    fn try_optimize_node(plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match &*plan {
            LogicalPlan::Source(source) => match source.source_info.as_ref() {
                SourceInfo::Physical(physical_scan_info) => match &physical_scan_info.scan_state {
                    ScanState::Tasks(scan_tasks) => match &physical_scan_info.pushdowns.sharder {
                        Some(sharder) => {
                            let new_scan_tasks = sharder.shard_scan_tasks(scan_tasks)?;
                            let new_scan_state = ScanState::Tasks(Arc::new(new_scan_tasks));
                            let new_physical_scan_info =
                                physical_scan_info.with_scan_state(new_scan_state);
                            let new_source = Source::new(
                                source.output_schema.clone(),
                                SourceInfo::Physical(new_physical_scan_info).into(),
                            );
                            Ok(Transformed::yes(LogicalPlan::Source(new_source).into()))
                        }
                        None => Ok(Transformed::no(plan)),
                    },
                    ScanState::Operator(_) => Ok(Transformed::no(plan)),
                },
                _ => Ok(Transformed::no(plan)),
            },
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use common_scan_info::{
        test::DummyScanOperator, Pushdowns, ScanOperator, Sharder, ShardingStrategy,
    };
    use daft_schema::{dtype::DataType, field::Field, schema::Schema};

    use super::*;

    #[test]
    fn test_shard_scan_tasks() -> DaftResult<()> {
        let world_size = 10;
        let num_scan_tasks = 100;
        let scan_operator = Arc::new(DummyScanOperator {
            schema: Arc::new(Schema::new(vec![Field::new(
                "dummy_field",
                DataType::Int32,
            )])),
            num_scan_tasks,
            num_rows_per_task: Some(1000),
        });

        let scan_tasks = scan_operator.to_scan_tasks(Pushdowns::default())?;

        let mut file_set = HashSet::new();
        for i in 0..world_size {
            let sharder = Sharder::new(ShardingStrategy::File, world_size, i);
            let filtered = sharder.shard_scan_tasks(&scan_tasks)?;
            for task in filtered {
                for file in task.get_file_paths() {
                    assert!(
                        file_set.insert(file.clone()),
                        "Each shard should have unique files"
                    );
                }
            }
        }
        assert_eq!(
            file_set.len(),
            num_scan_tasks as usize,
            "All files should be assigned to a shard"
        );

        Ok(())
    }
}
