use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::{StatSnapshotView, ops::NodeInfo};
use daft_micropartition::MicroPartitionRef;

#[derive(Debug)]
pub struct DebugSubscriber;

use crate::subscribers::{NodeID, QuerySubscriber};

impl QuerySubscriber for DebugSubscriber {
    fn on_query_start(&self, query_id: String, unoptimized_plan: String) -> DaftResult<()> {
        eprintln!(
            "Started query `{}` with unoptimized plan:\n{}",
            query_id, unoptimized_plan
        );
        Ok(())
    }

    fn on_query_end(&self, query_id: String, results: Vec<MicroPartitionRef>) -> DaftResult<()> {
        eprintln!(
            "Ended query `{}` with result of {} rows",
            query_id,
            results.iter().map(|part| part.len()).sum::<usize>()
        );
        Ok(())
    }

    fn on_plan_start(&self, query_id: String) -> DaftResult<()> {
        eprintln!("Started planning query `{}`", query_id);
        Ok(())
    }

    fn on_plan_end(&self, query_id: String, optimized_plan: String) -> DaftResult<()> {
        eprintln!(
            "Finished planning query `{}` with optimized plan:\n{}",
            query_id, optimized_plan
        );
        Ok(())
    }

    fn on_exec_start(&self, query_id: String, node_infos: &[Arc<NodeInfo>]) -> DaftResult<()> {
        eprintln!("Started executing query `{}`", query_id);
        for node_info in node_infos {
            eprintln!("  - Node {}: {}", node_info.id, node_info.name);
        }
        Ok(())
    }

    fn on_exec_operator_start(&self, query_id: String, node_id: NodeID) -> DaftResult<()> {
        eprintln!(
            "Started executing operator `{}` in query `{}`",
            node_id, query_id
        );
        Ok(())
    }

    fn on_exec_emit_stats(
        &self,
        query_id: String,
        stats: &[(NodeID, StatSnapshotView)],
    ) -> DaftResult<()> {
        eprintln!("Emitting execution stats for query `{}`", query_id);
        for node_id in stats {
            eprintln!("  Node `{}`", node_id.0);
            for (name, stat) in node_id.1.clone() {
                eprintln!("  - {} = {}", name, stat);
            }
        }
        Ok(())
    }

    fn on_exec_operator_end(&self, query_id: String, node_id: NodeID) -> DaftResult<()> {
        eprintln!(
            "Finished executing operator `{}` in query `{}`",
            node_id, query_id
        );
        Ok(())
    }

    fn on_exec_end(&self, query_id: String) -> DaftResult<()> {
        eprintln!("Finished executing query `{}`", query_id);
        Ok(())
    }
}
