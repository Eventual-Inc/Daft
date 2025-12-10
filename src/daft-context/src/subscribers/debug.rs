use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::{NodeID, QueryID, QueryPlan, StatSnapshot};
use daft_micropartition::MicroPartitionRef;
use dashmap::DashMap;

use crate::subscribers::{QueryMetadata, QueryResult, Subscriber};

#[derive(Debug)]
pub struct DebugSubscriber {
    rows_out: DashMap<QueryID, usize>,
}

impl DebugSubscriber {
    pub fn new() -> Self {
        Self {
            rows_out: DashMap::new(),
        }
    }
}

#[async_trait]
impl Subscriber for DebugSubscriber {
    fn on_query_start(&self, query_id: QueryID, metadata: Arc<QueryMetadata>) -> DaftResult<()> {
        eprintln!(
            "Started query `{}` with unoptimized plan:\n{}",
            query_id,
            metadata.unoptimized_plan.as_ref()
        );
        self.rows_out.insert(query_id, 0);
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_query_end(&self, query_id: QueryID, end_result: QueryResult) -> DaftResult<()> {
        eprintln!(
            "Ended query `{}` with result of {} rows",
            query_id,
            self.rows_out
                .get(&query_id)
                .expect("Query not found")
                .value()
        );
        Ok(())
    }

    fn on_result_out(&self, query_id: QueryID, result: MicroPartitionRef) -> DaftResult<()> {
        *self
            .rows_out
            .get_mut(&query_id)
            .expect("Query not found")
            .value_mut() += result.len();
        Ok(())
    }

    fn on_optimization_start(&self, query_id: QueryID) -> DaftResult<()> {
        eprintln!("Started planning query `{}`", query_id);
        Ok(())
    }

    fn on_optimization_end(&self, query_id: QueryID, optimized_plan: QueryPlan) -> DaftResult<()> {
        eprintln!(
            "Finished planning query `{}` with optimized plan:\n{}",
            query_id, optimized_plan
        );
        Ok(())
    }

    fn on_exec_start(&self, query_id: QueryID, physical_plan: QueryPlan) -> DaftResult<()> {
        eprintln!(
            "Started executing query `{}` with physical plan:\n{}",
            query_id, physical_plan
        );
        Ok(())
    }

    async fn on_exec_operator_start(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()> {
        eprintln!(
            "Started executing operator `{}` in query `{}`",
            node_id, query_id
        );
        Ok(())
    }

    async fn on_exec_emit_stats(
        &self,
        query_id: QueryID,
        stats: &[(NodeID, StatSnapshot)],
    ) -> DaftResult<()> {
        eprintln!("Emitting execution stats for query `{}`", query_id);
        for node_id in stats {
            eprintln!("  Node `{}`", node_id.0);
            for (name, stat) in node_id.1.clone() {
                eprintln!("  - {} = {}", name.as_ref(), stat);
            }
        }
        Ok(())
    }

    async fn on_exec_operator_end(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()> {
        eprintln!(
            "Finished executing operator `{}` in query `{}`",
            node_id, query_id
        );
        Ok(())
    }

    async fn on_exec_end(&self, query_id: QueryID) -> DaftResult<()> {
        eprintln!("Finished executing query `{}`", query_id);
        Ok(())
    }
}
