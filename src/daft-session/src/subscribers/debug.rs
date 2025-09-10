use common_error::DaftResult;

use crate::subscribers::{NodeID, QuerySubscriber};

/// Simple Debug Subscriber that prints events to stderr.
/// Note: this is feature gated to `#[cfg(debug_assertions)]` and should only be used for debugging purposes.
#[derive(Debug)]
pub struct DebugSubscriber;

impl QuerySubscriber for DebugSubscriber {
    fn on_query_start(&self) -> DaftResult<()> {
        eprintln!("Query started");
        Ok(())
    }

    fn on_query_end(&self) -> DaftResult<()> {
        eprintln!("Query ended");
        Ok(())
    }

    fn on_plan_start(&self) -> DaftResult<()> {
        eprintln!("Planning started");
        Ok(())
    }

    fn on_plan_end(&self) -> DaftResult<()> {
        eprintln!("Planning ended");
        Ok(())
    }
    
    fn on_exec_start(&self) -> DaftResult<()> {
        eprintln!("Execution started");
        Ok(())
    }

    fn on_exec_operator_start(&self, node_id: NodeID) -> DaftResult<()> {
        eprintln!("Execution for operator started: {}", node_id);
        Ok(())
    }
    
    fn on_exec_emit_stats(&self) -> DaftResult<()> {
        eprintln!("Emitting stats from execution");
        Ok(())
    }

    fn on_exec_operator_end(&self, node_id: NodeID) -> DaftResult<()> {
        eprintln!("Execution for operator ended: {}", node_id);
        Ok(())
    }
    
    fn on_exec_end(&self) -> DaftResult<()> {
        eprintln!("Execution ended");
        Ok(())
    }
}
