use common_error::DaftResult;
use common_metrics::{QueryID, Stat};
use dashmap::DashMap;

use crate::subscribers::{
    Event, Subscriber,
    events::{
        ExecEndEvent, ExecStartEvent, OperatorEndEvent, OperatorStartEvent,
        OptimizationCompleteEvent, OptimizationStartEvent, ProcessStatsEvent, QueryEndEvent,
        QueryStartEvent, ResultOutEvent, StatsEvent, TaskEndEvent, TaskSubmitEvent,
    },
};

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

    fn handle_query_start(&self, event: &QueryStartEvent) -> DaftResult<()> {
        eprintln!(
            "query_start query_id={} runner={} unoptimized_plan=\n{}",
            event.header.query_id,
            event.metadata.runner,
            event.metadata.unoptimized_plan.as_ref()
        );
        self.rows_out.insert(event.header.query_id.clone(), 0);
        Ok(())
    }

    fn handle_query_end(&self, event: &QueryEndEvent) -> DaftResult<()> {
        let rows_out = self
            .rows_out
            .get(&event.header.query_id)
            .map(|rows| *rows.value())
            .unwrap_or(0);

        match event.duration_ms {
            Some(duration_ms) => eprintln!(
                "query_end query_id={} end_state={:?} rows_out={} duration_ms={duration_ms}",
                event.header.query_id, event.result.end_state, rows_out,
            ),
            None => eprintln!(
                "query_end query_id={} end_state={:?} rows_out={}",
                event.header.query_id, event.result.end_state, rows_out,
            ),
        }

        if let Some(error_message) = event.result.error_message.as_deref() {
            eprintln!(
                "query_end_error query_id={} error=\"{}\"",
                event.header.query_id, error_message
            );
        }

        Ok(())
    }

    fn handle_optimization_start(&self, event: &OptimizationStartEvent) -> DaftResult<()> {
        eprintln!("optimization_start query_id={}\n", event.header.query_id);
        Ok(())
    }

    fn handle_optimization_complete(&self, event: &OptimizationCompleteEvent) -> DaftResult<()> {
        eprintln!(
            "optimization_complete query_id={} optimized_plan=\n{}",
            event.header.query_id, event.optimized_plan
        );
        Ok(())
    }

    fn handle_exec_start(&self, event: &ExecStartEvent) -> DaftResult<()> {
        eprintln!(
            "exec_start query_id={} physical_plan=\n{}",
            event.header.query_id, event.physical_plan
        );
        Ok(())
    }

    fn handle_exec_end(&self, event: &ExecEndEvent) -> DaftResult<()> {
        match event.duration_ms {
            Some(duration_ms) => eprintln!(
                "exec_end query_id={} duration_ms={duration_ms}",
                event.header.query_id
            ),
            None => eprintln!("exec_end query_id={}", event.header.query_id),
        }
        Ok(())
    }

    fn handle_operator_start(&self, event: &OperatorStartEvent) -> DaftResult<()> {
        if let Some(origin_node_id) = event.operator.origin_node_id {
            eprintln!(
                "operator_start query_id={} node_id={} origin_node_id={} name=\"{}\" type={:?} category={:?}",
                event.header.query_id,
                event.operator.node_id,
                origin_node_id,
                event.operator.name,
                event.operator.node_type,
                event.operator.node_category,
            );
        } else {
            eprintln!(
                "operator_start query_id={} node_id={} name=\"{}\" type={:?} category={:?}",
                event.header.query_id,
                event.operator.node_id,
                event.operator.name,
                event.operator.node_type,
                event.operator.node_category,
            );
        }
        Ok(())
    }

    fn handle_operator_end(&self, event: &OperatorEndEvent) -> DaftResult<()> {
        if let Some(origin_node_id) = event.operator.origin_node_id {
            eprintln!(
                "operator_end query_id={} node_id={} origin_node_id={} name=\"{}\"",
                event.header.query_id, event.operator.node_id, origin_node_id, event.operator.name,
            );
        } else {
            eprintln!(
                "operator_end query_id={} node_id={} name=\"{}\"",
                event.header.query_id, event.operator.node_id, event.operator.name,
            );
        }
        Ok(())
    }

    fn handle_stats(&self, event: &StatsEvent) -> DaftResult<()> {
        for (node_id, stats) in event.stats.iter() {
            let rendered = stats
                .0
                .iter()
                .map(|(key, stat)| format!("{key}={}", render_stat(stat)))
                .collect::<Vec<_>>()
                .join(" ");

            eprintln!(
                "stats query_id={} node_id={} {}",
                event.header.query_id, node_id, rendered,
            );
        }
        Ok(())
    }

    fn handle_process_stats(&self, event: &ProcessStatsEvent) -> DaftResult<()> {
        let rendered = event
            .stats
            .0
            .iter()
            .map(|(key, stat)| format!("{key}={}", render_stat(stat)))
            .collect::<Vec<_>>()
            .join(" ");

        eprintln!(
            "process_stats query_id={} {}",
            event.header.query_id, rendered,
        );
        Ok(())
    }

    fn handle_result_out(&self, event: &ResultOutEvent) -> DaftResult<()> {
        if let Some(mut rows_out) = self.rows_out.get_mut(&event.header.query_id) {
            *rows_out.value_mut() += event.num_rows as usize;
        }
        eprintln!(
            "result_out query_id={} num_rows={}",
            event.header.query_id, event.num_rows
        );
        Ok(())
    }

    fn handle_task_submit(&self, event: &TaskSubmitEvent) -> DaftResult<()> {
        eprintln!(
            "task_submit query_id={} task_id={} node_ids={:?}",
            event.header.query_id, event.task.id, event.task.node_ids
        );
        Ok(())
    }

    fn handle_task_end(&self, event: &TaskEndEvent) -> DaftResult<()> {
        let rendered_stats = event
            .stats
            .iter()
            .map(|(node_info, snapshot)| {
                if let Some(origin_node_id) = node_info.node_origin_id {
                    format!(
                        "node_id={} origin_node_id={} stats={snapshot:?}",
                        node_info.id, origin_node_id
                    )
                } else {
                    format!("node_id={} stats={snapshot:?}", node_info.id)
                }
            })
            .collect::<Vec<_>>()
            .join(" ");

        match (&event.worker_id, &event.outcome, rendered_stats.is_empty()) {
            (Some(worker_id), outcome, true) => eprintln!(
                "task_end query_id={} task_id={} worker_id={} outcome={outcome:?} node_ids={:?}",
                event.header.query_id, event.task.id, worker_id, event.task.node_ids
            ),
            (None, outcome, true) => eprintln!(
                "task_end query_id={} task_id={} outcome={outcome:?} node_ids={:?}",
                event.header.query_id, event.task.id, event.task.node_ids
            ),
            (Some(worker_id), outcome, false) => eprintln!(
                "task_end query_id={} task_id={} worker_id={} outcome={outcome:?} node_ids={:?} {}",
                event.header.query_id,
                event.task.id,
                worker_id,
                event.task.node_ids,
                rendered_stats
            ),
            (None, outcome, false) => eprintln!(
                "task_end query_id={} task_id={} outcome={outcome:?} node_ids={:?} {}",
                event.header.query_id, event.task.id, event.task.node_ids, rendered_stats
            ),
        }
        Ok(())
    }
}

impl Subscriber for DebugSubscriber {
    fn on_event(&self, event: Event) -> DaftResult<()> {
        match event {
            Event::QueryStart(e) => self.handle_query_start(&e)?,
            // ignore heartbeats for debug, too verbose
            Event::QueryHeartbeat(_) => (),
            Event::QueryEnd(e) => self.handle_query_end(&e)?,
            Event::OptimizationStart(e) => self.handle_optimization_start(&e)?,
            Event::OptimizationComplete(e) => self.handle_optimization_complete(&e)?,
            Event::ExecStart(e) => self.handle_exec_start(&e)?,
            // Not logged by debug subscriber — too verbose. The dashboard is
            // the intended consumer.
            Event::ExecDistributedPhysicalPlan(_) => (),
            Event::ExecEnd(e) => self.handle_exec_end(&e)?,
            Event::OperatorStart(e) => self.handle_operator_start(&e)?,
            Event::OperatorEnd(e) => self.handle_operator_end(&e)?,
            Event::Stats(e) => self.handle_stats(&e)?,
            Event::ProcessStats(e) => self.handle_process_stats(&e)?,
            Event::ResultOut(e) => self.handle_result_out(&e)?,
            Event::TaskSubmit(e) => self.handle_task_submit(&e)?,
            Event::TaskEnd(e) => self.handle_task_end(&e)?,
        }
        Ok(())
    }
}

fn render_stat(stat: &Stat) -> String {
    match stat {
        Stat::Count(v) => v.to_string(),
        Stat::Bytes(v) => v.to_string(),
        Stat::Duration(v) => format!("{v:?}"),
        Stat::Percent(v) => format!("{v}%"),
        Stat::Float(v) => v.to_string(),
    }
}
