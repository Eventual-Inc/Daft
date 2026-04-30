use std::{
    collections::HashMap,
    fmt,
    fs::File,
    io,
    io::{BufRead, BufReader},
    path::Path,
    sync::Arc,
    time::Duration,
};

use axum::http::StatusCode;
use daft_common::metrics::Stat;

use crate::{
    engine::{
        ExecEmitStatsArgsRecv, ExecEndArgs, ExecStartArgs, FinalizeArgs, PlanEndArgs,
        PlanStartArgs, StartQueryArgs, apply_emit_stats, apply_exec_end, apply_exec_start,
        apply_operator_end, apply_operator_start, apply_plan_end, apply_plan_start,
        apply_query_end, apply_query_start,
    },
    events::{Event, MetricValue},
    state::DashboardState,
};

#[derive(Debug)]
pub(crate) enum EventLogError {
    Io(io::Error),
    Json(serde_json::Error),
    ImportStatus { status: StatusCode },
}

impl fmt::Display for EventLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error while importing event log: {err}"),
            Self::Json(err) => write!(f, "JSON parse error while importing event log: {err}"),
            Self::ImportStatus { status } => {
                write!(f, "event import returned non-success status: {status}")
            }
        }
    }
}

impl std::error::Error for EventLogError {}

impl From<io::Error> for EventLogError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<serde_json::Error> for EventLogError {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

fn metric_value_to_stat(value: &MetricValue) -> Stat {
    match value {
        MetricValue::Count(v) => Stat::Count(*v),
        MetricValue::Bytes(v) => Stat::Bytes(*v),
        MetricValue::Percent(v) => Stat::Percent(*v),
        MetricValue::Float(v) => Stat::Float(*v),
        MetricValue::DurationMicros(v) => Stat::Duration(Duration::from_micros(*v)),
    }
}

fn metrics_to_stats(metrics: &HashMap<String, MetricValue>) -> HashMap<String, Stat> {
    metrics
        .iter()
        .map(|(name, value)| (name.clone(), metric_value_to_stat(value)))
        .collect()
}

pub(crate) fn import_event_log(
    path: impl AsRef<Path>,
    state: Arc<DashboardState>,
) -> Result<(), EventLogError> {
    let path = path.as_ref();
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let source = path.display().to_string();
    import_events(reader, &source, state.as_ref())?;
    eprintln!("✅ Success! event log imported from {}", path.display());
    Ok(())
}

fn import_events<R: BufRead>(
    reader: R,
    source: &str,
    state: &DashboardState,
) -> Result<(), EventLogError> {
    for (line_num, line) in reader.lines().enumerate() {
        let json_str = line?;
        if json_str.trim().is_empty() {
            continue;
        }

        let event = match serde_json::from_str::<Event>(&json_str) {
            Ok(event) => event,
            Err(err) => {
                tracing::warn!(
                    source = %source,
                    line = line_num + 1,
                    error = %err,
                    json = %json_str,
                    "failed to parse event"
                );
                continue;
            }
        };

        if let Err(err) = import_event(&event, state) {
            tracing::warn!(
                source = %source,
                line = line_num + 1,
                error = %err,
                json = %json_str,
                "failed to import event"
            );
        }
    }

    Ok(())
}

fn import_event(event: &Event, state: &DashboardState) -> Result<(), EventLogError> {
    let status = match event {
        Event::QueryStarted(e) => apply_query_start(
            state,
            e.query_id.clone().into(),
            StartQueryArgs {
                start_sec: e.timestamp,
                unoptimized_plan: e.plan.clone().into(),
                runner: e.runner.clone(),
                ray_dashboard_url: e.dashboard_url.clone(),
                entrypoint: e.entrypoint.clone(),
                daft_version: e.daft_version.clone(),
                ray_version: e.runner_version.clone(),
                python_version: e.python_version.clone(),
            },
        ),
        Event::OptimizationStarted(e) => apply_plan_start(
            state,
            e.query_id.clone().into(),
            PlanStartArgs {
                plan_start_sec: e.timestamp,
            },
        ),
        Event::OptimizationEnded(e) => apply_plan_end(
            state,
            e.query_id.clone().into(),
            PlanEndArgs {
                plan_end_sec: e.timestamp,
                optimized_plan: e.plan.clone().into(),
            },
        ),
        Event::ExecutionStarted(e) => apply_exec_start(
            state,
            e.query_id.clone().into(),
            ExecStartArgs {
                exec_start_sec: e.timestamp,
                physical_plan: e.physical_plan.clone().into(),
            },
        ),
        Event::OperatorStarted(e) => {
            apply_operator_start(state, e.query_id.clone().into(), e.node_id, e.timestamp)
        }
        Event::OperatorEnded(e) => {
            apply_operator_end(state, e.query_id.clone().into(), e.node_id, e.timestamp)
        }
        Event::Stats(e) => apply_emit_stats(
            state,
            e.query_id.clone().into(),
            ExecEmitStatsArgsRecv {
                source_id: e.query_id.clone(),
                stats: vec![(e.node_id, metrics_to_stats(&e.metrics))],
            },
        ),
        Event::ExecutionEnded(e) => apply_exec_end(
            state,
            e.query_id.clone().into(),
            ExecEndArgs {
                exec_end_sec: e.timestamp,
            },
        ),
        Event::QueryEnded(e) => apply_query_end(
            state,
            e.query_id.clone().into(),
            FinalizeArgs {
                end_sec: e.timestamp,
                end_state: e.state.clone(),
                error_message: e.error_message.clone(),
                results: None,
            },
        ),
        // TODO handle results from the event log.
        // We don't write data out in the event log
        // so at this point it does not make sense to send
        // this event to dashboard.
        Event::ResultProduced(_e) => return Ok(()),
        // TODO add support in  dashboard for process stats
        Event::ProcessStats(_e) => return Ok(()),
        Event::EventLogStarted(_e) => return Ok(()),
    };

    if status.is_success() {
        Ok(())
    } else {
        Err(EventLogError::ImportStatus { status })
    }
}
