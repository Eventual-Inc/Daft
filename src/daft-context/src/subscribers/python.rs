use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_metrics::{NodeID, Stats};
use pyo3::{
    Bound, IntoPyObject, Py, PyAny, PyResult, Python, intern,
    types::{PyAnyMethods, PyModule},
};

use crate::{
    python::{PyQueryMetadata, PyQueryResult},
    subscribers::{
        Event, Subscriber,
        events::{
            ExecEndEvent, ExecStartEvent, OperatorEndEvent, OperatorStartEvent,
            OptimizationCompleteEvent, OptimizationStartEvent, ProcessStatsEvent, QueryEndEvent,
            QueryHeartbeatEvent, QueryStartEvent, ResultOutEvent, StatsEvent,
        },
    },
};

/// Wrapper around a Python object that implements the Subscriber trait
#[derive(Debug)]
pub struct PySubscriberWrapper(pub(crate) Py<PyAny>);

impl Subscriber for PySubscriberWrapper {
    fn on_event(&self, event: Event) -> DaftResult<()> {
        Python::attach(|py| {
            if let Some(py_event) = build_py_event(py, event)? {
                self.0
                    .call_method1(py, intern!(py, "on_event"), (py_event,))?;
            }
            Ok(())
        })
    }
}

// Build Python subscriber event objects from Rust execution events.

fn events_module(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    py.import("daft.subscribers.events")
}

fn event_class<'py>(py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
    events_module(py)?.getattr(name)
}

fn build_operator_started(py: Python<'_>, event: &OperatorStartEvent) -> PyResult<Py<PyAny>> {
    event_class(py, "OperatorStarted")?
        .call1((
            event.header.query_id.to_string(),
            event.operator.node_id,
            event.operator.name.as_ref(),
            event.operator.origin_node_id,
        ))
        .map(Into::into)
}

fn build_operator_finished(py: Python<'_>, event: &OperatorEndEvent) -> PyResult<Py<PyAny>> {
    event_class(py, "OperatorFinished")?
        .call1((
            event.header.query_id.to_string(),
            event.operator.node_id,
            event.operator.name.as_ref(),
            event.operator.origin_node_id,
        ))
        .map(Into::into)
}

fn build_query_started(py: Python<'_>, event: &QueryStartEvent) -> PyResult<Py<PyAny>> {
    event_class(py, "QueryStarted")?
        .call1((
            event.header.query_id.to_string(),
            PyQueryMetadata::from(event.metadata.clone()),
        ))
        .map(Into::into)
}

fn build_query_heartbeat(py: Python<'_>, event: &QueryHeartbeatEvent) -> PyResult<Py<PyAny>> {
    event_class(py, "QueryHeartbeat")?
        .call1((event.header.query_id.to_string(),))
        .map(Into::into)
}

fn build_query_finished(py: Python<'_>, event: &QueryEndEvent) -> PyResult<Py<PyAny>> {
    event_class(py, "QueryFinished")?
        .call1((
            event.header.query_id.to_string(),
            PyQueryResult::from(Arc::new(event.result.clone())),
            event.duration_ms,
        ))
        .map(Into::into)
}

fn build_optimization_completed(
    py: Python<'_>,
    event: &OptimizationCompleteEvent,
) -> PyResult<Py<PyAny>> {
    event_class(py, "OptimizationCompleted")?
        .call1((
            event.header.query_id.to_string(),
            event.optimized_plan.to_string(),
        ))
        .map(Into::into)
}

fn build_optimization_started(
    py: Python<'_>,
    event: &OptimizationStartEvent,
) -> PyResult<Py<PyAny>> {
    event_class(py, "OptimizationStarted")?
        .call1((event.header.query_id.to_string(),))
        .map(Into::into)
}

fn build_execution_started(py: Python<'_>, event: &ExecStartEvent) -> PyResult<Py<PyAny>> {
    event_class(py, "ExecutionStarted")?
        .call1((
            event.header.query_id.to_string(),
            event.physical_plan.to_string(),
        ))
        .map(Into::into)
}

fn build_execution_finished(py: Python<'_>, event: &ExecEndEvent) -> PyResult<Py<PyAny>> {
    event_class(py, "ExecutionFinished")?
        .call1((event.header.query_id.to_string(), event.duration_ms))
        .map(Into::into)
}

fn build_py_stats<'py>(py: Python<'py>, stats: &[(NodeID, Stats)]) -> PyResult<Bound<'py, PyAny>> {
    let stats_map = stats
        .iter()
        .map(|(node_id, stats)| {
            let stat_map = stats
                .iter()
                .map(|(name, stat)| Ok((name.to_string(), stat.clone().into_py_contents(py)?)))
                .collect::<PyResult<HashMap<_, _>>>()?;
            Ok((*node_id, stat_map))
        })
        .collect::<PyResult<HashMap<_, _>>>()?;

    stats_map.into_pyobject(py).map(|obj| obj.into_any())
}

fn build_stats(py: Python<'_>, event: &StatsEvent) -> PyResult<Py<PyAny>> {
    let py_stats = build_py_stats(py, event.stats.as_ref())?;
    event_class(py, "Stats")?
        .call1((event.header.query_id.to_string(), py_stats))
        .map(Into::into)
}

fn build_process_stats(py: Python<'_>, event: &ProcessStatsEvent) -> PyResult<Py<PyAny>> {
    let py_stats = event
        .stats
        .iter()
        .map(|(name, stat)| Ok((name.to_string(), stat.clone().into_py_contents(py)?)))
        .collect::<PyResult<HashMap<_, _>>>()?;

    event_class(py, "ProcessStats")?
        .call1((event.header.query_id.to_string(), py_stats))
        .map(Into::into)
}

fn build_result_produced(py: Python<'_>, event: &ResultOutEvent) -> PyResult<Py<PyAny>> {
    event_class(py, "ResultProduced")?
        .call1((event.header.query_id.to_string(), event.num_rows))
        .map(Into::into)
}

fn build_py_event(py: Python<'_>, event: Event) -> PyResult<Option<Py<PyAny>>> {
    match event {
        Event::QueryStart(event) => build_query_started(py, &event).map(Some),
        Event::QueryHeartbeat(event) => build_query_heartbeat(py, &event).map(Some),
        Event::QueryEnd(event) => build_query_finished(py, &event).map(Some),
        Event::OptimizationStart(event) => build_optimization_started(py, &event).map(Some),
        Event::OptimizationComplete(event) => build_optimization_completed(py, &event).map(Some),
        Event::ExecStart(event) => build_execution_started(py, &event).map(Some),
        // No Python-side event type for the aggregated distributed physical plan —
        // it flows directly to the dashboard subscriber.
        Event::ExecDistributedPhysicalPlan(_event) => Ok(None),
        Event::ExecEnd(event) => build_execution_finished(py, &event).map(Some),
        Event::OperatorStart(event) => build_operator_started(py, &event).map(Some),
        Event::OperatorEnd(event) => build_operator_finished(py, &event).map(Some),
        Event::Stats(event) => build_stats(py, &event).map(Some),
        Event::ProcessStats(event) => build_process_stats(py, &event).map(Some),
        Event::ResultOut(event) => build_result_produced(py, &event).map(Some),
        Event::TaskSubmit(_event) => Ok(None),
        Event::TaskEnd(_event) => Ok(None),
    }
}
