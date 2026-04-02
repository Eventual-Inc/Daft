use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::{QueryID, QueryPlan, Stats};
use daft_micropartition::{MicroPartitionRef, python::PyMicroPartition};
use pyo3::{
    Bound, IntoPyObject, Py, PyAny, PyResult, Python, intern,
    types::{PyAnyMethods, PyModule},
};

use crate::{
    python::{PyQueryMetadata, PyQueryResult},
    subscribers::{
        Event, NodeID, QueryMetadata, QueryResult, Subscriber,
        events::{OperatorEndEvent, OperatorStartEvent, StatsEvent},
    },
};

/// Wrapper around a Python object that implements the Subscriber trait
#[derive(Debug)]
pub struct PySubscriberWrapper(pub(crate) Py<PyAny>);

#[async_trait]
impl Subscriber for PySubscriberWrapper {
    fn on_query_start(&self, query_id: QueryID, metadata: Arc<QueryMetadata>) -> DaftResult<()> {
        Python::attach(|py| {
            self.0.call_method1(
                py,
                intern!(py, "on_query_start"),
                (query_id.to_string(), PyQueryMetadata::from(metadata)),
            )?;
            Ok(())
        })
    }

    #[allow(unused_variables)]
    fn on_query_end(&self, query_id: QueryID, end_result: QueryResult) -> DaftResult<()> {
        Python::attach(|py| {
            self.0.call_method1(
                py,
                intern!(py, "on_query_end"),
                (query_id.to_string(), PyQueryResult::from(end_result)),
            )?;
            Ok(())
        })
    }

    fn on_result_out(&self, query_id: QueryID, result: MicroPartitionRef) -> DaftResult<()> {
        Python::attach(|py| {
            self.0.call_method1(
                py,
                intern!(py, "on_result_out"),
                (query_id.to_string(), PyMicroPartition::from(result)),
            )?;
            Ok(())
        })
    }

    fn on_optimization_start(&self, query_id: QueryID) -> DaftResult<()> {
        Python::attach(|py| {
            self.0.call_method1(
                py,
                intern!(py, "on_optimization_start"),
                (query_id.to_string(),),
            )?;
            Ok(())
        })
    }

    fn on_optimization_end(&self, query_id: QueryID, optimized_plan: QueryPlan) -> DaftResult<()> {
        Python::attach(|py| {
            self.0.call_method1(
                py,
                intern!(py, "on_optimization_end"),
                (query_id.to_string(), optimized_plan.to_string()),
            )?;
            Ok(())
        })
    }

    fn on_exec_start(&self, query_id: QueryID, physical_plan: QueryPlan) -> DaftResult<()> {
        Python::attach(|py| {
            self.0.call_method1(
                py,
                intern!(py, "on_exec_start"),
                (query_id.to_string(), physical_plan.to_string()),
            )?;
            Ok(())
        })
    }

    async fn on_exec_operator_start(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()> {
        Python::attach(|py| {
            self.0.call_method1(
                py,
                intern!(py, "on_exec_operator_start"),
                (query_id.to_string(), node_id),
            )?;
            Ok(())
        })
    }

    async fn on_exec_emit_stats(
        &self,
        query_id: QueryID,
        stats: Arc<Vec<(NodeID, Stats)>>,
    ) -> DaftResult<()> {
        Python::attach(|py| {
            let stats_map = stats
                .iter()
                .map(|(node_id, stats)| {
                    let stat_map = stats
                        .iter()
                        .map(|(name, stat)| {
                            (name.to_string(), stat.clone().into_py_contents(py).unwrap())
                        })
                        .collect::<HashMap<_, _>>();

                    (*node_id, stat_map)
                })
                .collect::<HashMap<_, _>>();
            let py_stats = stats_map.into_pyobject(py)?;

            self.0.call_method1(
                py,
                intern!(py, "on_exec_emit_stats"),
                (query_id.to_string(), py_stats),
            )?;
            Ok(())
        })
    }

    async fn on_exec_operator_end(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()> {
        Python::attach(|py| {
            self.0.call_method1(
                py,
                intern!(py, "on_exec_operator_end"),
                (query_id.to_string(), node_id),
            )?;
            Ok(())
        })
    }

    async fn on_exec_end(&self, query_id: QueryID) -> DaftResult<()> {
        Python::attach(|py| {
            self.0
                .call_method1(py, intern!(py, "on_exec_end"), (query_id.to_string(),))?;
            Ok(())
        })
    }

    async fn on_process_stats(&self, query_id: QueryID, stats: Stats) -> DaftResult<()> {
        Python::attach(|py| {
            let stat_map = stats
                .iter()
                .map(|(name, stat)| (name.to_string(), stat.clone().into_py_contents(py).unwrap()))
                .collect::<HashMap<_, _>>();
            let py_stats = stat_map.into_pyobject(py)?;

            self.0.call_method1(
                py,
                intern!(py, "on_process_stats"),
                (query_id.to_string(), py_stats),
            )?;
            Ok(())
        })
    }

    async fn on_event(&self, event: Event) -> DaftResult<()> {
        Python::attach(|py| {
            let py_event = build_py_event(py, event)?;
            self.0
                .call_method1(py, intern!(py, "on_event"), (py_event,))?;
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

fn build_operator_started(py: Python<'_>, event: Arc<OperatorStartEvent>) -> PyResult<Py<PyAny>> {
    event_class(py, "OperatorStarted")?
        .call1((
            event.header.query_id.to_string(),
            event.operator.node_id,
            event.operator.name.as_ref(),
        ))
        .map(Into::into)
}

fn build_operator_finished(py: Python<'_>, event: Arc<OperatorEndEvent>) -> PyResult<Py<PyAny>> {
    event_class(py, "OperatorFinished")?
        .call1((
            event.header.query_id.to_string(),
            event.operator.node_id,
            event.operator.name.as_ref(),
        ))
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

fn build_stats(py: Python<'_>, event: Arc<StatsEvent>) -> PyResult<Py<PyAny>> {
    let py_stats = build_py_stats(py, event.stats.as_ref())?;
    event_class(py, "Stats")?
        .call1((event.header.query_id.to_string(), py_stats))
        .map(Into::into)
}

fn build_py_event(py: Python<'_>, event: Event) -> PyResult<Py<PyAny>> {
    match event {
        Event::OperatorStart(event) => build_operator_started(py, event),
        Event::OperatorEnd(event) => build_operator_finished(py, event),
        Event::Stats(event) => build_stats(py, event),
    }
}
