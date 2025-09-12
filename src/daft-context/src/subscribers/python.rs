use std::collections::HashMap;

use common_error::DaftResult;
use common_metrics::StatSnapshotView;
use pyo3::{IntoPyObject, PyObject, Python, intern};

use crate::subscribers::{NodeID, QuerySubscriber};

/// Wrapper around a Python object that implements the QuerySubscriber trait
#[derive(Debug)]
pub struct PyQuerySubscriberWrapper(pub(crate) PyObject);

impl QuerySubscriber for PyQuerySubscriberWrapper {
    fn on_query_start(&self, query_id: String) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0
                .call_method1(py, intern!(py, "on_query_start"), (query_id,))?;
            Ok(())
        })
    }

    fn on_query_end(&self, query_id: String) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0
                .call_method1(py, intern!(py, "on_query_end"), (query_id,))?;
            Ok(())
        })
    }

    fn on_plan_start(&self, query_id: String) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0
                .call_method1(py, intern!(py, "on_plan_start"), (query_id,))?;
            Ok(())
        })
    }

    fn on_plan_end(&self, query_id: String) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0
                .call_method1(py, intern!(py, "on_plan_end"), (query_id,))?;
            Ok(())
        })
    }

    fn on_exec_start(&self, query_id: String) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0
                .call_method1(py, intern!(py, "on_exec_start"), (query_id,))?;
            Ok(())
        })
    }

    fn on_exec_operator_start(&self, query_id: String, node_id: NodeID) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0.call_method1(
                py,
                intern!(py, "on_exec_operator_start"),
                (query_id, node_id),
            )?;
            Ok(())
        })
    }

    fn on_exec_emit_stats(
        &self,
        query_id: String,
        stats: &[(NodeID, StatSnapshotView)],
    ) -> DaftResult<()> {
        Python::with_gil(|py| {
            let stats_map = stats
                .iter()
                .map(|(node_id, stats)| {
                    let stat_map = stats
                        .into_iter()
                        .map(|(name, stat)| (*name, stat.clone().into_py_contents(py).unwrap()))
                        .collect::<HashMap<_, _>>();

                    (node_id, stat_map)
                })
                .collect::<HashMap<_, _>>();
            let py_stats = stats_map.into_pyobject(py)?;

            self.0
                .call_method1(py, intern!(py, "on_exec_emit_stats"), (query_id, py_stats))?;
            Ok(())
        })
    }

    fn on_exec_operator_end(&self, query_id: String, node_id: NodeID) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0
                .call_method1(py, intern!(py, "on_exec_operator_end"), (query_id, node_id))?;
            Ok(())
        })
    }

    fn on_exec_end(&self, query_id: String) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0
                .call_method1(py, intern!(py, "on_exec_end"), (query_id,))?;
            Ok(())
        })
    }
}
