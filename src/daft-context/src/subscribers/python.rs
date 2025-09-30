use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::{StatSnapshotView, ops::NodeInfo, python::PyNodeInfo};
use daft_micropartition::{MicroPartitionRef, python::PyMicroPartition};
use pyo3::{IntoPyObject, PyObject, Python, intern};

use crate::subscribers::{NodeID, Subscriber};

/// Wrapper around a Python object that implements the Subscriber trait
#[derive(Debug)]
pub struct PySubscriberWrapper(pub(crate) PyObject);

#[async_trait]
impl Subscriber for PySubscriberWrapper {
    fn on_query_start(&self, query_id: String, unoptimized_plan: String) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0.call_method1(
                py,
                intern!(py, "on_query_start"),
                (query_id, unoptimized_plan),
            )?;
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

    fn on_result_out(&self, query_id: String, result: MicroPartitionRef) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0.call_method1(
                py,
                intern!(py, "on_result_out"),
                (query_id, PyMicroPartition::from(result)),
            )?;
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

    fn on_plan_end(&self, query_id: String, optimized_plan: String) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0
                .call_method1(py, intern!(py, "on_plan_end"), (query_id, optimized_plan))?;
            Ok(())
        })
    }

    fn on_exec_start(&self, query_id: String, node_infos: &[Arc<NodeInfo>]) -> DaftResult<()> {
        Python::with_gil(|py| {
            let py_node_infos = node_infos
                .iter()
                .map(|node_info| PyNodeInfo::from(node_info.clone()))
                .collect::<Vec<_>>();
            self.0
                .call_method1(py, intern!(py, "on_exec_start"), (query_id, py_node_infos))?;
            Ok(())
        })
    }

    async fn on_exec_operator_start(&self, query_id: String, node_id: NodeID) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0.call_method1(
                py,
                intern!(py, "on_exec_operator_start"),
                (query_id, node_id),
            )?;
            Ok(())
        })
    }

    async fn on_exec_emit_stats(
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

    async fn on_exec_operator_end(&self, query_id: String, node_id: NodeID) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0
                .call_method1(py, intern!(py, "on_exec_operator_end"), (query_id, node_id))?;
            Ok(())
        })
    }

    async fn on_exec_end(&self, query_id: String) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.0
                .call_method1(py, intern!(py, "on_exec_end"), (query_id,))?;
            Ok(())
        })
    }
}
