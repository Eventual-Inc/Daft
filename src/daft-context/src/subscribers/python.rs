use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::{QueryID, QueryPlan, StatSnapshotView, ops::NodeInfo, python::PyNodeInfo};
use daft_micropartition::{MicroPartitionRef, python::PyMicroPartition};
use pyo3::{IntoPyObject, Py, PyAny, Python, intern};

use crate::{
    python::PyQueryMetadata,
    subscribers::{NodeID, QueryMetadata, Subscriber},
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

    fn on_query_end(&self, query_id: QueryID) -> DaftResult<()> {
        Python::attach(|py| {
            self.0
                .call_method1(py, intern!(py, "on_query_end"), (query_id.to_string(),))?;
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

    fn on_exec_start(&self, query_id: QueryID, node_infos: &[Arc<NodeInfo>]) -> DaftResult<()> {
        Python::attach(|py| {
            let py_node_infos = node_infos
                .iter()
                .map(|node_info| PyNodeInfo::from(node_info.clone()))
                .collect::<Vec<_>>();
            self.0.call_method1(
                py,
                intern!(py, "on_exec_start"),
                (query_id.to_string(), py_node_infos),
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
        stats: &[(NodeID, StatSnapshotView)],
    ) -> DaftResult<()> {
        Python::attach(|py| {
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
}
