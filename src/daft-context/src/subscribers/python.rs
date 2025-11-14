use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::{QueryID, QueryPlan, StatSnapshot, ops::NodeInfo, python::PyNodeInfo};
use common_partitioning::{PartitionRef, python::PyPartitionRef};
use common_py_serde::PyObjectWrapper;
use pyo3::{IntoPyObject, Py, PyAny, Python, intern};
use serde::{Deserialize, Serialize};

use crate::{
    python::PyQueryMetadata,
    subscribers::{NodeID, QueryMetadata, Subscriber},
};

/// Wrapper around a Python object that implements the Subscriber trait
#[derive(Debug, Serialize, Deserialize)]
pub struct PySubscriberWrapper(pub(crate) PyObjectWrapper);

impl PySubscriberWrapper {
    pub fn new(inner: Py<PyAny>) -> Self {
        Self(PyObjectWrapper(Arc::new(inner)))
    }

    pub fn inner(&self) -> &Py<PyAny> {
        &self.0.0
    }
}

#[async_trait]
impl Subscriber for PySubscriberWrapper {
    fn on_query_start(&self, query_id: QueryID, metadata: Arc<QueryMetadata>) -> DaftResult<()> {
        Python::attach(|py| {
            self.inner().call_method1(
                py,
                intern!(py, "on_query_start"),
                (query_id.to_string(), PyQueryMetadata::from(metadata)),
            )?;
            Ok(())
        })
    }

    fn on_query_end(&self, query_id: QueryID) -> DaftResult<()> {
        Python::attach(|py| {
            self.inner()
                .call_method1(py, intern!(py, "on_query_end"), (query_id.to_string(),))?;
            Ok(())
        })
    }

    fn on_result_out(&self, query_id: QueryID, result: PartitionRef) -> DaftResult<()> {
        Python::attach(|py| {
            self.inner().call_method1(
                py,
                intern!(py, "on_result_out"),
                (query_id.to_string(), PyPartitionRef::from(result)),
            )?;
            Ok(())
        })
    }

    fn on_optimization_start(&self, query_id: QueryID) -> DaftResult<()> {
        Python::attach(|py| {
            self.inner().call_method1(
                py,
                intern!(py, "on_optimization_start"),
                (query_id.to_string(),),
            )?;
            Ok(())
        })
    }

    fn on_optimization_end(&self, query_id: QueryID, optimized_plan: QueryPlan) -> DaftResult<()> {
        Python::attach(|py| {
            self.inner().call_method1(
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
            self.inner().call_method1(
                py,
                intern!(py, "on_exec_start"),
                (query_id.to_string(), py_node_infos),
            )?;
            Ok(())
        })
    }

    async fn on_exec_operator_start(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()> {
        Python::attach(|py| {
            self.inner().call_method1(
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
        stats: &[(NodeID, StatSnapshot)],
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

            self.inner().call_method1(
                py,
                intern!(py, "on_exec_emit_stats"),
                (query_id.to_string(), py_stats),
            )?;
            Ok(())
        })
    }

    async fn on_exec_operator_end(&self, query_id: QueryID, node_id: NodeID) -> DaftResult<()> {
        Python::attach(|py| {
            self.inner().call_method1(
                py,
                intern!(py, "on_exec_operator_end"),
                (query_id.to_string(), node_id),
            )?;
            Ok(())
        })
    }

    async fn on_exec_end(&self, query_id: QueryID) -> DaftResult<()> {
        Python::attach(|py| {
            self.inner()
                .call_method1(py, intern!(py, "on_exec_end"), (query_id.to_string(),))?;
            Ok(())
        })
    }
}
