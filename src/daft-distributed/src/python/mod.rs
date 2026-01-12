mod progress_bar;
pub mod ray;
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

use common_daft_config::PyDaftExecutionConfig;
use common_display::DisplayLevel;
use common_partitioning::Partition;
use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use futures::StreamExt;
use progress_bar::FlotillaProgressBar;
use pyo3::prelude::*;
use ray::{RayPartitionRef, RaySwordfishTask, RaySwordfishWorker, RayWorkerManager};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    pipeline_node::{
        logical_plan_to_pipeline_node, viz_distributed_pipeline_ascii,
        viz_distributed_pipeline_mermaid,
    },
    plan::{DistributedPhysicalPlan, PlanConfig, PlanResultStream, PlanRunner},
    python::ray::RayTaskResult,
    scheduling::worker::WorkerConfig,
    statistics::StatisticsSubscriber,
};

#[pyclass(frozen)]
struct PythonPartitionRefStream {
    inner: Arc<Mutex<PlanResultStream>>,
}

#[pymethods]
impl PythonPartitionRefStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, pyo3::PyAny>> {
        let inner = self.inner.clone();
        // future into py requires that the future is Send + 'static, so we wrap the inner in an Arc<Mutex<>>
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let next = {
                let mut inner = inner.lock().await;
                inner.next().await
            };
            let next = match next {
                Some(result) => {
                    let result = result?;
                    let ray_part_ref = result
                        .as_any()
                        .downcast_ref::<RayPartitionRef>()
                        .expect("Failed to downcast to RayPartitionRef")
                        .clone();
                    Some(ray_part_ref)
                }
                None => None,
            };
            Ok(next)
        })
    }
}

#[pyclass(module = "daft.daft", name = "DistributedPhysicalPlan", frozen)]
#[derive(Serialize, Deserialize)]
struct PyDistributedPhysicalPlan {
    plan: DistributedPhysicalPlan,
}

#[pymethods]
impl PyDistributedPhysicalPlan {
    #[staticmethod]
    fn from_logical_plan_builder(
        builder: &PyLogicalPlanBuilder,
        query_id: String,
        config: &PyDaftExecutionConfig,
    ) -> PyResult<Self> {
        let plan = DistributedPhysicalPlan::from_logical_plan_builder(
            &builder.builder,
            query_id.into(),
            config.config.clone(),
        )?;
        Ok(Self { plan })
    }

    fn idx(&self) -> String {
        self.plan.idx().to_string()
    }

    fn num_partitions(&self) -> PyResult<usize> {
        // Create pipeline nodes from the logical plan
        let plan_config = PlanConfig::new(
            self.plan.idx(),
            self.plan.query_id(),
            self.plan.execution_config().clone(),
        );
        let pipeline_node = logical_plan_to_pipeline_node(
            plan_config,
            self.plan.logical_plan().clone(),
            Default::default(),
        )?;

        Ok(pipeline_node.num_partitions())
    }

    /// Visualize the distributed pipeline as ASCII text
    fn repr_ascii(&self, simple: bool) -> PyResult<String> {
        // Create pipeline nodes from the logical plan
        let plan_config = PlanConfig::new(
            self.plan.idx(),
            self.plan.query_id(),
            self.plan.execution_config().clone(),
        );
        let pipeline_node = logical_plan_to_pipeline_node(
            plan_config,
            self.plan.logical_plan().clone(),
            Default::default(),
        )?;

        Ok(viz_distributed_pipeline_ascii(&pipeline_node, simple))
    }

    /// Visualize the distributed pipeline as Mermaid markdown
    fn repr_mermaid(&self, simple: bool, bottom_up: bool) -> PyResult<String> {
        // Create a pipeline node from the stage plan
        let plan_config = PlanConfig::new(
            self.plan.idx(),
            self.plan.query_id(),
            self.plan.execution_config().clone(),
        );
        let pipeline_node = logical_plan_to_pipeline_node(
            plan_config,
            self.plan.logical_plan().clone(),
            Default::default(),
        )?;

        let display_level = if simple {
            DisplayLevel::Compact
        } else {
            DisplayLevel::Default
        };
        Ok(viz_distributed_pipeline_mermaid(
            &pipeline_node,
            display_level,
            bottom_up,
            None,
        ))
    }
}
impl_bincode_py_state_serialization!(PyDistributedPhysicalPlan);

#[pyclass(module = "daft.daft", name = "DistributedPhysicalPlanRunner", frozen)]
struct PyDistributedPhysicalPlanRunner {
    runner: Arc<PlanRunner<RaySwordfishWorker>>,
}

#[pymethods]
impl PyDistributedPhysicalPlanRunner {
    #[new]
    #[pyo3(signature = (cluster_config=None))]
    fn new(cluster_config: Option<Vec<PyWorkerConfig>>) -> PyResult<Self> {
        let worker_config =
            cluster_config.map(|configs| configs.into_iter().map(|c| c.into()).collect());
        let worker_manager = RayWorkerManager::new(worker_config);
        Ok(Self {
            runner: Arc::new(PlanRunner::new(Arc::new(worker_manager))),
        })
    }

    fn run_plan(
        &self,
        py: Python,
        plan: &PyDistributedPhysicalPlan,
        psets: HashMap<String, Vec<RayPartitionRef>>,
    ) -> PyResult<PythonPartitionRefStream> {
        let psets = psets
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    v.into_iter()
                        .map(|v| Arc::new(v) as Arc<dyn Partition>)
                        .collect(),
                )
            })
            .collect();

        let subscribers: Vec<Box<dyn StatisticsSubscriber>> =
            vec![Box::new(FlotillaProgressBar::try_new(py)?)];

        let plan_result = self.runner.run_plan(&plan.plan, psets, subscribers)?;
        let part_stream = PythonPartitionRefStream {
            inner: Arc::new(Mutex::new(plan_result.into_stream())),
        };
        Ok(part_stream)
    }
}

#[pyclass(module = "daft.daft", name = "WorkerConfig")]
#[derive(Debug, Clone)]
pub struct PyWorkerConfig {
    inner: WorkerConfig,
}

#[pymethods]
impl PyWorkerConfig {
    #[new]
    #[pyo3(signature = (num_replicas, num_cpus, memory_bytes, num_gpus=0.0))]
    pub fn new(
        num_replicas: usize,
        num_cpus: f64,
        memory_bytes: usize,
        num_gpus: f64,
    ) -> PyResult<Self> {
        let inner = WorkerConfig::new(
            NonZeroUsize::new(num_replicas).ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("num_replicas must be positive")
            })?,
            num_cpus,
            NonZeroUsize::new(memory_bytes).ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err("memory_bytes must be positive")
            })?,
            num_gpus,
        )?;
        Ok(Self { inner })
    }

    #[getter]
    fn num_replicas(&self) -> usize {
        self.inner.num_replicas.get()
    }

    #[getter]
    fn num_cpus(&self) -> f64 {
        self.inner.num_cpus
    }

    #[getter]
    fn memory_bytes(&self) -> usize {
        self.inner.memory_bytes.get()
    }

    #[getter]
    fn num_gpus(&self) -> f64 {
        self.inner.num_gpus
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkerConfig(num_replicas={}, num_cpus={}, memory_bytes={}, num_gpus={})",
            self.inner.num_replicas,
            self.inner.num_cpus,
            self.inner.memory_bytes,
            self.inner.num_gpus
        )
    }
}

impl From<PyWorkerConfig> for WorkerConfig {
    fn from(py_config: PyWorkerConfig) -> Self {
        py_config.inner
    }
}

impl From<&WorkerConfig> for PyWorkerConfig {
    fn from(config: &WorkerConfig) -> Self {
        Self {
            inner: WorkerConfig {
                num_replicas: config.num_replicas,
                num_cpus: config.num_cpus,
                memory_bytes: config.memory_bytes,
                num_gpus: config.num_gpus,
            },
        }
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyDistributedPhysicalPlan>()?;
    parent.add_class::<PyDistributedPhysicalPlanRunner>()?;
    parent.add_class::<RaySwordfishTask>()?;
    parent.add_class::<RayPartitionRef>()?;
    parent.add_class::<RaySwordfishWorker>()?;
    parent.add_class::<PyWorkerConfig>()?;
    parent.add_class::<RayTaskResult>()?;
    Ok(())
}
