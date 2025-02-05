//! Wrapper around the python RayRunner class
use std::sync::Arc;

#[cfg(feature = "python")]
use common_error::{DaftError, DaftResult};
#[cfg(feature = "python")]
use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
#[cfg(feature = "python")]
use daft_micropartition::{python::PyMicroPartition, MicroPartitionRef};
#[cfg(feature = "python")]
use pyo3::{
    intern,
    prelude::*,
    types::{PyDict, PyIterator},
};

#[cfg(feature = "python")]
#[derive(Debug)]
pub struct RayRunner {
    pub pyobj: Arc<PyObject>,
}

#[cfg(feature = "python")]
impl RayRunner {
    pub fn try_new(
        address: Option<String>,
        max_task_backlog: Option<usize>,
        force_client_mode: Option<bool>,
    ) -> DaftResult<Self> {
        Python::with_gil(|py| {
            let ray_runner_module = py.import(intern!(py, "daft.runners.ray_runner"))?;
            let ray_runner = ray_runner_module.getattr(intern!(py, "RayRunner"))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item(intern!(py, "address"), address)?;
            kwargs.set_item(intern!(py, "max_task_backlog"), max_task_backlog)?;
            kwargs.set_item(intern!(py, "force_client_mode"), force_client_mode)?;

            let instance = ray_runner.call((), Some(&kwargs))?;
            let instance = instance.unbind();

            Ok(Self {
                pyobj: Arc::new(instance),
            })
        })
    }
}

#[cfg(feature = "python")]
#[derive(Debug)]
pub struct NativeRunner {
    pub pyobj: Arc<PyObject>,
}

#[cfg(feature = "python")]
impl NativeRunner {
    pub fn try_new() -> DaftResult<Self> {
        Python::with_gil(|py| {
            let native_runner_module = py.import(intern!(py, "daft.runners.native_runner"))?;
            let native_runner = native_runner_module.getattr(intern!(py, "NativeRunner"))?;

            let instance = native_runner.call0()?;
            let instance = instance.unbind();

            Ok(Self {
                pyobj: Arc::new(instance),
            })
        })
    }
}

#[cfg(feature = "python")]
#[derive(Debug)]
pub struct PyRunner {
    pub pyobj: Arc<PyObject>,
}

#[cfg(feature = "python")]
impl PyRunner {
    pub fn try_new(use_thread_pool: Option<bool>) -> DaftResult<Self> {
        Python::with_gil(|py| {
            let native_runner_module = py.import(intern!(py, "daft.runners.pyrunner"))?;
            let native_runner = native_runner_module.getattr(intern!(py, "PyRunner"))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item(intern!(py, "use_thread_pool"), use_thread_pool)?;

            let instance = native_runner.call((), Some(&kwargs))?;
            let instance = instance.unbind();

            Ok(Self {
                pyobj: Arc::new(instance),
            })
        })
    }
}

#[derive(Debug)]
pub enum Runner {
    #[cfg(feature = "python")]
    Ray(RayRunner),
    #[cfg(feature = "python")]
    Native(NativeRunner),
    #[cfg(feature = "python")]
    Py(PyRunner),
}

#[cfg(feature = "python")]
impl Runner {
    pub fn from_pyobj(obj: PyObject) -> PyResult<Self> {
        Python::with_gil(|py| {
            let name = obj.getattr(py, "name")?.extract::<String>(py)?;
            match name.as_ref() {
                "ray" => {
                    let ray_runner = RayRunner {
                        pyobj: Arc::new(obj),
                    };
                    Ok(Self::Ray(ray_runner))
                }
                "native" => {
                    let native_runner = NativeRunner {
                        pyobj: Arc::new(obj),
                    };
                    Ok(Self::Native(native_runner))
                }
                "py" => {
                    let py_runner = PyRunner {
                        pyobj: Arc::new(obj),
                    };
                    Ok(Self::Py(py_runner))
                }
                _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Unknown runner type: {name}"
                ))),
            }
        })
    }

    fn get_runner_ref(&self) -> &PyObject {
        match self {
            Self::Ray(RayRunner { pyobj }) => pyobj.as_ref(),
            Self::Native(NativeRunner { pyobj }) => pyobj.as_ref(),
            Self::Py(PyRunner { pyobj }) => pyobj.as_ref(),
        }
    }
    pub fn run_iter_tables(
        &self,
        py: Python<'_>,
        lp: LogicalPlanBuilder,
        results_buffer_size: Option<usize>,
    ) -> DaftResult<Vec<DaftResult<MicroPartitionRef>>> {
        let pyobj = self.get_runner_ref();
        let py_lp = PyLogicalPlanBuilder::new(lp);
        let builder = py.import(intern!(py, "daft.logical.builder"))?;
        let builder = builder.getattr(intern!(py, "LogicalPlanBuilder"))?;
        let builder = builder.call((py_lp,), None)?;
        let result = pyobj.call_method(
            py,
            intern!(py, "run_iter_tables"),
            (builder, results_buffer_size),
            None,
        )?;

        let result = result.bind(py);
        let iter = PyIterator::from_object(result)?;

        let iter = iter.map(|item| {
            let item = item?;
            let partition = item.getattr(intern!(py, "_micropartition"))?;
            let partition = partition.extract::<PyMicroPartition>()?;
            let partition = partition.inner;
            Ok::<_, DaftError>(partition)
        });

        Ok(iter.collect())
    }

    pub fn to_pyobj(self: Arc<Self>, py: Python) -> PyObject {
        let runner = self.get_runner_ref();
        runner.clone_ref(py)
    }
}

#[derive(Debug)]
pub enum RunnerConfig {
    #[cfg(feature = "python")]
    Native,
    #[cfg(feature = "python")]
    Ray {
        address: Option<String>,
        max_task_backlog: Option<usize>,
        force_client_mode: Option<bool>,
    },
    #[cfg(feature = "python")]
    Py { use_thread_pool: Option<bool> },
}

#[cfg(feature = "python")]
impl RunnerConfig {
    pub fn create_runner(self) -> DaftResult<Runner> {
        match self {
            Self::Native => Ok(Runner::Native(NativeRunner::try_new()?)),
            Self::Ray {
                address,
                max_task_backlog,
                force_client_mode,
            } => Ok(Runner::Ray(RayRunner::try_new(
                address,
                max_task_backlog,
                force_client_mode,
            )?)),
            Self::Py { use_thread_pool } => Ok(Runner::Py(PyRunner::try_new(use_thread_pool)?)),
        }
    }
}
