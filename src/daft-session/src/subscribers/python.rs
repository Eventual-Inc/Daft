use common_error::DaftResult;
use pyo3::{intern, Python};
use pyo3::types::PyAnyMethods;
use pyo3::PyObject;

use crate::subscribers::{NodeID, QuerySubscriber};

/// Python object that implements the QuerySubscriber trait via a Python ABC class.
#[derive(Debug)]
pub struct PyQuerySubscriberWrapper(PyObject);

impl From<PyObject> for PyQuerySubscriberWrapper {
    fn from(value: PyObject) -> Self {
        Self(value)
    }
}

impl QuerySubscriber for PyQuerySubscriberWrapper {
    fn on_query_start(&self) -> DaftResult<()> {
        Python::with_gil(|py| {
            let subscriber = self.0.bind(py);
            subscriber.call_method0(intern!(py, "on_query_start"))?;
            Ok(())
        })
    }

    fn on_query_end(&self) -> DaftResult<()> {
        Python::with_gil(|py| {
            let subscriber = self.0.bind(py);
            subscriber.call_method0(intern!(py, "on_query_end"))?;
            Ok(())
        })
    }

    fn on_plan_start(&self) -> DaftResult<()> {
        Python::with_gil(|py| {
            let subscriber = self.0.bind(py);
            subscriber.call_method0(intern!(py, "on_plan_start"))?;
            Ok(())
        })
    }

    fn on_plan_end(&self) -> DaftResult<()> {
        Python::with_gil(|py| {
            let subscriber = self.0.bind(py);
            subscriber.call_method0(intern!(py, "on_plan_end"))?;
            Ok(())
        })
    }

    fn on_exec_start(&self) -> DaftResult<()> {
        Python::with_gil(|py| {
            let subscriber = self.0.bind(py);
            subscriber.call_method0(intern!(py, "on_exec_start"))?;
            Ok(())
        })
    }

    fn on_exec_operator_start(&self, node_id: NodeID) -> DaftResult<()> {
        Python::with_gil(|py| {
            let subscriber = self.0.bind(py);
            subscriber.call_method1(intern!(py, "on_exec_operator_start"), (node_id,))?;
            Ok(())
        })
    }

    fn on_exec_emit_stats(&self) -> DaftResult<()> {
        Python::with_gil(|py| {
            let subscriber = self.0.bind(py);
            subscriber.call_method0(intern!(py, "on_exec_emit_stats"))?;
            Ok(())
        })
    }

    fn on_exec_operator_end(&self, node_id: NodeID) -> DaftResult<()> {
        Python::with_gil(|py| {
            let subscriber = self.0.bind(py);
            subscriber.call_method1(intern!(py, "on_exec_operator_end"), (node_id,))?;
            Ok(())
        })
    }

    fn on_exec_end(&self) -> DaftResult<()> {
        Python::with_gil(|py| {
            let subscriber = self.0.bind(py);
            subscriber.call_method0(intern!(py, "on_exec_end"))?;
            Ok(())
        })
    }
}
