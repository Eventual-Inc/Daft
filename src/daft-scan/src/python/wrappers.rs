use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_recordbatch::python::PyRecordBatch;
use daft_schema::{python::schema::PySchema, schema::SchemaRef};
use pyo3::{Python, intern, prelude::*};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::{PyDataSourceTask, pylib_scan_info::PyPushdowns};
use crate::{
    DataSourceTaskRef,
    pushdowns::Pushdowns,
    source::{DataSource, DataSourceTask, DataSourceTaskStream, ReadOptions, RecordBatchStream},
};

/// Newtype to implement the DataSource trait for a Python data source.
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct PyDataSourceWrapper(Py<PyAny>);

impl From<Bound<'_, PyAny>> for PyDataSourceWrapper {
    fn from(obj: Bound<'_, PyAny>) -> Self {
        Self(obj.unbind())
    }
}

#[async_trait]
impl DataSource for PyDataSourceWrapper {
    fn name(&self) -> String {
        Python::attach(|py| {
            let source = self.0.bind(py);
            let name = source
                .getattr(intern!(py, "name"))
                .expect("DataSource.name should never fail");
            let name: String = name.extract().expect("DataSource.name must be a string");
            name
        })
    }

    fn schema(&self) -> SchemaRef {
        Python::attach(|py| {
            self.0
                .bind(py)
                .getattr(intern!(py, "schema"))
                .and_then(|s| s.getattr(intern!(py, "_schema")))
                .and_then(|s| Ok(s.extract::<PySchema>()?))
                .expect("DataSource.schema should never fail")
                .schema
        })
    }

    async fn get_tasks(&self, pushdowns: &Pushdowns) -> DaftResult<DataSourceTaskStream> {
        let py_source = Python::attach(|py| self.0.clone_ref(py));
        let pushdowns = pushdowns.clone();
        let (tx, rx) = mpsc::unbounded_channel();

        // The JoinHandle is intentionally dropped. If the closure panics the
        // sender is dropped, which ends the receiver stream gracefully.
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let py_pushdowns = PyPushdowns(Arc::new(pushdowns))
                    .into_pyobject(py)
                    .expect("PyPushdowns conversion should never fail");
                let async_gen = py_source
                    .call_method1(py, intern!(py, "get_tasks"), (py_pushdowns,))
                    .expect("DataSource.get_tasks call should never fail");

                let asyncio = py
                    .import(intern!(py, "asyncio"))
                    .expect("asyncio import should never fail");
                let event_loop = asyncio
                    .call_method0(intern!(py, "new_event_loop"))
                    .expect("asyncio.new_event_loop should never fail");

                loop {
                    let anext = match async_gen.call_method0(py, intern!(py, "__anext__")) {
                        Ok(coro) => coro,
                        Err(e) => {
                            let _ = tx.send(Err(e.into()));
                            break;
                        }
                    };

                    match event_loop.call_method1(intern!(py, "run_until_complete"), (anext,)) {
                        Ok(task_obj) => {
                            let task = datasource_task_ref_from_py(task_obj);
                            if tx.send(Ok(task)).is_err() {
                                break;
                            }
                        }
                        Err(e)
                            if e.is_instance_of::<pyo3::exceptions::PyStopAsyncIteration>(py) =>
                        {
                            break;
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e.into()));
                            break;
                        }
                    }
                }

                event_loop.call_method0(intern!(py, "close")).ok();
            });
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }
}

/// Converts a Python object to a [`DataSourceTaskRef`], unwrapping a [`PyDataSourceTask`] if present.
///
/// Handles both raw `PyDataSourceTask` objects and `_RustDataSourceTask` wrappers
/// (which store the inner `PyDataSourceTask` in a `_inner` attribute).
fn datasource_task_ref_from_py(obj: Bound<'_, PyAny>) -> DataSourceTaskRef {
    // Direct PyDataSourceTask (Rust pyclass)
    if let Ok(task) = obj.extract::<PyDataSourceTask>() {
        return task.0;
    }
    // _NativeDataSourceTask(DataSourceTask) wrapping a PyDataSourceTask in ._inner
    if let Ok(inner) = obj.getattr(intern!(obj.py(), "_inner"))
        && let Ok(task) = inner.extract::<PyDataSourceTask>()
    {
        return task.0;
    }
    Arc::new(PyDataSourceTaskWrapper::from(obj))
}

impl From<Bound<'_, PyAny>> for PyDataSourceTaskWrapper {
    fn from(obj: Bound<'_, PyAny>) -> Self {
        Self(obj.unbind())
    }
}

/// Newtype to implement the DataSourceTask trait for a Python data source task.
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct PyDataSourceTaskWrapper(Py<PyAny>);

#[async_trait]
impl DataSourceTask for PyDataSourceTaskWrapper {
    fn schema(&self) -> SchemaRef {
        Python::attach(|py| {
            self.0
                .bind(py)
                .getattr(intern!(py, "schema"))
                .and_then(|s| s.getattr(intern!(py, "_schema")))
                .and_then(|s| Ok(s.extract::<PySchema>()?))
                .expect("DataSourceTask.schema should never fail")
                .schema
        })
    }

    fn to_py(&self, py: pyo3::Python<'_>) -> Option<pyo3::Py<pyo3::PyAny>> {
        Some(self.0.clone_ref(py))
    }

    async fn read(&self, _: ReadOptions) -> DaftResult<RecordBatchStream> {
        let py_task = Python::attach(|py| self.0.clone_ref(py));
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let async_gen = py_task
                    .call_method0(py, intern!(py, "read"))
                    .expect("DataSourceTask.read call should never fail");

                let asyncio = py
                    .import(intern!(py, "asyncio"))
                    .expect("asyncio import should never fail");
                let event_loop = asyncio
                    .call_method0(intern!(py, "new_event_loop"))
                    .expect("asyncio.new_event_loop should never fail");

                loop {
                    let anext = match async_gen.call_method0(py, intern!(py, "__anext__")) {
                        Ok(coro) => coro,
                        Err(e) => {
                            let _ = tx.send(Err(e.into()));
                            break;
                        }
                    };

                    match event_loop.call_method1(intern!(py, "run_until_complete"), (anext,)) {
                        Ok(batch_obj) => {
                            let rb = batch_obj
                                .getattr(intern!(py, "_recordbatch"))
                                .and_then(|inner| Ok(inner.extract::<PyRecordBatch>()?))
                                .expect("DataSourceTask.read must yield RecordBatch")
                                .record_batch;
                            if tx.send(Ok(rb)).is_err() {
                                break;
                            }
                        }
                        Err(e)
                            if e.is_instance_of::<pyo3::exceptions::PyStopAsyncIteration>(py) =>
                        {
                            break;
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e.into()));
                            break;
                        }
                    }
                }

                event_loop.call_method0(intern!(py, "close")).ok();
            });
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }
}
