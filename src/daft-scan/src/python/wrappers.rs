use std::sync::Arc;

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_recordbatch::python::PyRecordBatch;
use daft_schema::{python::schema::PySchema, schema::SchemaRef};
use futures::StreamExt;
use pyo3::{Python, intern, prelude::*};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::{
    PyDataSourceTask, PythonTablesFactoryArgs,
    pylib_scan_info::{PyPartitionField, PyPushdowns},
};
use crate::{
    DataSourceTaskRef, PartitionField, Pushdowns, ScanOperator, ScanSource, ScanSourceKind,
    ScanTask, ScanTaskRef, SourceConfig,
    pushdowns::SupportsPushdownFilters,
    source::{DataSource, DataSourceTask, DataSourceTaskStream, ReadOptions, RecordBatchStream},
    storage_config::StorageConfig,
};

/// Unwrap `Ok(v)` or send the error through `$tx` and return from the enclosing function.
macro_rules! try_or_send {
    ($tx:expr, $expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                let _ = $tx.send(Err(e.into()));
                return;
            }
        }
    };
}

// ---------------------------------------------------------------------------
// PyDataSourceWrapper — bridges a Python DataSource to Rust DataSource +
//                       ScanOperator traits.
// ---------------------------------------------------------------------------

/// Wraps a Python `DataSource` and implements both [`DataSource`] and
/// [`ScanOperator`].
///
/// Name, schema, and partition fields are cached at construction time because
/// [`ScanOperator`] returns references (`&str`, `&[PartitionField]`).
#[derive(Debug)]
pub struct PyDataSourceWrapper {
    source: Py<PyAny>,
    name: String,
    schema: SchemaRef,
    partition_fields: Vec<PartitionField>,
}

impl PyDataSourceWrapper {
    pub fn new(source: Bound<'_, PyAny>) -> Self {
        let name: String = source
            .getattr(intern!(source.py(), "name"))
            .expect("DataSource.name should never fail")
            .extract()
            .expect("DataSource.name must be a string");

        let schema = source
            .getattr(intern!(source.py(), "schema"))
            .and_then(|s| s.getattr(intern!(source.py(), "_schema")))
            .and_then(|s| Ok(s.extract::<PySchema>()?))
            .expect("DataSource.schema should never fail")
            .schema;

        let partition_fields: Vec<PartitionField> = source
            .call_method0(intern!(source.py(), "get_partition_fields"))
            .and_then(|list| {
                list.try_iter()?
                    .map(|item| {
                        let pf = item?
                            .getattr(intern!(source.py(), "_partition_field"))?
                            .extract::<PyPartitionField>()?;
                        Ok(pf.0.as_ref().clone())
                    })
                    .collect::<PyResult<Vec<_>>>()
            })
            .unwrap_or_default();

        Self {
            source: source.unbind(),
            name,
            schema,
            partition_fields,
        }
    }

    /// Wrap a pure-Python [`DataSourceTask`] in a `python_factory_func_scan_task`.
    ///
    /// The resulting [`ScanTask`] calls back into
    /// `daft.io.__internal._get_record_batches` at execution time, which
    /// drains the task's async `read()` generator.
    fn make_python_factory_scan_task(
        &self,
        task: &dyn DataSourceTask,
        pushdowns: &Pushdowns,
    ) -> DaftResult<ScanTaskRef> {
        let py_obj = pyo3::Python::attach(|py| {
            task.to_py(py).ok_or_else(|| {
                DaftError::InternalError(
                    "Non-native DataSourceTask must be backed by a Python object".to_string(),
                )
            })
        })?;

        let source = ScanSource {
            size_bytes: None,
            last_modified: None,
            metadata: None,
            statistics: None,
            partition_spec: None,
            kind: ScanSourceKind::PythonFactoryFunction {
                module: "daft.io.__internal".to_string(),
                func_name: "_get_record_batches".to_string(),
                func_args: PythonTablesFactoryArgs::new(vec![Arc::new(py_obj)]),
            },
        };

        let source_config = Arc::new(SourceConfig::PythonFunction {
            source_name: Some(self.name.clone()),
            module_name: Some("daft.io.__internal".to_string()),
            function_name: Some("_get_record_batches".to_string()),
        });

        Ok(Arc::new(ScanTask::new(
            vec![source],
            source_config,
            task.schema(),
            Arc::new(StorageConfig::default()),
            pushdowns.clone(),
            None,
        )))
    }
}

#[async_trait]
impl DataSource for PyDataSourceWrapper {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partition_fields(&self) -> Vec<PartitionField> {
        self.partition_fields.clone()
    }

    async fn get_tasks(&self, pushdowns: &Pushdowns) -> DaftResult<DataSourceTaskStream> {
        let py_source = Python::attach(|py| self.source.clone_ref(py));
        let pushdowns = pushdowns.clone();
        let (tx, rx) = mpsc::unbounded_channel();

        // The JoinHandle is intentionally dropped: once the setup succeeds,
        // all loop errors are forwarded through `tx`. Setup errors are also
        // sent through `tx` so the caller always sees them as stream items.
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                // Wrap PyPushdowns in the Python Pushdowns dataclass so the
                // user's get_tasks() receives the public API type.
                let py_pushdowns =
                    try_or_send!(tx, PyPushdowns(Arc::new(pushdowns)).into_pyobject(py));
                let pushdowns_mod = try_or_send!(tx, py.import(intern!(py, "daft.io.pushdowns")));
                let pushdowns_cls =
                    try_or_send!(tx, pushdowns_mod.getattr(intern!(py, "Pushdowns")));
                let pushdowns_obj = try_or_send!(
                    tx,
                    pushdowns_cls.call_method1(intern!(py, "_from_pypushdowns"), (py_pushdowns,))
                );
                let async_gen = try_or_send!(
                    tx,
                    py_source.call_method1(py, intern!(py, "get_tasks"), (pushdowns_obj,))
                );

                let asyncio = try_or_send!(tx, py.import(intern!(py, "asyncio")));
                let event_loop =
                    try_or_send!(tx, asyncio.call_method0(intern!(py, "new_event_loop")));

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

impl ScanOperator for PyDataSourceWrapper {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &self.partition_fields
    }

    fn file_path_column(&self) -> Option<&str> {
        None
    }

    fn generated_fields(&self) -> Option<SchemaRef> {
        None
    }

    fn can_absorb_filter(&self) -> bool {
        false
    }

    fn can_absorb_select(&self) -> bool {
        false
    }

    fn can_absorb_limit(&self) -> bool {
        false
    }

    fn can_absorb_shard(&self) -> bool {
        false
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!("DataSource: {}", self.name),
            format!("Schema: {}", self.schema.short_string()),
        ]
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskRef>> {
        let io_runtime = get_io_runtime(true);
        let task_stream =
            io_runtime.block_on_current_thread(DataSource::get_tasks(self, &pushdowns))?;
        let tasks: Vec<_> = io_runtime.block_on_current_thread(task_stream.collect::<Vec<_>>());

        let mut scan_tasks = Vec::with_capacity(tasks.len());
        for result in tasks {
            let task = result?;
            if let Some(st) = task.as_scan_task() {
                scan_tasks.push(st.clone());
            } else {
                scan_tasks.push(self.make_python_factory_scan_task(task.as_ref(), &pushdowns)?);
            }
        }
        Ok(scan_tasks)
    }

    fn as_pushdown_filter(&self) -> Option<&dyn SupportsPushdownFilters> {
        None
    }
}

// ---------------------------------------------------------------------------
// PyDataSourceTaskWrapper — bridges a Python DataSourceTask to Rust.
// ---------------------------------------------------------------------------

/// Converts a Python object to a [`DataSourceTaskRef`], unwrapping a [`PyDataSourceTask`] if present.
///
/// Handles both raw `PyDataSourceTask` objects and `_RustDataSourceTask` wrappers
/// (which store the inner `PyDataSourceTask` in a `_inner` attribute).
fn datasource_task_ref_from_py(obj: Bound<'_, PyAny>) -> DataSourceTaskRef {
    // Direct PyDataSourceTask (Rust pyclass)
    if let Ok(task) = obj.extract::<PyDataSourceTask>() {
        return task.0;
    }
    // _RustDataSourceTask(DataSourceTask) wrapping a PyDataSourceTask in ._inner
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

/// Wraps a Python `DataSourceTask` as a Rust [`DataSourceTask`].
#[derive(Debug)]
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
                let async_gen = try_or_send!(tx, py_task.call_method0(py, intern!(py, "read")));
                let asyncio = try_or_send!(tx, py.import(intern!(py, "asyncio")));
                let event_loop =
                    try_or_send!(tx, asyncio.call_method0(intern!(py, "new_event_loop")));

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
                            let rb = match batch_obj
                                .getattr(intern!(py, "_recordbatch"))
                                .and_then(|inner| Ok(inner.extract::<PyRecordBatch>()?))
                            {
                                Ok(py_rb) => py_rb.record_batch,
                                Err(e) => {
                                    let _ = tx.send(Err(e.into()));
                                    break;
                                }
                            };
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
