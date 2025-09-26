use std::{
    fmt::Display,
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

use base64::Engine;
use common_error::DaftResult;
use crossbeam::channel::{Receiver, Sender, bounded};
use daft_core::prelude::*;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    Expr, ExprRef,
    functions::{python::RuntimePyObject, scalar::ScalarFn},
};

#[derive(derive_more::Display, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[display("{_0}")]
pub enum PyScalarFn {
    RowWise(RowWisePyFn),
}

impl PyScalarFn {
    pub fn name(&self) -> &str {
        match self {
            Self::RowWise(RowWisePyFn { function_name, .. }) => function_name,
        }
    }
    pub fn call(&self, args: &[Series]) -> DaftResult<(Series, std::time::Duration)> {
        match self {
            Self::RowWise(func) => func.call(args),
        }
    }

    pub fn args(&self) -> Vec<ExprRef> {
        match self {
            Self::RowWise(RowWisePyFn { args, .. }) => args.clone(),
        }
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        match self {
            Self::RowWise(RowWisePyFn {
                function_name: name,
                args,
                return_dtype,
                ..
            }) => {
                let field_name = if let Some(first_child) = args.first() {
                    first_child.get_name(schema)?
                } else {
                    name.to_string()
                };

                Ok(Field::new(field_name, return_dtype.clone()))
            }
        }
    }
}

pub fn row_wise_udf(
    name: &str,
    inner: RuntimePyObject,
    return_dtype: DataType,
    original_args: RuntimePyObject,
    args: Vec<ExprRef>,
) -> Expr {
    Expr::ScalarFn(ScalarFn::Python(PyScalarFn::RowWise(RowWisePyFn {
        function_name: Arc::from(name),
        inner,
        return_dtype,
        original_args,
        args,
    })))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RowWisePyFn {
    pub function_name: Arc<str>,
    pub inner: RuntimePyObject,
    pub return_dtype: DataType,
    pub original_args: RuntimePyObject,
    pub args: Vec<ExprRef>,
}

impl Display for RowWisePyFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let children_str = self.args.iter().map(|expr| expr.to_string()).join(", ");

        write!(f, "{}({})", self.function_name, children_str)
    }
}

impl RowWisePyFn {
    pub fn with_new_children(&self, children: Vec<ExprRef>) -> Self {
        assert_eq!(
            children.len(),
            self.args.len(),
            "There must be the same amount of new children as original."
        );

        Self {
            function_name: self.function_name.clone(),
            inner: self.inner.clone(),
            return_dtype: self.return_dtype.clone(),
            original_args: self.original_args.clone(),
            args: children,
        }
    }

    #[cfg(not(feature = "python"))]
    pub fn call(&self, _args: &[Series]) -> DaftResult<(Series, std::time::Duration)> {
        panic!("Cannot evaluate a RowWisePyFn without compiling for Python");
    }

    #[cfg(feature = "python")]
    pub fn call(&self, args: &[Series]) -> DaftResult<(Series, std::time::Duration)> {
        use pyo3::prelude::*;

        let num_rows = args
            .iter()
            .map(Series::len)
            .max()
            .expect("RowWisePyFn should have at least one argument");

        for a in args {
            assert!(
                a.len() == num_rows || a.len() == 1,
                "arg lengths differ: {} vs {}",
                num_rows,
                a.len()
            );
        }

        let is_async: bool = Python::with_gil(|py| {
            py.import(pyo3::intern!(py, "asyncio"))?
                .getattr(pyo3::intern!(py, "iscoroutinefunction"))?
                .call1((self.inner.as_ref(),))?
                .extract()
        })?;

        if is_async {
            self.call_async(args, num_rows)
        } else {
            self.call_serial(args, num_rows)
        }
    }

    #[cfg(feature = "python")]
    fn call_async(
        &self,
        args: &[Series],
        num_rows: usize,
    ) -> DaftResult<(Series, std::time::Duration)> {
        use daft_core::python::PySeries;
        use pyo3::prelude::*;
        let py_return_type = daft_core::python::PyDataType::from(self.return_dtype.clone());
        let inner_ref = self.inner.as_ref();
        let args_ref = self.original_args.as_ref();
        let start_time = std::time::Instant::now();

        Ok(pyo3::Python::with_gil(|py| {
            let gil_contention_time = start_time.elapsed();

            let f = py
                .import(pyo3::intern!(py, "daft.udf.row_wise"))?
                .getattr(pyo3::intern!(py, "__call_async_batch"))?;

            let mut evaluated_args = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                let py_args_for_row = args
                    .iter()
                    .map(|s| {
                        let idx = if s.len() == 1 { 0 } else { i };
                        let lit = s.get_lit(idx);
                        lit.into_pyobject(py).map_err(|e| e.into())
                    })
                    .collect::<DaftResult<Vec<_>>>()?;

                evaluated_args.push(py_args_for_row);
            }

            let res = f.call1((inner_ref, py_return_type.clone(), args_ref, evaluated_args))?;
            let name = args[0].name();

            let result_series = res.extract::<PySeries>()?.series;

            Ok::<_, PyErr>((result_series.rename(name), gil_contention_time))
        })?)
    }

    #[cfg(feature = "python")]
    fn call_serial(
        &self,
        args: &[Series],
        num_rows: usize,
    ) -> DaftResult<(Series, std::time::Duration)> {
        use common_py_serde::pickle_dumps;
        use rayon::iter::{IntoParallelIterator, ParallelIterator};

        let inner_ref = self.inner.as_ref();
        let args_ref = self.original_args.as_ref();
        let name = args[0].name();
        let start_time = std::time::Instant::now();
        let gil_contention_time = start_time.elapsed();
        let (pickled_function, pickled_args) = pyo3::Python::with_gil(|py| {
            let pickled_function = pickle_dumps(py, inner_ref).unwrap();
            let pickled_args = pickle_dumps(py, args_ref).unwrap();
            (pickled_function, pickled_args)
        });

        let num_cores = num_cpus::get();
        let pool = WorkerPool::new(num_cores, pickled_function, pickled_args);

        (0..num_rows).into_par_iter().for_each(|i| {
            let mut serde_args = Vec::with_capacity(args.len());
            for s in args {
                let idx = if s.len() == 1 { 0 } else { i };
                let lit = s.get_lit(idx);
                serde_args.push(lit);
            }
            pool.process(i, serde_args);
        });

        let results = pool.get_results();
        let results: Series = results.try_into().unwrap();
        pool.shutdown();

        Ok((
            results.rename(name).cast(&self.return_dtype).unwrap(),
            gil_contention_time,
        ))
    }
}

pub struct WorkerPool {
    senders: Vec<Sender<(usize, Vec<Literal>)>>,
    results: Arc<Mutex<Vec<(usize, Literal)>>>,
    worker_threads: Vec<thread::JoinHandle<()>>,
    pending_tasks: Arc<AtomicUsize>,
    total_tasks: Arc<AtomicUsize>,
}

impl WorkerPool {
    pub fn new(num_workers: usize, python_function: Vec<u8>, bound_args: Vec<u8>) -> Self {
        let results = Arc::new(Mutex::new(Vec::new()));
        let pending_tasks = Arc::new(AtomicUsize::new(0));
        let total_tasks = Arc::new(AtomicUsize::new(0));
        let mut senders = Vec::with_capacity(num_workers);
        let mut worker_threads = Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            let (tx, rx) = bounded(100); // Bounded channel with capacity
            senders.push(tx);

            let results = Arc::clone(&results);
            let python_function = python_function.clone();
            let bound_args = bound_args.clone();
            let pending = Arc::clone(&pending_tasks);

            let handle = thread::spawn(move || {
                Self::worker_process_loop(
                    worker_id,
                    rx,
                    results,
                    pending,
                    python_function,
                    bound_args,
                );
            });

            worker_threads.push(handle);
        }

        WorkerPool {
            senders,
            results,
            worker_threads,
            pending_tasks,
            total_tasks,
        }
    }

    fn worker_process_loop(
        worker_id: usize,
        receiver: Receiver<(usize, Vec<Literal>)>,
        results: Arc<Mutex<Vec<(usize, Literal)>>>,
        pending_tasks: Arc<AtomicUsize>,
        python_function: Vec<u8>,
        args: Vec<u8>,
    ) {
        let base64_engine = base64::engine::general_purpose::STANDARD;
        let f = base64_engine.encode(&python_function);
        let args = base64_engine.encode(&args);

        // 4. Use a more efficient base64 engine
        // Launch Python process
        let mut child = Command::new("python3")
            .arg("-u")
            .arg("-m")
            .arg("daft.execution.scalar_udf_worker")
            .arg(&f)
            .arg(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("Failed to start Python worker process");

        let mut stdin = child.stdin.take().expect("Failed to open stdin");
        let stdout = child.stdout.take().expect("Failed to open stdout");
        let mut stdout_reader = BufReader::new(stdout);

        while let Ok((batch_idx, batch)) = receiver.recv() {
            let serialized_batch: Vec<u8> = bincode::serialize(&batch).unwrap();

            let request = base64_engine.encode(&serialized_batch);
            match writeln!(stdin, "{}", request) {
                Ok(_) => match stdin.flush() {
                    Ok(_) => {
                        let mut response = String::new();
                        stdout_reader
                            .read_line(&mut response)
                            .expect("Failed to read from Python process");

                        let response = response.trim();

                        let result = base64_engine.decode(response).unwrap_or_default();
                        let result: Literal =
                            bincode::deserialize(&result).unwrap_or_else(|_| Literal::Null);
                        let mut results_guard = results.lock().unwrap();
                        results_guard.push((batch_idx, result));

                        pending_tasks.fetch_sub(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        eprintln!(
                            "Worker {}: Failed to flush stdin for batch {}: {:?}",
                            worker_id, batch_idx, e
                        );
                    }
                },
                Err(e) => {
                    eprintln!(
                        "Worker {}: Failed to flush stdin for batch {}: {:?}",
                        worker_id, batch_idx, e
                    );
                }
            };
        }
        eprintln!("Worker {}: All batches processed, sending EXIT", worker_id);

        // Graceful shutdown:
        // 1. Try to send EXIT command
        let exit_result = writeln!(stdin, "EXIT");
        eprintln!(
            "Worker {}: EXIT command result: {:?}",
            worker_id, exit_result
        );
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(100);

        // Check if process exited
        let mut exited = false;
        while start.elapsed() < timeout {
            match child.try_wait() {
                Ok(Some(_)) => {
                    // Process already exited
                    exited = true;
                    break;
                }
                _ => {
                    // Process still running, sleep a bit and check again
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
        }

        // If process didn't exit within timeout, force kill
        if !exited {
            let _ = child.kill();
        }

        // Wait for process to fully terminate
        let _ = child.wait();
    }
    pub fn process(&self, batch_idx: usize, literals: Vec<Literal>) {
        self.pending_tasks.fetch_add(1, Ordering::SeqCst);
        self.total_tasks.fetch_add(1, Ordering::SeqCst);

        let sender_idx = batch_idx % self.senders.len();
        self.senders[sender_idx]
            .send((batch_idx, literals))
            .unwrap();
    }

    pub fn get_results(&self) -> Vec<Literal> {
        loop {
            let n_pending = self.pending_tasks.load(Ordering::SeqCst);
            if n_pending == 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let results_guard = self.results.lock().unwrap();
        let mut sorted_results = results_guard.clone();
        sorted_results.sort_by_key(|(idx, _)| *idx);

        let total = self.total_tasks.load(Ordering::SeqCst);
        assert_eq!(sorted_results.len(), total, "Missing results");

        sorted_results.into_iter().map(|(_, lit)| lit).collect()
    }

    pub fn shutdown(self) {
        drop(self.senders);

        for handle in self.worker_threads {
            handle.join().unwrap();
        }
    }
}
