use std::{
    fmt::{Display, Formatter},
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, OnceLock,
    },
};

use common_error::{DaftError, DaftResult};
use futures::FutureExt;
use lazy_static::lazy_static;
use tokio::{runtime::RuntimeFlavor, task::JoinHandle};

static THREADED_IO_RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();
static SINGLE_THREADED_IO_RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();
static COMPUTE_RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();

lazy_static! {
    static ref NUM_CPUS: usize = std::thread::available_parallelism().unwrap().get();
    static ref THREADED_IO_RUNTIME_NUM_WORKER_THREADS: usize = 8.min(*NUM_CPUS);
    static ref COMPUTE_RUNTIME_NUM_WORKER_THREADS: usize = *NUM_CPUS;
    static ref COMPUTE_RUNTIME_MAX_BLOCKING_THREADS: usize = 1; // Compute thread should not use blocking threads, limit this to the minimum, i.e. 1
}

#[derive(Clone, Copy)]
enum PoolType {
    Compute,
    IO,
}

impl Display for PoolType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolType::Compute => write!(f, "Compute"),
            PoolType::IO => write!(f, "IO"),
        }
    }
}

pub type RuntimeRef = Arc<Runtime>;

pub struct Runtime {
    pool_type: PoolType,
    runtime: tokio::runtime::Runtime,
}

impl Runtime {
    fn new(runtime: tokio::runtime::Runtime, pool_type: PoolType) -> RuntimeRef {
        Arc::new(Self { runtime, pool_type })
    }

    async fn execute_task<F>(future: F, pool_type: PoolType) -> DaftResult<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        AssertUnwindSafe(future).catch_unwind().await.map_err(|e| {
            let s = if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = e.downcast_ref::<&str>() {
                (*s).to_string()
            } else {
                "unknown internal error".to_string()
            };
            DaftError::ComputeError(format!(
                "Caught panic when spawning blocking task in {pool_type} pool {s})"
            ))
        })
    }

    /// Similar to tokio's Runtime::block_on but requires static lifetime + Send
    /// You should use this when you are spawning IO tasks from an Expression Evaluator or in the Executor
    pub fn block_on<F>(&self, future: F) -> DaftResult<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let pool_type = self.pool_type;
        let _join_handle = self.spawn(async move {
            let task_output = Self::execute_task(future, pool_type).await;
            if tx.send(task_output).is_err() {
                log::warn!("Spawned task output ignored: receiver dropped");
            }
        });
        rx.recv().expect("Spawned task transmitter dropped")
    }

    /// Similar to block_on, but is async and can be awaited
    pub async fn await_on<F>(&self, future: F) -> DaftResult<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let pool_type = self.pool_type;
        let _join_handle = self.spawn(async move {
            let task_output = Self::execute_task(future, pool_type).await;
            if tx.send(task_output).is_err() {
                log::warn!("Spawned task output ignored: receiver dropped");
            }
        });
        rx.await.expect("Spawned task transmitter dropped")
    }

    /// Blocks current thread to compute future. Can not be called in tokio runtime context
    ///
    pub fn block_on_current_thread<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.spawn(future)
    }
}

fn init_runtime(
    num_worker_threads: usize,
    max_blocking_threads: Option<usize>,
    pool_type: PoolType,
) -> Arc<Runtime> {
    std::thread::spawn(move || {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .worker_threads(num_worker_threads)
            .enable_all()
            .thread_name_fn(move || match pool_type {
                PoolType::Compute => {
                    static COMPUTE_THREAD_ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                    let id = COMPUTE_THREAD_ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                    format!("Compute-Thread-{}", id)
                }
                PoolType::IO => {
                    static IO_THREAD_ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                    let id = IO_THREAD_ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                    format!("IO-Thread-{}", id)
                }
            });
        if let Some(max_blocking_threads) = max_blocking_threads {
            builder.max_blocking_threads(max_blocking_threads);
        }
        Runtime::new(builder.build().unwrap(), pool_type)
    })
    .join()
    .unwrap()
}

pub fn get_compute_runtime() -> DaftResult<RuntimeRef> {
    let runtime = COMPUTE_RUNTIME
        .get_or_init(|| {
            init_runtime(
                *COMPUTE_RUNTIME_NUM_WORKER_THREADS,
                Some(*COMPUTE_RUNTIME_MAX_BLOCKING_THREADS),
                PoolType::Compute,
            )
        })
        .clone();
    Ok(runtime)
}

pub fn get_io_runtime(multi_thread: bool) -> DaftResult<RuntimeRef> {
    if !multi_thread {
        let runtime = SINGLE_THREADED_IO_RUNTIME
            .get_or_init(|| init_runtime(1, None, PoolType::IO))
            .clone();
        Ok(runtime)
    } else {
        let runtime = THREADED_IO_RUNTIME
            .get_or_init(|| {
                init_runtime(*THREADED_IO_RUNTIME_NUM_WORKER_THREADS, None, PoolType::IO)
            })
            .clone();
        Ok(runtime)
    }
}

#[must_use]
pub fn get_io_pool_num_threads() -> Option<usize> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            match handle.runtime_flavor() {
                RuntimeFlavor::CurrentThread => Some(1),
                RuntimeFlavor::MultiThread => Some(*THREADED_IO_RUNTIME_NUM_WORKER_THREADS),
                // RuntimeFlavor is #non_exhaustive, so we default to 1 here to be conservative
                _ => Some(1),
            }
        }
        Err(_) => None,
    }
}
