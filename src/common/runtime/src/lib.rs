use std::{
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, OnceLock,
    },
    task::{Context, Poll},
};

use common_error::{DaftError, DaftResult};
use futures::FutureExt;
use lazy_static::lazy_static;
use tokio::{
    runtime::{Handle, RuntimeFlavor},
    task::JoinSet,
};

lazy_static! {
    static ref NUM_CPUS: usize = std::thread::available_parallelism().unwrap().get();
    static ref THREADED_IO_RUNTIME_NUM_WORKER_THREADS: usize = 8.min(*NUM_CPUS);
    static ref COMPUTE_RUNTIME_NUM_WORKER_THREADS: usize = *NUM_CPUS;
    static ref COMPUTE_RUNTIME_MAX_BLOCKING_THREADS: usize = 1; // Compute thread should not use blocking threads, limit this to the minimum, i.e. 1
}

static THREADED_IO_RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();
static SINGLE_THREADED_IO_RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();
static COMPUTE_RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();

pub type RuntimeRef = Arc<Runtime>;

#[derive(Debug, Clone, Copy)]
enum PoolType {
    Compute,
    IO,
}

// A spawned task on a Runtime that can be awaited
// This is a wrapper around a JoinSet that allows us to cancel the task by dropping it
pub struct RuntimeTask<T> {
    joinset: JoinSet<T>,
}

impl<T> RuntimeTask<T> {
    pub fn new<F>(handle: &Handle, future: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let mut joinset = JoinSet::new();
        joinset.spawn_on(future, handle);
        Self { joinset }
    }
}

impl<T: Send + 'static> Future for RuntimeTask<T> {
    type Output = DaftResult<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.joinset.poll_join_next(cx) {
            Poll::Ready(Some(result)) => {
                Poll::Ready(result.map_err(|e| DaftError::External(e.into())))
            }
            Poll::Ready(None) => panic!("JoinSet unexpectedly empty"),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct Runtime {
    runtime: tokio::runtime::Runtime,
    pool_type: PoolType,
}

impl Runtime {
    pub(crate) fn new(runtime: tokio::runtime::Runtime, pool_type: PoolType) -> RuntimeRef {
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
                "Caught panic when spawning blocking task in the {:?} runtime: {})",
                pool_type, s
            ))
        })
    }

    /// Spawns a task on the runtime and blocks the current thread until the task is completed.
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

    /// Blocks current thread to compute future. Can not be called in tokio runtime context
    ///
    pub fn block_on_current_thread<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    // Spawn a task on the runtime
    pub fn spawn<F>(&self, future: F) -> RuntimeTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        RuntimeTask::new(self.runtime.handle(), future)
    }
}

fn init_compute_runtime() -> RuntimeRef {
    std::thread::spawn(move || {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .worker_threads(*COMPUTE_RUNTIME_NUM_WORKER_THREADS)
            .enable_all()
            .thread_name_fn(move || {
                static COMPUTE_THREAD_ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = COMPUTE_THREAD_ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("Compute-Thread-{}", id)
            })
            .max_blocking_threads(*COMPUTE_RUNTIME_MAX_BLOCKING_THREADS);
        Runtime::new(builder.build().unwrap(), PoolType::Compute)
    })
    .join()
    .unwrap()
}

fn init_io_runtime(multi_thread: bool) -> RuntimeRef {
    std::thread::spawn(move || {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .worker_threads(if multi_thread {
                *THREADED_IO_RUNTIME_NUM_WORKER_THREADS
            } else {
                1
            })
            .enable_all()
            .thread_name_fn(move || {
                static COMPUTE_THREAD_ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = COMPUTE_THREAD_ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("IO-Thread-{}", id)
            });
        Runtime::new(builder.build().unwrap(), PoolType::IO)
    })
    .join()
    .unwrap()
}

pub fn get_compute_runtime() -> RuntimeRef {
    COMPUTE_RUNTIME.get_or_init(init_compute_runtime).clone()
}

pub fn get_io_runtime(multi_thread: bool) -> RuntimeRef {
    if !multi_thread {
        SINGLE_THREADED_IO_RUNTIME
            .get_or_init(|| init_io_runtime(false))
            .clone()
    } else {
        THREADED_IO_RUNTIME
            .get_or_init(|| init_io_runtime(true))
            .clone()
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

mod tests {

    #[tokio::test]
    async fn test_spawned_task_cancelled_when_dropped() {
        use super::*;

        let runtime = get_compute_runtime();
        let ptr = Arc::new(AtomicUsize::new(0));
        let ptr_clone = ptr.clone();

        // Spawn a task that just does work in a loop
        // The task should own a reference to the Arc, so the strong count should be 2
        let task = async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                ptr_clone.fetch_add(1, Ordering::SeqCst);
            }
        };
        let fut = runtime.spawn(task);
        assert!(Arc::strong_count(&ptr) == 2);

        // Drop the future, which should cancel the task
        drop(fut);

        // Wait for a while so that the task can be aborted
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        // The strong count should be 1 now
        assert!(Arc::strong_count(&ptr) == 1);
    }
}
