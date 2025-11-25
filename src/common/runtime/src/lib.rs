use std::{
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        Arc, LazyLock, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use common_error::{DaftError, DaftResult};
use futures::FutureExt;
use tokio::{
    runtime::{Handle, RuntimeFlavor},
    task::JoinSet,
};

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "python")]
pub use python::execute_python_coroutine;

static NUM_CPUS: LazyLock<usize> =
    LazyLock::new(|| std::thread::available_parallelism().unwrap().get());
static THREADED_IO_RUNTIME_NUM_WORKER_THREADS: LazyLock<usize> = LazyLock::new(|| 8.min(*NUM_CPUS));
static COMPUTE_RUNTIME_NUM_WORKER_THREADS: OnceLock<usize> = OnceLock::new();

pub fn get_or_init_compute_runtime_num_worker_threads() -> usize {
    *COMPUTE_RUNTIME_NUM_WORKER_THREADS.get_or_init(|| *NUM_CPUS)
}

pub fn set_compute_runtime_num_worker_threads(num_threads: usize) -> DaftResult<()> {
    COMPUTE_RUNTIME_NUM_WORKER_THREADS
        .set(num_threads)
        .map_err(|_| {
            DaftError::InternalError("Compute runtime num worker threads already set".to_string())
        })
}

static THREADED_IO_RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();
static SINGLE_THREADED_IO_RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();
static COMPUTE_RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();

pub type RuntimeRef = Arc<Runtime>;

#[derive(Clone, Debug)]
pub enum PoolType {
    Compute,
    IO,
    Custom(String),
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

impl<T> std::fmt::Debug for RuntimeTask<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RuntimeTask(num_inflight_tasks={})", self.joinset.len())
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Runtime {
    pub runtime: Arc<tokio::runtime::Runtime>,
    pool_type: PoolType,
}

impl Runtime {
    pub fn new(runtime: tokio::runtime::Runtime, pool_type: PoolType) -> RuntimeRef {
        Arc::new(Self {
            runtime: Arc::new(runtime),
            pool_type,
        })
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
    /// You should use this when you need to run an async task in a synchronous function, but you are already in a tokio runtime.
    ///
    /// For example, URL download is an async function, but it is called from a synchronous function in a tokio runtime,
    /// i.e. calling the Expression Evaluator from the Native Executor.
    ///
    /// Caution:
    /// If the tokio runtimes of the synchronous function and the asynchronous function are same, that can potentially cause
    /// a deadlock, so the caller needs to be careful to avoid nested calls that can cause this situation.
    ///
    /// Also, the calling runtime thread will not do any other work until this call returns, since it is blocked.
    ///
    /// In the future, we should refactor the code to be fully async, but for now, this is a workaround.
    pub fn block_within_async_context<F>(&self, future: F) -> DaftResult<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let pool_type = self.pool_type.clone();
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

    pub fn spawn_blocking<F, R>(&self, f: F) -> RuntimeTask<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        match self.pool_type {
            PoolType::Compute => {
                panic!("Cannot spawn blocking task on compute runtime from a non-compute thread");
            }
            PoolType::IO | PoolType::Custom(_) => {
                let mut join_set = JoinSet::new();
                join_set.spawn_blocking_on(f, self.runtime.handle());
                RuntimeTask { joinset: join_set }
            }
        }
    }
}

fn init_compute_runtime(num_worker_threads: usize) -> RuntimeRef {
    std::thread::spawn(move || {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .worker_threads(num_worker_threads)
            .enable_all()
            .thread_name_fn(move || {
                static COMPUTE_THREAD_ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = COMPUTE_THREAD_ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("DAFTCPU-{}", id)
            })
            .max_blocking_threads(1);
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
                static IO_THREAD_ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = IO_THREAD_ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("DAFTIO-{}", id)
            });
        Runtime::new(builder.build().unwrap(), PoolType::IO)
    })
    .join()
    .unwrap()
}

pub fn get_compute_runtime() -> RuntimeRef {
    COMPUTE_RUNTIME
        .get_or_init(|| init_compute_runtime(get_or_init_compute_runtime_num_worker_threads()))
        .clone()
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

pub fn get_compute_pool_num_threads() -> usize {
    get_or_init_compute_runtime_num_worker_threads()
}

// Helper function to combine a stream with a future that returns a result
pub fn combine_stream<T, E>(
    stream: impl futures::Stream<Item = Result<T, E>> + Unpin,
    future: impl Future<Output = Result<(), E>>,
) -> impl futures::Stream<Item = Result<T, E>> {
    use futures::{StreamExt, stream::unfold};

    let initial_state = (Some(future), stream);

    unfold(initial_state, |(mut future, mut stream)| async move {
        future.as_ref()?;

        match stream.next().await {
            Some(item) => Some((item, (future, stream))),
            None => match future.take().unwrap().await {
                Err(error) => Some((Err(error), (None, stream))),
                Ok(()) => None,
            },
        }
    })
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

    #[test]
    fn can_get_compute_runtime_after_setting_num_threads() {
        use super::*;

        set_compute_runtime_num_worker_threads(1).unwrap();
        let runtime = get_compute_runtime();
        assert!(runtime.runtime.metrics().num_workers() == 1);
    }
}
