pub mod compute;
pub mod io;

use std::sync::atomic::{AtomicUsize, Ordering};

use compute::{ComputeRuntime, ComputeRuntimeRef, COMPUTE_RUNTIME};
use io::{IORuntime, IORuntimeRef, SINGLE_THREADED_IO_RUNTIME, THREADED_IO_RUNTIME};
use lazy_static::lazy_static;
use tokio::runtime::RuntimeFlavor;

lazy_static! {
    static ref NUM_CPUS: usize = std::thread::available_parallelism().unwrap().get();
    static ref THREADED_IO_RUNTIME_NUM_WORKER_THREADS: usize = 8.min(*NUM_CPUS);
    static ref COMPUTE_RUNTIME_NUM_WORKER_THREADS: usize = *NUM_CPUS;
    static ref COMPUTE_RUNTIME_MAX_BLOCKING_THREADS: usize = 1; // Compute thread should not use blocking threads, limit this to the minimum, i.e. 1
}

fn init_compute_runtime() -> ComputeRuntimeRef {
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
        ComputeRuntime::new(builder.build().unwrap())
    })
    .join()
    .unwrap()
}

fn init_io_runtime(multi_thread: bool) -> IORuntimeRef {
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
        IORuntime::new(builder.build().unwrap())
    })
    .join()
    .unwrap()
}

pub fn get_compute_runtime() -> ComputeRuntimeRef {
    COMPUTE_RUNTIME.get_or_init(init_compute_runtime).clone()
}

pub fn get_io_runtime(multi_thread: bool) -> IORuntimeRef {
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
