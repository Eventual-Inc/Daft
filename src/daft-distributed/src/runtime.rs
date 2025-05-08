use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{Once, OnceLock},
};

use common_error::{DaftError, DaftResult};
use common_runtime::{PoolType, Runtime, RuntimeRef};
use tokio::task::{Id, JoinError};

pub static RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();
pub static PYO3_RUNTIME_INITIALIZED: Once = Once::new();

pub fn get_or_init_runtime() -> &'static Runtime {
    let runtime_ref = RUNTIME.get_or_init(|| {
        let mut tokio_runtime_builder = tokio::runtime::Builder::new_multi_thread();
        tokio_runtime_builder.enable_all();
        tokio_runtime_builder.worker_threads(1);
        tokio_runtime_builder.thread_name_fn(move || "Daft-Scheduler".to_string());
        let tokio_runtime = tokio_runtime_builder
            .build()
            .expect("Failed to build runtime");
        Runtime::new(
            tokio_runtime,
            PoolType::Custom("daft-scheduler".to_string()),
        )
    });
    #[cfg(feature = "python")]
    {
        PYO3_RUNTIME_INITIALIZED.call_once(|| {
            pyo3_async_runtimes::tokio::init_with_runtime(&runtime_ref.runtime)
                .expect("Failed to initialize python runtime");
        });
    }
    runtime_ref
}

pub type JoinSet<T> = tokio::task::JoinSet<T>;

#[allow(dead_code)]
pub fn create_join_set<T>() -> JoinSet<T> {
    tokio::task::JoinSet::new()
}

pub(crate) struct OrderedJoinSet<T> {
    join_set: JoinSet<T>,
    order: VecDeque<Id>,
    finished: HashMap<Id, T>,
}

impl<T: Send + 'static> OrderedJoinSet<T> {
    pub fn new() -> Self {
        Self {
            join_set: create_join_set(),
            order: VecDeque::new(),
            finished: HashMap::new(),
        }
    }

    pub fn spawn(&mut self, task: impl Future<Output = T> + Send + 'static) {
        let abort_handle = self.join_set.spawn(task);
        let id = abort_handle.id();
        self.order.push_back(id);
    }

    pub async fn join_next(&mut self) -> Option<DaftResult<T>> {
        let id = self.order.front()?;
        if let Some(result) = self.finished.remove(&id) {
            self.order.pop_front();
            return Some(Ok(result));
        }
        while let Some(result) = self.join_set.join_next_with_id().await {
            if let Err(e) = result {
                return Some(Err(DaftError::External(e.into())));
            }
            let (next_id, result) = result.unwrap();
            if next_id == *id {
                self.order.pop_front();
                return Some(Ok(result));
            }
            self.finished.insert(next_id, result);
        }
        None
    }

    pub fn num_pending(&self) -> usize {
        self.join_set.len() + self.order.len()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn test_ordered_join_set_basic() {
        let mut join_set = OrderedJoinSet::new();

        // Spawn tasks in order
        join_set.spawn(async { 1 });
        join_set.spawn(async { 2 });
        join_set.spawn(async { 3 });

        // Verify results come back in order
        assert_eq!(join_set.join_next().await.unwrap().unwrap(), 1);
        assert_eq!(join_set.join_next().await.unwrap().unwrap(), 2);
        assert_eq!(join_set.join_next().await.unwrap().unwrap(), 3);
        assert!(join_set.join_next().await.is_none());
    }

    #[tokio::test]
    async fn test_ordered_join_set_out_of_order() {
        let mut join_set = OrderedJoinSet::new();

        // Spawn tasks with different delays
        join_set.spawn(async {
            sleep(Duration::from_millis(100)).await;
            1
        });
        join_set.spawn(async {
            sleep(Duration::from_millis(50)).await;
            2
        });
        join_set.spawn(async {
            sleep(Duration::from_millis(200)).await;
            3
        });

        // Even though tasks complete out of order, results should come back in order
        assert_eq!(join_set.join_next().await.unwrap().unwrap(), 1);
        assert_eq!(join_set.join_next().await.unwrap().unwrap(), 2);
        assert_eq!(join_set.join_next().await.unwrap().unwrap(), 3);
        assert!(join_set.join_next().await.is_none());
    }

    #[tokio::test]
    async fn test_ordered_join_set_error_handling() {
        let mut join_set = OrderedJoinSet::<i32>::new();

        // Spawn a task that panics
        join_set.spawn(async {
            panic!("test panic");
        });

        // Verify error is propagated
        assert!(join_set.join_next().await.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_ordered_join_set_mixed_success_error() {
        let mut join_set = OrderedJoinSet::new();

        // Spawn a mix of successful and failing tasks
        join_set.spawn(async { 1 });
        join_set.spawn(async {
            panic!("test panic");
        });
        join_set.spawn(async { 3 });

        // First task should succeed
        assert_eq!(join_set.join_next().await.unwrap().unwrap(), 1);
        // Second task should fail
        assert!(join_set.join_next().await.unwrap().is_err());
        // Third task should succeed
        assert_eq!(join_set.join_next().await.unwrap().unwrap(), 3);
        assert!(join_set.join_next().await.is_none());
    }
}
