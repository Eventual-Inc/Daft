use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    task::{Context, Poll},
};

use common_error::{DaftError, DaftResult};

pub(crate) type JoinSetId = tokio::task::Id;
#[derive(Debug)]
pub(crate) struct JoinSet<T> {
    inner: tokio::task::JoinSet<T>,
}

impl<T: Send + 'static> JoinSet<T> {
    pub fn new() -> Self {
        Self {
            inner: tokio::task::JoinSet::new(),
        }
    }

    pub fn spawn<F>(&mut self, task: F) -> JoinSetId
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
        // Bounds from impl:
        T: 'static,
    {
        let handle = self.inner.spawn(task);
        handle.id()
    }

    pub async fn join_next(&mut self) -> Option<DaftResult<T>> {
        let res = self.inner.join_next().await;
        match res {
            Some(Ok(result)) => Some(Ok(result)),
            Some(Err(e)) => Some(Err(DaftError::External(e.into()))),
            None => None,
        }
    }

    pub fn poll_join_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<DaftResult<T>>> {
        let res = self.inner.poll_join_next(cx);
        match res {
            Poll::Ready(Some(Ok(result))) => Poll::Ready(Some(Ok(result))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(DaftError::External(e.into())))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    pub async fn join_next_with_id(&mut self) -> Option<(tokio::task::Id, DaftResult<T>)> {
        let res = self.inner.join_next_with_id().await;
        match res {
            Some(Ok((id, result))) => Some((id, Ok(result))),
            Some(Err(e)) => Some((e.id(), Err(DaftError::External(e.into())))),
            None => None,
        }
    }

    pub fn try_join_next_with_id(&mut self) -> Option<(JoinSetId, DaftResult<T>)> {
        let res = self.inner.try_join_next_with_id();
        match res {
            Some(Ok((id, result))) => Some((id, Ok(result))),
            Some(Err(e)) => Some((e.id(), Err(DaftError::External(e.into())))),
            None => None,
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T: Send + 'static> From<tokio::task::JoinSet<T>> for JoinSet<T> {
    fn from(inner: tokio::task::JoinSet<T>) -> Self {
        Self { inner }
    }
}

pub(crate) fn create_join_set<T: Send + 'static>() -> JoinSet<T> {
    JoinSet::new()
}

#[allow(dead_code)]
pub(crate) struct OrderedJoinSet<T> {
    join_set: JoinSet<T>,
    order: VecDeque<tokio::task::Id>,
    finished: HashMap<tokio::task::Id, DaftResult<T>>,
}

#[allow(dead_code)]
impl<T: Send + 'static> OrderedJoinSet<T> {
    pub fn new() -> Self {
        Self {
            join_set: create_join_set(),
            order: VecDeque::new(),
            finished: HashMap::new(),
        }
    }

    pub fn spawn(&mut self, task: impl Future<Output = T> + Send + 'static) {
        let id = self.join_set.spawn(task);
        self.order.push_back(id);
    }

    pub async fn join_next(&mut self) -> Option<DaftResult<T>> {
        // If the order is empty, return None
        let id = self.order.front()?;

        // If the task is already finished, return the result
        if let Some(result) = self.finished.remove(id) {
            self.order.pop_front();
            return Some(result);
        }

        // Keep joining tasks until the next task is the one we are looking for
        while let Some(result) = self.join_set.join_next_with_id().await {
            let (next_id, result) = result;
            if next_id == *id {
                self.order.pop_front();
                return Some(result);
            }
            self.finished.insert(next_id, result);
        }
        unreachable!(
            "OrderedJoinSet::join_next should never return None if the order is not empty"
        );
    }

    pub fn num_pending(&self) -> usize {
        self.join_set.len() + self.finished.len()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::Rng;
    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn test_ordered_joinset_basic() {
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
    async fn test_ordered_joinset_basic_out_of_order() {
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
    async fn test_ordered_joinset_large_out_of_order() {
        let mut join_set = OrderedJoinSet::new();

        // Spawn multiple tasks with different delays
        for i in 0..1000000 {
            join_set.spawn(async move {
                // random sleep between 0 and 1000ms
                let sleep_duration = rand::thread_rng().gen_range(0..1000);
                sleep(Duration::from_millis(sleep_duration)).await;
                i
            });
        }

        // Join all tasks
        let mut count = 0;
        while let Some(result) = join_set.join_next().await {
            assert_eq!(result.unwrap(), count);
            count += 1;
        }
    }

    #[tokio::test]
    async fn test_ordered_joinset_basic_error_handling() {
        let mut join_set = OrderedJoinSet::<i32>::new();

        // Spawn a task that panics
        join_set.spawn(async {
            panic!("test panic");
        });

        // Verify error is propagated
        assert!(join_set.join_next().await.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_ordered_joinset_basic_mixed_success_error() {
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

    #[tokio::test]
    async fn test_ordered_joinset_basic_mixed_success_error_out_of_order() {
        let mut join_set = OrderedJoinSet::new();

        // Spawn a mix of successful and failing tasks

        join_set.spawn(async {
            sleep(Duration::from_millis(100)).await;
            1
        });
        join_set.spawn(async {
            sleep(Duration::from_millis(50)).await;
            panic!("test panic");
        });
        join_set.spawn(async {
            sleep(Duration::from_millis(200)).await;
            3
        });

        // First task should succeed
        assert_eq!(join_set.join_next().await.unwrap().unwrap(), 1);
        // Second task should fail
        assert!(join_set.join_next().await.unwrap().is_err());
        // Third task should succeed
        assert_eq!(join_set.join_next().await.unwrap().unwrap(), 3);
        assert!(join_set.join_next().await.is_none());
    }
}
