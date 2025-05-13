use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use common_error::DaftResult;
use common_partitioning::{Partition, PartitionRef};
use tokio::sync::Mutex;

use crate::{
    scheduling::{
        dispatcher::TaskDispatcher,
        task::{SchedulingStrategy, Task, TaskId},
        tests::{MockPartition, MockTaskBuilder, MockWorkerManager},
        worker::{WorkerId, WorkerManager},
    },
    utils::joinset::JoinSet,
};

// Helper to create a mock partition
fn create_mock_partition(num_rows: usize, size_bytes: usize) -> PartitionRef {
    Arc::new(MockPartition::new(num_rows, size_bytes))
}

// Test basic task dispatch and execution
#[tokio::test]
async fn test_basic_task_dispatch() -> DaftResult<()> {
    // Create a mock worker manager with one worker
    let mut worker_manager = Box::new(MockWorkerManager::new());
    worker_manager.add_worker("worker1".to_string(), 4)?;

    // Create the task dispatcher
    let task_dispatcher = TaskDispatcher::new(worker_manager);

    // Spawn the dispatcher
    let mut joinset = JoinSet::new();
    let handle = TaskDispatcher::spawn_task_dispatcher(task_dispatcher, &mut joinset);

    // Create a task with Spread strategy using the builder
    let partition = create_mock_partition(100, 1024);
    let task = MockTaskBuilder::new(SchedulingStrategy::Spread, partition.clone()).build();

    // Submit the task
    let submitted_task = handle.submit_task(task).await?;

    // Wait for the task to complete and get the result
    let result = submitted_task.await;

    // Verify that we got a result
    assert!(result.is_some());

    // Verify the task result
    if let Some(Ok(mut result_partitions)) = result {
        assert_eq!(result_partitions.len(), 1);
        let result_partition = result_partitions.pop().expect("expected partition");
        let mock_partition = result_partition
            .as_any()
            .downcast_ref::<MockPartition>()
            .expect("Expected MockPartition");

        let num_rows = mock_partition.num_rows()?;
        assert_eq!(num_rows, 100);

        let size_bytes = mock_partition.size_bytes()?;
        assert_eq!(size_bytes, Some(1024));
    } else {
        panic!("Task did not return a valid result");
    }

    drop(handle);
    while let Some(result) = joinset.join_next().await {
        result.unwrap()?;
    }
    Ok(())
}

// Test multiple tasks dispatched concurrently
#[tokio::test]
async fn test_multiple_concurrent_tasks() -> DaftResult<()> {
    // Create a mock worker manager with two workers
    let mut worker_manager = MockWorkerManager::new();
    worker_manager.add_worker("worker1".to_string(), 4)?;
    worker_manager.add_worker("worker2".to_string(), 4)?;

    // Create the task dispatcher
    let task_dispatcher = TaskDispatcher::new(Box::new(worker_manager));

    // Spawn the dispatcher
    let mut joinset = JoinSet::new();
    let handle = TaskDispatcher::spawn_task_dispatcher(task_dispatcher, &mut joinset);

    // Create and submit multiple tasks concurrently
    let mut tasks = vec![];
    for i in 0..5 {
        let partition = create_mock_partition(100 + i, 1024 + i * 100);
        let task = MockTaskBuilder::new(SchedulingStrategy::Spread, partition.clone())
            .with_task_id(format!("task-{}", i))
            .build();

        // Submit the task
        let submitted_task = handle.submit_task(task).await?;
        tasks.push(submitted_task);
    }

    // Wait for all tasks to complete and verify results
    for (i, task) in tasks.into_iter().enumerate() {
        let result = task.await;

        assert!(result.is_some());
        if let Some(Ok(mut result_partitions)) = result {
            assert_eq!(result_partitions.len(), 1);
            let result_partition = result_partitions.pop().expect("expected partition");
            let mock_partition = result_partition
                .as_any()
                .downcast_ref::<MockPartition>()
                .expect("Expected MockPartition");

            let num_rows = mock_partition.num_rows()?;
            assert_eq!(num_rows, 100 + i);

            let size_bytes = mock_partition.size_bytes()?;
            assert_eq!(size_bytes, Some(1024 + i * 100));
        } else {
            panic!("Task {} did not return a valid result", i);
        }
    }

    drop(handle);
    while let Some(result) = joinset.join_next().await {
        result.unwrap()?;
    }

    Ok(())
}

// Test task cancellation
#[tokio::test]
async fn test_task_cancellation() -> DaftResult<()> {
    // Create a mock worker manager with one worker
    let mut worker_manager = MockWorkerManager::new();
    worker_manager.add_worker("worker1".to_string(), 1)?;

    // Create the task dispatcher
    let task_dispatcher = TaskDispatcher::new(Box::new(worker_manager));

    // Spawn the dispatcher
    let mut joinset = JoinSet::new();
    let handle = TaskDispatcher::spawn_task_dispatcher(task_dispatcher, &mut joinset);

    // Create a cancel marker to track cancellation
    let cancel_marker = Arc::new(AtomicBool::new(false));

    // Create a task with the cancel marker
    let partition = create_mock_partition(100, 1024);
    let task = MockTaskBuilder::new(SchedulingStrategy::Spread, partition.clone())
        .with_cancel_marker(cancel_marker.clone())
        .with_sleep_duration(Duration::from_millis(500))
        .build();

    // Submit the task
    let submitted_task = handle.submit_task(task).await?;

    // Drop the task without awaiting it, which should trigger cancellation
    drop(submitted_task);

    // Short delay to allow cancellation to propagate
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify the task was actually cancelled
    assert!(
        cancel_marker.load(Ordering::SeqCst),
        "Task was not cancelled"
    );
    println!("Task was cancelled");

    // Submit another task and verify it works
    let partition = create_mock_partition(200, 2048);
    let task = MockTaskBuilder::new(SchedulingStrategy::Spread, partition.clone()).build();

    println!("Submitting new task");
    let submitted_task = handle.submit_task(task).await?;
    println!("Submitted new task");
    // Verify the new task completes successfully
    let result = submitted_task.await;
    assert!(result.is_some());

    println!("Dropping handle");
    drop(handle);
    while let Some(result) = joinset.join_next().await {
        result.unwrap()?;
    }

    Ok(())
}

// Test task scheduling with node affinity
#[tokio::test]
async fn test_task_with_node_affinity() -> DaftResult<()> {
    // Create a mock worker manager with three workers
    let mut worker_manager = MockWorkerManager::new();
    worker_manager.add_worker("worker1".to_string(), 2)?;
    worker_manager.add_worker("worker2".to_string(), 2)?;
    worker_manager.add_worker("worker3".to_string(), 2)?;

    // Create the task dispatcher
    let task_dispatcher = TaskDispatcher::new(Box::new(worker_manager));

    // Spawn the dispatcher
    let mut joinset = JoinSet::new();
    let handle = TaskDispatcher::spawn_task_dispatcher(task_dispatcher, &mut joinset);

    // Create a task with node affinity to worker2
    let partition = create_mock_partition(100, 1024);
    let task = MockTaskBuilder::new(
        SchedulingStrategy::NodeAffinity {
            node_id: "worker2".to_string(),
            soft: false,
        },
        partition.clone(),
    )
    .build();

    // Submit the task
    let submitted_task = handle.submit_task(task).await?;

    // Verify task completes successfully
    let result = submitted_task.await;
    assert!(result.is_some());

    Ok(())
}

// Test dispatcher behavior when all workers are busy
#[tokio::test]
async fn test_dispatcher_with_busy_workers() -> DaftResult<()> {
    // Create a custom worker manager that can track task submissions
    #[derive(Clone)]
    struct TrackingWorkerManager {
        inner: MockWorkerManager,
        task_count: Arc<Mutex<usize>>,
    }

    impl TrackingWorkerManager {
        fn new() -> Self {
            Self {
                inner: MockWorkerManager::new(),
                task_count: Arc::new(Mutex::new(0)),
            }
        }

        async fn add_worker(&mut self, worker_id: WorkerId, num_cpus: usize) -> DaftResult<()> {
            self.inner.add_worker(worker_id, num_cpus)
        }

        async fn set_worker_full(&mut self, worker_id: &WorkerId) -> DaftResult<()> {
            if let Some(worker_mut) = self.inner.workers.get_mut(worker_id) {
                worker_mut.num_active_tasks = worker_mut.num_cpus;
            }
            Ok(())
        }
    }

    impl WorkerManager for TrackingWorkerManager {
        type Worker = crate::scheduling::tests::MockWorker;

        fn submit_task_to_worker(
            &mut self,
            task: Box<dyn Task>,
            worker_id: WorkerId,
        ) -> Box<dyn crate::scheduling::task::SwordfishTaskResultHandle> {
            // Increment task count
            let task_count = self.task_count.clone();
            tokio::spawn(async move {
                let mut count = task_count.lock().await;
                *count += 1;
            });

            self.inner.submit_task_to_worker(task, worker_id)
        }

        fn mark_task_finished(&mut self, task_id: TaskId, worker_id: WorkerId) {
            self.inner.mark_task_finished(task_id, worker_id);
        }

        fn workers(&self) -> &HashMap<WorkerId, Self::Worker> {
            // Delegate to inner's workers method
            panic!("workers() method not implemented for TrackingWorkerManager");
        }

        fn total_available_cpus(&self) -> usize {
            self.inner.total_available_cpus()
        }

        fn try_autoscale(&self, num_workers: usize) -> DaftResult<()> {
            self.inner.try_autoscale(num_workers)
        }

        fn shutdown(&mut self) -> DaftResult<()> {
            self.inner.shutdown()
        }
    }

    // Create tracking worker manager with one worker
    let mut worker_manager = TrackingWorkerManager::new();
    worker_manager.add_worker("worker1".to_string(), 2).await?;

    // Set the worker as full
    worker_manager
        .set_worker_full(&"worker1".to_string())
        .await?;

    // Create and spawn the task dispatcher
    let task_dispatcher = TaskDispatcher::new(Box::new(worker_manager.clone()));
    let mut joinset = JoinSet::new();
    let handle = TaskDispatcher::spawn_task_dispatcher(task_dispatcher, &mut joinset);

    // Submit a task using the builder
    let partition = create_mock_partition(100, 1024);
    let task = MockTaskBuilder::new(SchedulingStrategy::Spread, partition.clone()).build();

    let submitted_task = handle.submit_task(task).await?;

    // Free up a worker
    if let Some(worker) = worker_manager.inner.workers.get_mut("worker1") {
        worker.num_active_tasks = 0;
    }

    // Wait for the task to complete
    let result = submitted_task.await;
    assert!(result.is_some());

    // Verify task count
    let task_count = *worker_manager.task_count.lock().await;
    assert_eq!(task_count, 1);

    Ok(())
}

// Test handling of multiple tasks through one handle
#[tokio::test]
async fn test_dispatcher_handle() -> DaftResult<()> {
    // Create a mock worker manager with one worker
    let mut worker_manager = MockWorkerManager::new();
    worker_manager.add_worker("worker1".to_string(), 4)?;

    // Create the task dispatcher
    let task_dispatcher = TaskDispatcher::new(Box::new(worker_manager));

    // Spawn the dispatcher
    let mut joinset = JoinSet::new();
    let handle = TaskDispatcher::spawn_task_dispatcher(task_dispatcher, &mut joinset);

    // Submit tasks through the handle using the builder
    let partition1 = create_mock_partition(100, 1024);
    let task1 = MockTaskBuilder::new(SchedulingStrategy::Spread, partition1.clone())
        .with_task_id("task-1".to_string())
        .build();

    let submitted_task1 = handle.submit_task(task1).await?;

    // Create another task with different parameters
    let partition2 = create_mock_partition(200, 2048);
    let task2 = MockTaskBuilder::new(SchedulingStrategy::Spread, partition2.clone())
        .with_task_id("task-2".to_string())
        .build();

    let submitted_task2 = handle.submit_task(task2).await?;

    // Wait for both tasks and verify results
    let result1 = submitted_task1.await;
    let result2 = submitted_task2.await;

    assert!(result1.is_some());
    assert!(result2.is_some());

    Ok(())
}
