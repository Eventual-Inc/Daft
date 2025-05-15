use common_error::DaftResult;
use rand::{rngs::StdRng, Rng, SeedableRng};

use crate::{
    scheduling::{
        scheduler::DefaultScheduler,
        task::SchedulingStrategy,
        tests::{create_mock_partition_ref, MockTaskBuilder, MockTaskFailure, TestContext},
    },
    utils::channel::{create_channel, create_oneshot_channel},
};

// Test basic task dispatch and execution
#[tokio::test]
async fn test_task_dispatch_basic() -> DaftResult<()> {
    let test_context = TestContext::new(
        vec![("worker1".to_string(), 1)],
        Box::new(DefaultScheduler::new()),
    )?;

    let task = MockTaskBuilder::new(
        SchedulingStrategy::Spread,
        create_mock_partition_ref(100, 1024),
    )
    .build();

    let submitted_task = test_context.handle().submit_task(task).await?;
    let result = submitted_task.await.expect("Task should be completed")?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_rows().unwrap(), 100);
    assert_eq!(result[0].size_bytes().unwrap(), Some(1024));

    test_context.cleanup().await?;

    Ok(())
}

#[tokio::test]
async fn test_task_dispatch_large() -> DaftResult<()> {
    let mut test_context = TestContext::new(
        vec![("worker1".to_string(), 100)],
        Box::new(DefaultScheduler::new()),
    )?;

    let num_tasks = 1000;
    let (tx, mut rx) = create_channel(num_tasks);

    let handle = test_context.handle().clone();
    test_context.spawn_on_joinset(async move {
        let mut rng = StdRng::from_entropy();
        let tasks = (0..num_tasks)
            .map(|i| {
                MockTaskBuilder::new(
                    SchedulingStrategy::Spread,
                    create_mock_partition_ref(100 + i, 1024 * (i + 1)),
                )
                .with_task_id(format!("task-{}", i))
                .with_sleep_duration(std::time::Duration::from_millis(rng.gen_range(0..100)))
                .build()
            })
            .collect::<Vec<_>>();

        for task in tasks {
            let submitted_task = handle.submit_task(task).await?;
            tx.send(submitted_task).await.unwrap();
        }
        Ok(())
    });

    let mut count = 0;
    while let Some(submitted_task) = rx.recv().await {
        let result = submitted_task.await.expect("Task should be completed")?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows().unwrap(), 100 + count);
        assert_eq!(result[0].size_bytes().unwrap(), Some(1024 * (count + 1)));
        count += 1;
    }
    assert_eq!(count, num_tasks);

    test_context.cleanup().await?;

    Ok(())
}

#[tokio::test]
async fn test_task_dispatch_concurrent_large() -> DaftResult<()> {
    let mut test_context = TestContext::new(
        vec![("worker1".to_string(), 100)],
        Box::new(DefaultScheduler::new()),
    )?;

    let num_tasks_per_worker = 100;
    let num_concurrent_workers = 10;
    let (tx, mut rx) = create_channel(num_tasks_per_worker * num_concurrent_workers);

    for worker_id in 0..num_concurrent_workers {
        let tx = tx.clone();
        let handle = test_context.handle().clone();
        test_context.spawn_on_joinset(async move {
            let mut rng = StdRng::from_entropy();
            let tasks = (0..num_tasks_per_worker)
                .map(|i| {
                    MockTaskBuilder::new(
                        SchedulingStrategy::Spread,
                        create_mock_partition_ref(100 + i, 1024 * (i + 1)),
                    )
                    .with_task_id(format!("task-{}-from-{}", i, worker_id))
                    .with_sleep_duration(std::time::Duration::from_millis(rng.gen_range(0..100)))
                    .build()
                })
                .collect::<Vec<_>>();

            for (i, task) in tasks.into_iter().enumerate() {
                let submitted_task = handle.submit_task(task).await?;
                tx.send((i, submitted_task)).await.unwrap();
            }
            Ok(())
        });
    }
    drop(tx);

    let mut count = 0;
    while let Some((i, submitted_task)) = rx.recv().await {
        let result = submitted_task.await.expect("Task should be completed")?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows().unwrap(), 100 + i);
        assert_eq!(result[0].size_bytes().unwrap(), Some(1024 * (i + 1)));
        count += 1;
    }
    assert_eq!(count, num_tasks_per_worker * num_concurrent_workers);

    test_context.cleanup().await?;

    Ok(())
}

#[tokio::test]
async fn test_task_cancel_basic() -> DaftResult<()> {
    let test_context = TestContext::new(
        vec![("worker1".to_string(), 1)],
        Box::new(DefaultScheduler::new()),
    )?;

    let (cancel_tx, cancel_rx) = create_oneshot_channel();

    let task = MockTaskBuilder::new(
        SchedulingStrategy::Spread,
        create_mock_partition_ref(100, 1024),
    )
    .with_cancel_notifier(cancel_tx)
    .with_sleep_duration(std::time::Duration::from_secs(60))
    .build();

    let submitted_task = test_context.handle().submit_task(task).await?;
    drop(submitted_task);

    cancel_rx.await.unwrap();

    test_context.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_task_cancel_large() -> DaftResult<()> {
    let mut test_context = TestContext::new(
        vec![("worker1".to_string(), 100)],
        Box::new(DefaultScheduler::new()),
    )?;

    let num_tasks = 1000;
    let (tx, mut rx) = create_channel(num_tasks);
    let (cancel_txs, cancel_rxs): (Vec<_>, Vec<_>) =
        (0..num_tasks).map(|_| create_oneshot_channel()).unzip();

    let handle = test_context.handle().clone();
    test_context.spawn_on_joinset(async move {
        let tasks = cancel_txs
            .into_iter()
            .enumerate()
            .map(|(i, tx)| {
                MockTaskBuilder::new(
                    SchedulingStrategy::Spread,
                    create_mock_partition_ref(100, 1024),
                )
                .with_task_id(format!("task-{}", i))
                .with_sleep_duration(std::time::Duration::from_secs(60))
                .with_cancel_notifier(tx)
                .build()
            })
            .collect::<Vec<_>>();

        for task in tasks {
            let submitted_task = handle.submit_task(task).await?;
            tx.send(submitted_task).await.unwrap();
        }
        Ok(())
    });

    let mut count = 0;
    let mut rx_iter = cancel_rxs.into_iter();
    while let Some(submitted_task) = rx.recv().await {
        drop(submitted_task);
        let rx = rx_iter.next().unwrap();
        rx.await.unwrap();
        count += 1;
    }
    assert_eq!(count, num_tasks);

    test_context.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_task_error_basic() -> DaftResult<()> {
    let test_context = TestContext::new(
        vec![("worker1".to_string(), 1)],
        Box::new(DefaultScheduler::new()),
    )?;

    let task = MockTaskBuilder::new(
        SchedulingStrategy::Spread,
        create_mock_partition_ref(100, 1024),
    )
    .with_failure(MockTaskFailure::Error("test error".to_string()))
    .build();

    let submitted_task = test_context.handle().submit_task(task).await?;
    let result = submitted_task.await.expect("Task should be completed");
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap().to_string(),
        "DaftError::InternalError test error"
    );

    test_context.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_task_panic_basic() -> DaftResult<()> {
    let test_context = TestContext::new(
        vec![("worker1".to_string(), 1)],
        Box::new(DefaultScheduler::new()),
    )?;

    let task = MockTaskBuilder::new(
        SchedulingStrategy::Spread,
        create_mock_partition_ref(100, 1024),
    )
    .with_failure(MockTaskFailure::Panic("test panic".to_string()))
    .build();

    let submitted_task = test_context.handle().submit_task(task).await?;
    let result = submitted_task.await;
    assert!(result.is_none());

    let text_context_result = test_context.cleanup().await;
    assert!(text_context_result.is_err());
    assert_eq!(
        text_context_result.err().unwrap().to_string(),
        "DaftError::External task 2 panicked with message \"test panic\""
    );

    Ok(())
}
