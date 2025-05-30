use common_error::DaftResult;
use futures::{Stream, StreamExt};

use super::MaterializedOutput;
use crate::{
    pipeline_node::PipelineOutput,
    scheduling::{
        scheduler::{SchedulerHandle, SubmittedTask},
        task::Task,
    },
    utils::{
        channel::{create_channel, Receiver, Sender},
        joinset::{JoinSet, OrderedJoinSet},
        stream::JoinableForwardingStream,
    },
};

pub(crate) fn materialize_all_pipeline_outputs<T: Task>(
    input: impl Stream<Item = DaftResult<PipelineOutput<T>>> + Send + Unpin + 'static,
    scheduler_handle: SchedulerHandle<T>,
) -> impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin + 'static {
    enum FinalizedTask {
        Materialized(MaterializedOutput),
        Running(SubmittedTask),
    }

    /// Force all tasks in the `input`` stream to start running if un-submitted
    async fn task_finalizer<T: Task>(
        mut input: impl Stream<Item = DaftResult<PipelineOutput<T>>> + Unpin,
        tx: Sender<DaftResult<FinalizedTask>>,
        scheduler_handle: SchedulerHandle<T>,
    ) -> DaftResult<()> {
        while let Some(pipeline_result) = input.next().await {
            let pipeline_output = match pipeline_result {
                Ok(pipeline_output) => pipeline_output,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            };

            let finalized_task = match pipeline_output {
                // If the pipeline output is a materialized partition, we can just send it through the channel
                PipelineOutput::Materialized(partition) => FinalizedTask::Materialized(partition),
                // If the pipeline output is a task, we need to submit it to the task dispatcher
                PipelineOutput::Task(task) => {
                    let submitted_task = scheduler_handle.submit_task(task).await?;
                    FinalizedTask::Running(submitted_task)
                }
                // If the task is already running, we can just send it through the channel
                PipelineOutput::Running(submitted_task) => FinalizedTask::Running(submitted_task),
            };
            if tx.send(Ok(finalized_task)).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    /// Materialize the output of all running or finished tasks
    async fn task_materializer(
        mut finalized_tasks_receiver: Receiver<DaftResult<FinalizedTask>>,
        tx: Sender<MaterializedOutput>,
    ) -> DaftResult<()> {
        let mut pending_tasks: OrderedJoinSet<Option<DaftResult<Vec<MaterializedOutput>>>> =
            OrderedJoinSet::new();
        loop {
            let num_pending = pending_tasks.num_pending();

            tokio::select! {
                biased;
                Some(finalized_task) = finalized_tasks_receiver.recv() => {
                    let finalized_task = finalized_task?;
                    match finalized_task {
                        FinalizedTask::Materialized(materialized_output) => {
                            pending_tasks.spawn(async move { Some(Ok(vec![materialized_output])) });
                        }
                        FinalizedTask::Running(submitted_task) => {
                            pending_tasks.spawn(submitted_task);
                        }
                    }
                }
                Some(result) = pending_tasks.join_next(), if num_pending > 0 => {
                    let result = result?;
                    if let Some(result) = result {
                        let result = result?;
                        for materialized_output in result {
                            if tx.send(materialized_output).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }

        Ok(())
    }

    let (finalized_tasks_sender, finalized_tasks_receiver) = create_channel(1);
    let (materialized_results_sender, materialized_results_receiver) = create_channel(1);

    let mut joinset = JoinSet::new();
    joinset.spawn(task_finalizer(
        input,
        finalized_tasks_sender,
        scheduler_handle,
    ));
    joinset.spawn(task_materializer(
        finalized_tasks_receiver,
        materialized_results_sender,
    ));

    let materialized_result_stream =
        tokio_stream::wrappers::ReceiverStream::new(materialized_results_receiver);
    JoinableForwardingStream::new(materialized_result_stream, joinset)
}

// This function is responsible for awaiting the results of any running tasks
#[allow(dead_code)]
pub(crate) fn materialize_running_pipeline_outputs<T: Task>(
    input: impl Stream<Item = DaftResult<PipelineOutput<T>>> + Send + Unpin + 'static,
) -> impl Stream<Item = DaftResult<PipelineOutput<T>>> + Send + Unpin + 'static {
    async fn result_awaiter<T: Task>(
        mut pipeline_output_stream: impl Stream<Item = DaftResult<PipelineOutput<T>>>
            + Send
            + Unpin
            + 'static,
        tx: Sender<PipelineOutput<T>>,
    ) -> DaftResult<()> {
        let mut pending_tasks = OrderedJoinSet::new();
        loop {
            let num_pending = pending_tasks.num_pending();

            tokio::select! {
                biased;
                Some(pipeline_output) = pipeline_output_stream.next() => {
                    let pipeline_output = pipeline_output?;
                    match pipeline_output {
                        PipelineOutput::Materialized(partition) => {
                            pending_tasks.spawn(async move { Some(Ok(vec![PipelineOutput::Materialized(partition)])) });
                        }
                        PipelineOutput::Task(tasks) => {
                            pending_tasks.spawn(async move { Some(Ok(vec![PipelineOutput::Task(tasks)])) });
                        }
                        PipelineOutput::Running(submitted_task) => {
                            pending_tasks.spawn(async move {
                                let result = submitted_task.await;
                                if let Some(result) = result {
                                    if let Err(e) = result {
                                        return Some(Err(e));
                                    }
                                    let partitions = result.unwrap();
                                    Some(Ok(partitions.into_iter().map(|partition| PipelineOutput::Materialized(partition)).collect()))
                                } else {
                                    None
                                }
                            });
                        }
                    }
                }
                Some(result) = pending_tasks.join_next(), if num_pending > 0 => {
                    let result = result?;
                    if let Some(result) = result {
                        let result = result?;
                        for pipeline_output in result {
                            if tx.send(pipeline_output).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }

        Ok(())
    }

    let (tx, rx) = create_channel(1);
    let mut joinset = JoinSet::new();
    joinset.spawn(result_awaiter(input, tx));
    let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    JoinableForwardingStream::new(output_stream, joinset)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, OnceLock},
        time::Duration,
    };

    use common_error::{DaftError, DaftResult};
    use futures::{stream, StreamExt};
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::scheduling::{
        scheduler::spawn_default_scheduler_actor,
        task::Task,
        tests::{
            create_mock_partition_ref, setup_workers, MockTask, MockTaskBuilder, MockWorkerManager,
        },
        worker::WorkerId,
    };

    struct TestContext {
        scheduler_handle: SchedulerHandle<MockTask>,
        joinset: JoinSet<DaftResult<()>>,
    }

    impl TestContext {
        fn new(worker_configs: &[(WorkerId, usize)]) -> DaftResult<Self> {
            let workers = setup_workers(worker_configs);
            let worker_manager = Arc::new(MockWorkerManager::new(workers));
            let mut joinset = JoinSet::new();
            let scheduler_handle = spawn_default_scheduler_actor(worker_manager, &mut joinset);
            Ok(Self {
                scheduler_handle,
                joinset,
            })
        }

        fn handle(&self) -> &SchedulerHandle<MockTask> {
            &self.scheduler_handle
        }

        fn joinset(&mut self) -> &mut JoinSet<DaftResult<()>> {
            &mut self.joinset
        }

        async fn cleanup(mut self) -> DaftResult<()> {
            drop(self.scheduler_handle);
            while let Some(result) = self.joinset.join_next().await {
                result??;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_materialize_all_pipeline_outputs_basic() -> DaftResult<()> {
        // Setup test context and partitions
        let test_context = TestContext::new(&[("worker1".into(), 4)])?;
        let partition_rows_and_bytes = vec![
            (100, 1024), // partition1
            (200, 2048), // partition2
            (300, 3072), // partition3
        ];
        let partitions = partition_rows_and_bytes
            .iter()
            .map(|(rows, bytes)| create_mock_partition_ref(*rows, *bytes))
            .collect::<Vec<_>>();

        // Create and submit a mock task
        let task = MockTaskBuilder::new(partitions[2].clone())
            .with_task_id("test-task".into())
            .with_sleep_duration(Duration::from_millis(50))
            .build();
        let submitted_task = test_context.handle().submit_task(task).await?;

        // Create input stream with different pipeline output types
        let inputs = vec![
            Ok(PipelineOutput::Materialized(MaterializedOutput::new(
                partitions[0].clone(),
                "".into(),
            ))),
            Ok(PipelineOutput::Task(
                MockTaskBuilder::new(partitions[1].clone())
                    .with_task_id("test-task-2".into())
                    .with_sleep_duration(Duration::from_millis(100))
                    .build(),
            )),
            Ok(PipelineOutput::Running(submitted_task)),
        ];

        // Process stream and collect results
        let results: Vec<_> =
            materialize_all_pipeline_outputs(stream::iter(inputs), test_context.handle().clone())
                .collect::<Vec<_>>()
                .await;

        // Verify results
        assert_eq!(results.len(), 3);
        for (i, materialized_output) in results.iter().enumerate() {
            let materialized_output = materialized_output.as_ref().expect("Result should be Ok");
            assert_eq!(
                materialized_output.partition().num_rows()?,
                partition_rows_and_bytes[i].0
            );
            assert_eq!(
                materialized_output.partition().size_bytes()?,
                Some(partition_rows_and_bytes[i].1)
            );
        }

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_all_pipeline_outputs_large() -> DaftResult<()> {
        // Setup test context and partitions
        let mut test_context = TestContext::new(&[("worker1".into(), 100)])?;

        let num_partitions = 1000;

        // Create 1000 partitions with varying sizes
        let partition_rows_and_bytes: Vec<(usize, usize)> = (0..num_partitions)
            .map(|i| (100 + i, 1024 + i * 10))
            .collect();
        let partitions = partition_rows_and_bytes
            .iter()
            .map(|(rows, bytes)| create_mock_partition_ref(*rows, *bytes))
            .collect::<Vec<_>>();

        // Create task to emit pipeline_outputs
        let (tx, rx) = create_channel(1);
        let handle = test_context.handle().clone();
        test_context.joinset().spawn(async move {
            let mut rng = rand::rngs::StdRng::from_entropy();
            for i in 0..num_partitions {
                let which_pipeline_output = rng.gen_range(0..3);
                match which_pipeline_output {
                    0 => {
                        // Materialized output
                        let pipeline_output = Ok(PipelineOutput::Materialized(
                            MaterializedOutput::new(partitions[i].clone(), "".into()),
                        ));
                        tx.send(pipeline_output).await.unwrap();
                    }
                    1 => {
                        // Tasks output - shorter duration
                        let sleep_duration = Duration::from_millis(rng.gen_range(100..300));
                        let pipeline_output = Ok(PipelineOutput::Task(
                            MockTaskBuilder::new(partitions[i].clone())
                                .with_task_id(format!("test-task-{}", i).into())
                                .with_sleep_duration(sleep_duration)
                                .build(),
                        ));
                        tx.send(pipeline_output).await.unwrap();
                    }
                    2 => {
                        // Running output - longer duration
                        let sleep_duration = Duration::from_millis(rng.gen_range(200..500));
                        let task = MockTaskBuilder::new(partitions[i].clone())
                            .with_task_id(format!("test-running-task-{}", i).into())
                            .with_sleep_duration(sleep_duration)
                            .build();
                        let submitted_task = handle.submit_task(task).await?;
                        let pipeline_output = Ok(PipelineOutput::Running(submitted_task));
                        tx.send(pipeline_output).await.unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            Ok(())
        });
        let input_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        // Process stream and collect results
        let results: Vec<_> =
            materialize_all_pipeline_outputs(input_stream, test_context.handle().clone())
                .collect::<Vec<_>>()
                .await;

        // Verify results
        assert_eq!(results.len(), num_partitions);
        for (i, materialized_output) in results.iter().enumerate() {
            let materialized_output = materialized_output.as_ref().expect("Result should be Ok");
            assert_eq!(
                materialized_output.partition().num_rows()?,
                partition_rows_and_bytes[i].0
            );
            assert_eq!(
                materialized_output.partition().size_bytes()?,
                Some(partition_rows_and_bytes[i].1)
            );
        }

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_all_pipeline_outputs_with_error() -> DaftResult<()> {
        // Create mock partitions
        let mut test_context = TestContext::new(&[("worker1".into(), 10)])?;

        let num_partitions = 100;

        // Create partitions with varying sizes
        let partition_rows_and_bytes: Vec<(usize, usize)> = (0..num_partitions)
            .map(|i| (100 + i, 1024 + i * 10))
            .collect();
        let partitions = partition_rows_and_bytes
            .iter()
            .map(|(rows, bytes)| create_mock_partition_ref(*rows, *bytes))
            .collect::<Vec<_>>();

        // Create task to emit pipeline_outputs
        let (tx, rx) = create_channel(1);
        let handle = test_context.handle().clone();

        // Track what kind of output was sent for each index
        let mut output_types = vec![0; num_partitions];
        let mut rng = rand::rngs::StdRng::from_entropy();
        for i in 0..num_partitions {
            output_types[i] = rng.gen_range(0..3);
        }
        let owned_output_types = output_types.clone();

        // Track the first error index
        let first_error_idx = Arc::new(OnceLock::new());
        let first_error_idx_clone = first_error_idx.clone();

        // Spawn the task to emit pipeline outputs
        test_context.joinset().spawn(async move {
            let mut has_sent_error = false;

            for i in 0..num_partitions {
                // Randomly inject errors
                if rng.gen_bool(0.1) {
                    let pipeline_output = Err(DaftError::InternalError(format!(
                        "Error at iteration {}",
                        i
                    )));
                    if tx.send(pipeline_output).await.is_err() {
                        break;
                    }
                    has_sent_error = true;
                    first_error_idx_clone.get_or_init(|| i);
                    continue;
                }

                let which_pipeline_output = owned_output_types[i];
                match which_pipeline_output {
                    0 => {
                        let pipeline_output = Ok(PipelineOutput::Materialized(
                            MaterializedOutput::new(partitions[i].clone(), "".into()),
                        ));
                        if tx.send(pipeline_output).await.is_err() {
                            break;
                        }
                    }
                    1 => {
                        let pipeline_output = Ok(PipelineOutput::Task(
                            MockTaskBuilder::new(partitions[i].clone())
                                .with_task_id(format!("test-task-{}", i).into())
                                .with_sleep_duration(Duration::from_millis(100))
                                .build(),
                        ));
                        if tx.send(pipeline_output).await.is_err() {
                            break;
                        }
                    }
                    2 => {
                        let task = MockTaskBuilder::new(partitions[i].clone())
                            .with_task_id(format!("test-running-task-{}", i).into())
                            .with_sleep_duration(Duration::from_millis(100))
                            .build();
                        let submitted_task = handle.submit_task(task).await?;
                        let pipeline_output = Ok(PipelineOutput::Running(submitted_task));
                        if tx.send(pipeline_output).await.is_err() {
                            break;
                        }
                    }
                    _ => unreachable!(),
                }
            }

            // If no error was sent during the loop, send one at the end
            if !has_sent_error {
                let pipeline_output = Err(DaftError::InternalError(format!(
                    "Error at iteration {}",
                    num_partitions
                )));
                let _ = tx.send(pipeline_output).await;
                first_error_idx_clone.get_or_init(|| num_partitions);
            }

            Ok(())
        });

        // Create input stream from the channel
        let input_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        // Process stream and collect results
        let results = materialize_all_pipeline_outputs(input_stream, test_context.handle().clone())
            .collect::<Vec<_>>()
            .await;

        // Verify that the error is propagated
        assert!(results.iter().any(|result| result.is_err()));
        // Verify that there is only 1 error
        assert_eq!(
            results.iter().filter(|result| result.is_err()).count(),
            1,
            "Expected 1 error, got {:?}",
            results
        );
        // Verify that the error is the first error
        let res = results.iter().find(|result| result.is_err()).unwrap();

        let err = res.as_ref().unwrap_err();
        let iteration = first_error_idx.get().unwrap();
        assert!(matches!(err, DaftError::InternalError(_)));
        assert_eq!(
            err.to_string(),
            format!("DaftError::InternalError Error at iteration {}", iteration)
        );

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_running_pipeline_outputs_basic() -> DaftResult<()> {
        // Setup test context and partitions
        let test_context = TestContext::new(&[("worker1".into(), 4)])?;
        let partition_rows_and_bytes = vec![
            (100, 1024), // partition1
            (200, 2048), // partition2
            (300, 3072), // partition3
        ];
        let partitions = partition_rows_and_bytes
            .iter()
            .map(|(rows, bytes)| create_mock_partition_ref(*rows, *bytes))
            .collect::<Vec<_>>();

        // Create and submit a mock task
        let task = MockTaskBuilder::new(partitions[2].clone())
            .with_task_id("test-task".into())
            .with_sleep_duration(Duration::from_millis(50))
            .build();
        let submitted_task = test_context.handle().submit_task(task).await?;

        // Create input stream with different pipeline output types
        let inputs = vec![
            Ok(PipelineOutput::Materialized(MaterializedOutput::new(
                partitions[0].clone(),
                "".into(),
            ))),
            Ok(PipelineOutput::Task(
                MockTaskBuilder::new(partitions[1].clone())
                    .with_task_id("test-task-2".into())
                    .with_sleep_duration(Duration::from_millis(100))
                    .build(),
            )),
            Ok(PipelineOutput::Running(submitted_task)),
        ];

        // Process stream and collect results
        let results: Vec<_> = materialize_running_pipeline_outputs(stream::iter(inputs))
            .collect::<Vec<_>>()
            .await;

        // Verify results
        assert_eq!(results.len(), 3);

        // Check that the first result is a materialized partition
        match &results[0] {
            Ok(PipelineOutput::Materialized(materialized_output)) => {
                assert_eq!(
                    materialized_output.partition().num_rows()?,
                    partition_rows_and_bytes[0].0
                );
                assert_eq!(
                    materialized_output.partition().size_bytes()?,
                    Some(partition_rows_and_bytes[0].1)
                );
            }
            _ => panic!("Expected Materialized output"),
        }

        // Check that the second result is a tasks output
        match &results[1] {
            Ok(PipelineOutput::Task(task)) => {
                assert_eq!(task.task_id().as_ref(), "test-task-2");
                // Tasks should pass through
            }
            _ => panic!("Expected Tasks output"),
        }

        // Check that the third result is materialized (from the Running task)
        match &results[2] {
            Ok(PipelineOutput::Materialized(materialized_output)) => {
                assert_eq!(
                    materialized_output.partition().num_rows()?,
                    partition_rows_and_bytes[2].0
                );
                assert_eq!(
                    materialized_output.partition().size_bytes()?,
                    Some(partition_rows_and_bytes[2].1)
                );
            }
            _ => panic!("Expected Materialized output from Running task"),
        }

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_running_pipeline_outputs_large() -> DaftResult<()> {
        // Setup test context and partitions
        let mut test_context = TestContext::new(&[("worker1".into(), 100)])?;

        let num_partitions = 1000; // Using fewer partitions than the original test for faster execution

        // Create partitions with varying sizes
        let partition_rows_and_bytes: Vec<(usize, usize)> = (0..num_partitions)
            .map(|i| (100 + i, 1024 + i * 10))
            .collect();
        let partitions = partition_rows_and_bytes
            .iter()
            .map(|(rows, bytes)| create_mock_partition_ref(*rows, *bytes))
            .collect::<Vec<_>>();

        // Create task to emit pipeline_outputs
        let (tx, rx) = create_channel(1);
        let handle = test_context.handle().clone();

        // Track what kind of output was sent for each index
        let mut output_types = vec![0; num_partitions];
        let mut rng = rand::rngs::StdRng::from_entropy();
        for i in 0..num_partitions {
            output_types[i] = rng.gen_range(0..3);
        }
        let owned_output_types = output_types.clone();

        // Spawn the task to emit pipeline outputs
        test_context.joinset().spawn(async move {
            for i in 0..num_partitions {
                let which_pipeline_output = owned_output_types[i];
                match which_pipeline_output {
                    0 => {
                        // Materialized output
                        let pipeline_output = Ok(PipelineOutput::Materialized(
                            MaterializedOutput::new(partitions[i].clone(), "".into()),
                        ));
                        tx.send(pipeline_output).await.unwrap();
                    }
                    1 => {
                        // Tasks output - shorter duration
                        let sleep_duration = Duration::from_millis(rng.gen_range(100..300)); // Shorter for test speed
                        let pipeline_output = Ok(PipelineOutput::Task(
                            MockTaskBuilder::new(partitions[i].clone())
                                .with_task_id(format!("test-task-{}", i).into())
                                .with_sleep_duration(sleep_duration)
                                .build(),
                        ));
                        tx.send(pipeline_output).await.unwrap();
                    }
                    2 => {
                        // Running output - longer duration
                        let sleep_duration = Duration::from_millis(rng.gen_range(200..500)); // Shorter for test speed
                        let task = MockTaskBuilder::new(partitions[i].clone())
                            .with_task_id(format!("test-running-task-{}", i).into())
                            .with_sleep_duration(sleep_duration)
                            .build();
                        let submitted_task = handle.submit_task(task).await?;
                        let pipeline_output = Ok(PipelineOutput::Running(submitted_task));
                        tx.send(pipeline_output).await.unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            Ok(())
        });
        let input_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        // Process stream and collect results
        let results: Vec<_> = materialize_running_pipeline_outputs(input_stream)
            .collect::<Vec<_>>()
            .await;

        // Verify results
        assert_eq!(results.len(), num_partitions);

        // For each result, verify it's the expected type of output:
        // - Materialized stays Materialized
        // - Tasks stays Tasks
        // - Running becomes Materialized
        for (i, result) in results.iter().enumerate() {
            let result = result.as_ref().expect("Result should be Ok");
            match result {
                PipelineOutput::Materialized(materialized_output) => {
                    assert_eq!(
                        materialized_output.partition().num_rows()?,
                        partition_rows_and_bytes[i].0
                    );
                    assert_eq!(
                        materialized_output.partition().size_bytes()?,
                        Some(partition_rows_and_bytes[i].1)
                    );
                    // Either it was already materialized (type 0) or it was a running task (type 2)
                    assert!(output_types[i] == 0 || output_types[i] == 2);
                }
                PipelineOutput::Task(_) => {
                    // Only Tasks outputs should remain Tasks
                    assert_eq!(output_types[i], 1);
                }
                PipelineOutput::Running(_) => {
                    panic!("No Running outputs should remain in results");
                }
            }
        }

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_running_pipeline_outputs_with_error() -> DaftResult<()> {
        // Create mock partitions
        let mut test_context = TestContext::new(&[("worker1".into(), 10)])?;

        let num_partitions = 100;

        // Create partitions with varying sizes
        let partition_rows_and_bytes: Vec<(usize, usize)> = (0..num_partitions)
            .map(|i| (100 + i, 1024 + i * 10))
            .collect();
        let partitions = partition_rows_and_bytes
            .iter()
            .map(|(rows, bytes)| create_mock_partition_ref(*rows, *bytes))
            .collect::<Vec<_>>();

        // Create task to emit pipeline_outputs
        let (tx, rx) = create_channel(1);
        let handle = test_context.handle().clone();

        // Track what kind of output was sent for each index
        let mut output_types = vec![0; num_partitions];
        let mut rng = rand::rngs::StdRng::from_entropy();
        for i in 0..num_partitions {
            output_types[i] = rng.gen_range(0..3);
        }
        let owned_output_types = output_types.clone();

        // Track the first error index
        let first_error_idx = Arc::new(OnceLock::new());
        let first_error_idx_clone = first_error_idx.clone();

        // Spawn the task to emit pipeline outputs
        test_context.joinset().spawn(async move {
            let mut has_sent_error = false;

            for i in 0..num_partitions {
                // Randomly inject errors
                if rng.gen_bool(0.1) {
                    let pipeline_output = Err(DaftError::InternalError(format!(
                        "Error at iteration {}",
                        i
                    )));
                    if tx.send(pipeline_output).await.is_err() {
                        break;
                    }
                    has_sent_error = true;
                    first_error_idx_clone.get_or_init(|| i);
                    continue;
                }

                let which_pipeline_output = owned_output_types[i];
                match which_pipeline_output {
                    0 => {
                        let pipeline_output = Ok(PipelineOutput::Materialized(
                            MaterializedOutput::new(partitions[i].clone(), "".into()),
                        ));
                        if tx.send(pipeline_output).await.is_err() {
                            break;
                        }
                    }
                    1 => {
                        let pipeline_output = Ok(PipelineOutput::Task(
                            MockTaskBuilder::new(partitions[i].clone())
                                .with_task_id(format!("test-task-{}", i).into())
                                .with_sleep_duration(Duration::from_millis(100))
                                .build(),
                        ));
                        if tx.send(pipeline_output).await.is_err() {
                            break;
                        }
                    }
                    2 => {
                        let task = MockTaskBuilder::new(partitions[i].clone())
                            .with_task_id(format!("test-running-task-{}", i).into())
                            .with_sleep_duration(Duration::from_millis(100))
                            .build();
                        let submitted_task = handle.submit_task(task).await?;
                        let pipeline_output = Ok(PipelineOutput::Running(submitted_task));
                        if tx.send(pipeline_output).await.is_err() {
                            break;
                        }
                    }
                    _ => unreachable!(),
                }
            }

            // If no error was sent during the loop, send one at the end
            if !has_sent_error {
                let pipeline_output = Err(DaftError::InternalError(format!(
                    "Error at iteration {}",
                    num_partitions
                )));
                let _ = tx.send(pipeline_output).await;
                first_error_idx_clone.get_or_init(|| num_partitions);
            }

            Ok(())
        });

        // Create input stream from the channel
        let input_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        // Process stream and collect results
        let results = materialize_running_pipeline_outputs(input_stream)
            .collect::<Vec<_>>()
            .await;

        // Verify that the error is propagated
        assert!(results.iter().any(|result| result.is_err()));
        // Verify that there is only 1 error
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        // Verify that the error is the first error
        let res = results.iter().find(|result| result.is_err()).unwrap();

        let err = res.as_ref().unwrap_err();
        let iteration = first_error_idx.get().unwrap();
        assert!(matches!(err, DaftError::InternalError(_)));
        assert_eq!(
            err.to_string(),
            format!("DaftError::InternalError Error at iteration {}", iteration)
        );

        test_context.cleanup().await?;
        Ok(())
    }
}
