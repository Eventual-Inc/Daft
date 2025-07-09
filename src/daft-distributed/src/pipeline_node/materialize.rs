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
                    let submitted_task = task.submit(&scheduler_handle)?;
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
        let mut pending_tasks: OrderedJoinSet<DaftResult<Option<MaterializedOutput>>> =
            OrderedJoinSet::new();
        loop {
            let num_pending = pending_tasks.num_pending();
            tokio::select! {
                biased;
                Some(finalized_task) = finalized_tasks_receiver.recv() => {
                    let finalized_task = finalized_task?;
                    match finalized_task {
                        FinalizedTask::Materialized(materialized_output) => {
                            pending_tasks.spawn(async move { Ok(Some(materialized_output)) });
                        }
                        FinalizedTask::Running(submitted_task) => {
                            pending_tasks.spawn(submitted_task);
                        }
                    }
                }
                Some(result) = pending_tasks.join_next(), if num_pending > 0 => {
                    let materialized_output = result??;
                    if let Some(materialized_output) = materialized_output {
                        if tx.send(materialized_output).await.is_err() {
                            break;
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
        let mut pending_tasks: OrderedJoinSet<DaftResult<Vec<PipelineOutput<T>>>> =
            OrderedJoinSet::new();
        loop {
            let num_pending = pending_tasks.num_pending();

            tokio::select! {
                biased;
                Some(pipeline_output) = pipeline_output_stream.next() => {
                    let pipeline_output = pipeline_output?;
                    match pipeline_output {
                        PipelineOutput::Materialized(partition) => {
                            pending_tasks.spawn(async move { Ok(vec![PipelineOutput::Materialized(partition)]) });
                        }
                        PipelineOutput::Task(tasks) => {
                            pending_tasks.spawn(async move { Ok(vec![PipelineOutput::Task(tasks)]) });
                        }
                        PipelineOutput::Running(submitted_task) => {
                            pending_tasks.spawn(async move {
                                let partitions = submitted_task.await?;
                                Ok(partitions.into_iter().map(|partition| PipelineOutput::Materialized(partition)).collect())
                            });
                        }
                    }
                }
                Some(result) = pending_tasks.join_next(), if num_pending > 0 => {
                    for pipeline_output in result?? {
                        if tx.send(pipeline_output).await.is_err() {
                            break;
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
    use common_partitioning::PartitionRef;
    use futures::{stream, StreamExt};
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::{
        scheduling::{
            scheduler::{spawn_default_scheduler_actor, SubmittableTask},
            tests::{
                create_mock_partition_ref, setup_workers, MockTask, MockTaskBuilder,
                MockWorkerManager,
            },
            worker::WorkerId,
        },
        statistics::StatisticsManagerRef,
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
            let scheduler_handle = spawn_default_scheduler_actor(
                worker_manager,
                &mut joinset,
                StatisticsManagerRef::default(),
            );
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

    // Helper function to create test partitions
    fn create_test_partitions(specs: &[(usize, usize)]) -> Vec<PartitionRef> {
        specs
            .iter()
            .map(|(rows, bytes)| create_mock_partition_ref(*rows, *bytes))
            .collect()
    }

    // Helper function to generate incremental partition specs
    fn create_incremental_partition_specs(count: usize) -> Vec<(usize, usize)> {
        (0..count).map(|i| (100 + i, 1024 + i * 10)).collect()
    }

    // Helper function to verify materialized results
    fn verify_materialized_results(
        results: &[DaftResult<MaterializedOutput>],
        expected_specs: &[(usize, usize)],
    ) -> DaftResult<()> {
        assert_eq!(results.len(), expected_specs.len());

        // Sort both results and expected specs by num_rows to ensure consistent ordering
        let mut sorted_results: Vec<_> = results.iter().collect();
        let mut sorted_expected: Vec<_> = expected_specs.iter().collect();

        sorted_results.sort_by(|a, b| {
            let a_rows = a.as_ref().unwrap().num_rows().unwrap();
            let b_rows = b.as_ref().unwrap().num_rows().unwrap();
            a_rows.cmp(&b_rows)
        });

        sorted_expected.sort_by(|a, b| a.0.cmp(&b.0));

        for (result, expected) in sorted_results.iter().zip(sorted_expected.iter()) {
            let materialized_output = result.as_ref().expect("Result should be Ok");
            assert_eq!(materialized_output.num_rows()?, expected.0);
            assert_eq!(materialized_output.size_bytes()?, expected.1);
        }
        Ok(())
    }

    // Helper function to verify error propagation
    fn verify_error_propagation(
        results: &[DaftResult<impl std::fmt::Debug>],
        first_error_idx: &Arc<OnceLock<usize>>,
    ) {
        assert!(results.iter().any(|result| result.is_err()));
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);

        let res = results.iter().find(|result| result.is_err()).unwrap();
        let err = res.as_ref().unwrap_err();
        let iteration = first_error_idx.get().unwrap();
        assert!(matches!(err, DaftError::InternalError(_)));
        assert_eq!(
            err.to_string(),
            format!("DaftError::InternalError Error at iteration {}", iteration)
        );
    }

    #[tokio::test]
    async fn test_materialize_all_pipeline_outputs_basic() -> DaftResult<()> {
        let worker_slots = 4;
        let task_sleep_ms = 50;
        let task2_sleep_ms = 100;

        let test_context = TestContext::new(&[("worker1".into(), worker_slots)])?;
        let partition_specs = vec![(100, 1024), (200, 2048), (300, 3072)];
        let partitions = create_test_partitions(&partition_specs);

        // Create and submit a mock task
        let task = MockTaskBuilder::new(partitions[2].clone())
            .with_task_id(0)
            .with_sleep_duration(Duration::from_millis(task_sleep_ms))
            .build();
        let submitted_task = SubmittableTask::new(task).submit(&test_context.handle())?;

        // Create input stream with different pipeline output types
        let inputs = vec![
            Ok(PipelineOutput::Materialized(MaterializedOutput::new(
                vec![partitions[0].clone()],
                "".into(),
            ))),
            Ok(PipelineOutput::Task(SubmittableTask::new(
                MockTaskBuilder::new(partitions[1].clone())
                    .with_task_id(1)
                    .with_sleep_duration(Duration::from_millis(task2_sleep_ms))
                    .build(),
            ))),
            Ok(PipelineOutput::Running(submitted_task)),
        ];

        let results: Vec<_> =
            materialize_all_pipeline_outputs(stream::iter(inputs), test_context.handle().clone())
                .collect::<Vec<_>>()
                .await;

        verify_materialized_results(&results, &partition_specs)?;
        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_all_pipeline_outputs_large() -> DaftResult<()> {
        let num_partitions = 1000;
        let num_workers = 100;

        let mut test_context = TestContext::new(
            &(0..num_workers)
                .map(|i| (format!("worker{}", i).into(), 1))
                .collect::<Vec<_>>(),
        )?;
        let partition_specs = create_incremental_partition_specs(num_partitions);
        let partitions = create_test_partitions(&partition_specs);

        // Create task to emit pipeline_outputs
        let (tx, rx) = create_channel(1);
        let handle = test_context.handle().clone();
        test_context.joinset().spawn(async move {
            let mut rng = rand::rngs::StdRng::from_entropy();
            for i in 0..num_partitions {
                let which_pipeline_output = rng.gen_range(0..3);
                let pipeline_output = match which_pipeline_output {
                    0 => Ok(PipelineOutput::Materialized(MaterializedOutput::new(
                        vec![partitions[i].clone()],
                        "".into(),
                    ))),
                    1 => {
                        let sleep_duration = Duration::from_millis(rng.gen_range(100..300));
                        Ok(PipelineOutput::Task(SubmittableTask::new(
                            MockTaskBuilder::new(partitions[i].clone())
                                .with_task_id(i as u32)
                                .with_sleep_duration(sleep_duration)
                                .build(),
                        )))
                    }
                    2 => {
                        let sleep_duration = Duration::from_millis(rng.gen_range(200..500));
                        let task = MockTaskBuilder::new(partitions[i].clone())
                            .with_task_id(i as u32)
                            .with_sleep_duration(sleep_duration)
                            .build();
                        let submitted_task = SubmittableTask::new(task).submit(&handle)?;
                        Ok(PipelineOutput::Running(submitted_task))
                    }
                    _ => unreachable!(),
                };
                tx.send(pipeline_output).await.unwrap();
            }
            Ok(())
        });
        let input_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let results: Vec<_> =
            materialize_all_pipeline_outputs(input_stream, test_context.handle().clone())
                .collect::<Vec<_>>()
                .await;
        verify_materialized_results(&results, &partition_specs)?;
        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_all_pipeline_outputs_with_error() -> DaftResult<()> {
        let num_partitions = 100;
        let worker_slots = 10;
        let error_probability = 0.1;
        let task_sleep_ms = 100;

        let mut test_context = TestContext::new(&[("worker1".into(), worker_slots)])?;
        let partition_specs = create_incremental_partition_specs(num_partitions);
        let partitions = create_test_partitions(&partition_specs);

        let (tx, rx) = create_channel(1);
        let handle = test_context.handle().clone();
        let first_error_idx = Arc::new(OnceLock::new());
        let first_error_idx_clone = first_error_idx.clone();

        test_context.joinset().spawn(async move {
            let mut rng = rand::rngs::StdRng::from_entropy();
            let mut has_sent_error = false;

            for i in 0..num_partitions {
                // Randomly inject errors
                if rng.gen_bool(error_probability) {
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

                let which_pipeline_output = rng.gen_range(0..3);
                let pipeline_output = match which_pipeline_output {
                    0 => Ok(PipelineOutput::Materialized(MaterializedOutput::new(
                        vec![partitions[i].clone()],
                        "".into(),
                    ))),
                    1 => Ok(PipelineOutput::Task(SubmittableTask::new(
                        MockTaskBuilder::new(partitions[i].clone())
                            .with_task_id(i as u32)
                            .with_sleep_duration(Duration::from_millis(task_sleep_ms))
                            .build(),
                    ))),
                    2 => {
                        let task = MockTaskBuilder::new(partitions[i].clone())
                            .with_task_id(i as u32)
                            .with_sleep_duration(Duration::from_millis(task_sleep_ms))
                            .build();
                        let submitted_task = SubmittableTask::new(task).submit(&handle)?;
                        Ok(PipelineOutput::Running(submitted_task))
                    }
                    _ => unreachable!(),
                };
                if tx.send(pipeline_output).await.is_err() {
                    break;
                }
            }

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

        let input_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let results = materialize_all_pipeline_outputs(input_stream, test_context.handle().clone())
            .collect::<Vec<_>>()
            .await;

        verify_error_propagation(&results, &first_error_idx);
        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_running_pipeline_outputs_basic() -> DaftResult<()> {
        let worker_slots = 4;
        let task_sleep_ms = 50;
        let task2_sleep_ms = 100;

        let test_context = TestContext::new(&[("worker1".into(), worker_slots)])?;
        let partition_specs = vec![(100, 1024), (200, 2048), (300, 3072)];
        let partitions = create_test_partitions(&partition_specs);

        // Create and submit a mock task
        let task = MockTaskBuilder::new(partitions[2].clone())
            .with_task_id(0)
            .with_sleep_duration(Duration::from_millis(task_sleep_ms))
            .build();
        let submitted_task = SubmittableTask::new(task).submit(&test_context.handle())?;

        let inputs = vec![
            Ok(PipelineOutput::Materialized(MaterializedOutput::new(
                vec![partitions[0].clone()],
                "".into(),
            ))),
            Ok(PipelineOutput::Task(SubmittableTask::new(
                MockTaskBuilder::new(partitions[1].clone())
                    .with_task_id(1)
                    .with_sleep_duration(Duration::from_millis(task2_sleep_ms))
                    .build(),
            ))),
            Ok(PipelineOutput::Running(submitted_task)),
        ];

        let mut materialized_running_stream =
            materialize_running_pipeline_outputs(stream::iter(inputs));
        while let Some(result) = materialized_running_stream.next().await {
            let pipeline_output = result?;
            assert!(!matches!(pipeline_output, PipelineOutput::Running(_)));
        }

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_running_pipeline_outputs_large() -> DaftResult<()> {
        let num_partitions = 1000;
        let num_workers = 100;

        let mut test_context = TestContext::new(
            &(0..num_workers)
                .map(|i| (format!("worker{}", i).into(), 1))
                .collect::<Vec<_>>(),
        )?;
        let partition_specs = create_incremental_partition_specs(num_partitions);
        let partitions = create_test_partitions(&partition_specs);

        // Track output types for verification
        let mut output_types = vec![0; num_partitions];
        let mut rng = rand::rngs::StdRng::from_entropy();
        for i in 0..num_partitions {
            output_types[i] = rng.gen_range(0..3);
        }

        let (tx, rx) = create_channel(1);
        let handle = test_context.handle().clone();
        let owned_output_types = output_types.clone();

        test_context.joinset().spawn(async move {
            for i in 0..num_partitions {
                let pipeline_output = match owned_output_types[i] {
                    0 => Ok(PipelineOutput::Materialized(MaterializedOutput::new(
                        vec![partitions[i].clone()],
                        "".into(),
                    ))),
                    1 => {
                        let sleep_duration = Duration::from_millis(rng.gen_range(100..300));
                        Ok(PipelineOutput::Task(SubmittableTask::new(
                            MockTaskBuilder::new(partitions[i].clone())
                                .with_task_id(i as u32)
                                .with_sleep_duration(sleep_duration)
                                .build(),
                        )))
                    }
                    2 => {
                        let sleep_duration = Duration::from_millis(rng.gen_range(200..500));
                        let task = MockTaskBuilder::new(partitions[i].clone())
                            .with_task_id(i as u32)
                            .with_sleep_duration(sleep_duration)
                            .build();
                        let submitted_task = SubmittableTask::new(task).submit(&handle)?;
                        Ok(PipelineOutput::Running(submitted_task))
                    }
                    _ => unreachable!(),
                };
                tx.send(pipeline_output).await.unwrap();
            }
            Ok(())
        });

        let input_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut materialized_running_stream = materialize_running_pipeline_outputs(input_stream);
        while let Some(result) = materialized_running_stream.next().await {
            let pipeline_output = result?;
            assert!(!matches!(pipeline_output, PipelineOutput::Running(_)));
        }

        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_running_pipeline_outputs_with_error() -> DaftResult<()> {
        let num_partitions = 100;
        let worker_slots = 10;
        let error_probability = 0.1;
        let task_sleep_ms = 100;

        let mut test_context = TestContext::new(&[("worker1".into(), worker_slots)])?;
        let partition_specs = create_incremental_partition_specs(num_partitions);
        let partitions = create_test_partitions(&partition_specs);

        let (tx, rx) = create_channel(1);
        let handle = test_context.handle().clone();
        let first_error_idx = Arc::new(OnceLock::new());
        let first_error_idx_clone = first_error_idx.clone();

        test_context.joinset().spawn(async move {
            let mut rng = rand::rngs::StdRng::from_entropy();
            let mut has_sent_error = false;

            for i in 0..num_partitions {
                if rng.gen_bool(error_probability) {
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

                let which_pipeline_output = rng.gen_range(0..3);
                let pipeline_output = match which_pipeline_output {
                    0 => Ok(PipelineOutput::Materialized(MaterializedOutput::new(
                        vec![partitions[i].clone()],
                        "".into(),
                    ))),
                    1 => Ok(PipelineOutput::Task(SubmittableTask::new(
                        MockTaskBuilder::new(partitions[i].clone())
                            .with_task_id(i as u32)
                            .with_sleep_duration(Duration::from_millis(task_sleep_ms))
                            .build(),
                    ))),
                    2 => {
                        let task = MockTaskBuilder::new(partitions[i].clone())
                            .with_task_id(i as u32)
                            .with_sleep_duration(Duration::from_millis(task_sleep_ms))
                            .build();
                        let submitted_task = SubmittableTask::new(task).submit(&handle)?;
                        Ok(PipelineOutput::Running(submitted_task))
                    }
                    _ => unreachable!(),
                };
                if tx.send(pipeline_output).await.is_err() {
                    break;
                }
            }

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

        let input_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let results = materialize_running_pipeline_outputs(input_stream)
            .collect::<Vec<_>>()
            .await;

        verify_error_propagation(&results, &first_error_idx);
        test_context.cleanup().await?;
        Ok(())
    }
}
