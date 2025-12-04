use common_error::DaftResult;
use futures::{Stream, StreamExt};

use super::MaterializedOutput;
use crate::{
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask, SubmittedTask},
        task::Task,
    },
    utils::{
        channel::{Receiver, Sender, create_channel},
        joinset::{JoinSet, OrderedJoinSet},
        stream::JoinableForwardingStream,
    },
};

pub(crate) fn materialize_all_pipeline_outputs<T: Task>(
    input: impl Stream<Item = SubmittableTask<T>> + Send + Unpin + 'static,
    scheduler_handle: SchedulerHandle<T>,
    joinset: Option<JoinSet<DaftResult<()>>>,
) -> impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin + 'static {
    /// Force all tasks in the `input`` stream to start running if un-submitted
    async fn task_finalizer<T: Task>(
        mut input: impl Stream<Item = SubmittableTask<T>> + Unpin,
        tx: Sender<SubmittedTask>,
        scheduler_handle: SchedulerHandle<T>,
    ) -> DaftResult<()> {
        while let Some(pipeline_output) = input.next().await {
            let finalized_task = pipeline_output.submit(&scheduler_handle)?;
            if tx.send(finalized_task).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    /// Materialize the output of all running or finished tasks
    async fn task_materializer(
        mut finalized_tasks_receiver: Receiver<SubmittedTask>,
        tx: Sender<DaftResult<MaterializedOutput>>,
    ) -> DaftResult<()> {
        let mut pending_tasks: OrderedJoinSet<DaftResult<Option<MaterializedOutput>>> =
            OrderedJoinSet::new();
        loop {
            let num_pending = pending_tasks.num_pending();
            tokio::select! {
                biased;
                Some(finalized_task) = finalized_tasks_receiver.recv() => {
                    pending_tasks.spawn(finalized_task);
                }
                Some(result) = pending_tasks.join_next(), if num_pending > 0 => {
                    match result {
                        Ok(Ok(Some(materialized_output))) => {
                            if tx.send(Ok(materialized_output)).await.is_err() {
                                break;
                            }
                        }
                        Ok(Ok(None)) => {}
                        Ok(Err(e)) | Err(e) => {
                            if tx.send(Err(e)).await.is_err() {
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

    let mut joinset = joinset.unwrap_or_else(JoinSet::new);
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
        .map(|result| result.and_then(|result| result))
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use common_partitioning::PartitionRef;
    use futures::{StreamExt, stream};
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::{
        scheduling::{
            scheduler::{SubmittableTask, spawn_scheduler_actor},
            tests::{
                MockTask, MockTaskBuilder, MockTaskFailure, MockWorkerManager,
                create_mock_partition_ref, setup_workers,
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
            let scheduler_handle = spawn_scheduler_actor(
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
        should_error: &[bool],
    ) {
        assert_eq!(results.len(), expected_specs.len());

        for ((result, expected), should_error) in results
            .iter()
            .zip(expected_specs.iter())
            .zip(should_error.iter())
        {
            if *should_error {
                assert!(result.is_err());
            } else {
                let materialized_output = result.as_ref().expect("Result should be Ok");
                assert_eq!(materialized_output.num_rows(), expected.0);
                assert_eq!(materialized_output.size_bytes(), expected.1);
            }
        }
    }

    #[tokio::test]
    async fn test_materialize_all_pipeline_outputs_basic() -> DaftResult<()> {
        let worker_slots = 4;
        let test_context = TestContext::new(&[("worker1".into(), worker_slots)])?;

        let partition_specs = vec![(100, 1024), (200, 2048), (300, 3072)];
        let partitions = create_test_partitions(&partition_specs);
        let task_sleep_durations = vec![
            Duration::from_millis(100),
            Duration::from_millis(50),
            Duration::from_millis(10),
        ];
        let task_ids = vec![0, 1, 2];

        // Create input stream with different pipeline output types
        let inputs = partitions
            .into_iter()
            .zip(task_sleep_durations)
            .zip(task_ids)
            .map(|((partition, sleep_duration), task_id)| {
                SubmittableTask::new(
                    MockTaskBuilder::new(partition)
                        .with_task_id(task_id)
                        .with_sleep_duration(sleep_duration)
                        .build(),
                )
            });

        let results: Vec<_> = materialize_all_pipeline_outputs(
            stream::iter(inputs),
            test_context.handle().clone(),
            None,
        )
        .collect::<Vec<_>>()
        .await;

        verify_materialized_results(
            &results,
            &partition_specs,
            &std::iter::repeat(false)
                .take(partition_specs.len())
                .collect::<Vec<_>>(),
        );
        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_all_pipeline_outputs_large() -> DaftResult<()> {
        let num_partitions = 1000;
        let num_workers = 100;

        let test_context = TestContext::new(
            &(0..num_workers)
                .map(|i| (format!("worker{}", i).into(), 1))
                .collect::<Vec<_>>(),
        )?;
        let partition_specs = create_incremental_partition_specs(num_partitions);
        let partitions = create_test_partitions(&partition_specs);

        let mut rng = rand::rngs::StdRng::from_entropy();
        let task_iter = (0..num_partitions).map(move |i| {
            let sleep_duration = Duration::from_millis(rng.gen_range(100..300));
            SubmittableTask::new(
                MockTaskBuilder::new(partitions[i].clone())
                    .with_task_id(i as u32)
                    .with_sleep_duration(sleep_duration)
                    .build(),
            )
        });

        let results = materialize_all_pipeline_outputs(
            stream::iter(task_iter),
            test_context.handle().clone(),
            None,
        )
        .collect::<Vec<_>>()
        .await;

        verify_materialized_results(
            &results,
            &partition_specs,
            &std::iter::repeat(false)
                .take(partition_specs.len())
                .collect::<Vec<_>>(),
        );
        test_context.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_materialize_all_pipeline_outputs_with_error() -> DaftResult<()> {
        let num_partitions = 100;
        let worker_slots = 10;
        let error_probability = 0.1;
        let task_sleep_ms = 100;
        let mut rng = rand::rngs::StdRng::from_entropy();

        let test_context = TestContext::new(&[("worker1".into(), worker_slots)])?;
        let partition_specs = create_incremental_partition_specs(num_partitions);
        let partitions = create_test_partitions(&partition_specs);

        let should_error = (0..num_partitions)
            .map(move |_| rng.gen_bool(error_probability))
            .collect::<Vec<_>>();

        let task_iter = (0..num_partitions)
            .zip(should_error.clone())
            .map(move |(i, error)| {
                // Randomly inject errors
                if error {
                    SubmittableTask::new(
                        MockTaskBuilder::new(partitions[i].clone())
                            .with_task_id(i as u32)
                            .with_sleep_duration(Duration::from_millis(task_sleep_ms))
                            .with_failure(MockTaskFailure::Error("Error".into()))
                            .build(),
                    )
                } else {
                    SubmittableTask::new(
                        MockTaskBuilder::new(partitions[i].clone())
                            .with_task_id(i as u32)
                            .with_sleep_duration(Duration::from_millis(task_sleep_ms))
                            .build(),
                    )
                }
            });

        let results = materialize_all_pipeline_outputs(
            stream::iter(task_iter),
            test_context.handle().clone(),
            None,
        )
        .collect::<Vec<_>>()
        .await;

        verify_materialized_results(&results, &partition_specs, &should_error);
        test_context.cleanup().await?;
        Ok(())
    }
}
