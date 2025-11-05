use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_runtime::get_compute_runtime;
use daft_micropartition::MicroPartition;
use tokio::sync::{Mutex, mpsc};

use crate::{
    RuntimeHandle,
    buffer::RowBasedBuffer,
    channel::{Sender, create_channel},
    dispatcher::{DispatchSpawner, SpawnedDispatchResult},
    pipeline::MorselSizeRequirement,
    runtime_stats::InitializingCountingReceiver,
};

#[derive(Debug, Clone)]
struct ChannelStats {
    total_sent: Arc<Mutex<usize>>,
    inflight_count: Arc<Mutex<usize>>,
}

impl ChannelStats {
    fn new() -> Self {
        Self {
            total_sent: Arc::new(Mutex::new(0)),
            inflight_count: Arc::new(Mutex::new(0)),
        }
    }

    async fn increment_sent(&self) {
        let mut total = self.total_sent.lock().await;
        let mut inflight = self.inflight_count.lock().await;
        *total += 1;
        *inflight += 1;
    }

    async fn decrement_inflight(&self) {
        let mut inflight = self.inflight_count.lock().await;
        *inflight = inflight.saturating_sub(1);
    }

    async fn get_stats(&self) -> (usize, usize) {
        let total = *self.total_sent.lock().await;
        let inflight = *self.inflight_count.lock().await;
        (total, inflight)
    }
}

/// Adaptive dispatcher that uses a shared queue and worker coroutines
pub struct AdaptiveDispatcher {
    morsel_size_lower_bound: usize,
    morsel_size_upper_bound: usize,
}

impl AdaptiveDispatcher {
    pub fn new(morsel_size_requirement: MorselSizeRequirement) -> Self {
        let (lower_bound, upper_bound) = match morsel_size_requirement {
            MorselSizeRequirement::Strict(size) => (size, size),
            MorselSizeRequirement::Flexible(lower, upper) => (lower, upper),
        };
        Self {
            morsel_size_lower_bound: lower_bound,
            morsel_size_upper_bound: upper_bound,
        }
    }

    async fn dispatch_inner(
        worker_senders: Vec<Sender<Arc<MicroPartition>>>,
        input_receivers: Vec<InitializingCountingReceiver>,
        morsel_size_lower_bound: usize,
        morsel_size_upper_bound: usize,
    ) -> DaftResult<()> {
        let (work_tx, work_rx) = mpsc::channel::<Arc<MicroPartition>>(worker_senders.len());
        let work_rx = Arc::new(Mutex::new(work_rx));
        let stats = ChannelStats::new();
        let compute_runtime = get_compute_runtime();

        let mut worker_tasks = vec![];
        for (worker_idx, worker_sender) in worker_senders.into_iter().enumerate() {
            let work_rx = Arc::clone(&work_rx);
            let stats = stats.clone();

            let worker_task = compute_runtime.spawn(async move {
                log::debug!(
                    "Worker {} started, will pull from shared work queue when idle",
                    worker_idx
                );
                let mut processed_count = 0;

                loop {
                    let morsel = {
                        let mut rx = work_rx.lock().await;
                        let (_, inflight) = stats.get_stats().await;
                        log::debug!(
                            "Worker {}: got lock, waiting for morsel, queue size: {}",
                            worker_idx,
                            inflight
                        );
                        let morsel = match rx.recv().await {
                            Some(morsel) => {
                                log::debug!(
                                    "Worker {}: got a morsel, queue size: {}",
                                    worker_idx,
                                    inflight - 1
                                );
                                morsel
                            }
                            None => {
                                log::debug!(
                                    "Worker {}: work queue closed, processed {} morsels total",
                                    worker_idx,
                                    processed_count
                                );
                                break;
                            }
                        };
                        log::debug!("Worker {}: release lock", worker_idx);
                        morsel
                    };

                    stats.decrement_inflight().await;
                    if worker_sender.send(morsel.clone()).await.is_err() {
                        log::debug!("Worker {}: send failed, exiting", worker_idx);
                        break;
                    } else {
                        log::debug!("Worker {}: send morsel success", worker_idx);
                    }

                    processed_count += 1;
                    if processed_count % 100 == 0 {
                        log::debug!(
                            "Worker {} processed {} morsels",
                            worker_idx,
                            processed_count
                        );
                    }
                }
            });

            worker_tasks.push(worker_task);
        }

        let stats_value = stats.clone();
        let producer_task = compute_runtime.spawn(async move {
            log::debug!("Producer task started, pushing to shared work queue");
            let mut total_morsels = 0;
            let stats = stats_value.clone();

            for receiver in input_receivers {
                let mut buffer =
                    RowBasedBuffer::new(morsel_size_lower_bound, morsel_size_upper_bound);

                while let Some(morsel) = receiver.recv().await {
                    buffer.push(&morsel);

                    while let Some(ready_morsels) = buffer.pop_enough()? {
                        for ready_morsel in ready_morsels {
                            if work_tx.send(ready_morsel).await.is_err() {
                                log::debug!("Producer: work queue send failed");
                                return Ok::<(), DaftError>(());
                            } else {
                                let (_, inflight) = stats.get_stats().await;
                                log::debug!(
                                    "Producer send morsel success, queue size: {}",
                                    inflight + 1
                                );
                            }
                            stats.increment_sent().await;
                            total_morsels += 1;
                        }
                    }
                }

                if let Some(last_morsel) = buffer.pop_all()? {
                    if work_tx.send(last_morsel).await.is_err() {
                        log::debug!("Producer: final work queue send failed");
                        return Ok(());
                    } else {
                        let (_, inflight) = stats.get_stats().await;
                        log::debug!("Producer send morsel success, queue size: {}", inflight + 1);
                    }
                    stats.increment_sent().await;
                    total_morsels += 1;
                }
            }

            log::debug!("Producer finished, sent {} morsels total", total_morsels);

            drop(work_tx);

            Ok(())
        });

        match producer_task.await {
            Ok(Ok(())) => {
                let (total_sent, inflight) = stats.get_stats().await;
                log::info!(
                    "Producer task completed successfully, Final stats - Total sent: {}, Inflight: {}",
                    total_sent,
                    inflight
                );
            }
            Ok(Err(e)) => log::error!("Producer task failed with DaftError: {:?}", e),
            Err(e) => log::error!("Producer task panicked: {:?}", e),
        }

        for (idx, worker_task) in worker_tasks.into_iter().enumerate() {
            if let Err(e) = worker_task.await {
                log::error!("Worker {} task failed: {:?}", idx, e);
            }
        }

        let (total_sent, inflight) = stats.get_stats().await;
        log::info!(
            "Dispatcher shutdown - Total sent: {}, Inflight: {}",
            total_sent,
            inflight
        );

        log::info!("Adaptive dispatcher completed");
        Ok(())
    }
}

impl DispatchSpawner for AdaptiveDispatcher {
    fn spawn_dispatch(
        &self,
        input_receivers: Vec<InitializingCountingReceiver>,
        num_workers: usize,
        runtime_handle: &mut RuntimeHandle,
    ) -> SpawnedDispatchResult {
        let (worker_senders, worker_receivers): (Vec<_>, Vec<_>) =
            (0..num_workers).map(|_| create_channel(0)).unzip();

        let morsel_size_lower_bound = self.morsel_size_lower_bound;
        let morsel_size_upper_bound = self.morsel_size_upper_bound;

        let task = runtime_handle.spawn(async move {
            Self::dispatch_inner(
                worker_senders,
                input_receivers,
                morsel_size_lower_bound,
                morsel_size_upper_bound,
            )
            .await
        });

        SpawnedDispatchResult {
            worker_receivers,
            spawned_dispatch_task: task,
        }
    }
}
