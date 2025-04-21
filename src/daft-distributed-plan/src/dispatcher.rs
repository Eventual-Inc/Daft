use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use futures::StreamExt;

use crate::{
    task::{Task, TaskHandle},
    worker::WorkerManager,
};

pub(crate) struct TaskDispatcher {
    worker_manager: Arc<dyn WorkerManager>,
}

impl TaskDispatcher {
    pub fn new(worker_manager: Arc<dyn WorkerManager>) -> Self {
        Self { worker_manager }
    }

    pub async fn run_task_dispatch(
        dispatcher: Self,
        mut task_rx: tokio::sync::mpsc::Receiver<Task>,
        result_tx: tokio::sync::mpsc::Sender<Vec<PartitionRef>>,
    ) -> DaftResult<()> {
        let mut pending_tasks = futures::stream::FuturesOrdered::new();
        loop {
            let next_available_worker = dispatcher.get_available_worker();
            let has_available_worker = next_available_worker.is_some();
            println!("has_available_worker: {}", has_available_worker);
            let num_pending_tasks = pending_tasks.len();
            println!("num_pending_tasks: {}", num_pending_tasks);
            tokio::select! {
                biased;
                Some(task) = task_rx.recv(), if has_available_worker => {
                    println!("dispatching task to worker");
                    let task_handle = dispatcher
                        .worker_manager
                        .submit_task_to_worker(task, next_available_worker.unwrap());
                    pending_tasks.push_back(async move {
                        task_handle.get_result().await
                    });
                }
                Some(result) = pending_tasks.next(), if num_pending_tasks > 0 => {
                    println!("received result from task");
                    let result = result?;
                    println!("sending result to result_tx");
                    if let Err(e) = result_tx.send(result).await {
                        eprintln!("Error sending result to result_tx: {}", e);
                        break;
                    }
                }
                else => {
                    break;
                }
            }
            println!("num_pending_tasks: {}", num_pending_tasks);
        }
        println!("exiting task dispatch");
        Ok(())
    }

    pub fn get_available_worker(&self) -> Option<String> {
        let worker_resources = self.worker_manager.get_worker_resources();
        println!("worker_resources: {:?}", worker_resources);
        // get the worker with the most available memory
        worker_resources
            .into_iter()
            .max_by_key(|(_, _, memory)| *memory)
            .map(|(worker_id, _, _)| worker_id)
    }
}
