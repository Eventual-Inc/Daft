use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use futures::StreamExt;

use crate::{
    dispatcher::{TaskDispatcherHandle, TaskResultReceiver},
    program::TaskProducer,
    task::SwordfishTask,
};

pub struct Operator {
    pub task_producer: TaskProducer,
    pub task_dispatcher_handle: TaskDispatcherHandle,
    pub result_tx: tokio::sync::mpsc::Sender<PartitionRef>,
}

impl Operator {
    pub fn new(
        task_producer: TaskProducer,
        task_dispatcher_handle: TaskDispatcherHandle,
        result_tx: tokio::sync::mpsc::Sender<PartitionRef>,
    ) -> Self {
        Self {
            task_producer,
            task_dispatcher_handle,
            result_tx,
        }
    }

    async fn receive_inputs_and_submit_tasks(
        mut task_producer: TaskProducer,
        task_dispatcher_handle: TaskDispatcherHandle,
        result_tx: tokio::sync::mpsc::Sender<TaskResultReceiver>,
    ) -> DaftResult<()> {
        while let Some(task) = task_producer.next().await {
            let result_rx = match task_dispatcher_handle.submit_task(task?).await {
                Ok(result_rx) => result_rx,
                Err(_) => {
                    break;
                }
            };
            println!("operator sending result rx");
            if let Err(_) = result_tx.send(result_rx).await {
                break;
            }
            println!("operator sent result rx");
        }
        Ok(())
    }

    async fn await_results_and_send_to_output(
        mut result_rx: tokio::sync::mpsc::Receiver<TaskResultReceiver>,
        result_tx: tokio::sync::mpsc::Sender<PartitionRef>,
    ) -> DaftResult<()> {
        while let Some(result_rx) = result_rx.recv().await {
            println!("got result rx");
            let result = match result_rx.await {
                Some(result) => {
                    println!("got result in await_results_and_send_to_output");
                    if let Err(e) = &result {
                        println!("error sending result: {}", e);
                    }
                    result?
                }
                None => {
                    println!("no result in await_results_and_send_to_output");
                    break;
                }
            };
            println!("operator sending result");
            if let Err(_) = result_tx.send(result).await {
                break;
            }
        }
        Ok(())
    }

    pub async fn run(self) -> DaftResult<()> {
        let (result_tx, result_rx) = tokio::sync::mpsc::channel(1);

        let input_task = Self::receive_inputs_and_submit_tasks(
            self.task_producer,
            self.task_dispatcher_handle.clone(),
            result_tx,
        );

        let output_task = Self::await_results_and_send_to_output(result_rx, self.result_tx);

        tokio::try_join!(input_task, output_task)?;

        Ok(())
    }
}
