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
    program::PlanProducer,
    task::SwordfishTask,
};

pub struct Operator {
    pub input_plan_producer: PlanProducer,
    pub task_dispatcher_handle: TaskDispatcherHandle,
    pub config: Arc<DaftExecutionConfig>,
    pub result_tx: tokio::sync::mpsc::Sender<PartitionRef>,
    pub psets: HashMap<String, Vec<PartitionRef>>,
}

impl Operator {
    pub fn new(
        input_plan_producer: PlanProducer,
        task_dispatcher_handle: TaskDispatcherHandle,
        config: Arc<DaftExecutionConfig>,
        result_tx: tokio::sync::mpsc::Sender<PartitionRef>,
        psets: HashMap<String, Vec<PartitionRef>>,
    ) -> Self {
        Self {
            input_plan_producer,
            task_dispatcher_handle,
            config,
            result_tx,
            psets,
        }
    }

    async fn receive_inputs_and_submit_tasks(
        mut input_plan_producer: PlanProducer,
        task_dispatcher_handle: TaskDispatcherHandle,
        config: Arc<DaftExecutionConfig>,
        psets: HashMap<String, Vec<PartitionRef>>,
        result_tx: tokio::sync::mpsc::Sender<TaskResultReceiver>,
    ) -> DaftResult<()> {
        while let Some(plan) = input_plan_producer.next().await {
            let task = SwordfishTask::new(plan?, config.clone(), psets.clone());
            let result_rx = match task_dispatcher_handle.submit_task(task).await {
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
            self.input_plan_producer,
            self.task_dispatcher_handle.clone(),
            self.config,
            self.psets,
            result_tx,
        );

        let output_task = Self::await_results_and_send_to_output(result_rx, self.result_tx);

        tokio::try_join!(input_task, output_task)?;

        Ok(())
    }
}

// pub struct LimitOperator {
//     pub input_plan_rx: tokio::sync::mpsc::Receiver<LocalPhysicalPlanRef>,
//     pub task_dispatcher_handle: Arc<TaskDispatcherHandle>,
//     pub config: Arc<DaftExecutionConfig>,
//     pub result_tx: tokio::sync::mpsc::Sender<PartitionRef>,
//     pub limit: Arc<Mutex<usize>>,
// }

// impl LimitOperator {
//     pub fn new(
//         input_plan_rx: tokio::sync::mpsc::Receiver<LocalPhysicalPlanRef>,
//         task_dispatcher_handle: Arc<TaskDispatcherHandle>,
//         config: Arc<DaftExecutionConfig>,
//         result_tx: tokio::sync::mpsc::Sender<PartitionRef>,
//         limit: usize,
//     ) -> Self {
//         Self {
//             input_plan_rx,
//             task_dispatcher_handle,
//             config,
//             result_tx,
//             limit: Arc::new(Mutex::new(limit)),
//         }
//     }

//     async fn receive_inputs_and_submit_tasks(
//         mut input_plan_rx: tokio::sync::mpsc::Receiver<LocalPhysicalPlanRef>,
//         task_dispatcher_handle: Arc<TaskDispatcherHandle>,
//         config: Arc<DaftExecutionConfig>,
//         result_tx: tokio::sync::mpsc::Sender<TaskHandle>,
//         limit: Arc<Mutex<usize>>,
//     ) -> DaftResult<()> {
//         while let Some(plan) = input_plan_rx.recv().await {
//             let task = {
//                 let limit = limit.lock().unwrap();
//                 if *limit <= 0 {
//                     None
//                 } else {
//                     let stats_state = plan.get_stats_state().clone();
//                     let plan_with_limit =
//                         LocalPhysicalPlan::limit(plan, *limit as i64, stats_state);
//                     Some(Task::new(plan_with_limit, config.clone()))
//                 }
//             };
//             match task {
//                 Some(task) => {
//                     let result_rx = match task_dispatcher_handle.submit_task(task).await {
//                         Ok(result_rx) => result_rx,
//                         Err(_) => {
//                             break;
//                         }
//                     };
//                     if let Err(_) = result_tx.send(result_rx).await {
//                         break;
//                     }
//                 }
//                 None => {
//                     break;
//                 }
//             }
//         }
//         Ok(())
//     }

//     async fn await_results_and_send_to_output(
//         mut result_rx: tokio::sync::mpsc::Receiver<TaskHandle>,
//         result_tx: tokio::sync::mpsc::Sender<PartitionRef>,
//         limit: Arc<Mutex<usize>>,
//     ) -> DaftResult<()> {
//         while let Some(result_rx) = result_rx.recv().await {
//             let result = match result_rx.await {
//                 Some(result) => result,
//                 None => {
//                     break;
//                 }
//             };
//             for partition in result {
//                 let should_break = {
//                     let mut limit = limit.lock().unwrap();
//                     *limit -= partition.num_rows()?;
//                     *limit <= 0
//                 };
//                 if should_break {
//                     break;
//                 }
//                 if let Err(_) = result_tx.send(partition).await {
//                     break;
//                 }
//             }
//         }
//         Ok(())
//     }

//     pub async fn run(self) -> DaftResult<()> {
//         let (result_tx, result_rx) = tokio::sync::mpsc::channel(1);

//         let input_task = Self::receive_inputs_and_submit_tasks(
//             self.input_plan_rx,
//             self.task_dispatcher_handle,
//             self.config,
//             result_tx,
//             self.limit.clone(),
//         );

//         let output_task =
//             Self::await_results_and_send_to_output(result_rx, self.result_tx, self.limit);

//         tokio::try_join!(input_task, output_task)?;

//         Ok(())
//     }
// }
