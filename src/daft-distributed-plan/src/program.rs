use std::sync::{Arc, OnceLock};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use pyo3::Python;

use crate::{
    dispatcher::TaskDispatcher,
    stage::{CollectStage, LimitStage, Stage},
    task::Task,
};

pub enum Program {
    Collect(CollectProgram),
    Limit(LimitProgram),
}

impl Program {
    pub fn from_stage(
        stage: Stage,
        dispatcher: TaskDispatcher,
        config: Arc<DaftExecutionConfig>,
    ) -> Self {
        match stage {
            Stage::Collect(stage) => Self::Collect(CollectProgram::new(stage, dispatcher, config)),
            Stage::Limit(stage) => Self::Limit(LimitProgram::new(stage, dispatcher)),
        }
    }

    pub fn run_program(self) -> ProgramResultIterator {
        match self {
            Program::Collect(program) => program.run_program(),
            Program::Limit(program) => program.run_program(),
        }
    }
}
pub(crate) struct CollectProgram {
    collect_stage: CollectStage,
    task_dispatcher: TaskDispatcher,
    config: Arc<DaftExecutionConfig>,
}

impl CollectProgram {
    pub fn new(
        collect_stage: CollectStage,
        task_dispatcher: TaskDispatcher,
        config: Arc<DaftExecutionConfig>,
    ) -> Self {
        Self {
            collect_stage,
            task_dispatcher,
            config,
        }
    }

    pub fn run_program(self) -> ProgramResultIterator {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mut collect_stage = self.collect_stage;
        let task_dispatcher = self.task_dispatcher;
        let config = self.config;
        pyo3::prepare_freethreaded_python();
        let handle = std::thread::spawn(move || {
            Python::with_gil(|py| {
                println!("initializing tokio runtime");
                let mut local_thread_runtime = tokio::runtime::Builder::new_current_thread();
                local_thread_runtime.enable_all();
                pyo3_async_runtimes::tokio::init(local_thread_runtime);
                std::thread::spawn(move || {
                    pyo3_async_runtimes::tokio::get_runtime()
                        .block_on(futures::future::pending::<()>());
                });
                println!("initialized tokio runtime");

                println!("running tokio runtime");
                pyo3_async_runtimes::tokio::run(py, async move {
                    println!("starting task dispatcher actor");

                    let (task_dispatcher_tx, task_dispatcher_rx) = tokio::sync::mpsc::channel(1);
                    let (result_tx, mut result_rx) = tokio::sync::mpsc::channel(1);
                    let task_dispatcher_task = async move {
                        let r = TaskDispatcher::run_task_dispatch(
                            task_dispatcher,
                            task_dispatcher_rx,
                            result_tx,
                        )
                        .await;
                        println!("task dispatcher task done");
                        r
                    };
                    let plan_sender_task = async move {
                        while let Some(plan) = collect_stage.next_plan()? {
                            let next_task = Task::new(plan, config.clone());
                            tokio::select! {
                                biased;
                                send_result = task_dispatcher_tx.send(next_task) => {
                                    println!("sending task to task dispatcher");
                                    if let Err(e) = send_result {
                                        eprintln!("Error sending task to task dispatcher: {}", e);
                                        break;
                                    }
                                }
                                Some(result) = result_rx.recv() => {
                                    println!("received result from result_rx");
                                    if let Err(e) = tx.send(result).await {
                                        eprintln!("Error sending result to result_tx: {}", e);
                                        break;
                                    }
                                }
                                else => {
                                    break;
                                }
                            }
                        }
                        drop(task_dispatcher_tx);
                        println!("no more plans to execute");
                        while let Some(result) = result_rx.recv().await {
                            println!("received result from result_rx");
                            if let Err(e) = tx.send(result).await {
                                eprintln!("Error sending result to result_tx: {}", e);
                                break;
                            }
                        }
                        println!("no more results to send");
                        drop(tx);
                        DaftResult::Ok(())
                    };
                    println!("awaiting task dispatcher actor");
                    let (first, second) = tokio::join!(task_dispatcher_task, plan_sender_task);
                    println!("task dispatcher actor done");
                    first?;
                    second?;
                    Ok(())
                })?;
                DaftResult::Ok(())
            })?;
            Ok(())
        });
        ProgramResultIterator::new(rx, handle)
    }
}

pub(crate) struct LimitProgram {
    limit_stage: LimitStage,
    task_dispatcher: TaskDispatcher,
}

impl LimitProgram {
    pub fn new(limit_stage: LimitStage, task_dispatcher: TaskDispatcher) -> Self {
        Self {
            limit_stage,
            task_dispatcher,
        }
    }

    pub fn run_program(self) -> ProgramResultIterator {
        todo!()
    }
}

pub(crate) struct ProgramResultIterator {
    receiver: tokio::sync::mpsc::Receiver<Vec<PartitionRef>>,
    handle: Option<std::thread::JoinHandle<DaftResult<()>>>,
}

impl ProgramResultIterator {
    pub fn new(
        receiver: tokio::sync::mpsc::Receiver<Vec<PartitionRef>>,
        handle: std::thread::JoinHandle<DaftResult<()>>,
    ) -> Self {
        Self {
            receiver,
            handle: Some(handle),
        }
    }
}

impl Iterator for ProgramResultIterator {
    type Item = DaftResult<Vec<PartitionRef>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.blocking_recv() {
            Some(result) => Some(Ok(result)),
            None => {
                if let Some(handle) = self.handle.take() {
                    let handle_result = handle.join().unwrap();
                    match handle_result {
                        Ok(_) => None,
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    None
                }
            }
        }
    }
}
