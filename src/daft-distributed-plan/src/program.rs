use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;

use crate::{
    dispatcher::TaskDispatcher,
    stage::{CollectStage, Stage},
    task::Task,
};

pub enum Program {
    Collect(CollectProgram),
}

impl Program {
    pub fn from_stage(
        stage: Stage,
        dispatcher: TaskDispatcher,
        config: Arc<DaftExecutionConfig>,
    ) -> Self {
        match stage {
            Stage::Collect(stage) => Self::Collect(CollectProgram::new(stage, dispatcher, config)),
        }
    }

    pub fn run_program(self) -> ProgramResultIterator {
        match self {
            Program::Collect(program) => program.run_program(),
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
        #[cfg(feature = "python")]
        {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let mut collect_stage = self.collect_stage;
            let task_dispatcher = self.task_dispatcher;
            let config = self.config;
            use pyo3::Python;
            pyo3::prepare_freethreaded_python();
            let handle = std::thread::spawn(move || {
                Python::with_gil(|py| {
                    let mut local_thread_runtime = tokio::runtime::Builder::new_current_thread();
                    local_thread_runtime.enable_all();
                    pyo3_async_runtimes::tokio::init(local_thread_runtime);
                    std::thread::spawn(move || {
                        pyo3_async_runtimes::tokio::get_runtime()
                            .block_on(futures::future::pending::<()>());
                    });

                    pyo3_async_runtimes::tokio::run(py, async move {
                        let (task_dispatcher_tx, task_dispatcher_rx) =
                            tokio::sync::mpsc::channel(1);
                        let (result_tx, mut result_rx) = tokio::sync::mpsc::channel(1);
                        let task_dispatcher_task = TaskDispatcher::run_task_dispatch(
                            task_dispatcher,
                            task_dispatcher_rx,
                            result_tx,
                        );
                        let plan_sender_task = async move {
                            while let Some(plan) = collect_stage.next_plan()? {
                                let next_task = Task::new(plan, config.clone());
                                tokio::select! {
                                    biased;
                                    send_result = task_dispatcher_tx.send(next_task) => {
                                        if let Err(e) = send_result {
                                            eprintln!("Error sending task to task dispatcher: {}", e);
                                            break;
                                        }
                                    }
                                    Some(result) = result_rx.recv() => {
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
                            while let Some(result) = result_rx.recv().await {
                                if let Err(e) = tx.send(result).await {
                                    eprintln!("Error sending result to result_tx: {}", e);
                                    break;
                                }
                            }
                            drop(tx);
                            DaftResult::Ok(())
                        };
                        let (first, second) = tokio::join!(task_dispatcher_task, plan_sender_task);
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
        #[cfg(not(feature = "python"))]
        {
            panic!("Python is not enabled");
        }
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
