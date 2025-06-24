use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use crate::{
    scheduling::worker::{Worker, WorkerManager},
    utils::{
        channel::{create_channel, Receiver, Sender},
        joinset::JoinSet,
    },
};

#[derive(Debug)]
pub(super) enum AutoscalerRequest {
    ScaleUp {
        workers: usize,
    },
    #[allow(dead_code)]
    ScaleDown {
        workers: usize,
    },
}

pub(super) struct AutoscalerActor<W: Worker> {
    worker_manager: Arc<dyn WorkerManager<Worker = W>>,
}

impl<W: Worker> AutoscalerActor<W> {
    pub fn new(worker_manager: Arc<dyn WorkerManager<Worker = W>>) -> Self {
        Self { worker_manager }
    }

    pub fn spawn_autoscaler_actor(
        autoscaler: Self,
        joinset: &mut JoinSet<DaftResult<()>>,
    ) -> AutoscalerHandle {
        let (autoscaler_sender, autoscaler_receiver) = create_channel(1);
        joinset.spawn(Self::run_autoscaler_loop(autoscaler, autoscaler_receiver));
        AutoscalerHandle::new(autoscaler_sender)
    }

    async fn run_autoscaler_loop(
        autoscaler: Self,
        mut autoscaler_receiver: Receiver<AutoscalerRequest>,
    ) -> DaftResult<()> {
        while let Some(request) = autoscaler_receiver.recv().await {
            match request {
                AutoscalerRequest::ScaleUp { workers } => {
                    autoscaler.worker_manager.try_autoscale(workers)?;
                }
                AutoscalerRequest::ScaleDown { workers } => {
                    autoscaler.worker_manager.try_autoscale(workers)?;
                }
            }
        }
        Ok(())
    }
}

pub(super) struct AutoscalerHandle {
    autoscaler_sender: Sender<AutoscalerRequest>,
}

impl AutoscalerHandle {
    pub fn new(autoscaler_sender: Sender<AutoscalerRequest>) -> Self {
        Self { autoscaler_sender }
    }

    pub async fn send_autoscaling_request(&self, request: AutoscalerRequest) -> DaftResult<()> {
        self.autoscaler_sender.send(request).await.map_err(|_| {
            DaftError::InternalError("Failed to send autoscaler request".to_string())
        })?;
        Ok(())
    }
}
