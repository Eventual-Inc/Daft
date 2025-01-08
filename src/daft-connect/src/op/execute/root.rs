use std::{future::ready, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_runtime::{get_compute_runtime, RuntimeRef};
use daft_local_execution::{ExecutionRuntimeContext, NativeExecutor};
use daft_ray_execution::RayRunnerShim;
use futures::stream;
use pyo3::Python;
use spark_connect::{ExecutePlanResponse, Relation};
use tonic::{codegen::tokio_stream::wrappers::ReceiverStream, Status};

use crate::{
    op::execute::{ExecuteStream, PlanIds},
    session::Session,
    translation,
};

impl Session {
    pub async fn handle_root_command(
        &self,
        command: Relation,
        operation_id: String,
    ) -> Result<ExecuteStream, Status> {
        use futures::{StreamExt, TryStreamExt};

        let context = PlanIds {
            session: self.client_side_session_id().to_string(),
            server_side_session: self.server_side_session_id().to_string(),
            operation: operation_id,
        };

        let finished = context.finished();

        let (tx, rx) = tokio::sync::mpsc::channel::<eyre::Result<ExecutePlanResponse>>(1);

        let pset = self.psets.clone();

        self.runtime
            .block_on(async move {
                let execution_fut = async {
                    let translator = translation::SparkAnalyzer::new(&pset);
                    let lp = translator.to_logical_plan(command).await?;

                    let runner = RayRunnerShim::try_new(None, None, None)?;
                    let result_set = tokio::task::spawn_blocking(move || {
                        Python::with_gil(|py| {
                            let res = runner.run_iter_impl(py, lp, None).unwrap();

                            let res = res.into_iter().flat_map(|res| {
                                let result = res.unwrap();
                                let tables = result.get_tables().unwrap();
                                Arc::unwrap_or_clone(tables)
                            });
                            res.collect::<Vec<_>>()
                        })
                    })
                    .await?;

                    let mut iter = result_set.into_iter();

                    while let Some(table) = iter.next() {
                        let response = context.gen_response(&table)?;
                        if tx.send(Ok(response)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Ok(())
                };

                if let Err(e) = execution_fut.await {
                    let _ = tx.send(Err(e)).await;
                }
            })
            .unwrap();

        let stream = ReceiverStream::new(rx);

        let stream = stream
            .map_err(|e| Status::internal(format!("Error in Daft server: {e:?}")))
            .chain(stream::once(ready(Ok(finished))));

        Ok(Box::pin(stream))
    }
}
