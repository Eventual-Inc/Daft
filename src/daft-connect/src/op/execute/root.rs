use std::{future::ready, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use daft_local_execution::NativeExecutor;
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::MicroPartition;
use daft_ray_execution::RayEngine;
use eyre::bail;
use futures::stream::{self, BoxStream};
use itertools::Itertools;
use pyo3::Python;
use spark_connect::{ExecutePlanResponse, Relation};
use tonic::{codegen::tokio_stream::wrappers::ReceiverStream, Status};

use crate::{
    op::execute::{ExecuteStream, PlanIds},
    session::Session,
    translation, Runner,
};

impl Session {
    pub fn get_runner(&self) -> eyre::Result<Runner> {
        let runner = match self.config_values().get("daft.runner") {
            Some(runner) => match runner.as_str() {
                "ray" => Runner::Ray,
                "native" => Runner::Native,
                _ => bail!("Invalid runner: {}", runner),
            },
            None => Runner::Native,
        };
        Ok(runner)
    }

    pub async fn run_query(
        &self,
        lp: LogicalPlanBuilder,
    ) -> eyre::Result<BoxStream<DaftResult<Arc<MicroPartition>>>> {
        match self.get_runner()? {
            Runner::Ray => {
                let runner = RayEngine::try_new(None, None, None)?;
                let result_set = tokio::task::spawn_blocking(move || {
                    Python::with_gil(|py| {
                        let res = runner.run_iter_impl(py, lp, None).unwrap();

                        res
                    })
                })
                .await?;

                Ok(Box::pin(stream::iter(result_set)))
            }

            Runner::Native => {
                let this = self.clone();
                let result_stream = tokio::task::spawn_blocking(move || {
                    let plan = lp.optimize()?;
                    let cfg = Arc::new(DaftExecutionConfig::default());
                    let native_executor = NativeExecutor::from_logical_plan_builder(&plan)?;
                    let results = native_executor.run(&*this.psets, cfg, None)?;
                    let it = results.into_iter();
                    Ok::<_, DaftError>(it.collect_vec())
                })
                .await??;

                Ok(Box::pin(stream::iter(result_stream)))
            }
        }
    }

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

        let this = self.clone();

        tokio::spawn(async move {
            let execution_fut = async {
                let translator = translation::SparkAnalyzer::new(&this);
                let lp = translator.to_logical_plan(command).await?;

                let mut result_stream = this.run_query(lp).await?;

                while let Some(result) = result_stream.next().await {
                    let result = result?;
                    let tables = result.get_tables()?;
                    for table in tables.as_slice() {
                        let response = context.gen_response(table)?;
                        if tx.send(Ok(response)).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                Ok(())
            };

            if let Err(e) = execution_fut.await {
                let _ = tx.send(Err(e)).await;
            }
        });

        let stream = ReceiverStream::new(rx);

        let stream = stream
            .map_err(|e| Status::internal(format!("Error in Daft server: {e:?}")))
            .chain(stream::once(ready(Ok(finished))));

        Ok(Box::pin(stream))
    }
}
