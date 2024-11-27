use std::{collections::HashMap, future::ready, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use daft_local_execution::NativeExecutor;
use futures::stream;
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
        tokio::spawn(async move {
            let execution_fut = async {
                let plan = translation::to_logical_plan(command)?;
                let optimized_plan = plan.optimize()?;
                let cfg = Arc::new(DaftExecutionConfig::default());
                let native_executor = NativeExecutor::from_logical_plan_builder(&optimized_plan)?;
                let mut result_stream = native_executor
                    .run(HashMap::new(), cfg, None)?
                    .into_stream();

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
