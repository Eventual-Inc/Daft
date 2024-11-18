use std::{collections::HashMap, future::ready};

use common_daft_config::DaftExecutionConfig;
use futures::stream;
use spark_connect::{ExecutePlanResponse, Relation};
use tokio_util::sync::CancellationToken;
use tonic::{codegen::tokio_stream::wrappers::UnboundedReceiverStream, Status};

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

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<eyre::Result<ExecutePlanResponse>>();

        std::thread::spawn(move || {
            let plan = match translation::to_logical_plan(command) {
                Ok(plan) => plan,
                Err(e) => {
                    tx.send(Err(eyre::eyre!(e))).unwrap();
                    return;
                }
            };

            let logical_plan = plan.build();
            let physical_plan = match daft_local_plan::translate(&logical_plan) {
                Ok(plan) => plan,
                Err(e) => {
                    tx.send(Err(eyre::eyre!(e))).unwrap();
                    return;
                }
            };

            let cfg = DaftExecutionConfig::default();
            let result = match daft_local_execution::run_local(
                &physical_plan,
                HashMap::new(),
                cfg.into(),
                None,
                CancellationToken::new(), // todo: maybe implement cancelling
            ) {
                Ok(result) => result,
                Err(e) => {
                    tx.send(Err(eyre::eyre!(e))).unwrap();
                    return;
                }
            };

            for result in result {
                let result = match result {
                    Ok(result) => result,
                    Err(e) => {
                        tx.send(Err(eyre::eyre!(e))).unwrap();
                        return;
                    }
                };

                let tables = match result.get_tables() {
                    Ok(tables) => tables,
                    Err(e) => {
                        tx.send(Err(eyre::eyre!(e))).unwrap();
                        return;
                    }
                };

                for table in tables.as_slice() {
                    let response = context.gen_response(table);

                    let response = match response {
                        Ok(response) => response,
                        Err(e) => {
                            tx.send(Err(eyre::eyre!(e))).unwrap();
                            return;
                        }
                    };

                    tx.send(Ok(response)).unwrap();
                }
            }
        });

        let stream = UnboundedReceiverStream::new(rx);

        let stream = stream
            .map_err(|e| Status::internal(format!("Error in Daft server: {e:?}")))
            .chain(stream::once(ready(Ok(finished))));

        Ok(Box::pin(stream))
    }
}
