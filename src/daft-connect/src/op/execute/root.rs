use std::{collections::HashMap, future::ready};

use common_daft_config::DaftExecutionConfig;
use futures::stream;
use spark_connect::{ExecutePlanResponse, Relation};
use tokio_util::sync::CancellationToken;
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

        let (tx, rx) = tokio::sync::mpsc::channel::<eyre::Result<ExecutePlanResponse>>(16);
        std::thread::spawn(move || {
            let result = (|| -> eyre::Result<()> {
                let plan = translation::to_logical_plan(command)?;
                let logical_plan = plan.build();
                let physical_plan = daft_local_plan::translate(&logical_plan)?;

                let cfg = DaftExecutionConfig::default();
                let results = daft_local_execution::run_local(
                    &physical_plan,
                    HashMap::new(),
                    cfg.into(),
                    None,
                    CancellationToken::new(), // todo: maybe implement cancelling
                )?;

                for result in results {
                    let result = result?;
                    let tables = result.get_tables()?;

                    for table in tables.as_slice() {
                        let response = context.gen_response(table)?;
                        tx.blocking_send(Ok(response)).unwrap();
                    }
                }
                Ok(())
            })();

            if let Err(e) = result {
                tx.blocking_send(Err(e)).unwrap();
            }
        });

        let stream = ReceiverStream::new(rx);

        let stream = stream
            .map_err(|e| Status::internal(format!("Error in Daft server: {e:?}")))
            .chain(stream::once(ready(Ok(finished))));

        Ok(Box::pin(stream))
    }
}
