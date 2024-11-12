use std::thread;

use arrow2::io::ipc::write::StreamWriter;
use daft_table::Table;
use eyre::Context;
use futures::TryStreamExt;
use spark_connect::{
    execute_plan_response::{ArrowBatch, ResponseType, ResultComplete},
    spark_connect_service_server::SparkConnectService,
    ExecutePlanResponse, Relation,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Status;
use uuid::Uuid;

use crate::{convert::convert_data, DaftSparkConnectService, Session};

type DaftStream = <DaftSparkConnectService as SparkConnectService>::ExecutePlanStream;

struct ExecutablePlanChannel {
    session_id: String,
    server_side_session_id: String,
    operation_id: String,
    tx: tokio::sync::mpsc::UnboundedSender<eyre::Result<ExecutePlanResponse>>,
}

pub trait ConcreteDataChannel {
    fn send_table(&mut self, table: &Table) -> eyre::Result<()>;
}

impl ConcreteDataChannel for ExecutablePlanChannel {
    fn send_table(&mut self, table: &Table) -> eyre::Result<()> {
        let mut data = Vec::new();

        let mut writer = StreamWriter::new(
            &mut data,
            arrow2::io::ipc::write::WriteOptions { compression: None },
        );

        let row_count = table.num_rows();

        let schema = table
            .schema
            .to_arrow()
            .wrap_err("Failed to convert Daft schema to Arrow schema")?;

        writer
            .start(&schema, None)
            .wrap_err("Failed to start Arrow stream writer with schema")?;

        let arrays = table.get_inner_arrow_arrays().collect();
        let chunk = arrow2::chunk::Chunk::new(arrays);

        writer
            .write(&chunk, None)
            .wrap_err("Failed to write Arrow chunk to stream writer")?;

        let response = ExecutePlanResponse {
            session_id: self.session_id.to_string(),
            server_side_session_id: self.server_side_session_id.to_string(),
            operation_id: self.operation_id.to_string(),
            response_id: Uuid::new_v4().to_string(), // todo: implement this
            metrics: None,                           // todo: implement this
            observed_metrics: vec![],
            schema: None,
            response_type: Some(ResponseType::ArrowBatch(ArrowBatch {
                row_count: row_count as i64,
                data,
                start_offset: None,
            })),
        };

        self.tx
            .send(Ok(response))
            .wrap_err("Error sending response to client")?;

        Ok(())
    }
}

impl Session {
    pub async fn handle_root_command(
        &self,
        command: Relation,
        operation_id: String,
    ) -> Result<DaftStream, Status> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let mut channel = ExecutablePlanChannel {
            session_id: self.client_side_session_id().to_string(),
            server_side_session_id: self.server_side_session_id().to_string(),
            operation_id: operation_id.clone(),
            tx: tx.clone(),
        };

        thread::spawn({
            let session_id = self.client_side_session_id().to_string();
            let server_side_session_id = self.server_side_session_id().to_string();
            move || {
                let result = convert_data(command, &mut channel);

                if let Err(e) = result {
                    tx.send(Err(e)).unwrap();
                } else {
                    let finished = ExecutePlanResponse {
                        session_id,
                        server_side_session_id,
                        operation_id: operation_id.to_string(),
                        response_id: Uuid::new_v4().to_string(),
                        metrics: None,
                        observed_metrics: vec![],
                        schema: None,
                        response_type: Some(ResponseType::ResultComplete(ResultComplete {})),
                    };

                    tx.send(Ok(finished)).unwrap();
                }
            }
        });

        let recv_stream =
            UnboundedReceiverStream::new(rx).map_err(|e| Status::internal(e.to_string()));

        Ok(Box::pin(recv_stream))
    }
}
