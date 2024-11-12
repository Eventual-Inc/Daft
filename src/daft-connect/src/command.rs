use std::future::ready;

use arrow2::io::ipc::write::StreamWriter;
use daft_table::Table;
use eyre::Context;
use futures::stream;
use spark_connect::{
    execute_plan_response::{ArrowBatch, ResponseType, ResultComplete},
    spark_connect_service_server::SparkConnectService,
    ExecutePlanResponse, Relation,
};
use tonic::Status;
use uuid::Uuid;

use crate::{convert::convert_data, DaftSparkConnectService, Session};

type DaftStream = <DaftSparkConnectService as SparkConnectService>::ExecutePlanStream;

pub struct PlanIds {
    session: String,
    server_side_session: String,
    operation: String,
}

impl PlanIds {
    pub fn gen_response(&self, table: &Table) -> eyre::Result<ExecutePlanResponse> {
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
            session_id: self.session.to_string(),
            server_side_session_id: self.server_side_session.to_string(),
            operation_id: self.operation.to_string(),
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

        Ok(response)
    }
}

impl Session {
    pub async fn handle_root_command(
        &self,
        command: Relation,
        operation_id: String,
    ) -> Result<DaftStream, Status> {
        use futures::{StreamExt, TryStreamExt};

        let context = PlanIds {
            session: self.client_side_session_id().to_string(),
            server_side_session: self.server_side_session_id().to_string(),
            operation: operation_id.clone(),
        };

        let finished = ExecutePlanResponse {
            session_id: self.client_side_session_id().to_string(),
            server_side_session_id: self.server_side_session_id().to_string(),
            operation_id,
            response_id: Uuid::new_v4().to_string(),
            metrics: None,
            observed_metrics: vec![],
            schema: None,
            response_type: Some(ResponseType::ResultComplete(ResultComplete {})),
        };

        let stream = convert_data(command, &context)
            .map_err(|e| Status::internal(e.to_string()))?
            .chain(stream::once(ready(Ok(finished))));

        Ok(Box::pin(
            stream.map_err(|e| Status::internal(e.to_string())),
        ))
    }
}
