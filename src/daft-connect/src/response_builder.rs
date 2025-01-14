use arrow2::io::ipc::write::StreamWriter;
use daft_table::Table;
use eyre::Context;
use spark_connect::{
    execute_plan_response::{ArrowBatch, ResponseType, ResultComplete},
    ExecutePlanResponse,
};
use uuid::Uuid;

use crate::session::Session;

/// spark responses are stateful, so we need to keep track of the session id, operation id, and server side session id
#[derive(Clone)]
pub struct ResponseBuilder {
    pub(crate) session: String,
    pub(crate) operation_id: String,
    pub(crate) server_side_session_id: String,
}

impl ResponseBuilder {
    /// Create a new response builder
    pub fn new(session: &Session) -> Self {
        Self::new_with_op_id(
            session.client_side_session_id(),
            session.server_side_session_id(),
            Uuid::new_v4().to_string(),
        )
    }

    pub fn new_with_op_id(
        client_side_session_id: impl Into<String>,
        server_side_session_id: impl Into<String>,
        operation_id: impl Into<String>,
    ) -> Self {
        let client_side_session_id = client_side_session_id.into();
        let server_side_session_id = server_side_session_id.into();
        let operation_id = operation_id.into();

        Self {
            session: client_side_session_id,
            server_side_session_id,
            operation_id,
        }
    }

    /// Send a result complete response to the client
    pub fn result_complete_response(&self) -> ExecutePlanResponse {
        ExecutePlanResponse {
            session_id: self.session.to_string(),
            server_side_session_id: self.server_side_session_id.to_string(),
            operation_id: self.operation_id.to_string(),
            response_id: Uuid::new_v4().to_string(),
            metrics: None,
            observed_metrics: vec![],
            schema: None,
            response_type: Some(ResponseType::ResultComplete(ResultComplete {})),
        }
    }

    /// Send an arrow batch response to the client
    pub fn arrow_batch_response(&self, table: &Table) -> eyre::Result<ExecutePlanResponse> {
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
            session_id: self.session.clone(),
            server_side_session_id: self.server_side_session_id.clone(),
            operation_id: self.operation_id.clone(),
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
