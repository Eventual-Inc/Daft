use arrow2::io::ipc::write::StreamWriter;
use daft_recordbatch::RecordBatch;
use spark_connect::{
    analyze_plan_response,
    execute_plan_response::{ArrowBatch, ResponseType, ResultComplete},
    AnalyzePlanResponse, DataType, ExecutePlanResponse,
};
use uuid::Uuid;

use crate::{
    error::{ConnectResult, Context},
    session::ConnectSession,
};

/// A utility for constructing responses to send back to the client,
/// It's generic over the type of response it can build, which is determined by the type parameter `T`
///
/// spark responses are stateful, so we need to keep track of the session id, operation id, and server side session id
#[derive(Clone)]
pub struct ResponseBuilder<T> {
    pub(crate) session: String,
    pub(crate) operation_id: String,
    pub(crate) server_side_session_id: String,
    pub(crate) phantom: std::marker::PhantomData<T>,
}
impl<T> ResponseBuilder<T> {
    pub fn new(session: &ConnectSession, operation_id: String) -> Self {
        Self::new_with_op_id(
            session.client_side_session_id(),
            session.server_side_session_id(),
            operation_id,
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
            phantom: std::marker::PhantomData,
        }
    }
}

impl ResponseBuilder<ExecutePlanResponse> {
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
    pub fn arrow_batch_response(&self, table: &RecordBatch) -> ConnectResult<ExecutePlanResponse> {
        let mut data = Vec::new();

        let mut writer = StreamWriter::new(
            &mut data,
            arrow2::io::ipc::write::WriteOptions { compression: None },
        );

        let row_count = table.num_rows();

        let schema = table.schema.to_arrow()?;

        writer
            .start(&schema, None)
            .wrap_err("Failed to start Arrow stream writer")?;

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

impl ResponseBuilder<AnalyzePlanResponse> {
    pub fn schema_response(&self, dtype: DataType) -> AnalyzePlanResponse {
        let schema = analyze_plan_response::Schema {
            schema: Some(dtype),
        };

        AnalyzePlanResponse {
            session_id: self.session.clone(),
            server_side_session_id: self.server_side_session_id.clone(),
            result: Some(analyze_plan_response::Result::Schema(schema)),
        }
    }

    pub fn treestring_response(&self, tree_string: String) -> AnalyzePlanResponse {
        AnalyzePlanResponse {
            session_id: self.session.clone(),
            server_side_session_id: self.server_side_session_id.clone(),
            result: Some(analyze_plan_response::Result::TreeString(
                analyze_plan_response::TreeString { tree_string },
            )),
        }
    }
    pub fn explain_response(&self, explain_string: String) -> AnalyzePlanResponse {
        AnalyzePlanResponse {
            session_id: self.session.clone(),
            server_side_session_id: self.server_side_session_id.clone(),
            result: Some(analyze_plan_response::Result::Explain(
                analyze_plan_response::Explain { explain_string },
            )),
        }
    }
}
