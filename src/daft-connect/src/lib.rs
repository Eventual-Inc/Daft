use tonic::{Request, Response, Status};
use futures::stream;
use spark_connect::spark_connect_service_server::SparkConnectService;
use spark_connect::{
    ExecutePlanRequest, ExecutePlanResponse, AnalyzePlanRequest, AnalyzePlanResponse, ConfigRequest,
    ConfigResponse, AddArtifactsRequest, AddArtifactsResponse, ArtifactStatusesRequest, ArtifactStatusesResponse,
    InterruptRequest, InterruptResponse, ReattachExecuteRequest, ReleaseExecuteRequest, ReleaseExecuteResponse,
    ReleaseSessionRequest, ReleaseSessionResponse, FetchErrorDetailsRequest, FetchErrorDetailsResponse,
};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use std::collections::{BTreeMap, HashMap};
use dashmap::DashMap;
use crate::spark_connect::config_request::Operation;
use crate::spark_connect::config_request::operation::OpType;

pub mod spark_connect {
    tonic::include_proto!("spark.connect");
}

#[derive(Default)]
struct Session {
    /// so order is preserved
    ///
    /// Also, <https://users.rust-lang.org/t/hashmap-vs-btreemap/13804/4>
    values: BTreeMap<String, String>,
}

#[derive(Default)]
pub struct MySparkConnectService {
    client_to_server_session: DashMap<Uuid, Session>, // To track session data
}

#[tonic::async_trait]
impl SparkConnectService for MySparkConnectService {
    type ExecutePlanStream = std::pin::Pin<Box<dyn futures::Stream<Item=Result<ExecutePlanResponse, Status>> + Send + Sync + 'static>>;
    type ReattachExecuteStream = std::pin::Pin<Box<dyn futures::Stream<Item=Result<ExecutePlanResponse, Status>> + Send + Sync + 'static>>;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let request = request.into_inner();

        let client_session_id = request.session_id;

        let Ok(client_session_id) = Uuid::parse_str(&client_session_id) else {
            return Err(Status::invalid_argument("Invalid session_id format, must be a UUID"));
        };


        let Some(server_session_id) = self.client_to_server_session.get(&client_session_id).as_deref() else {
            return Err(Status::invalid_argument("Invalid session ID; no session found for this client"));
        };


        // Proceed with executing the plan...
        let plan = request.plan.ok_or_else(|| Status::invalid_argument("Plan is required"))?;

        if let Some(spark_connect::plan::OpType::Root(root)) = &plan.op_type {
            if let Some(spark_connect::relation::RelType::Range(range)) = &root.rel_type {
                // Handle the Range type plan
                let start = range.start.unwrap_or(0);
                let end = range.end;
                let step = range.step;

                let mut dummy_data = Vec::new();
                for i in (start..end).step_by(step as usize) {
                    dummy_data.extend_from_slice(&i.to_le_bytes());
                }

                let response = ExecutePlanResponse {
                    response_type: Some(spark_connect::execute_plan_response::ResponseType::ArrowBatch(
                        spark_connect::execute_plan_response::ArrowBatch {
                            row_count: (end - start) / step,
                            data: dummy_data,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                };

                let stream = stream::once(async move { Ok(response) });
                return Ok(Response::new(Box::pin(stream) as Self::ExecutePlanStream));
            }
        }

        Err(Status::unimplemented(format!("Unsupported plan type: {:?}", plan.op_type)))
    }

    async fn config(&self, request: Request<ConfigRequest>) -> Result<Response<ConfigResponse>, Status> {
        let req = request.into_inner();

        let session_id = req.session_id.clone();

        let Ok(session_id_uuid) = Uuid::parse_str(&session_id) else {
            return Err(Status::invalid_argument("Invalid session_id format, must be a UUID"));
        };

        let Some(operation) = req.operation.and_then(|op| op.op_type) else {
            return Err(Status::invalid_argument("Missing operation"));
        };

        let server_session = self.client_to_server_session.entry(session_id_uuid).or_default();

        use spark_connect::config_request::*;
        use spark_connect::KeyValue;

        match operation {
            OpType::Set(Set { pairs }) => {
                for KeyValue { key, value } in pairs {
                    match value {
                        None => {
                            server_session.values.remove(&key);
                        }
                        Some(value) => {
                            server_session.values.insert(key, value);
                        }
                    }
                }
            }
            OpType::Get(_) => {}
            OpType::GetWithDefault(_) => {}
            OpType::GetOption(_) => {}
            OpType::GetAll(_) => {}
            OpType::Unset(_) => {}
            OpType::IsModifiable(_) => {}
        }

        // Create a ConfigResponse echoing back the session_id
        let response = ConfigResponse {
            session_id,
            server_side_session_id: server_session.to_string(),
            pairs: vec![],
            warnings: vec![],
        };

        Ok(Response::new(response))
    }

    async fn add_artifacts(&self, _request: Request<tonic::Streaming<AddArtifactsRequest>>) -> Result<Response<AddArtifactsResponse>, Status> {
        Err(Status::unimplemented("add_artifacts operation is not yet implemented"))
    }

    async fn analyze_plan(&self, _request: Request<AnalyzePlanRequest>) -> Result<Response<AnalyzePlanResponse>, Status> {
        Err(Status::unimplemented("analyze_plan operation is not yet implemented"))
    }

    async fn artifact_status(&self, _request: Request<ArtifactStatusesRequest>) -> Result<Response<ArtifactStatusesResponse>, Status> {
        Err(Status::unimplemented("artifact_status operation is not yet implemented"))
    }

    async fn interrupt(&self, _request: Request<InterruptRequest>) -> Result<Response<InterruptResponse>, Status> {
        Err(Status::unimplemented("interrupt operation is not yet implemented"))
    }

    async fn reattach_execute(&self, _request: Request<ReattachExecuteRequest>) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        Err(Status::unimplemented("reattach_execute operation is not yet implemented"))
    }

    async fn release_execute(&self, request: Request<ReleaseExecuteRequest>) -> Result<Response<ReleaseExecuteResponse>, Status> {
        let request = request.into_inner();
        let client_session_id = request.session_id;

        let Ok(client_session_id_uuid) = Uuid::parse_str(&client_session_id) else {
            return Err(Status::invalid_argument("Invalid session_id format, must be a UUID"));
        };

        let Some(server_session_id) = self.client_to_server_session.get(&client_session_id_uuid).as_deref().cloned() else {
            return Err(Status::invalid_argument("Invalid session ID; no session found for this client"));
        };


        // Dummy release execute implementation
        let response = ReleaseExecuteResponse {
            session_id: client_session_id,
            server_side_session_id: server_session_id.to_string(),
            operation_id: Some("dummy_operation_id".to_string()),
        };

        Ok(Response::new(response))
    }
    async fn release_session(&self, _request: Request<ReleaseSessionRequest>) -> Result<Response<ReleaseSessionResponse>, Status> {
        Err(Status::unimplemented("release_session operation is not yet implemented"))
    }

    async fn fetch_error_details(&self, _request: Request<FetchErrorDetailsRequest>) -> Result<Response<FetchErrorDetailsResponse>, Status> {
        Err(Status::unimplemented("fetch_error_details operation is not yet implemented"))
    }
}
