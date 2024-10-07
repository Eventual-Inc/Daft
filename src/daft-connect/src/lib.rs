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
use std::collections::HashMap;

pub mod spark_connect {
    tonic::include_proto!("spark.connect");
}

#[derive(Default)]
pub struct MySparkConnectService {
    sessions: Arc<Mutex<HashMap<String, String>>>, // To track session data
    default_session_id: Arc<Mutex<Option<String>>>, // To track default session
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
        let session_id = request.session_id.clone();

        println!("Received session ID: {}", session_id);

        // Determine the actual session_id to use
        let actual_session_id = if session_id.is_empty() {
            let default_session = self.default_session_id.lock().unwrap();
            if let Some(ref default_id) = *default_session {
                default_id.clone()
            } else {
                return Err(Status::invalid_argument("No default session ID set"));
            }
        } else {
            session_id.clone()
        };

        // Validate session ID
        let sessions = self.sessions.lock().unwrap();
        if !sessions.contains_key(&actual_session_id) {
            return Err(Status::invalid_argument("Invalid session ID"));
        }

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
                            row_count: ((end - start) / step) as i64,
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

        // Determine if a new session_id needs to be generated
        let new_session_id = if session_id.is_empty() {
            // Generate a new unique session ID
            let generated_session_id = Uuid::new_v4().to_string();

            // Store the new session ID
            {
                let mut sessions = self.sessions.lock().unwrap();
                sessions.insert(generated_session_id.clone(), generated_session_id.clone());
            }

            // Set the generated session ID as the default
            {
                let mut default_session = self.default_session_id.lock().unwrap();
                *default_session = Some(generated_session_id.clone());
            }

            println!("Generated and stored new session ID: {}", generated_session_id);
            generated_session_id
        } else {
            // Validate the session_id format
            if Uuid::parse_str(&session_id).is_err() {
                return Err(Status::invalid_argument("Invalid session_id format"));
            }

            // Store the session_id provided by the client
            let mut sessions = self.sessions.lock().unwrap();
            if sessions.contains_key(&session_id) {
                println!("Session ID already exists: {}", session_id);
                // Optionally, handle re-configuration of an existing session
            } else {
                sessions.insert(session_id.clone(), session_id.clone());
                println!("Stored new session ID: {}", session_id);
            }

            // Optionally, set the first valid session as the default
            let mut default_session = self.default_session_id.lock().unwrap();
            if default_session.is_none() {
                *default_session = Some(session_id.clone());
            }

            session_id.clone()
        };

        // Create a ConfigResponse echoing back the session_id
        let response = ConfigResponse {
            session_id: new_session_id.clone(),
            server_side_session_id: new_session_id.clone(), // Use the same ID for simplicity
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
        let session_id = request.session_id.clone();

        println!("Received session ID for release_execute: {}", session_id);

        // let actual_session_id = if session_id.is_empty() {
        // //     let default_session = self.default_session_id.lock().unwrap();
        // //     if let Some(ref default_id) = *default_session {
        // //         default_id.clone()
        // //     } else {
        // //         return Err(Status::invalid_argument("No default session ID set"));
        // //     }
        // // } else {
        // //     session_id.clone()
        //     panic!("oof");
        // };

        // Validate session ID
        let sessions = self.sessions.lock().unwrap();
        let Some(server_side_session_id) = sessions.get(&session_id).cloned() else {
            return Err(Status::invalid_argument("Invalid session ID"));
        };

        // Dummy release execute implementation
        let response = ReleaseExecuteResponse {
            session_id: server_side_session_id.clone(),
            server_side_session_id,
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
