use tonic::{Request, Response, Status};
use futures::stream;
use spark_connect::spark_connect_service_server::SparkConnectService;
use spark_connect::{ExecutePlanRequest, ExecutePlanResponse, AnalyzePlanRequest, AnalyzePlanResponse, ConfigRequest, ConfigResponse, AddArtifactsRequest, AddArtifactsResponse, ArtifactStatusesRequest, ArtifactStatusesResponse, InterruptRequest, InterruptResponse, ReattachExecuteRequest, ReleaseExecuteRequest, ReleaseExecuteResponse, ReleaseSessionRequest, ReleaseSessionResponse, FetchErrorDetailsRequest, FetchErrorDetailsResponse};

pub mod spark_connect {
    tonic::include_proto!("spark.connect");
}

#[derive(Default)]
pub struct MySparkConnectService {}

#[tonic::async_trait]
impl SparkConnectService for MySparkConnectService {
    type ExecutePlanStream = std::pin::Pin<Box<dyn futures::Stream<Item=Result<ExecutePlanResponse, Status>> + Send + Sync + 'static>>;
    type ReattachExecuteStream = std::pin::Pin<Box<dyn futures::Stream<Item=Result<ExecutePlanResponse, Status>> + Send + Sync + 'static>>;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        println!("Got execute_plan request");

        let plan = request.into_inner().plan.ok_or_else(|| Status::invalid_argument("Plan is required"))?;

        if let Some(spark_connect::plan::OpType::Root(root)) = &plan.op_type {
            if let Some(spark_connect::relation::RelType::Read(read)) = &root.rel_type {
                // Instead of looking for LocalRelation, we'll handle any ReadType
                let dummy_data = vec![0u8; 20]; // Dummy data

                // For simplicity, we're just returning a dummy response
                let response = ExecutePlanResponse {
                    response_type: Some(spark_connect::execute_plan_response::ResponseType::ArrowBatch(
                        spark_connect::execute_plan_response::ArrowBatch {
                            row_count: 5,
                            data: dummy_data,
                            ..Default::default()
                        }
                    )),
                    ..Default::default()
                };

                let stream = stream::once(async move { Ok(response) });
                return Ok(Response::new(Box::pin(stream) as Self::ExecutePlanStream));
            }
        }
        Err(Status::unimplemented(format!("Unsupported plan type: {:?}", plan.op_type)))
    }

    async fn analyze_plan(&self, _request: Request<AnalyzePlanRequest>) -> Result<Response<AnalyzePlanResponse>, Status> {
        Err(Status::unimplemented("analyze_plan operation is not yet implemented"))
    }

    async fn config(&self, _request: Request<ConfigRequest>) -> Result<Response<ConfigResponse>, Status> {
        // Create a dummy ConfigResponse
        let response = ConfigResponse {
            session_id: "dummy_session_id".to_string(),
            server_side_session_id: "dummy_server_side_session_id".to_string(),
            pairs: vec![],
            warnings: vec![],
        };

        Ok(Response::new(response))
    }

    async fn add_artifacts(&self, _request: Request<tonic::Streaming<AddArtifactsRequest>>) -> Result<Response<AddArtifactsResponse>, Status> {
        Err(Status::unimplemented("add_artifacts operation is not yet implemented"))
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

    async fn release_execute(&self, _request: Request<ReleaseExecuteRequest>) -> Result<Response<ReleaseExecuteResponse>, Status> {
        // Create a dummy ReleaseExecuteResponse
        let response = ReleaseExecuteResponse {
            session_id: "dummy_session_id".to_string(),
            server_side_session_id: "dummy_server_side_session_id".to_string(),
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