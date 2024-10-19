#![feature(iterator_try_collect)]
#![feature(let_chains)]

use std::collections::BTreeMap;

use dashmap::DashMap;
#[cfg(feature = "python")]
use pyo3::types::PyModuleMethods;
use spark_connect::{
    command::CommandType,
    spark_connect_service_server::{SparkConnectService, SparkConnectServiceServer},
    AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse,
    ArtifactStatusesRequest, ArtifactStatusesResponse, ConfigRequest, ConfigResponse,
    ExecutePlanRequest, ExecutePlanResponse, FetchErrorDetailsRequest, FetchErrorDetailsResponse,
    InterruptRequest, InterruptResponse, ReattachExecuteRequest, ReleaseExecuteRequest,
    ReleaseExecuteResponse, ReleaseSessionRequest, ReleaseSessionResponse,
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;
use uuid::Uuid;

mod command;
mod config;
mod convert;

#[derive(Default)]
struct Session {
    /// so order is preserved
    ///
    /// Also, <https://users.rust-lang.org/t/hashmap-vs-btreemap/13804/4>
    config_values: BTreeMap<String, String>,

    id: String,

    server_side_session_id: String,
}

pub fn start() {
    let addr = "[::1]:50051".parse().unwrap();
    let service = DaftSparkConnectService::default();

    println!("Daft-Connect server listening on {}", addr);

    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            Server::builder()
                .add_service(SparkConnectServiceServer::new(service))
                .serve(addr)
                .await
                .unwrap();
        });

        println!("done with runtime")
    });
}

#[derive(Default)]
pub struct DaftSparkConnectService {
    client_to_session: DashMap<Uuid, Session>, // To track session data
}

impl DaftSparkConnectService {
    fn get_session(
        &self,
        session_id: &str,
    ) -> Result<dashmap::mapref::one::RefMut<Uuid, Session>, Status> {
        let Ok(uuid) = Uuid::parse_str(session_id) else {
            return Err(Status::invalid_argument(
                "Invalid session_id format, must be a UUID",
            ));
        };

        let res = self
            .client_to_session
            .entry(uuid)
            .or_insert_with(|| Session {
                config_values: BTreeMap::new(),
                id: session_id.to_string(),
                server_side_session_id: Uuid::new_v4().to_string(),
            });

        Ok(res)
    }
}

#[tonic::async_trait]
impl SparkConnectService for DaftSparkConnectService {
    type ExecutePlanStream = std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<ExecutePlanResponse, Status>> + Send + Sync + 'static,
        >,
    >;
    type ReattachExecuteStream = std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<ExecutePlanResponse, Status>> + Send + Sync + 'static,
        >,
    >;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let request = request.into_inner();

        let session = self.get_session(&request.session_id)?;

        let operation = request
            .operation_id
            .ok_or_else(|| Status::invalid_argument("Operation ID is required"))?;

        // Proceed with executing the plan...
        let plan = request
            .plan
            .ok_or_else(|| Status::invalid_argument("Plan is required"))?;
        let plan = plan
            .op_type
            .ok_or_else(|| Status::invalid_argument("Plan operation is required"))?;

        use spark_connect::plan::OpType;
        // println!("plan {:#?}", plan);

        let command = match plan {
            OpType::Root(relation) => {
                let result = session.handle_root_command(relation, operation).await?;
                return Ok(Response::new(result));
            }
            OpType::Command(command) => command,
        };

        let command = command
            .command_type
            .ok_or_else(|| Status::invalid_argument("Command type is required"))?;

        // println!("command {:#?}", command);

        match command {
            CommandType::RegisterFunction(_) => {}
            CommandType::WriteOperation(write) => {
                let result = session.write_operation(write)?;
                return Ok(Response::new(result));
            }
            CommandType::CreateDataframeView(_) => {}
            CommandType::WriteOperationV2(_) => {}
            CommandType::SqlCommand(_) => {}
            CommandType::WriteStreamOperationStart(_) => {}
            CommandType::StreamingQueryCommand(_) => {}
            CommandType::GetResourcesCommand(_) => {}
            CommandType::StreamingQueryManagerCommand(_) => {}
            CommandType::RegisterTableFunction(_) => {}
            CommandType::StreamingQueryListenerBusCommand(_) => {}
            CommandType::RegisterDataSource(_) => {}
            CommandType::CreateResourceProfileCommand(_) => {}
            CommandType::CheckpointCommand(_) => {}
            CommandType::RemoveCachedRemoteRelationCommand(_) => {}
            CommandType::MergeIntoTableCommand(_) => {}
            CommandType::Extension(_) => {}
        }

        Err(Status::unimplemented("Unsupported plan type"))
    }

    async fn config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        let request = request.into_inner();

        let mut session = self.get_session(&request.session_id)?;

        let Some(operation) = request.operation.and_then(|op| op.op_type) else {
            return Err(Status::invalid_argument("Missing operation"));
        };

        use spark_connect::config_request::operation::OpType;

        let response = match operation {
            OpType::Set(op) => session.set(op),
            OpType::Get(op) => session.get(op),
            OpType::GetWithDefault(op) => session.get_with_default(op),
            OpType::GetOption(op) => session.get_option(op),
            OpType::GetAll(op) => session.get_all(op),
            OpType::Unset(op) => session.unset(op),
            OpType::IsModifiable(op) => session.is_modifiable(op),
        }?;

        info!("Response: {response:?}");

        Ok(Response::new(response))
    }

    async fn add_artifacts(
        &self,
        _request: Request<tonic::Streaming<AddArtifactsRequest>>,
    ) -> Result<Response<AddArtifactsResponse>, Status> {
        Err(Status::unimplemented(
            "add_artifacts operation is not yet implemented",
        ))
    }

    async fn analyze_plan(
        &self,
        request: Request<AnalyzePlanRequest>,
    ) -> Result<Response<AnalyzePlanResponse>, Status> {
        let request = request.into_inner();
        // println!("AnalyzePlanRequest: {request:#?}");

        // let session = self.get_session(&request.session_id)?;

        // Err(Status::unimplemented("analyze_plan operation is not yet implemented"))

        use spark_connect::analyze_plan_response::Result as AnalyzePlanResponseResult;

        let tree_string = AnalyzePlanResponseResult::TreeString(
            spark_connect::analyze_plan_response::TreeString {
                tree_string: "schema [unimplemented]".to_string(),
            },
        );

        let response = AnalyzePlanResponse {
            session_id: request.session_id.clone(),
            server_side_session_id: request.session_id,
            result: Some(tree_string),
        };

        Ok(Response::new(response))
    }

    async fn artifact_status(
        &self,
        _request: Request<ArtifactStatusesRequest>,
    ) -> Result<Response<ArtifactStatusesResponse>, Status> {
        Err(Status::unimplemented(
            "artifact_status operation is not yet implemented",
        ))
    }

    async fn interrupt(
        &self,
        _request: Request<InterruptRequest>,
    ) -> Result<Response<InterruptResponse>, Status> {
        Err(Status::unimplemented(
            "interrupt operation is not yet implemented",
        ))
    }

    async fn reattach_execute(
        &self,
        request: Request<ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        // let request = request.into_inner();

        // println!("reattach_execute request: {request:#?}");

        // let session = self.get_session(&request.session_id)?;

        // Return an empty stream
        let empty_stream = futures::stream::empty();
        Ok(Response::new(Box::pin(empty_stream)))
    }

    async fn release_execute(
        &self,
        request: Request<ReleaseExecuteRequest>,
    ) -> Result<Response<ReleaseExecuteResponse>, Status> {
        let request = request.into_inner();

        let session = self.get_session(&request.session_id)?;

        // println!("release request: {request:#?}");

        let response = ReleaseExecuteResponse {
            session_id: session.id.clone(),
            server_side_session_id: session.server_side_session_id.clone(),
            operation_id: Some(request.operation_id), // todo: impl properly
        };

        Ok(Response::new(response))
    }
    async fn release_session(
        &self,
        _request: Request<ReleaseSessionRequest>,
    ) -> Result<Response<ReleaseSessionResponse>, Status> {
        Err(Status::unimplemented(
            "release_session operation is not yet implemented",
        ))
    }

    async fn fetch_error_details(
        &self,
        _request: Request<FetchErrorDetailsRequest>,
    ) -> Result<Response<FetchErrorDetailsResponse>, Status> {
        Err(Status::unimplemented(
            "fetch_error_details operation is not yet implemented",
        ))
    }
}
#[cfg(feature = "python")]
#[pyo3::pyfunction]
#[pyo3(name = "connect_start")]
pub fn py_connect_start() {
    start();
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &pyo3::Bound<pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    parent.add_function(pyo3::wrap_pyfunction_bound!(py_connect_start, parent)?)?;
    Ok(())
}
