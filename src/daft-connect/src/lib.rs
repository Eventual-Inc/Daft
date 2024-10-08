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
use dashmap::{DashMap, Entry};
use tracing::info;
use crate::spark_connect::command::CommandType;
use crate::spark_connect::config_request::{Operation, Set};
use crate::spark_connect::KeyValue;

pub mod spark_connect {
    tonic::include_proto!("spark.connect");
}

mod config;
mod command;

#[derive(Default)]
struct Session {
    /// so order is preserved
    ///
    /// Also, <https://users.rust-lang.org/t/hashmap-vs-btreemap/13804/4>
    values: BTreeMap<String, String>,

    session_id: String,

    server_side_session_id: String,
}


#[derive(Default)]
pub struct DaftSparkConnectService {
    client_to_session: DashMap<Uuid, Session>, // To track session data
}


impl DaftSparkConnectService {
    fn get_session(&self, session_id: &str) -> Result<dashmap::mapref::one::RefMut<Uuid, Session>, Status> {
        let Ok(uuid) = Uuid::parse_str(session_id) else {
            return Err(Status::invalid_argument("Invalid session_id format, must be a UUID"));
        };

        let res = self.client_to_session.entry(uuid).or_insert_with(|| {
            Session {
                values: BTreeMap::new(),
                session_id: session_id.to_string(),
                server_side_session_id: Uuid::new_v4().to_string(),
            }
        });

        Ok(res)
    }
}

#[tonic::async_trait]
impl SparkConnectService for DaftSparkConnectService {
    type ExecutePlanStream = std::pin::Pin<Box<dyn futures::Stream<Item=Result<ExecutePlanResponse, Status>> + Send + Sync + 'static>>;
    type ReattachExecuteStream = std::pin::Pin<Box<dyn futures::Stream<Item=Result<ExecutePlanResponse, Status>> + Send + Sync + 'static>>;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let request = request.into_inner();

        let session = self.get_session(&request.session_id)?;

        // Proceed with executing the plan...
        let plan = request.plan.ok_or_else(|| Status::invalid_argument("Plan is required"))?;
        let plan = plan.op_type.ok_or_else(|| Status::invalid_argument("Plan operation is required"))?;

        use crate::spark_connect::plan::OpType;

        let command = match plan {
            OpType::Root(..) => {
                return Err(Status::unimplemented("Not yet implemented"));
            }
            OpType::Command(command) => {
                command
            }
        };

        let command = command.command_type.ok_or_else(|| Status::invalid_argument("Command type is required"))?;

        match command {
            CommandType::RegisterFunction(_) => {}
            CommandType::WriteOperation(write) => {
                session.write_operation(write);
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

    async fn config(&self, request: Request<ConfigRequest>) -> Result<Response<ConfigResponse>, Status> {
        let request = request.into_inner();

        let mut session = self.get_session(&request.session_id)?;

        let Some(operation) = request.operation.and_then(|op| op.op_type) else {
            return Err(Status::invalid_argument("Missing operation"));
        };


        use crate::spark_connect::config_request::operation::OpType;

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

        todo!()
    }
    async fn release_session(&self, _request: Request<ReleaseSessionRequest>) -> Result<Response<ReleaseSessionResponse>, Status> {
        Err(Status::unimplemented("release_session operation is not yet implemented"))
    }

    async fn fetch_error_details(&self, _request: Request<FetchErrorDetailsRequest>) -> Result<Response<FetchErrorDetailsResponse>, Status> {
        Err(Status::unimplemented("fetch_error_details operation is not yet implemented"))
    }
}
