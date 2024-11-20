#![feature(iterator_try_collect)]
#![feature(let_chains)]
#![feature(try_trait_v2)]
#![feature(coroutines)]
#![feature(iter_from_coroutine)]
#![feature(stmt_expr_attributes)]
#![feature(try_trait_v2_residual)]

use dashmap::DashMap;
use eyre::Context;
#[cfg(feature = "python")]
use pyo3::types::PyModuleMethods;
use spark_connect::{
    analyze_plan_response,
    command::CommandType,
    plan::OpType,
    spark_connect_service_server::{SparkConnectService, SparkConnectServiceServer},
    AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse,
    ArtifactStatusesRequest, ArtifactStatusesResponse, ConfigRequest, ConfigResponse,
    ExecutePlanRequest, ExecutePlanResponse, FetchErrorDetailsRequest, FetchErrorDetailsResponse,
    InterruptRequest, InterruptResponse, Plan, ReattachExecuteRequest, ReleaseExecuteRequest,
    ReleaseExecuteResponse, ReleaseSessionRequest, ReleaseSessionResponse,
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;
use uuid::Uuid;

use crate::session::Session;

mod config;
mod err;
mod op;
mod session;
mod translation;
pub mod util;

#[cfg_attr(feature = "python", pyo3::pyclass)]
pub struct ConnectionHandle {
    shutdown_signal: Option<tokio::sync::oneshot::Sender<()>>,
    port: u16,
}

#[cfg_attr(feature = "python", pyo3::pymethods)]
impl ConnectionHandle {
    pub fn shutdown(&mut self) {
        let Some(shutdown_signal) = self.shutdown_signal.take() else {
            return;
        };
        shutdown_signal.send(()).unwrap();
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

pub fn start(addr: &str) -> eyre::Result<ConnectionHandle> {
    info!("Daft-Connect server listening on {addr}");
    let addr = util::parse_spark_connect_address(addr)?;

    let listener = std::net::TcpListener::bind(addr)?;
    let port = listener.local_addr()?.port();

    let service = DaftSparkConnectService::default();

    info!("Daft-Connect server listening on {addr}");

    let (shutdown_signal, shutdown_receiver) = tokio::sync::oneshot::channel();

    let handle = ConnectionHandle {
        shutdown_signal: Some(shutdown_signal),
        port,
    };

    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(async {
            let incoming = {
                let listener = tokio::net::TcpListener::from_std(listener)
                    .wrap_err("Failed to create TcpListener from std::net::TcpListener")?;

                async_stream::stream! {
                    loop {
                        match listener.accept().await {
                            Ok((stream, _)) => yield Ok(stream),
                            Err(e) => yield Err(e),
                        }
                    }
                }
            };

            let result = tokio::select! {
                result = Server::builder()
                    .add_service(SparkConnectServiceServer::new(service))
                    .serve_with_incoming(incoming)=> {
                    result
                }
                _ = shutdown_receiver => {
                    info!("Received shutdown signal");
                    Ok(())
                }
            };

            result.wrap_err_with(|| format!("Failed to start server on {addr}"))
        });

        if let Err(e) = result {
            eprintln!("Daft-Connect server error: {e:?}");
        }

        eyre::Result::<_>::Ok(())
    });

    Ok(handle)
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
            .or_insert_with(|| Session::new(session_id.to_string()));

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

    #[tracing::instrument(skip_all)]
    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let request = request.into_inner();

        let session = self.get_session(&request.session_id)?;

        let Some(operation) = request.operation_id else {
            return invalid_argument_err!("Operation ID is required");
        };

        // Proceed with executing the plan...
        let Some(plan) = request.plan else {
            return invalid_argument_err!("Plan is required");
        };

        let Some(plan) = plan.op_type else {
            return invalid_argument_err!("Plan operation is required");
        };

        use spark_connect::plan::OpType;

        match plan {
            OpType::Root(relation) => {
                let result = session.handle_root_command(relation, operation).await?;
                return Ok(Response::new(result));
            }
            OpType::Command(command) => {
                let Some(command) = command.command_type else {
                    return invalid_argument_err!("Command type is required");
                };

                match command {
                    CommandType::RegisterFunction(_) => {
                        unimplemented_err!("RegisterFunction not implemented")
                    }
                    CommandType::WriteOperation(_) => {
                        unimplemented_err!("WriteOperation not implemented")
                    }
                    CommandType::CreateDataframeView(_) => {
                        unimplemented_err!("CreateDataframeView not implemented")
                    }
                    CommandType::WriteOperationV2(_) => {
                        unimplemented_err!("WriteOperationV2 not implemented")
                    }
                    CommandType::SqlCommand(..) => {
                        unimplemented_err!("SQL execution not yet implemented")
                    }
                    CommandType::WriteStreamOperationStart(_) => {
                        unimplemented_err!("WriteStreamOperationStart not implemented")
                    }
                    CommandType::StreamingQueryCommand(_) => {
                        unimplemented_err!("StreamingQueryCommand not implemented")
                    }
                    CommandType::GetResourcesCommand(_) => {
                        unimplemented_err!("GetResourcesCommand not implemented")
                    }
                    CommandType::StreamingQueryManagerCommand(_) => {
                        unimplemented_err!("StreamingQueryManagerCommand not implemented")
                    }
                    CommandType::RegisterTableFunction(_) => {
                        unimplemented_err!("RegisterTableFunction not implemented")
                    }
                    CommandType::StreamingQueryListenerBusCommand(_) => {
                        unimplemented_err!("StreamingQueryListenerBusCommand not implemented")
                    }
                    CommandType::RegisterDataSource(_) => {
                        unimplemented_err!("RegisterDataSource not implemented")
                    }
                    CommandType::CreateResourceProfileCommand(_) => {
                        unimplemented_err!("CreateResourceProfileCommand not implemented")
                    }
                    CommandType::CheckpointCommand(_) => {
                        unimplemented_err!("CheckpointCommand not implemented")
                    }
                    CommandType::RemoveCachedRemoteRelationCommand(_) => {
                        unimplemented_err!("RemoveCachedRemoteRelationCommand not implemented")
                    }
                    CommandType::MergeIntoTableCommand(_) => {
                        unimplemented_err!("MergeIntoTableCommand not implemented")
                    }
                    CommandType::Extension(_) => unimplemented_err!("Extension not implemented"),
                }
            }
        }?
    }

    #[tracing::instrument(skip_all)]
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

        Ok(Response::new(response))
    }

    #[tracing::instrument(skip_all)]
    async fn add_artifacts(
        &self,
        _request: Request<tonic::Streaming<AddArtifactsRequest>>,
    ) -> Result<Response<AddArtifactsResponse>, Status> {
        unimplemented_err!("add_artifacts operation is not yet implemented")
    }

    #[tracing::instrument(skip_all)]
    async fn analyze_plan(
        &self,
        request: Request<AnalyzePlanRequest>,
    ) -> Result<Response<AnalyzePlanResponse>, Status> {
        use spark_connect::analyze_plan_request::*;
        let request = request.into_inner();

        let AnalyzePlanRequest {
            session_id,
            analyze,
            ..
        } = request;

        let Some(analyze) = analyze else {
            return Err(Status::invalid_argument("analyze is required"));
        };

        match analyze {
            Analyze::Schema(Schema { plan }) => {
                let Some(Plan { op_type }) = plan else {
                    return Err(Status::invalid_argument("plan is required"));
                };

                let Some(OpType::Root(relation)) = op_type else {
                    return Err(Status::invalid_argument("op_type is required to be root"));
                };

                let result = match translation::relation_to_schema(relation) {
                    Ok(schema) => schema,
                    Err(e) => {
                        return invalid_argument_err!(
                            "Failed to translate relation to schema: {e}"
                        );
                    }
                };

                let schema = analyze_plan_response::DdlParse {
                    parsed: Some(result),
                };

                let response = AnalyzePlanResponse {
                    session_id,
                    server_side_session_id: String::new(),
                    result: Some(analyze_plan_response::Result::DdlParse(schema)),
                };

                println!("response: {response:#?}");

                Ok(Response::new(response))
            }
            _ => unimplemented_err!("Analyze plan operation is not yet implemented"),
        }
    }

    #[tracing::instrument(skip_all)]
    async fn artifact_status(
        &self,
        _request: Request<ArtifactStatusesRequest>,
    ) -> Result<Response<ArtifactStatusesResponse>, Status> {
        unimplemented_err!("artifact_status operation is not yet implemented")
    }

    #[tracing::instrument(skip_all)]
    async fn interrupt(
        &self,
        _request: Request<InterruptRequest>,
    ) -> Result<Response<InterruptResponse>, Status> {
        println!("got interrupt");
        unimplemented_err!("interrupt operation is not yet implemented")
    }

    #[tracing::instrument(skip_all)]
    async fn reattach_execute(
        &self,
        _request: Request<ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        unimplemented_err!("reattach_execute operation is not yet implemented")
    }

    #[tracing::instrument(skip_all)]
    async fn release_execute(
        &self,
        _request: Request<ReleaseExecuteRequest>,
    ) -> Result<Response<ReleaseExecuteResponse>, Status> {
        unimplemented_err!("release_execute operation is not yet implemented")
    }

    #[tracing::instrument(skip_all)]
    async fn release_session(
        &self,
        _request: Request<ReleaseSessionRequest>,
    ) -> Result<Response<ReleaseSessionResponse>, Status> {
        println!("got release session");
        unimplemented_err!("release_session operation is not yet implemented")
    }

    #[tracing::instrument(skip_all)]
    async fn fetch_error_details(
        &self,
        _request: Request<FetchErrorDetailsRequest>,
    ) -> Result<Response<FetchErrorDetailsResponse>, Status> {
        println!("got fetch error details");
        unimplemented_err!("fetch_error_details operation is not yet implemented")
    }
}

#[cfg(feature = "python")]
#[pyo3::pyfunction]
#[pyo3(name = "connect_start", signature = (addr = "sc://0.0.0.0:0"))]
pub fn py_connect_start(addr: &str) -> pyo3::PyResult<ConnectionHandle> {
    start(addr).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e:?}")))
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &pyo3::Bound<pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    parent.add_function(pyo3::wrap_pyfunction_bound!(py_connect_start, parent)?)?;
    parent.add_class::<ConnectionHandle>()?;
    Ok(())
}
