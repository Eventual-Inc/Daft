#![feature(iterator_try_collect)]
#![feature(let_chains)]
#![feature(try_trait_v2)]
#![feature(coroutines)]
#![feature(iter_from_coroutine)]
#![feature(stmt_expr_attributes)]
#![feature(try_trait_v2_residual)]
#![warn(unused)]

use dashmap::DashMap;
use eyre::Context;
#[cfg(feature = "python")]
use pyo3::types::PyModuleMethods;
use spark_connect::{
    spark_connect_service_server::{SparkConnectService, SparkConnectServiceServer},
    AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse,
    ArtifactStatusesRequest, ArtifactStatusesResponse, ConfigRequest, ConfigResponse,
    ExecutePlanRequest, ExecutePlanResponse, FetchErrorDetailsRequest, FetchErrorDetailsResponse,
    InterruptRequest, InterruptResponse, ReattachExecuteRequest, ReleaseExecuteRequest,
    ReleaseExecuteResponse, ReleaseSessionRequest, ReleaseSessionResponse,
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, warn};
use uuid::Uuid;

use crate::session::Session;

mod config;
mod err;
mod session;
pub mod util;

pub fn start(addr: &str) -> eyre::Result<()> {
    info!("Daft-Connect server listening on {addr}");
    let addr = util::parse_spark_connect_address(addr)?;

    let service = DaftSparkConnectService::default();

    info!("Daft-Connect server listening on {addr}");

    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime
            .block_on(async {
                Server::builder()
                    .add_service(SparkConnectServiceServer::new(service))
                    .serve(addr)
                    .await
            })
            .wrap_err_with(|| format!("Failed to start server on {addr}"));

        if let Err(e) = result {
            eprintln!("Daft-Connect server error: {e:?}");
        }

        println!("done with runtime");

        eyre::Result::<_>::Ok(())
    });

    Ok(())
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
        _request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        unimplemented_err!("Unsupported plan type")
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
        _request: Request<AnalyzePlanRequest>,
    ) -> Result<Response<AnalyzePlanResponse>, Status> {
        unimplemented_err!("Analyze plan operation is not yet implemented")
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
#[pyo3(name = "connect_start")]
pub fn py_connect_start(addr: &str) -> pyo3::PyResult<()> {
    start(addr).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e:?}")))
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &pyo3::Bound<pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    parent.add_function(pyo3::wrap_pyfunction_bound!(py_connect_start, parent)?)?;
    Ok(())
}
