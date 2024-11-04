#![feature(iterator_try_collect)]
#![feature(let_chains)]
#![feature(try_trait_v2)]
#![feature(coroutines)]
#![feature(iter_from_coroutine)]
#![feature(stmt_expr_attributes)]
#![feature(try_trait_v2_residual)]

use std::ops::ControlFlow;

use dashmap::DashMap;
use eyre::Context;
#[cfg(feature = "python")]
use pyo3::types::PyModuleMethods;
use ron::extensions::Extensions;
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
use tracing::{info, warn};
use uuid::Uuid;

use crate::{convert::map_to_tables, session::Session};

mod command;
mod config;
mod convert;
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

fn pretty_config() -> ron::ser::PrettyConfig {
    ron::ser::PrettyConfig::default()
        .extensions(
            Extensions::IMPLICIT_SOME
                | Extensions::UNWRAP_NEWTYPES
                | Extensions::UNWRAP_VARIANT_NEWTYPES,
        )
        .indentor("  ".to_string())
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

        let plan_readable = ron::ser::to_string_pretty(&request, pretty_config()).unwrap();
        info!("got plan\n{plan_readable}");

        let session = self.get_session(&request.session_id)?;

        let operation = request
            .operation_id
            .ok_or_else(|| invalid_argument!("Operation ID is required"))?;

        // Proceed with executing the plan...
        let plan = request
            .plan
            .ok_or_else(|| invalid_argument!("Plan is required"))?;
        let plan = plan
            .op_type
            .ok_or_else(|| invalid_argument!("Plan operation is required"))?;

        use spark_connect::plan::OpType;

        match plan {
            OpType::Root(relation) => {
                let result = session.handle_root_command(relation, operation).await?;
                return Ok(Response::new(result));
            }
            OpType::Command(command) => {
                let command = command
                    .command_type
                    .ok_or_else(|| invalid_argument!("Command type is required"))?;

                match command {
                    CommandType::RegisterFunction(_) => {
                        Err(unimplemented!("RegisterFunction not implemented"))
                    }
                    CommandType::WriteOperation(_) => {
                        Err(unimplemented!("WriteOperation not implemented"))
                    }
                    CommandType::CreateDataframeView(_) => {
                        Err(unimplemented!("CreateDataframeView not implemented"))
                    }
                    CommandType::WriteOperationV2(_) => {
                        Err(unimplemented!("WriteOperationV2 not implemented"))
                    }
                    CommandType::SqlCommand(..) => {
                        Err(unimplemented!("SQL execution not yet implemented"))
                    }
                    CommandType::WriteStreamOperationStart(_) => {
                        Err(unimplemented!("WriteStreamOperationStart not implemented"))
                    }
                    CommandType::StreamingQueryCommand(_) => {
                        Err(unimplemented!("StreamingQueryCommand not implemented"))
                    }
                    CommandType::GetResourcesCommand(_) => {
                        Err(unimplemented!("GetResourcesCommand not implemented"))
                    }
                    CommandType::StreamingQueryManagerCommand(_) => Err(unimplemented!(
                        "StreamingQueryManagerCommand not implemented"
                    )),
                    CommandType::RegisterTableFunction(_) => {
                        Err(unimplemented!("RegisterTableFunction not implemented"))
                    }
                    CommandType::StreamingQueryListenerBusCommand(_) => Err(unimplemented!(
                        "StreamingQueryListenerBusCommand not implemented"
                    )),
                    CommandType::RegisterDataSource(_) => {
                        Err(unimplemented!("RegisterDataSource not implemented"))
                    }
                    CommandType::CreateResourceProfileCommand(_) => Err(unimplemented!(
                        "CreateResourceProfileCommand not implemented"
                    )),
                    CommandType::CheckpointCommand(_) => {
                        Err(unimplemented!("CheckpointCommand not implemented"))
                    }
                    CommandType::RemoveCachedRemoteRelationCommand(_) => Err(unimplemented!(
                        "RemoveCachedRemoteRelationCommand not implemented"
                    )),
                    CommandType::MergeIntoTableCommand(_) => {
                        Err(unimplemented!("MergeIntoTableCommand not implemented"))
                    }
                    CommandType::Extension(_) => Err(unimplemented!("Extension not implemented")),
                }
            }
        }?;

        Err(unimplemented!("Unsupported plan type"))
    }

    #[tracing::instrument(skip_all)]
    async fn config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        println!("got config");
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

    #[tracing::instrument(skip_all)]
    async fn add_artifacts(
        &self,
        _request: Request<tonic::Streaming<AddArtifactsRequest>>,
    ) -> Result<Response<AddArtifactsResponse>, Status> {
        Err(unimplemented!(
            "add_artifacts operation is not yet implemented"
        ))
    }

    #[tracing::instrument(skip_all)]
    async fn analyze_plan(
        &self,
        request: Request<AnalyzePlanRequest>,
    ) -> Result<Response<AnalyzePlanResponse>, Status> {
        use spark_connect::analyze_plan_request::*;
        let request = request.into_inner();

        let plan_readable = ron::ser::to_string_pretty(&request, pretty_config()).unwrap();
        info!("got analyze plan\n{plan_readable}");

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

                let result = convert::connect_schema(relation)?;

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
            Analyze::TreeString(tree_string) => {
                if let Some(level) = tree_string.level {
                    warn!("Ignoring level {level} in TreeString");
                }

                let Some(plan) = tree_string.plan else {
                    return Err(invalid_argument!("TreeString must have a plan"));
                };

                let Some(op_type) = plan.op_type else {
                    return Err(invalid_argument!("plan must have an op_type"));
                };

                println!("op_type: {op_type:?}");

                let OpType::Root(plan) = op_type else {
                    return Err(invalid_argument!("Only op_type Root is supported"));
                };

                let logical_plan = match convert::to_logical_plan(plan) {
                    Ok(lp) => lp,
                    Err(e) => {
                        return Err(invalid_argument!(
                            "Failed to convert to logical plan: {e:?}"
                        ));
                    }
                };

                let logical_plan = logical_plan.build();

                let res = std::thread::spawn(move || {
                    let result = map_to_tables(
                        &logical_plan,
                        |table| {
                            let table = format!("{table}");
                            ControlFlow::Break(table)
                        },
                        || ControlFlow::Continue(()),
                    )
                    .unwrap();

                    let result = match result {
                        ControlFlow::Break(x) => Some(x),
                        ControlFlow::Continue(()) => None,
                    }
                    .unwrap();

                    AnalyzePlanResponse {
                        session_id,
                        server_side_session_id: String::new(),
                        result: Some(analyze_plan_response::Result::TreeString(
                            analyze_plan_response::TreeString {
                                tree_string: result,
                            },
                        )),
                    }
                });

                let res = res.join().unwrap();

                let response = Response::new(res);
                Ok(response)
            }
            _ => Err(unimplemented!(
                "Analyze plan operation is not yet implemented"
            )),
        }
    }

    #[tracing::instrument(skip_all)]
    async fn artifact_status(
        &self,
        _request: Request<ArtifactStatusesRequest>,
    ) -> Result<Response<ArtifactStatusesResponse>, Status> {
        println!("got artifact status");
        Err(unimplemented!(
            "artifact_status operation is not yet implemented"
        ))
    }

    #[tracing::instrument(skip_all)]
    async fn interrupt(
        &self,
        _request: Request<InterruptRequest>,
    ) -> Result<Response<InterruptResponse>, Status> {
        println!("got interrupt");
        Err(unimplemented!("interrupt operation is not yet implemented"))
    }

    #[tracing::instrument(skip_all)]
    async fn reattach_execute(
        &self,
        request: Request<ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        let request = request.into_inner();

        let plan_readable = ron::ser::to_string_pretty(&request, pretty_config()).unwrap();
        info!("got reattach execute\n{plan_readable}");

        warn!("reattach_execute operation is not yet implemented");

        let singleton_stream = futures::stream::once(async {
            Err(Status::unimplemented(
                "reattach_execute operation is not yet implemented",
            ))
        });

        Ok(Response::new(Box::pin(singleton_stream)))
    }

    #[tracing::instrument(skip_all)]
    async fn release_execute(
        &self,
        request: Request<ReleaseExecuteRequest>,
    ) -> Result<Response<ReleaseExecuteResponse>, Status> {
        let request = request.into_inner();

        let plan_readable = ron::ser::to_string_pretty(&request, pretty_config()).unwrap();
        info!("got release execute\n{plan_readable}");

        let session = self.get_session(&request.session_id)?;

        let response = ReleaseExecuteResponse {
            session_id: session.id().to_string(),
            server_side_session_id: session.server_side_session_id().to_string(),
            operation_id: Some(request.operation_id), // todo: impl properly
        };

        Ok(Response::new(response))
    }

    #[tracing::instrument(skip_all)]
    async fn release_session(
        &self,
        _request: Request<ReleaseSessionRequest>,
    ) -> Result<Response<ReleaseSessionResponse>, Status> {
        println!("got release session");
        Err(unimplemented!(
            "release_session operation is not yet implemented"
        ))
    }

    #[tracing::instrument(skip_all)]
    async fn fetch_error_details(
        &self,
        _request: Request<FetchErrorDetailsRequest>,
    ) -> Result<Response<FetchErrorDetailsResponse>, Status> {
        println!("got fetch error details");
        Err(unimplemented!(
            "fetch_error_details operation is not yet implemented"
        ))
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
