use dashmap::DashMap;
use spark_connect::{
    analyze_plan_request::explain::ExplainMode, command::CommandType, plan::OpType,
    spark_connect_service_server::SparkConnectService, AddArtifactsRequest, AddArtifactsResponse,
    AnalyzePlanRequest, AnalyzePlanResponse, ArtifactStatusesRequest, ArtifactStatusesResponse,
    ConfigRequest, ConfigResponse, ExecutePlanRequest, ExecutePlanResponse,
    FetchErrorDetailsRequest, FetchErrorDetailsResponse, InterruptRequest, InterruptResponse, Plan,
    ReattachExecuteRequest, ReleaseExecuteRequest, ReleaseExecuteResponse, ReleaseSessionRequest,
    ReleaseSessionResponse,
};
use tonic::{Request, Response, Status};
use tracing::debug;
use uuid::Uuid;

use crate::{
    display::SparkDisplay,
    error::Context,
    invalid_argument_err, not_yet_implemented,
    response_builder::ResponseBuilder,
    session::ConnectSession,
    spark_analyzer::{to_spark_datatype, SparkAnalyzer},
    util::FromOptionalField,
};

#[derive(Default)]
pub struct DaftSparkConnectService {
    client_to_session: DashMap<Uuid, ConnectSession>, // To track session data
}
type ExecutePlanStream = std::pin::Pin<
    Box<dyn futures::Stream<Item = Result<ExecutePlanResponse, Status>> + Send + 'static>,
>;

impl DaftSparkConnectService {
    #[allow(clippy::result_large_err)]
    fn get_session(
        &self,
        session_id: &str,
    ) -> Result<dashmap::mapref::one::RefMut<Uuid, ConnectSession>, Status> {
        let Ok(uuid) = Uuid::parse_str(session_id) else {
            return Err(Status::invalid_argument(
                "Invalid session_id format, must be a UUID",
            ));
        };

        let res = self
            .client_to_session
            .entry(uuid)
            .or_insert_with(|| ConnectSession::new(session_id.to_string()));

        Ok(res)
    }

    async fn execute_plan_impl(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<ExecutePlanStream>, Status> {
        let request = request.into_inner();

        let session = self.get_session(&request.session_id)?;
        let operation_id = request
            .operation_id
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let rb = ResponseBuilder::new(&session, operation_id);

        // Proceed with executing the plan...
        let plan = request.plan.required("plan")?;
        let plan = plan.op_type.required("op_type")?;

        match plan {
            OpType::Root(relation) => {
                let result = session.execute_command(relation, rb).await?;
                Ok(Response::new(result))
            }
            OpType::Command(command) => {
                let command = command.command_type.required("command_type")?;
                match command {
                    CommandType::WriteOperation(op) => {
                        let result = session.execute_write_operation(op, rb).await?;
                        Ok(Response::new(result))
                    }
                    CommandType::CreateDataframeView(create_dataframe) => {
                        let result = session
                            .execute_create_dataframe_view(create_dataframe, rb)
                            .await?;
                        Ok(Response::new(result))
                    }
                    CommandType::SqlCommand(sql) => {
                        let result = session.execute_sql_command(sql, rb).await?;
                        Ok(Response::new(result))
                    }
                    other => {
                        not_yet_implemented!("CommandType '{:?}'", command_type_to_str(&other))
                    }
                }
            }
        }
    }

    async fn config_impl(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        let request = request.into_inner();

        let mut session = self.get_session(&request.session_id)?;

        let operation = request
            .operation
            .and_then(|op| op.op_type)
            .required("operation.op_type")?;

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

    async fn analyze_plan_impl(
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

        let session = self.get_session(&session_id)?;
        let rb = ResponseBuilder::new(&session, Uuid::new_v4().to_string());

        let analyze = analyze.required("analyze")?;

        match analyze {
            Analyze::Schema(Schema { plan }) => {
                let Plan { op_type } = plan.required("plan")?;

                let OpType::Root(relation) = op_type.required("op_type")? else {
                    invalid_argument_err!("op_type must be Root");
                };

                let translator = SparkAnalyzer::new(&session);

                let result = match translator.relation_to_spark_schema(relation).await {
                    Ok(schema) => schema,
                    Err(e) => {
                        invalid_argument_err!("Failed to translate relation to schema: {e:?}");
                    }
                };
                Ok(Response::new(rb.schema_response(result)))
            }
            Analyze::DdlParse(DdlParse { ddl_string }) => {
                let daft_schema = match daft_sql::sql_schema(&ddl_string) {
                    Ok(daft_schema) => daft_schema,
                    Err(e) => invalid_argument_err!("{e}"),
                };

                let daft_schema = daft_schema.to_struct();

                let schema = to_spark_datatype(&daft_schema);

                Ok(Response::new(rb.schema_response(schema)))
            }
            Analyze::TreeString(TreeString { plan, level }) => {
                let plan = plan.required("plan")?;

                if let Some(level) = level {
                    debug!("ignoring tree string level: {level:?}");
                }

                let OpType::Root(input) = plan.op_type.required("op_type")? else {
                    invalid_argument_err!("op_type must be Root");
                };

                if let Some(common) = &input.common {
                    if common.origin.is_some() {
                        debug!("Ignoring common metadata for relation: {common:?}; not yet implemented");
                    }
                }

                let translator = SparkAnalyzer::new(&session);
                let plan = Box::pin(translator.to_logical_plan(input))
                    .await
                    .unwrap()
                    .build();

                let schema = plan.schema();
                let tree_string = schema.repr_spark_string();
                Ok(Response::new(rb.treestring_response(tree_string)))
            }
            Analyze::Explain(Explain { plan, explain_mode }) => {
                let plan = plan.required("plan")?;

                let explain_mode = ExplainMode::try_from(explain_mode).wrap_err("explain_mode")?;
                match explain_mode {
                    ExplainMode::Unspecified | ExplainMode::Simple => {}
                    ExplainMode::Extended => {}
                    _ => not_yet_implemented!("ExplainMode '{explain_mode:?}'"),
                }

                let OpType::Root(input) = plan.op_type.required("op_type")? else {
                    invalid_argument_err!("op_type must be Root");
                };

                let translator = SparkAnalyzer::new(&session);
                let plan = Box::pin(translator.to_logical_plan(input))
                    .await
                    .unwrap()
                    .build();

                let explain = plan.repr_ascii(false);

                Ok(Response::new(rb.explain_response(explain)))
            }
            other => not_yet_implemented!("Analyze '{other:?}'"),
        }
    }
}

// note, the #[tonic::async_trait] messes up autocomplete and other IDE features.
// so I moved the impls out of the macro to make it easier to work with.
#[tonic::async_trait]
impl SparkConnectService for DaftSparkConnectService {
    type ExecutePlanStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<ExecutePlanResponse, Status>> + Send + 'static>,
    >;
    type ReattachExecuteStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<ExecutePlanResponse, Status>> + Send + 'static>,
    >;

    #[tracing::instrument(skip_all)]
    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        Box::pin(self.execute_plan_impl(request)).await
    }

    #[tracing::instrument(skip_all)]
    async fn config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        self.config_impl(request).await
    }

    #[tracing::instrument(skip_all)]
    async fn add_artifacts(
        &self,
        _request: Request<tonic::Streaming<AddArtifactsRequest>>,
    ) -> Result<Response<AddArtifactsResponse>, Status> {
        not_yet_implemented!("add_artifacts operation")
    }

    #[tracing::instrument(skip_all)]
    async fn analyze_plan(
        &self,
        request: Request<AnalyzePlanRequest>,
    ) -> Result<Response<AnalyzePlanResponse>, Status> {
        self.analyze_plan_impl(request).await
    }

    #[tracing::instrument(skip_all)]
    async fn artifact_status(
        &self,
        _request: Request<ArtifactStatusesRequest>,
    ) -> Result<Response<ArtifactStatusesResponse>, Status> {
        not_yet_implemented!("artifact_status operation")
    }

    #[tracing::instrument(skip_all)]
    async fn interrupt(
        &self,
        _request: Request<InterruptRequest>,
    ) -> Result<Response<InterruptResponse>, Status> {
        not_yet_implemented!("interrupt operation")
    }

    #[tracing::instrument(skip_all)]
    async fn reattach_execute(
        &self,
        _request: Request<ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        not_yet_implemented!("reattach_execute operation")
    }

    #[tracing::instrument(skip_all)]
    async fn release_execute(
        &self,
        request: Request<ReleaseExecuteRequest>,
    ) -> Result<Response<ReleaseExecuteResponse>, Status> {
        let request = request.into_inner();

        let session = self.get_session(&request.session_id)?;

        let response = ReleaseExecuteResponse {
            session_id: session.client_side_session_id().to_string(),
            server_side_session_id: session.server_side_session_id().to_string(),
            operation_id: None, // todo: set but not strictly required
        };

        Ok(Response::new(response))
    }

    #[tracing::instrument(skip_all)]
    async fn release_session(
        &self,
        _request: Request<ReleaseSessionRequest>,
    ) -> Result<Response<ReleaseSessionResponse>, Status> {
        not_yet_implemented!("release_session operation")
    }

    #[tracing::instrument(skip_all)]
    async fn fetch_error_details(
        &self,
        _request: Request<FetchErrorDetailsRequest>,
    ) -> Result<Response<FetchErrorDetailsResponse>, Status> {
        not_yet_implemented!("fetch_error_details operation")
    }
}

fn command_type_to_str(cmd_type: &CommandType) -> &str {
    match cmd_type {
        CommandType::RegisterFunction(_) => "RegisterFunction",
        CommandType::WriteOperation(_) => "WriteOperation",
        CommandType::CreateDataframeView(_) => "CreateDataframeView",
        CommandType::WriteOperationV2(_) => "WriteOperationV2",
        CommandType::SqlCommand(_) => "SqlCommand",
        CommandType::WriteStreamOperationStart(_) => "WriteStreamOperationStart",
        CommandType::StreamingQueryCommand(_) => "StreamingQueryCommand",
        CommandType::GetResourcesCommand(_) => "GetResourcesCommand",
        CommandType::StreamingQueryManagerCommand(_) => "StreamingQueryManagerCommand",
        CommandType::RegisterTableFunction(_) => "RegisterTableFunction",
        CommandType::StreamingQueryListenerBusCommand(_) => "StreamingQueryListenerBusCommand",
        CommandType::RegisterDataSource(_) => "RegisterDataSource",
        CommandType::CreateResourceProfileCommand(_) => "CreateResourceProfileCommand",
        CommandType::CheckpointCommand(_) => "CheckpointCommand",
        CommandType::RemoveCachedRemoteRelationCommand(_) => "RemoveCachedRemoteRelationCommand",
        CommandType::MergeIntoTableCommand(_) => "MergeIntoTableCommand",
        CommandType::Extension(_) => "Extension",
    }
}
