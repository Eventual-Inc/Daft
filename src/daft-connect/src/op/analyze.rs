use std::pin::Pin;

use spark_connect::{analyze_plan_response, AnalyzePlanResponse};

pub type AnalyzeStream =
    Pin<Box<dyn futures::Stream<Item = Result<AnalyzePlanResponse, Status>> + Send + Sync>>;

use spark_connect::{analyze_plan_request::explain::ExplainMode, Relation};
use tonic::Status;

use crate::{session::Session, translation};

pub struct PlanIds {
    session: String,
    server_side_session: String,
}

impl PlanIds {
    pub fn response(&self, result: analyze_plan_response::Result) -> AnalyzePlanResponse {
        AnalyzePlanResponse {
            session_id: self.session.to_string(),
            server_side_session_id: self.server_side_session.to_string(),
            result: Some(result),
        }
    }
}

impl Session {
    pub async fn handle_explain_command(
        &self,
        command: Relation,
        _mode: ExplainMode,
    ) -> eyre::Result<AnalyzePlanResponse> {
        let context = PlanIds {
            session: self.client_side_session_id().to_string(),
            server_side_session: self.server_side_session_id().to_string(),
        };

        let plan = translation::to_logical_plan(command)?;
        let optimized_plan = plan.optimize()?;

        let optimized_plan = optimized_plan.build();

        // todo: what do we want this to display
        let explain_string = format!("{optimized_plan}");

        let schema = analyze_plan_response::Explain { explain_string };

        let response = context.response(analyze_plan_response::Result::Explain(schema));
        Ok(response)
    }
}
