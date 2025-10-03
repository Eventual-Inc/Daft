use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use common_metrics::QueryID;
use serde_json::Value;

use crate::state::{DashboardState, QueryInfo};

async fn get_query_summaries(State(state): State<Arc<DashboardState>>) -> Json<Value> {
    let query_informations = state
        .queries
        .iter()
        .map(|query| {
            let query = query.value();
            let (status, time_sec) = query.summarize();
            serde_json::json!({
                "id": query.id.clone(),
                "status": status,
                "time_sec": time_sec,
            })
        })
        .collect::<Vec<_>>();

    Json(Value::Array(query_informations))
}

async fn get_query(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
) -> Result<Json<QueryInfo>, StatusCode> {
    let Some(query) = state.queries.get(&query_id) else {
        return Err(StatusCode::NOT_FOUND);
    };

    Ok(Json(query.value().clone()))
}

pub(crate) fn routes() -> Router<Arc<DashboardState>> {
    Router::new()
        .route("/queries", get(get_query_summaries))
        .route("/query/{query_id}", get(get_query))
}
