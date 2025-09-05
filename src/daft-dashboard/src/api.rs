use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, State},
    routing::{get, post},
};
use daft_recordbatch::RecordBatch;
use reqwest::StatusCode;
use serde::Deserialize;

use crate::{
    dataframe::get_query_dataframe,
    state::{AppState, QueryInfo, QueryMetadata},
};

/// Ping the server to check if running
async fn ping() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// Get metadata about all known queries
async fn get_queries(State(state): State<Arc<AppState>>) -> Json<Vec<QueryMetadata>> {
    let mut queries = state.queries();

    // Sort by status, then start time
    queries.sort_by(|a, b| {
        a.status
            .cmp(&b.status)
            .then_with(|| a.start_time.cmp(&b.start_time))
    });

    Json(queries)
}

async fn start_query(State(state): State<Arc<AppState>>, Json(query): Json<QueryInfo>) {
    state.add_query(query);
}

/// Get details about a specific query
async fn get_query(
    State(state): State<Arc<AppState>>,
    Path(query_id): Path<Arc<str>>,
) -> Result<Json<QueryMetadata>, StatusCode> {
    let query = state.get_query(query_id).ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(query))
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
struct UpdateMetricsParams {
    metrics: Vec<(usize, HashMap<String, String>)>,
}
async fn update_metrics(
    State(state): State<Arc<AppState>>,
    Path(query_id): Path<Arc<str>>,
    Json(params): Json<UpdateMetricsParams>,
) -> Result<(), StatusCode> {
    let UpdateMetricsParams { metrics } = params;
    eprintln!("update_metrics: {:?}", metrics);
    state.update_metrics(query_id, metrics);
    Ok(())
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
struct CompleteQueryParams {
    duration: Duration,
    rb_bytes: Vec<u8>,
}

async fn complete_query(
    State(state): State<Arc<AppState>>,
    Path(query_id): Path<Arc<str>>,
    Json(params): Json<CompleteQueryParams>,
) -> Result<(), StatusCode> {
    let CompleteQueryParams { duration, rb_bytes } = params;
    // TODO: Worry about this later
    // let rb = RecordBatch::from_ipc_stream(rb_bytes.as_ref())
    //     .map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;

    let rb = RecordBatch::empty(None);
    state.complete_query(query_id, duration, rb);
    Ok(())
}

/// All API routes
pub fn api_routes() -> Router {
    let state = AppState::new();
    Router::new()
        .route("/api/ping", get(ping))
        .route("/api/queries", get(get_queries))
        .route("/api/queries", post(start_query))
        // Individual query endpoints
        .route("/api/queries/:query_id", get(get_query))
        .route("/api/queries/:query_id", post(complete_query))
        .route("/api/queries/:query_id/dataframe", get(get_query_dataframe))
        .route("/api/queries/:query_id/metrics", post(update_metrics))
        // .route("/api/queries/:query_id/logs", get(get_query_logs))
        .with_state(Arc::new(state))
}
