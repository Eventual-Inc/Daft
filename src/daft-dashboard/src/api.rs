use std::{sync::Arc, time::Duration};

use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
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
    let rb = RecordBatch::from_ipc_stream(rb_bytes.as_ref())
        .map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;

    state.complete_query(query_id, duration, rb);
    Ok(())
}

/// All API routes
pub fn api_routes() -> Router {
    Router::new()
        .route("/ping", get(ping))
        .route("/queries", get(get_queries))
        .route("/queries", post(start_query))
        // Individual query endpoints
        .route("/queries/:query_id", get(get_query))
        .route("/queries/:query_id", post(complete_query))
        .route("/queries/:query_id/dataframe", get(get_query_dataframe))
        .with_state(Arc::new(AppState::new()))
}
