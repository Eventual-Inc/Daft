use std::{collections::HashMap, convert::Infallible, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{Sse, sse::Event},
    routing::get,
};
use common_metrics::QueryID;
use futures::future::ok;
use tokio_stream::{
    Stream, StreamExt as _,
    wrappers::{BroadcastStream, errors::BroadcastStreamRecvError},
};

use crate::state::{DashboardState, QueryInfo, QuerySummary};

/// Get the summaries of all queries
/// Note, this is used for internal testing, not by the frontend
async fn get_query_summaries(
    State(state): State<Arc<DashboardState>>,
) -> Json<HashMap<QueryID, QuerySummary>> {
    Json(
        state
            .queries
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().summarize()))
            .collect::<HashMap<_, _>>(),
    )
}

async fn subscribe_queries_updates(
    State(state): State<Arc<DashboardState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.clients.subscribe();

    // Consecutive events of all query status updates
    let stream = BroadcastStream::new(rx).filter_map(|event| match event {
        Ok((id, summary)) => {
            let event = Event::default()
                .id(id.to_string())
                .event("status_update")
                .data(serde_json::to_string(&summary).unwrap());
            Some(Ok(event))
        }
        Err(BroadcastStreamRecvError::Lagged(_)) => None,
    });

    // Initial event with the current state
    let queries = state
        .queries
        .iter()
        .map(|entry| (entry.key().clone(), entry.value().summarize()))
        .collect::<HashMap<_, _>>();
    let stream = futures::stream::once(ok(Event::default()
        .event("initial_state")
        .data(serde_json::to_string(&queries).unwrap())))
    .chain(stream);

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    )
}

/// Get the info of a specific query
/// Note, this is used for internal testing, not by the frontend
async fn get_query(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
) -> Result<Json<QueryInfo>, StatusCode> {
    let query = state.queries.get(&query_id);

    if let Some(query) = query {
        Ok(Json(query.value().clone()))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub(crate) fn routes() -> Router<Arc<DashboardState>> {
    Router::new()
        .route("/queries", get(get_query_summaries))
        .route("/queries/subscribe", get(subscribe_queries_updates))
        .route("/query/{query_id}", get(get_query))
}
