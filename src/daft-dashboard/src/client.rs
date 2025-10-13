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
use tokio::sync::watch;
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
        tracing::error!("Query `{}` not found", query_id);
        Err(StatusCode::NOT_FOUND)
    }
}

async fn subscribe_query_updates(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    // Initial event with the current query state
    let Some(query_info) = state.queries.get(&query_id) else {
        tracing::error!("Query `{}` not found", query_id);
        return Err(StatusCode::NOT_FOUND);
    };

    let query_info = query_info.value().clone();
    let operator_infos = HashMap::new();

    let (mut query_rx, mut operator_rx) = {
        let tx = state.query_clients.entry(query_id).or_insert_with(|| {
            (
                watch::Sender::new(query_info.clone()),
                watch::Sender::new(operator_infos),
            )
        });

        let query_rx = tx.0.subscribe();
        let operator_rx = tx.1.subscribe();
        (query_rx, operator_rx)
    };

    let stream = async_stream::stream! {
        yield Ok(Event::default()
            .id("0")
            .event("initial_state")
            .data(serde_json::to_string(&query_info).unwrap()));

        let mut counter = 1;
        loop {
            tokio::select! {
                query_status = query_rx.changed() => {
                    match query_status {
                        Ok(()) => {
                            let query_info = query_rx.borrow_and_update().clone();
                            yield Ok(Event::default()
                                .id(counter.to_string())
                                .event("query_info")
                                .data(serde_json::to_string(&query_info).unwrap()));
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
                operator_status = operator_rx.changed() => {
                    match operator_status {
                        Ok(()) => {
                            let operator_infos = operator_rx.borrow_and_update().clone();
                            yield Ok(Event::default()
                                .id(counter.to_string())
                                .event("operator_info")
                                .data(serde_json::to_string(&operator_infos).unwrap()));
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
            }
            counter += 1;
        }
    };

    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

pub(crate) fn routes() -> Router<Arc<DashboardState>> {
    Router::new()
        .route("/queries", get(get_query_summaries))
        .route("/queries/subscribe", get(subscribe_queries_updates))
        .route("/query/{query_id}", get(get_query))
        .route("/query/{query_id}/subscribe", get(subscribe_query_updates))
}
