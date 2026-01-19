use std::{collections::HashMap, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::post,
};
use common_metrics::{QueryEndState, QueryID, QueryPlan, Stat};
use daft_recordbatch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::state::{
    DashboardState, ExecInfo, NodeInfo, OperatorInfo, OperatorInfos, OperatorStatus, PlanInfo,
    QueryInfo, QueryState,
};

#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct StartQueryArgs {
    pub start_sec: f64,
    pub unoptimized_plan: QueryPlan,
    pub runner: Option<String>,
    pub ray_dashboard_url: Option<String>,
    pub entrypoint: Option<String>,
}

async fn query_start(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<StartQueryArgs>,
) -> StatusCode {
    if state.queries.contains_key(&query_id) {
        return StatusCode::BAD_REQUEST;
    }

    let query_info = QueryInfo {
        id: query_id.clone().into(),
        start_sec: args.start_sec,
        unoptimized_plan: args.unoptimized_plan,
        runner: args
            .runner
            .unwrap_or_else(|| "Native (Swordfish)".to_string()),
        ray_dashboard_url: args.ray_dashboard_url,
        entrypoint: args.entrypoint,
        state: QueryState::Pending,
    };

    state.queries.insert(query_id.clone(), query_info);

    // Ping clients
    let Some(query_info) = state.queries.get(&query_id) else {
        tracing::error!("Query `{}` not found", query_id);
        return StatusCode::BAD_REQUEST;
    };
    state.ping_clients_on_query_update(query_info.value());
    StatusCode::OK
}

#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct PlanStartArgs {
    pub plan_start_sec: f64,
}

async fn plan_start(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<PlanStartArgs>,
) -> StatusCode {
    let query_info = state.queries.get_mut(&query_id);
    let Some(mut query_info) = query_info else {
        tracing::error!("Query `{}` not found", query_id);
        return StatusCode::BAD_REQUEST;
    };
    if !matches!(query_info.state, QueryState::Pending) {
        tracing::error!("Query `{}` is not pending", query_id);
        return StatusCode::BAD_REQUEST;
    }

    query_info.state = QueryState::Optimizing {
        plan_start_sec: args.plan_start_sec,
    };

    state.ping_clients_on_query_update(query_info.value());
    StatusCode::OK
}

#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct PlanEndArgs {
    pub plan_end_sec: f64,
    pub optimized_plan: QueryPlan,
}

async fn plan_end(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<PlanEndArgs>,
) -> StatusCode {
    let query_info = state.queries.get_mut(&query_id);
    let Some(mut query_info) = query_info else {
        return StatusCode::BAD_REQUEST;
    };
    let QueryState::Optimizing { plan_start_sec } = query_info.state else {
        return StatusCode::BAD_REQUEST;
    };

    query_info.state = QueryState::Setup {
        plan_info: PlanInfo {
            plan_start_sec,
            plan_end_sec: args.plan_end_sec,
            optimized_plan: args.optimized_plan,
        },
        pending_source_stats: HashMap::new(),
    };

    state.ping_clients_on_query_update(query_info.value());
    StatusCode::OK
}

#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ExecStartArgs {
    pub exec_start_sec: f64,
    pub physical_plan: QueryPlan,
}

#[derive(Clone, Deserialize)]
struct PlanJsonConfig {
    pub id: usize,
    pub name: String,
    #[serde(rename = "type")]
    pub node_type: Arc<str>,
    pub category: Arc<str>,
    pub children: Option<Vec<PlanJsonConfig>>,
}

fn parse_physical_plan(physical_plan: &QueryPlan) -> OperatorInfos {
    let parsed_plan = serde_json::from_str::<PlanJsonConfig>(physical_plan)
        .expect("Failed to parse physical plan");
    let mut operators = HashMap::new();

    let mut plans = vec![parsed_plan];
    while let Some(plan) = plans.pop() {
        let node_id = plan.id;
        let node_info = NodeInfo {
            id: node_id,
            name: plan.name,
            node_type: plan.node_type.clone(),
            node_category: plan.category.clone(),
        };

        operators.insert(
            node_id,
            OperatorInfo {
                status: OperatorStatus::Pending,
                node_info,
                stats: HashMap::new(),
                source_stats: HashMap::new(),
            },
        );

        if let Some(children) = plan.children {
            plans.extend(children);
        }
    }
    operators
}

async fn exec_start(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<ExecStartArgs>,
) -> StatusCode {
    tracing::info!("Received exec_start for query {}", query_id);
    let query_info = state.queries.get_mut(&query_id);
    let Some(mut query_info) = query_info else {
        tracing::error!("Query {} not found in exec_start", query_id);
        return StatusCode::BAD_REQUEST;
    };

    match &mut query_info.state {
        QueryState::Setup {
            plan_info,
            pending_source_stats,
        } => {
            let plan_info = plan_info.clone();
            let pending_source_stats = std::mem::take(pending_source_stats);
            query_info.state = QueryState::Executing {
                plan_info,
                exec_info: ExecInfo {
                    exec_start_sec: args.exec_start_sec,
                    physical_plan: args.physical_plan.clone(),
                    operators: parse_physical_plan(&args.physical_plan),
                },
            };

            if let QueryState::Executing { exec_info, .. } = &mut query_info.state {
                for (source_id, per_op_stats) in pending_source_stats {
                    let stats = per_op_stats.into_iter().collect::<Vec<_>>();
                    apply_exec_emit_stats(exec_info, &query_id, &source_id, stats);
                }
            }

            state.ping_clients_on_query_update(query_info.value());
            StatusCode::OK
        }
        QueryState::Executing { .. }
        | QueryState::Finalizing { .. }
        | QueryState::Finished { .. } => {
            tracing::debug!(
                "Query {} is already executing or finished, ignoring duplicate exec_start",
                query_id
            );
            StatusCode::OK
        }
        _ => {
            tracing::error!(
                "Query {} not in Setup state (actual: {:?})",
                query_id,
                query_info.state
            );
            StatusCode::BAD_REQUEST
        }
    }
}

async fn exec_op_start(
    State(state): State<Arc<DashboardState>>,
    Path((query_id, op_id)): Path<(QueryID, usize)>,
) -> StatusCode {
    let query_info = state.queries.get_mut(&query_id);
    let Some(mut query_info) = query_info else {
        tracing::error!("Query {} not found in exec_op_start", query_id);
        return StatusCode::BAD_REQUEST;
    };
    let QueryState::Executing { exec_info, .. } = &mut query_info.state else {
        // If it's in Setup state, just return OK. The worker is faster than the driver's exec_start.
        return StatusCode::OK;
    };

    if let Some(op) = exec_info.operators.get_mut(&op_id) {
        op.status = OperatorStatus::Executing;
    } else {
        tracing::warn!("Operator {} not found for query {}", op_id, query_id);
    }

    state.ping_clients_on_operator_update(query_info.value());
    StatusCode::OK
}

async fn exec_op_end(
    State(state): State<Arc<DashboardState>>,
    Path((query_id, op_id)): Path<(QueryID, usize)>,
) -> StatusCode {
    let query_info = state.queries.get_mut(&query_id);
    let Some(mut query_info) = query_info else {
        tracing::error!("Query {} not found in exec_op_end", query_id);
        return StatusCode::BAD_REQUEST;
    };
    let QueryState::Executing { exec_info, .. } = &mut query_info.state else {
        // If it's in Setup state, just return OK.
        return StatusCode::OK;
    };

    if let Some(op) = exec_info.operators.get_mut(&op_id) {
        op.status = OperatorStatus::Finished;
    } else {
        tracing::warn!("Operator {} not found for query {}", op_id, query_id);
    }

    state.ping_clients_on_operator_update(query_info.value());
    StatusCode::OK
}

#[derive(Clone, Serialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ExecEmitStatsArgsSend {
    pub source_id: String,
    pub stats: Vec<(usize, HashMap<String, Stat>)>,
}

#[derive(Clone, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ExecEmitStatsArgsRecv {
    pub source_id: String,
    pub stats: Vec<(usize, HashMap<String, Stat>)>,
}

fn merge_stats_map(dst: &mut HashMap<String, Stat>, src: HashMap<String, Stat>) {
    for (name, stat) in src {
        dst.entry(name)
            .and_modify(|old| match (old, stat.clone()) {
                (Stat::Count(a), Stat::Count(b)) => *a += b,
                (Stat::Bytes(a), Stat::Bytes(b)) => *a += b,
                (Stat::Duration(a), Stat::Duration(b)) => *a += b,
                (Stat::Float(a), Stat::Float(b)) => *a += b,
                (Stat::Percent(a), Stat::Percent(b)) => *a = (*a).max(b),
                (a, b) => *a = b,
            })
            .or_insert(stat);
    }
}

fn apply_exec_emit_stats(
    exec_info: &mut ExecInfo,
    query_id: &QueryID,
    source_id: &str,
    stats: Vec<(usize, HashMap<String, Stat>)>,
) {
    for (operator_id, operator_stats) in stats {
        if let Some(op) = exec_info.operators.get_mut(&operator_id) {
            op.source_stats
                .insert(source_id.to_string(), operator_stats);

            let mut aggregated_stats: HashMap<String, Stat> = HashMap::new();
            for source_stat in op.source_stats.values() {
                for (name, stat) in source_stat {
                    aggregated_stats
                        .entry(name.clone())
                        .and_modify(|old| match (old, stat.clone()) {
                            (Stat::Count(a), Stat::Count(b)) => *a += b,
                            (Stat::Bytes(a), Stat::Bytes(b)) => *a += b,
                            (Stat::Duration(a), Stat::Duration(b)) => *a += b,
                            (Stat::Float(a), Stat::Float(b)) => *a += b,
                            (Stat::Percent(a), Stat::Percent(b)) => *a = (*a).max(b),
                            (a, b) => *a = b,
                        })
                        .or_insert_with(|| stat.clone());
                }
            }

            if op.node_info.node_type.contains("Repartition") {
                let rows_in = aggregated_stats.get("rows in").and_then(|s| match s {
                    Stat::Count(v) => Some(*v),
                    _ => None,
                });
                let rows_out = aggregated_stats.get("rows out").and_then(|s| match s {
                    Stat::Count(v) => Some(*v),
                    _ => None,
                });
                if let (Some(rows_in), Some(rows_out)) = (rows_in, rows_out)
                    && rows_out > rows_in
                {
                    aggregated_stats
                        .insert("rows out".to_string(), Stat::Count(rows_out - rows_in));
                }
            }

            op.stats = aggregated_stats;

            if op.status == OperatorStatus::Pending {
                op.status = OperatorStatus::Executing;
            }
        } else {
            tracing::warn!(
                "Operator {} not found for query {} in exec_emit_stats",
                operator_id,
                query_id
            );
        }
    }
}

async fn exec_emit_stats(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<ExecEmitStatsArgsRecv>,
) -> StatusCode {
    let query_info = state.queries.get_mut(&query_id);
    let Some(mut query_info) = query_info else {
        tracing::error!("Query {} not found in exec_emit_stats", query_id);
        return StatusCode::BAD_REQUEST;
    };

    let ExecEmitStatsArgsRecv { source_id, stats } = args;
    match &mut query_info.state {
        QueryState::Setup {
            pending_source_stats,
            ..
        } => {
            let per_source = pending_source_stats.entry(source_id).or_default();
            for (operator_id, stats) in stats {
                let entry = per_source.entry(operator_id).or_default();
                merge_stats_map(entry, stats);
            }
            return StatusCode::OK;
        }
        QueryState::Executing { exec_info, .. }
        | QueryState::Finalizing { exec_info, .. }
        | QueryState::Finished { exec_info, .. } => {
            apply_exec_emit_stats(exec_info, &query_id, &source_id, stats);
        }
        QueryState::Failed {
            exec_info: Some(exec_info),
            ..
        }
        | QueryState::Canceled {
            exec_info: Some(exec_info),
            ..
        } => {
            apply_exec_emit_stats(exec_info, &query_id, &source_id, stats);
        }
        QueryState::Failed {
            exec_info: None, ..
        }
        | QueryState::Canceled {
            exec_info: None, ..
        } => {
            return StatusCode::OK;
        }
        _ => {
            tracing::warn!(
                "Query {} not in an executing state in exec_emit_stats (actual: {:?})",
                query_id,
                query_info.state
            );
            return StatusCode::BAD_REQUEST;
        }
    }

    state.ping_clients_on_operator_update(query_info.value());
    StatusCode::OK
}

#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ExecEndArgs {
    pub exec_end_sec: f64,
}

async fn exec_end(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<ExecEndArgs>,
) -> StatusCode {
    tracing::info!("Received exec_end for query {}", query_id);
    let query_info = state.queries.get_mut(&query_id);
    let Some(mut query_info) = query_info else {
        tracing::error!("Query {} not found in exec_end", query_id);
        return StatusCode::BAD_REQUEST;
    };

    if !matches!(query_info.state, QueryState::Executing { .. }) {
        tracing::debug!(
            "Query {} is in state {:?}, ignoring exec_end",
            query_id,
            query_info.state
        );
        return StatusCode::OK;
    }

    let QueryState::Executing {
        mut exec_info,
        plan_info,
    } = query_info.state.clone()
    else {
        unreachable!();
    };

    // Mark all operators as finished
    for op in exec_info.operators.values_mut() {
        if op.status == OperatorStatus::Pending || op.status == OperatorStatus::Executing {
            op.status = OperatorStatus::Finished;
        }
    }

    query_info.state = QueryState::Finalizing {
        plan_info,
        exec_info: exec_info.clone(),
        exec_end_sec: args.exec_end_sec,
    };

    state.ping_clients_on_query_update(query_info.value());
    StatusCode::OK
}

#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct FinalizeArgs {
    pub end_sec: f64,
    pub end_state: QueryEndState,
    pub error_message: Option<String>,
    // IPC-serialized RecordBatch
    pub results: Option<Vec<u8>>,
}

async fn query_end(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<FinalizeArgs>,
) -> StatusCode {
    let query_info = state.queries.get_mut(&query_id);
    let Some(mut query_info) = query_info else {
        tracing::error!("Query `{}` not found in query_end", query_id);
        return StatusCode::BAD_REQUEST;
    };

    let (plan_info, mut exec_info, exec_end_sec) = match &query_info.state {
        QueryState::Finalizing {
            exec_info,
            plan_info,
            exec_end_sec,
        } => (
            Some(plan_info.clone()),
            Some(exec_info.clone()),
            Some(*exec_end_sec),
        ),
        QueryState::Executing {
            exec_info,
            plan_info,
        } => (
            Some(plan_info.clone()),
            Some(exec_info.clone()),
            Some(args.end_sec),
        ),
        _ => (None, None, Some(args.end_sec)),
    };

    if let Some(ref mut exec_info) = exec_info {
        for op in exec_info.operators.values_mut() {
            if op.status == OperatorStatus::Pending || op.status == OperatorStatus::Executing {
                op.status = OperatorStatus::Finished;
            }
        }
    }

    let results = if let Some(results) = &args.results {
        match RecordBatch::from_ipc_stream(results) {
            Ok(results) => Some(results),
            Err(e) => {
                tracing::error!(
                    "Failed to deserialize results for query `{}`: {}",
                    query_id,
                    e
                );
                return StatusCode::BAD_REQUEST;
            }
        }
    } else {
        None
    };

    query_info.state = match args.end_state {
        QueryEndState::Finished => match (plan_info, exec_info, exec_end_sec) {
            (Some(plan_info), Some(exec_info), Some(exec_end_sec)) => QueryState::Finished {
                plan_info,
                exec_info,
                exec_end_sec,
                end_sec: args.end_sec,
                results,
            },
            (plan_info, exec_info, exec_end_sec) => {
                // If we are missing info but the query is finished, we still transition to Finished
                // but with empty/placeholder info. This is better than returning 400 and leaving
                // the query in an inconsistent state.
                tracing::warn!(
                    "Query `{}` finished but is missing plan or exec info. Transitioning anyway.",
                    query_id
                );
                QueryState::Finished {
                    plan_info: plan_info.unwrap_or_else(|| PlanInfo {
                        plan_start_sec: query_info.start_sec,
                        plan_end_sec: query_info.start_sec,
                        optimized_plan: query_info.unoptimized_plan.clone(),
                    }),
                    exec_info: exec_info.unwrap_or_else(|| ExecInfo {
                        exec_start_sec: query_info.start_sec,
                        physical_plan: query_info.unoptimized_plan.clone(),
                        operators: HashMap::new(),
                    }),
                    exec_end_sec: exec_end_sec.unwrap_or(args.end_sec),
                    end_sec: args.end_sec,
                    results,
                }
            }
        },
        QueryEndState::Canceled => {
            let mut exec_info = exec_info;
            if let Some(ref mut info) = exec_info {
                for op in info.operators.values_mut() {
                    if !matches!(op.status, OperatorStatus::Finished) {
                        op.status = OperatorStatus::Failed;
                    }
                }
            }
            QueryState::Canceled {
                plan_info,
                exec_info,
                end_sec: args.end_sec,
                message: args.error_message,
            }
        }
        QueryEndState::Failed => {
            let mut exec_info = exec_info;
            if let Some(ref mut info) = exec_info {
                for op in info.operators.values_mut() {
                    if !matches!(op.status, OperatorStatus::Finished) {
                        op.status = OperatorStatus::Failed;
                    }
                }
            }
            QueryState::Failed {
                plan_info,
                exec_info,
                end_sec: args.end_sec,
                message: args.error_message,
            }
        }
        QueryEndState::Dead => todo!(),
    };

    state.ping_clients_on_query_update(query_info.value());
    StatusCode::OK
}

pub(crate) fn routes() -> Router<Arc<DashboardState>> {
    Router::new()
        // Query lifecycle
        // TODO: Consider replacing with websocket for active engine -> server communication
        .route("/query/{query_id}/start", post(query_start))
        .route("/query/{query_id}/plan_start", post(plan_start))
        .route("/query/{query_id}/plan_end", post(plan_end))
        .route("/query/{query_id}/exec/start", post(exec_start))
        .route("/query/{query_id}/exec/{op_id}/start", post(exec_op_start))
        .route("/query/{query_id}/exec/{op_id}/end", post(exec_op_end))
        .route("/query/{query_id}/exec/emit_stats", post(exec_emit_stats))
        .route("/query/{query_id}/exec/end", post(exec_end))
        .route("/query/{query_id}/end", post(query_end))
}
