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
    pub start_sec: u64,
    pub unoptimized_plan: QueryPlan,
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
    pub plan_start_sec: u64,
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
    pub plan_end_sec: u64,
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
    };

    state.ping_clients_on_query_update(query_info.value());
    StatusCode::OK
}

#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ExecStartArgs {
    pub exec_start_sec: u64,
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
    let query_info = state.queries.get_mut(&query_id);
    let Some(mut query_info) = query_info else {
        return StatusCode::BAD_REQUEST;
    };
    let QueryState::Setup { plan_info } = &query_info.state else {
        return StatusCode::BAD_REQUEST;
    };

    // Parse physical plan JSON to extract node info
    query_info.state = QueryState::Executing {
        plan_info: plan_info.clone(),
        exec_info: ExecInfo {
            exec_start_sec: args.exec_start_sec,
            physical_plan: args.physical_plan.clone(),
            operators: parse_physical_plan(&args.physical_plan),
        },
    };

    state.ping_clients_on_query_update(query_info.value());
    StatusCode::OK
}

async fn exec_op_start(
    State(state): State<Arc<DashboardState>>,
    Path((query_id, op_id)): Path<(QueryID, usize)>,
) -> StatusCode {
    let query_info = state.queries.get_mut(&query_id);
    let Some(mut query_info) = query_info else {
        return StatusCode::BAD_REQUEST;
    };
    let QueryState::Executing { exec_info, .. } = &mut query_info.state else {
        return StatusCode::BAD_REQUEST;
    };

    exec_info.operators.get_mut(&op_id).unwrap().status = OperatorStatus::Executing;

    state.ping_clients_on_operator_update(query_info.value());
    StatusCode::OK
}

async fn exec_op_end(
    State(state): State<Arc<DashboardState>>,
    Path((query_id, op_id)): Path<(QueryID, usize)>,
) -> StatusCode {
    let query_info = state.queries.get_mut(&query_id);
    let Some(mut query_info) = query_info else {
        return StatusCode::BAD_REQUEST;
    };
    let QueryState::Executing { exec_info, .. } = &mut query_info.state else {
        return StatusCode::BAD_REQUEST;
    };

    exec_info.operators.get_mut(&op_id).unwrap().status = OperatorStatus::Finished;

    state.ping_clients_on_operator_update(query_info.value());
    StatusCode::OK
}

#[derive(Clone, Serialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ExecEmitStatsArgsSend {
    pub stats: Vec<(usize, HashMap<String, Stat>)>,
}

#[derive(Clone, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ExecEmitStatsArgsRecv {
    pub stats: Vec<(usize, HashMap<String, Stat>)>,
}

async fn exec_emit_stats(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<ExecEmitStatsArgsRecv>,
) -> StatusCode {
    let query_info = state.queries.get_mut(&query_id);
    let Some(mut query_info) = query_info else {
        return StatusCode::BAD_REQUEST;
    };
    let QueryState::Executing { exec_info, .. } = &mut query_info.state else {
        return StatusCode::BAD_REQUEST;
    };

    for (operator_id, stats) in args.stats {
        exec_info.operators.get_mut(&operator_id).unwrap().stats = stats;
    }

    state.ping_clients_on_operator_update(query_info.value());
    StatusCode::OK
}

#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ExecEndArgs {
    pub exec_end_sec: u64,
}

async fn exec_end(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<ExecEndArgs>,
) -> StatusCode {
    let query_info = state.queries.get_mut(&query_id);
    let Some(mut query_info) = query_info else {
        return StatusCode::BAD_REQUEST;
    };
    let QueryState::Executing {
        exec_info,
        plan_info,
    } = &query_info.state
    else {
        return StatusCode::BAD_REQUEST;
    };

    query_info.state = QueryState::Finalizing {
        plan_info: plan_info.clone(),
        exec_info: exec_info.clone(),
        exec_end_sec: args.exec_end_sec,
    };

    state.ping_clients_on_query_update(query_info.value());
    StatusCode::OK
}

#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct FinalizeArgs {
    pub end_sec: u64,
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
        tracing::error!("Query `{}` not found", query_id);
        return StatusCode::BAD_REQUEST;
    };
    let QueryState::Finalizing {
        exec_info,
        plan_info,
        exec_end_sec,
    } = &query_info.state
    else {
        tracing::error!("Query `{}` not in finalizing state", query_id);
        return StatusCode::BAD_REQUEST;
    };

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
        QueryEndState::Finished => QueryState::Finished {
            plan_info: plan_info.clone(),
            exec_info: exec_info.clone(),
            exec_end_sec: *exec_end_sec,
            end_sec: args.end_sec,
            results,
        },
        QueryEndState::Canceled => QueryState::Canceled {
            plan_info: plan_info.clone(),
            exec_info: exec_info.clone(),
            end_sec: args.end_sec,
            message: args.error_message,
        },
        QueryEndState::Failed => QueryState::Failed {
            plan_info: plan_info.clone(),
            exec_info: exec_info.clone(),
            end_sec: args.end_sec,
            message: args.error_message,
        },
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
