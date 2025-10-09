use std::{collections::HashMap, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::post,
};
use common_metrics::{QueryID, QueryPlan, Stat, ops::NodeInfo};
use daft_recordbatch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::state::{
    DashboardState, ExecInfo, OperatorInfo, OperatorStatus, PlanInfo, QueryInfo, QueryState,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StartQueryArgs {
    pub start_sec: u64,
    pub unoptimized_plan: QueryPlan,
}

async fn query_start(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<StartQueryArgs>,
) -> StatusCode {
    state.queries.insert(
        query_id.clone(),
        QueryInfo {
            id: query_id.into(),
            start_sec: args.start_sec,
            unoptimized_plan: args.unoptimized_plan,
            status: QueryState::Pending,
        },
    );
    StatusCode::OK
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PlanStartArgs {
    pub plan_start_sec: u64,
}

async fn plan_start(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<PlanStartArgs>,
) -> StatusCode {
    state.queries.get_mut(&query_id).unwrap().value_mut().status = QueryState::Optimizing {
        plan_start_sec: args.plan_start_sec,
    };
    StatusCode::OK
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PlanEndArgs {
    pub plan_end_sec: u64,
    pub optimized_plan: QueryPlan,
}

async fn plan_end(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<PlanEndArgs>,
) -> StatusCode {
    let mut query_info = state.queries.get_mut(&query_id).unwrap();
    let QueryState::Optimizing { plan_start_sec } = &query_info.status else {
        return StatusCode::BAD_REQUEST;
    };

    query_info.value_mut().status = QueryState::Setup(PlanInfo {
        plan_start_sec: *plan_start_sec,
        plan_end_sec: args.plan_end_sec,
        optimized_plan: args.optimized_plan,
    });
    StatusCode::OK
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExecStartArgs {
    pub exec_start_sec: u64,
    pub node_infos: Vec<NodeInfo>,
}

async fn exec_start(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<ExecStartArgs>,
) -> StatusCode {
    let mut query_info = state.queries.get_mut(&query_id).unwrap();
    let QueryState::Setup(plan_info) = &query_info.status else {
        return StatusCode::BAD_REQUEST;
    };

    query_info.value_mut().status = QueryState::Executing {
        plan_info: plan_info.clone(),
        exec_info: ExecInfo {
            exec_start_sec: args.exec_start_sec,
            operators: args
                .node_infos
                .into_iter()
                .map(|node_info| {
                    (
                        node_info.id,
                        OperatorInfo {
                            status: OperatorStatus::Pending,
                            node_info,
                            stats: HashMap::new(),
                        },
                    )
                })
                .collect(),
        },
    };
    StatusCode::OK
}

async fn exec_op_start(
    State(state): State<Arc<DashboardState>>,
    Path((query_id, op_id)): Path<(QueryID, usize)>,
) -> StatusCode {
    let mut query_info = state.queries.get_mut(&query_id).unwrap();
    let QueryState::Executing { exec_info, .. } = &mut query_info.status else {
        return StatusCode::BAD_REQUEST;
    };

    exec_info.operators.get_mut(&op_id).unwrap().status = OperatorStatus::Executing;
    StatusCode::OK
}

async fn exec_op_end(
    State(state): State<Arc<DashboardState>>,
    Path((query_id, op_id)): Path<(QueryID, usize)>,
) -> StatusCode {
    let mut query_info = state.queries.get_mut(&query_id).unwrap();
    let QueryState::Executing { exec_info, .. } = &mut query_info.status else {
        return StatusCode::BAD_REQUEST;
    };

    exec_info.operators.get_mut(&op_id).unwrap().status = OperatorStatus::Finished;
    StatusCode::OK
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecEmitStatsArgsSend<'a> {
    pub stats: Vec<(usize, HashMap<&'a str, Stat>)>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecEmitStatsArgsRecv {
    pub stats: Vec<(usize, HashMap<String, Stat>)>,
}

async fn exec_emit_stats(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<ExecEmitStatsArgsRecv>,
) -> StatusCode {
    let mut query_info = state.queries.get_mut(&query_id).unwrap();
    let QueryState::Executing { exec_info, .. } = &mut query_info.status else {
        return StatusCode::BAD_REQUEST;
    };

    for (operator_id, stats) in args.stats {
        exec_info.operators.get_mut(&operator_id).unwrap().stats = stats;
    }
    StatusCode::OK
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExecEndArgs {
    pub exec_end_sec: u64,
}

async fn exec_end(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<ExecEndArgs>,
) -> StatusCode {
    let mut query_info = state.queries.get_mut(&query_id).unwrap();
    let QueryState::Executing {
        exec_info,
        plan_info,
    } = &mut query_info.status
    else {
        return StatusCode::BAD_REQUEST;
    };

    query_info.value_mut().status = QueryState::Finalizing {
        plan_info: plan_info.clone(),
        exec_info: exec_info.clone(),
        exec_end_sec: args.exec_end_sec,
    };
    StatusCode::OK
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FinalizeArgs {
    pub end_sec: u64,
    // IPC-serialized RecordBatch
    pub results: Vec<u8>,
}

async fn query_end(
    State(state): State<Arc<DashboardState>>,
    Path(query_id): Path<QueryID>,
    Json(args): Json<FinalizeArgs>,
) -> StatusCode {
    let mut query_info = state.queries.get_mut(&query_id).unwrap();
    let QueryState::Finalizing {
        exec_info,
        plan_info,
        exec_end_sec,
    } = &mut query_info.status
    else {
        tracing::error!("Failed to get query info");
        return StatusCode::BAD_REQUEST;
    };

    let rb = RecordBatch::from_ipc_stream(&args.results);
    let Ok(results) = rb else {
        tracing::error!("Failed to deserialize results");
        return StatusCode::BAD_REQUEST;
    };
    query_info.value_mut().status = QueryState::Finished {
        plan_info: plan_info.clone(),
        exec_info: exec_info.clone(),
        exec_end_sec: *exec_end_sec,
        end_sec: args.end_sec,
        results,
    };
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
