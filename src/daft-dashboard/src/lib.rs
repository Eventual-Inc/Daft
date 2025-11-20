pub(crate) mod assets;
pub(crate) mod client;
pub mod engine;
#[cfg(feature = "python")]
pub mod python;
pub(crate) mod state;

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use daft_recordbatch::RecordBatch;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{
    LatencyUnit,
    cors::CorsLayer,
    trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
};
use tracing::Level;

use crate::state::{DashboardState, GLOBAL_DASHBOARD_STATE};

// NOTE(void001): default listen to all ipv4 address, which pose a security risk in production environment
pub const DEFAULT_SERVER_ADDR: Ipv4Addr = Ipv4Addr::UNSPECIFIED;
pub const DEFAULT_SERVER_PORT: u16 = 3238;

fn generate_interactive_html(record_batch: &RecordBatch, df_id: &str) -> String {
    // Start with the basic table HTML from repr_html
    let table_html = record_batch.repr_html();
    // Build the complete interactive HTML with side pane layout
    let mut html = vec![
        r#"
        <style>
        .dashboard-container {
            display: flex;
            gap: 20px;
            max-width: 100%;
            height: 100%;
        }
        .table-container {
            flex: 1;
            overflow: auto;
        }
        .side-pane {
            width: 35%;
            max-height: 500px;
            border: 1px solid;
            border-radius: 4px;
            padding: 15px;
            display: none;
            overflow: auto;
        }
        .side-pane.visible {
            display: flex;
            flex-direction: column;
        }
        .side-pane-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
            padding-bottom: 10px;
            border-bottom: 1px solid;
        }
        .side-pane-title {
            font-weight: bold;
        }
        .close-button {
            cursor: pointer;
        }
        .side-pane-content {
            word-wrap: break-word;
            overflow: auto;
        }
        .dataframe td.clickable {
            cursor: pointer;
            transition: background-color 0.2s;
        }
        .dataframe td.clickable:hover {
            opacity: 0.8;
        }
        .dataframe td.clickable.selected {
            opacity: 0.6;
        }
        </style>
        <div class="dashboard-container">
            <div class="table-container">
        "#
        .to_string(),
    ];

    // Build full cell HTML matrix for side-pane
    let mut full_matrix: Vec<Vec<String>> = Vec::with_capacity(record_batch.len());
    for r in 0..record_batch.len() {
        let mut row_vec: Vec<String> = Vec::with_capacity(record_batch.num_columns());
        for c in 0..record_batch.num_columns() {
            let col = record_batch.get_column(c);
            row_vec.push(daft_recordbatch::html_value(col, r, false));
        }
        full_matrix.push(row_vec);
    }
    let full_matrix_json = serde_json::to_string(&full_matrix).unwrap();
    let full_matrix_attr = full_matrix_json
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\'', "&#39;");

    // Add the table HTML with ID and embed full matrix in data attribute
    html.push(format!(
        r#"<div id="dataframe-{}" data-full-matrix='{}'>{}</div>"#,
        df_id, full_matrix_attr, table_html
    ));

    // Add the side pane
    html.push(format!(
        r#"
            </div>
            <div class="side-pane" id="side-pane-{df_id}">
                <div class="side-pane-header">
                    <div class="side-pane-title" id="side-pane-title-{df_id}">Cell Details</div>
                    <button class="close-button" id="close-button-{df_id}">×</button>
                </div>
                <div class="side-pane-content" id="side-pane-content-{df_id}">
                    <p style="font-style: italic;">Click on a cell to view its full content</p>
                </div>
            </div>
        </div>
    "#,
    ));

    // Add JavaScript for side pane functionality
    html.push(format!(
        r#"
        <script>
        (function() {{
            const dfId = '{}';
            const serverUrl = '';
            const dataframeElement = document.getElementById('dataframe-' + dfId);
            const cells = dataframeElement ? dataframeElement.querySelectorAll('td') : [];
            const sidePane = document.getElementById('side-pane-' + dfId);
            const sidePaneTitle = document.getElementById('side-pane-title-' + dfId);
            const sidePaneContent = document.getElementById('side-pane-content-' + dfId);
            const closeButton = document.getElementById('close-button-' + dfId);
            const fullMatrix = dataframeElement ? JSON.parse(dataframeElement.getAttribute('data-full-matrix') || '[]') : [];
            let selectedCell = null;

            function closeSidePane(paneId) {{
                const pane = document.getElementById('side-pane-' + paneId);
                if (pane) {{
                    pane.classList.remove('visible');
                    if (selectedCell) {{
                        selectedCell.classList.remove('selected');
                        selectedCell = null;
                    }}
                }}
            }}

            function showSidePane(row, col, content) {{
                sidePaneTitle.textContent = 'Cell (' + row + ', ' + col + ')';
                sidePaneContent.innerHTML = content;
                sidePane.classList.add('visible');
            }}

            function showLoadingContent() {{
                sidePaneContent.innerHTML = '<div style="text-align:center; padding:20px;"><span style="font-style:italic">Loading full content...</span></div>';
            }}

            // Add event listener for close button
            if (closeButton) {{
                closeButton.addEventListener('click', function() {{
                    closeSidePane(dfId);
                }});
            }}

            cells.forEach((cell) => {{
                // Skip cells that do not have data-row and data-col attributes (e.g., ellipsis row)
                const rowAttr = cell.getAttribute('data-row');
                const colAttr = cell.getAttribute('data-col');
                if (rowAttr === null || colAttr === null) return;

                const row = parseInt(rowAttr);
                const col = parseInt(colAttr);
                cell.classList.add('clickable');

                cell.onclick = function() {{
                    // Remove selection from previously selected cell
                    if (selectedCell && selectedCell !== cell) {{
                        selectedCell.classList.remove('selected');
                    }}

                    // Toggle selection for current cell
                    if (selectedCell === cell) {{
                        cell.classList.remove('selected');
                        selectedCell = null;
                        closeSidePane(dfId);
                        return;
                    }} else {{
                        cell.classList.add('selected');
                        selectedCell = cell;
                    }}

                    // Show the side pane immediately
                    showSidePane(row, col, '');

                    // Set a timeout to show loading content after 1 second
                    const loadingTimeout = setTimeout(() => {{
                        showLoadingContent();
                    }}, 100);

                    // Use pre-rendered full content from embedded matrix (no network)
                    let content = (fullMatrix[row] && fullMatrix[row][col]) ? fullMatrix[row][col] : cell.innerHTML;
                    clearTimeout(loadingTimeout);
                    showSidePane(row, col, content);
                }};
            }});
        }})();
        </script>
        "#,
        df_id
    ));

    html.join("")
}

#[derive(Deserialize)]
struct GetDataframeCellParams {
    row: usize,
    col: usize,
}

#[derive(Serialize)]
pub(crate) struct CellResponse {
    value: String,
    data_type: String,
}

async fn get_dataframe_cell(
    State(state): State<Arc<DashboardState>>,
    Path(dataframe_id): Path<String>,
    Query(params): Query<GetDataframeCellParams>,
) -> Result<Json<CellResponse>, (StatusCode, String)> {
    let record_batch = state.get_dataframe_preview(&dataframe_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("DataFrame not found: {}", dataframe_id),
        )
    })?;

    if params.row >= record_batch.len() || params.col >= record_batch.num_columns() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Row or column index out of bounds".to_string(),
        ));
    }

    let column = record_batch.get_column(params.col);
    let cell_html = daft_recordbatch::html_value(column, params.row, false);

    Ok(Json(CellResponse {
        value: cell_html,
        data_type: format!("{:?}", column.data_type()),
    }))
}

async fn ping() -> StatusCode {
    StatusCode::NO_CONTENT
}

pub async fn launch_server(
    addr: IpAddr,
    port: u16,
    shutdown_fn: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let app = Router::new()
        .nest("/engine", engine::routes())
        .nest("/client", client::routes())
        .route("/api/ping", get(ping))
        // TODO: Replace with the query subscribers stuff
        .route(
            "/api/dataframes/{dataframe_id}/cell",
            get(get_dataframe_cell),
        )
        .merge(assets::routes())
        .layer(
            ServiceBuilder::new()
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                        .on_response(
                            DefaultOnResponse::new()
                                .level(Level::INFO)
                                .latency_unit(LatencyUnit::Micros),
                        ),
                )
                .layer(CorsLayer::very_permissive()),
        )
        .with_state(GLOBAL_DASHBOARD_STATE.clone());

    // Start the server
    let listener = TcpListener::bind((addr, port)).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_fn)
        .await
        .unwrap();

    Ok(())
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    const DAFT_DASHBOARD_ENV_NAME: &str = "DAFT_DASHBOARD";

    let module = PyModule::new(parent.py(), "dashboard")?;
    module.add_wrapped(wrap_pyfunction!(python::launch))?;
    module.add_wrapped(wrap_pyfunction!(python::register_dataframe_for_display))?;
    module.add_wrapped(wrap_pyfunction!(python::generate_interactive_html))?;
    module.add("DAFT_DASHBOARD_ENV_NAME", DAFT_DASHBOARD_ENV_NAME)?;
    parent.add_submodule(&module)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::*;
    use daft_recordbatch::RecordBatch;

    use super::generate_interactive_html;

    #[test]
    fn generates_full_matrix_and_js_markers() {
        let a = Int64Array::from(("A", vec![1, 2, 3])).into_series();
        let b = Utf8Array::from(("B", ["a", "b", "c"].as_slice())).into_series();
        let schema = Schema::new(vec![a.field().clone(), b.field().clone()]);
        let rb = RecordBatch::new_with_size(schema, vec![a, b], 3).unwrap();

        let html = generate_interactive_html(&rb, "df-1");

        assert!(html.contains("dashboard-container"));
        assert!(html.contains("side-pane-"));
        assert!(html.contains("showSidePane"));
        assert!(html.contains("serverUrl"));
        assert!(html.contains("<table class=\"dataframe\""));
        assert!(html.contains("data-full-matrix="));
        assert!(html.contains("\"1\""));
        assert!(html.contains("\"a\""));
    }
}
