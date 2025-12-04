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

fn generate_interactive_html(
    record_batch: &RecordBatch,
    df_id: &str,
    host: &str,
    port: u16,
) -> String {
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

    // Add the table HTML with ID
    html.push(format!(
        r#"<div id="dataframe-{}">{}</div>"#,
        df_id, table_html
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
            const serverUrl = 'http://{}:{}';
            const dfId = '{}';
            const dataframeElement = document.getElementById('dataframe-' + dfId);
            const cells = dataframeElement ? dataframeElement.querySelectorAll('td') : [];
            const sidePane = document.getElementById('side-pane-' + dfId);
            const sidePaneTitle = document.getElementById('side-pane-title-' + dfId);
            const sidePaneContent = document.getElementById('side-pane-content-' + dfId);
            const closeButton = document.getElementById('close-button-' + dfId);
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

                    // Fetch the cell content
                    fetch(serverUrl + '/api/dataframes/' + dfId + '/cell?row=' + row + '&col=' + col)
                        .then(response => response.json())
                        .then(data => {{
                            clearTimeout(loadingTimeout);
                            showSidePane(row, col, data.value);
                        }})
                        .catch(err => {{
                            clearTimeout(loadingTimeout);
                            // Get the original cell content from the table
                            const cell = selectedCell;
                            if (cell) {{
                                const originalContent = cell.innerHTML;
                                showSidePane(row, col, originalContent);
                            }}
                        }});
                }};
            }});
        }})();
        </script>
        "#,
        host, port, df_id
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
