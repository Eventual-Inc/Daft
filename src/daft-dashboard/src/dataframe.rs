use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    Json,
};
use daft_recordbatch::RecordBatch;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::state::AppState;

// ----------------- Serving Cell Contents ----------------- //

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct CellResponse {
    value: String,
    data_type: String,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct GetCellParams {
    row: usize,
    col: usize,
}

/// Get the cell contents of the df output of a query
pub async fn get_query_dataframe(
    State(state): State<Arc<AppState>>,
    Path(query_id): Path<Arc<str>>,
    Query(params): Query<GetCellParams>,
) -> Result<Json<CellResponse>, (StatusCode, &'static str)> {
    let GetCellParams { row, col } = params;

    let rb = state
        .get_query_dataframe(query_id)
        .ok_or((StatusCode::NOT_FOUND, "Query not found"))?;

    if row >= rb.len() || col >= rb.num_columns() {
        return Err((StatusCode::BAD_REQUEST, "Row or column index out of bounds"));
    }

    let column = rb.get_column(col);
    let cell_html = daft_recordbatch::html_value(column, row, false);

    Ok(Json(CellResponse {
        value: cell_html,
        data_type: format!("{:?}", column.data_type()),
    }))
}

// ----------------- Serving Interactive HTML ----------------- //

#[allow(dead_code)]
pub fn generate_interactive_html(
    record_batch: &RecordBatch,
    df_id: &str,
    host: &str,
    port: u16,
) -> String {
    // Start with the basic table HTML from repr_html
    let table_html = record_batch.repr_html();
    // Build the complete interactive HTML with side pane layout
    let mut html = vec![r#"
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
    .to_string()];

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
