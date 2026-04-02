use std::sync::Arc;

use common_io_config::IOConfig;
use daft_io::{IOStatsContext, get_io_client};

use crate::error::{OnnxPlannerError, OnnxPlannerResult};

pub use onnx_rs::ast;

/// Load raw bytes from an ONNX model file.
///
/// Supports local paths, S3, GCS, Azure, and any URI that Daft's IO client handles.
pub fn load_onnx_bytes(uri: &str, io_config: Option<&IOConfig>) -> OnnxPlannerResult<Vec<u8>> {
    let io_config = Arc::new(io_config.cloned().unwrap_or_default());
    let io_client = get_io_client(true, io_config).map_err(|e| OnnxPlannerError::InvalidGraph {
        message: format!("failed to create IO client: {e}"),
    })?;
    let io_stats = IOStatsContext::new("onnx_model_load");

    let runtime = common_runtime::get_io_runtime(true);
    let bytes = runtime
        .block_on_current_thread(async {
            let result = io_client
                .single_url_get(uri.to_string(), None, Some(io_stats))
                .await?;
            result.bytes().await
        })
        .map_err(|e| OnnxPlannerError::InvalidGraph {
            message: format!("failed to read ONNX file '{}': {e}", uri),
        })?;

    Ok(bytes.to_vec())
}

/// Parse an ONNX model from a byte slice.
pub fn parse_model<'a>(bytes: &'a [u8]) -> OnnxPlannerResult<ast::Model<'a>> {
    onnx_rs::parse(bytes).map_err(|e| OnnxPlannerError::InvalidGraph {
        message: format!("failed to parse ONNX protobuf: {e}"),
    })
}
