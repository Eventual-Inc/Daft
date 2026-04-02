use std::path::Path;

use crate::error::{OnnxPlannerError, OnnxPlannerResult};

pub use onnx_rs::ast;

/// Load raw bytes from an ONNX file on disk.
///
/// The caller then passes the bytes to [`parse_model`] to get a parsed model.
/// The bytes must outlive the returned [`ast::Model`] since it borrows from them.
pub fn load_onnx_bytes(path: impl AsRef<Path>) -> OnnxPlannerResult<Vec<u8>> {
    std::fs::read(path.as_ref()).map_err(|e| OnnxPlannerError::InvalidGraph {
        message: format!("failed to read ONNX file: {e}"),
    })
}

/// Parse an ONNX model from a byte slice.
pub fn parse_model<'a>(bytes: &'a [u8]) -> OnnxPlannerResult<ast::Model<'a>> {
    onnx_rs::parse(bytes).map_err(|e| OnnxPlannerError::InvalidGraph {
        message: format!("failed to parse ONNX protobuf: {e}"),
    })
}
