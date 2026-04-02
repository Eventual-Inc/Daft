use snafu::Snafu;

pub type OnnxPlannerResult<T> = Result<T, OnnxPlannerError>;

#[derive(Debug, Snafu)]
pub enum OnnxPlannerError {
    #[snafu(display("Unsupported ONNX op: {op_type}"))]
    UnsupportedOp { op_type: String },

    #[snafu(display("Missing input: node '{node}' references '{input}' which is not in the graph"))]
    MissingInput { node: String, input: String },

    #[snafu(display("Invalid graph: {message}"))]
    InvalidGraph { message: String },

    #[snafu(display("Daft error: {source}"))]
    DaftError { source: common_error::DaftError },
}

impl From<common_error::DaftError> for OnnxPlannerError {
    fn from(source: common_error::DaftError) -> Self {
        Self::DaftError { source }
    }
}
