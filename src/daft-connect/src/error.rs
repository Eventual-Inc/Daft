use common_error::DaftError;
use snafu::Snafu;
use tonic::Status;

pub type ConnectResult<T, E = ConnectError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum ConnectError {
    #[snafu(display("Unsupported operation: {op}"))]
    UnsupportedOperation { op: String },
    #[snafu(display("Invalid argument: {arg}"))]
    InvalidArgument { arg: String },
    #[snafu(display(r#"Feature: {msg} is not yet implemented, please open an issue at https://github.com/Eventual-Inc/Daft/issues/new?assignees=&labels=enhancement%2Cneeds+triage&projects=&template=feature_request.yaml"#))]
    NotYetImplemented { msg: String },

    #[snafu(display("Daft error: {source}"))]
    DaftError { source: DaftError },

    #[snafu(display("Invalid Relation: {relation}"))]
    InvalidRelation { relation: String },

    #[snafu(display("Tonic error: {source}"))]
    TonicError { source: Status },

    #[snafu(display("Internal error: {msg}"))]
    InternalError { msg: String },
}

impl From<DaftError> for ConnectError {
    fn from(value: DaftError) -> Self {
        Self::InvalidArgument {
            arg: value.to_string(),
        }
    }
}

impl From<Status> for ConnectError {
    fn from(value: Status) -> Self {
        Self::TonicError { source: value }
    }
}

impl From<ConnectError> for Status {
    fn from(value: ConnectError) -> Self {
        match value {
            ConnectError::TonicError { source } => source,
            _ => Status::internal(value.to_string()),
        }
    }
}

impl<T> From<ConnectError> for Result<T, Status> {
    fn from(value: ConnectError) -> Self {
        Err(value.into())
    }
}

impl<T> From<ConnectError> for Result<T, ConnectError> {
    fn from(value: ConnectError) -> Self {
        Err(value)
    }
}

impl<T> From<ConnectError> for Result<T, eyre::Error> {
    fn from(value: ConnectError) -> Self {
        eyre::Result::Err(eyre::eyre!(value))
    }
}

impl From<pyo3::PyErr> for ConnectError {
    fn from(value: pyo3::PyErr) -> Self {
        Self::InternalError {
            msg: value.to_string(),
        }
    }
}

impl ConnectError {
    pub fn invalid_relation<S: Into<String>>(relation: S) -> Self {
        Self::InvalidRelation {
            relation: relation.into(),
        }
    }

    pub fn unsupported_operation<S: Into<String>>(op: S) -> Self {
        Self::UnsupportedOperation { op: op.into() }
    }

    pub fn invalid_argument<S: Into<String>>(arg: S) -> Self {
        Self::InvalidArgument { arg: arg.into() }
    }
    pub fn internal<S: Into<String>>(msg: S) -> Self {
        Self::InternalError { msg: msg.into() }
    }
}

#[macro_export]
macro_rules! invalid_argument_err {
    ($($arg:tt)*)  => {{
        let msg = format!($($arg)*);
        return $crate::error::ConnectError::invalid_argument(msg).into();
    }};
}

#[macro_export]
macro_rules! not_yet_implemented {
    ($($arg:tt)*)  => {{
        let msg = format!($($arg)*);
        return $crate::error::ConnectError::NotYetImplemented { msg }.into();
    }};
}

// not found
#[macro_export]
macro_rules! not_found_err {
    ($arg: tt) => {{
        let msg = format!($arg);
        Err(::tonic::Status::not_found(msg))
    }};
}

#[macro_export]
macro_rules! internal_err {
    ($arg: tt) => {{
        let msg = format!($arg);
        let msg = textwrap::wrap(&msg, 120).join("\n");
        return $crate::error::ConnectError::internal(msg).into();
    }};
}

#[macro_export]
macro_rules! ensure {
    ($condition:expr, $($arg:tt)*) => {
        if !$condition {
            return Err($crate::error::ConnectError::invalid_argument(format!($($arg)*)))
        }
    };
}

pub trait Context<T> {
    fn wrap_err(self, msg: &str) -> ConnectResult<T>;
}

impl<T, E> Context<T> for Result<T, E>
where
    E: std::error::Error + std::fmt::Debug,
{
    fn wrap_err(self, msg: &str) -> ConnectResult<T> {
        self.map_err(|e| ConnectError::InternalError {
            msg: format!("{}: {}", msg, e),
        })
    }
}
