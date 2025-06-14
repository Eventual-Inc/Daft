#[macro_use]
pub mod rel;
pub mod rex;
pub mod schema;

use std::sync::Arc;

use thiserror::Error;

/// This trait defines the from/to protobuf message methods.
pub trait FromToProto {
    /// This rust type's corresponding protobuf message type.
    type Message: prost::Message + Default;

    /// Convert a message to this rust type.
    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized;

    /// Converts an instance of this type to a protobuf message.
    fn to_proto(&self) -> ProtoResult<Self::Message>;
}

/// This enables calling like `let p: P = from_proto(m)` dealing with prost's optionals.
pub(crate) fn from_proto<T, M>(message: Option<M>) -> ProtoResult<T>
where
    T: FromToProto<Message = M>,
    M: prost::Message + Default,
{
    T::from_proto(message.expect("expected non-null!"))
}

/// Helper for dealing with recursive deps and boxes which become boxed types themselves.
pub(crate) fn from_proto_box<P, M>(message: Option<Box<M>>) -> ProtoResult<Box<P>>
where
    P: FromToProto<Message = M>,
    M: prost::Message + Default + ToOwned + Clone,
{
    let message = message.expect("expected non-null!");
    let proto = P::from_proto(*message)?;
    Ok(Box::new(proto))
}

/// Helper for dealing with recursive deps and boxes which become arc types in daft.
pub(crate) fn from_proto_arc<P, M>(message: Option<Box<M>>) -> ProtoResult<Arc<P>>
where
    P: FromToProto<Message = M>,
    M: prost::Message + Default + ToOwned + Clone,
{
    let message = message.expect("expected non-null!");
    let proto = P::from_proto(*message)?;
    Ok(Arc::new(proto))
}

/// This macro creates a ProtoError::FromProto.
#[macro_export]
macro_rules! from_proto_err {
    ($($arg:tt)*) => {
        return Err($crate::proto::ProtoError::FromProto(format!($($arg)*)))
    };
}

/// This macro creates an ProtoError::ToProto.
#[macro_export]
macro_rules! to_proto_err {
    ($($arg:tt)*) => {
        return Err($crate::proto::ProtoError::ToProto(format!($($arg)*)))
    };
}

/// This macro creates an ProtoError::Unsuppported.
#[macro_export]
macro_rules! unsupported_err {
    ($($arg:tt)*) => {
        return Err($crate::proto::ProtoError::Unsupported(format!($($arg)*)))
    };
}

/// The daft_proto conversion error.
#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("ProtoError::FromProto {0}")]
    FromProto(String),
    #[error("ProtoError::ToProto {0}")]
    ToProto(String),
    #[error("ProtoError::Unsupported {0}")]
    Unsupported(String),
}

/// The daft_proto result type.
pub type ProtoResult<T> = std::result::Result<T, ProtoError>;
