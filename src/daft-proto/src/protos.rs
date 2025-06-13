use thiserror::Error;

//
// !! PLEASE ENSURE THE MODULE STRUCTURE FOLLOWS PROTO DIR !!
//

pub mod echo {
    tonic::include_proto!("echo");
}

pub mod schema {
    tonic::include_proto!("schema");
}

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
pub(crate) fn from_proto<P, M>(message: impl Into<Option<M>>) -> ProtoResult<P> 
where
    P: FromToProto<Message = M>,
    M: prost::Message + Default
{
    P::from_proto(message.into().expect("expected non-null!"))
}

/// This macro creates a FromProtoError.
#[macro_export]
macro_rules! from_proto_err {
    ($($arg:tt)*) => {
        return Err($crate::proto::ProtoError::FromProtoError(format!($($arg)*)))
    };
}

/// This macro creates a ToProtoError.
#[macro_export]
macro_rules! to_proto_err {
    ($($arg:tt)*) => {
        return Err($crate::protos::ProtoError::ToProtoError(format!($($arg)*)))
    };
}

/// The daft_proto conversion error.
#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("ProtoError::FromProtoError {0}")]
    FromProtoError(String),
    #[error("ProtoError::ToProtoError {0}")]
    ToProtoError(String),
}

/// The daft_proto result type.
pub type ProtoResult<T> = std::result::Result<T, ProtoError>;


impl From<prost::DecodeError> for ProtoError {
    fn from(value: prost::DecodeError) -> Self {
        todo!()
    }
}

impl From<prost::EncodeError> for ProtoError {
    fn from(value: prost::EncodeError) -> Self {
        todo!()
    }
}
