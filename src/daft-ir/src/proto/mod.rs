#[macro_use]
pub mod rex;
pub mod functions;
pub mod rel;
pub mod schema;

use std::sync::Arc;

use thiserror::Error;

/// The unit type which is used in many type definitions.
pub(crate) const UNIT: daft_proto::protos::daft::v1::Unit = daft_proto::protos::daft::v1::Unit {};

/// This trait defines the from/to protobuf message methods.
pub trait ToFromProto {
    /// This rust type's corresponding protobuf message type (M)
    type Message: prost::Message;

    /// Convert a message to this rust type.
    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized;

    /// Converts an instance of this type to a protobuf message.
    fn to_proto(&self) -> ProtoResult<Self::Message>;
}

/// Blank impl for converting to/from arc'd types.
impl<T> ToFromProto for Arc<T>
where
    T: ToFromProto,
{
    type Message = T::Message;

    /// Convert a message to this rust type.
    fn from_proto(message: Self::Message) -> ProtoResult<Self>
    where
        Self: Sized,
    {
        Ok(Self::new(T::from_proto(message)?))
    }

    /// Converts an instance of this type to a protobuf message.
    fn to_proto(&self) -> ProtoResult<Self::Message> {
        self.as_ref().to_proto()
    }
}

/// Macro to assert that an Option<T> is not None, returning a ProtoError::FromProto if it is.
#[macro_export]
macro_rules! non_null {
    ($opt:expr) => {
        match $opt {
            Some(val) => val,
            None => {
                return Err($crate::proto::ProtoError::FromProto(format!(
                    "Expected field to be non-null"
                )))
            }
        }
    };
}

// todo(conner): better helpers and names
// Helper to convert a list of proto expressions to new daft ir expressions.
pub(crate) fn from_protos(
    exprs: Vec<daft_proto::protos::daft::v1::Expr>,
) -> ProtoResult<Vec<crate::Expr>> {
    exprs.into_iter().map(crate::Expr::from_proto).collect()
}

// todo(conner): better helpers and names
// Helper to convert a list of ExprRef (typically in the ir) to proto expressions.
pub(crate) fn to_protos(
    exprs: &[crate::ExprRef],
) -> ProtoResult<Vec<daft_proto::protos::daft::v1::Expr>> {
    exprs.iter().map(|e| e.to_proto()).collect()
}

/// Maps some daft-ir Vec<T> into a Vec<M> without taking ownership.
pub(crate) fn to_proto_vec<'a, I, T, M>(iter: I) -> ProtoResult<Vec<M>>
where
    I: IntoIterator<Item = &'a T>,
    T: ToFromProto<Message = M> + 'a,
    M: prost::Message + Default,
{
    iter.into_iter().map(|t| t.to_proto()).collect()
}

/// Maps an iterator of protobuf messages into a Vec<T> where T implements ToFromProto.
pub(crate) fn from_proto_vec<I, T, M>(iter: I) -> ProtoResult<Vec<T>>
where
    I: IntoIterator<Item = M>,
    T: ToFromProto<Message = M>,
    M: prost::Message + Default,
{
    iter.into_iter().map(|m| T::from_proto(m)).collect()
}

/// This enables calling like `let p: P = from_proto(m)` dealing with prost's optionals.
pub(crate) fn from_proto<T, M>(message: Option<M>) -> ProtoResult<T>
where
    T: ToFromProto<Message = M>,
    M: prost::Message + Default,
{
    T::from_proto(message.expect("expected non-null!"))
}

/// Helper for dealing with recursive deps and boxes which become boxed types themselves.
pub(crate) fn from_proto_box<P, M>(message: Option<Box<M>>) -> ProtoResult<Box<P>>
where
    P: ToFromProto<Message = M>,
    M: prost::Message + Default + ToOwned + Clone,
{
    let message = message.expect("expected non-null!");
    let proto = P::from_proto(*message)?;
    Ok(Box::new(proto))
}

/// Helper for dealing with recursive deps and boxes which become arc types in daft.
pub(crate) fn from_proto_arc<P, M>(message: Option<Box<M>>) -> ProtoResult<Arc<P>>
where
    P: ToFromProto<Message = M>,
    M: prost::Message + Default + ToOwned + Clone,
{
    let message = message.expect("expected non-null!");
    let proto = P::from_proto(*message)?;
    Ok(Arc::new(proto))
}

/// The daft_proto result type.
pub type ProtoResult<T> = std::result::Result<T, ProtoError>;

/// The daft_proto conversion error.
#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("ProtoError::FromProto({0})")]
    FromProto(String),
    #[error("ProtoError::ToProto({0})")]
    ToProto(String),
    #[error("ProtoError::NotImplemented({0})")]
    NotImplemented(String),
    #[error("ProtoError::NotOptimized({0})")]
    NotOptimized(String),
    #[error("ProtoError::Bincode({0})")]
    Bincode(#[from] Box<bincode::ErrorKind>),
    #[error("ProtoError::DaftError({0}")]
    Daft(#[from] common_error::DaftError),
}

#[macro_export]
macro_rules! from_proto_err {
    ($($arg:tt)*) => {
        return Err($crate::proto::ProtoError::FromProto(format!($($arg)*)))
    };
}

#[macro_export]
macro_rules! to_proto_err {
    ($($arg:tt)*) => {
        return Err($crate::proto::ProtoError::ToProto(format!($($arg)*)))
    };
}

#[macro_export]
macro_rules! not_implemented_err {
    ($($arg:tt)*) => {
        return Err($crate::proto::ProtoError::NotImplemented(format!($($arg)*)))
    };
}

#[macro_export]
macro_rules! not_optimized_err {
    ($($arg:tt)*) => {
        return Err($crate::proto::ProtoError::NotOptimized(format!($($arg)*)))
    };
}
