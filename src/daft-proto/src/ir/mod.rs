mod rex;

use common_error::DaftResult;

/// This trait defines the from/to protobuf messages.
pub trait Proto {
    /// This rust type's corresponding protobuf message type.
    type Message: prost::Message + Default;

    /// Convert a message to this rust type.
    fn from_proto(message: Self::Message) -> DaftResult<Self>
    where
        Self: Sized;

    /// Converts an instance of this type to a protobuf message.
    fn to_proto(&self) -> DaftResult<Self::Message>;
}
