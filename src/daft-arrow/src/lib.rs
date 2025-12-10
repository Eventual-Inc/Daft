pub mod array;
pub mod ord;

// Re-export arrow2::* modules for centralized access
pub use arrow2::{
    chunk, compute, datatypes, error, ffi, io, offset, scalar, temporal_conversions,
    trusted_len,
};

pub mod buffer {
    pub use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, NullBuffer, NullBufferBuilder};
    pub use arrow2::buffer::*;

    /// Convert an arrow_buffer::buffer::NullBuffer to an arrow2::bitmap::Bitmap.
    ///
    /// This is a wrapper around arrow2::bitmap::Bitmap::from_null_buffer
    /// so we can easily replace it with a no-op in the future.
    pub fn from_null_buffer(value: arrow_buffer::buffer::NullBuffer) -> arrow2::bitmap::Bitmap {
        arrow2::bitmap::Bitmap::from_null_buffer(value)
    }

    /// Convert an Option<arrow_buffer::buffer::NullBuffer> to an Option<arrow2::bitmap::Bitmap>.
    ///
    /// This is a wrapper around arrow2::bitmap::Bitmap::from_null_buffer
    /// so we can easily replace it with a no-op in the future.
    pub fn wrap_null_buffer(
        value: Option<arrow_buffer::buffer::NullBuffer>,
    ) -> Option<arrow2::bitmap::Bitmap> {
        value.map(arrow2::bitmap::Bitmap::from_null_buffer)
    }
}

/// Explicitly isolate old arrow2::bitmap code to quickly remove in future
pub mod bitmap {
    // Uses come from arrow2::array::BooleanArray values buffer
    // And direct access to arrow2::array::Array objects
    pub use arrow2::bitmap::*;
}

pub mod types {
    pub use arrow_array::types::*;

    pub use arrow_buffer::ArrowNativeType;
}
