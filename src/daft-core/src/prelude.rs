//! Prelude for Daft core
//!
//! This module re-exports commonly used items from the Daft core library.

// Re-export arrow2 bitmap
pub use arrow2::bitmap;
// Re-export core series structures
pub use daft_schema::schema::{Schema, SchemaRef};

// Re-export count mode enum
pub use crate::count_mode::CountMode;
// Re-export common data types and arrays
pub use crate::datatypes::prelude::*;
// Re-export join-related types
pub use crate::join::{JoinStrategy, JoinType};
pub use crate::{
    array::prelude::*,
    series::{IntoSeries, Series},
};

// You might want to include a glob import for users who want everything
pub mod all {
    pub use super::*;
}
