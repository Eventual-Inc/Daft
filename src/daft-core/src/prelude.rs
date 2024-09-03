//! Prelude for Daft core
//!
//! This module re-exports commonly used items from the Daft core library.

// Re-export core data structures
pub use crate::series::Series;

// Re-export common data types and arrays
pub use crate::datatypes::prelude::*;

// Re-export other array types
pub use crate::array::{ListArray, StructArray};

// Re-export count mode enum
pub use crate::count_mode::CountMode;

// Re-export join-related types
pub use crate::join::{JoinStrategy, JoinType};

// Re-export version information
pub use crate::DAFT_BUILD_TYPE;
pub use crate::VERSION;

// You might want to include a glob import for users who want everything
pub mod all {
    pub use super::*;
}
