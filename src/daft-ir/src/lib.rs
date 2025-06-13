//! This organizes daft's ir via re-exports.
pub use daft_logical_plan as rel;
pub use daft_dsl as rex;
pub use daft_functions as functions;

/// Daft's schema types for its IR.
pub mod schema {
    pub use daft_schema;
    pub use daft_schema::schema::*;
    pub use daft_schema::dtype::*;
    pub use daft_schema::field::*;
}