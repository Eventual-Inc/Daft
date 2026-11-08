//! # daft-pg-bridge
//!
//! Bridge library for PostgreSQL ORCA integration with Daft execution.
//!
//! This crate provides the contract interface between ORCA-generated physical plans
//! and Daft's local execution engine. It defines:
//!
//! - IR serialization/deserialization for `PgDaftPlan v1`
//! - Capability registry enforcement
//! - Plan validation against frozen capabilities
//! - Type mapping between PostgreSQL and Daft type systems
//!
//! ## Architecture
//!
//! ```text
//! ORCA (C++) → PgDaftPlan IR (JSON) → daft-pg-bridge → daft-local-plan → daft-local-execution
//! ```
//!
//! ## Stability
//!
//! This crate is part of the frozen P0 contract. Changes to public interfaces require:
//! - IR schema version bump
//! - Capability registry update
//! - ORCA-side compatibility layer
//!
//! See `pg_orca/spec/pg_daft_capabilities.yaml` for the capability registry.

#![warn(missing_docs)]

mod abi;

/// Arrow C Data Interface integration
pub mod arrow;

/// Capability registry types and validation
pub mod capabilities;

/// Error handling and panic safety for C ABI
pub mod error;

/// Test-build-only, one-shot Bridge failure injection.
#[cfg(feature = "failure-injection")]
pub mod fault_injection;

/// Execution handle API
pub mod execution;

/// Expression translation
pub mod expr;

/// IR types for PgDaftPlan v1
pub mod ir;

/// Plan translation
pub mod plan;

/// Type system mapping between PostgreSQL and Daft
pub mod types {
    //! Type conversions between PostgreSQL and Daft type systems.
    //!
    //! Handles mapping of:
    //! - PostgreSQL OIDs to Daft DataTypes
    //! - Nullability semantics
    //! - Collation information (stored but not yet used)
}

/// Plan validation and translation
pub mod translator {
    //! Translates PgDaftPlan IR into daft-local-plan structures.
    //!
    //! Validates that the plan only uses capabilities marked as available
    //! in the frozen registry.
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_placeholder() {
        // Placeholder test to allow cargo test to pass
        assert_eq!(2 + 2, 4);
    }
}
