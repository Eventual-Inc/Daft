//! Relation handling for Spark Connect protocol.
//!
//! A Relation represents a structured dataset or transformation in Spark Connect.
//! It can be either a base relation (direct data source) or derived relation
//! (result of operations on other relations).
//!
//! The protocol represents relations as trees of operations where:
//! - Each node is a Relation with metadata and an operation type
//! - Operations can reference other relations, forming a DAG
//! - The tree describes how to derive the final result
//!
//! Example flow for: SELECT age, COUNT(*) FROM employees WHERE dept='Eng' GROUP BY age
//!
//! ```text
//! Aggregate (grouping by age)
//!   ↳ Filter (department = 'Engineering')
//!       ↳ Read (employees table)
//! ```
//!
//! Relations abstract away:
//! - Physical storage details
//! - Distributed computation
//! - Query optimization
//! - Data source specifics
//!
//! This allows Spark to optimize and execute queries efficiently across a cluster
//! while providing a consistent API regardless of the underlying data source.
//! ```mermaid
//!
//! ```
use eyre::{eyre, Context};
use spark_connect::{relation::RelType, Relation};
use tracing::trace;

use crate::{command::ConcreteDataChannel, convert::formatting::RelTypeExt};

mod show_string;
use show_string::show_string;

mod range;
use range::range;

pub fn convert_data(plan: Relation, encoder: &mut impl ConcreteDataChannel) -> eyre::Result<()> {
    // First check common fields if needed
    if let Some(common) = &plan.common {
        // contains metadata shared across all relation types
        // Log or handle common fields if necessary
        trace!("Processing relation with plan_id: {:?}", common.plan_id);
    }

    let rel_type = plan.rel_type.ok_or_else(|| eyre!("rel_type is None"))?;

    match rel_type {
        RelType::ShowString(input) => show_string(*input, encoder).wrap_err("parsing ShowString"),
        RelType::Range(input) => range(input, encoder).wrap_err("parsing Range"),
        other => Err(eyre!("Unsupported top-level relation: {}", other.name())),
    }
}
