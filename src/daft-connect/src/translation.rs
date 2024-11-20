//! Translation between Spark Connect and Daft

mod logical_plan;
mod schema;

pub use logical_plan::to_logical_plan;
pub use schema::relation_to_schema;
