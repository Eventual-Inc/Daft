use daft_logical_plan::LogicalPlanBuilder;

use crate::error;

/// A Table in a Data Catalog
/// 
/// TODO move to daft-table in later refactor
///
/// This is a trait because there are many different implementations of this, for example
/// Iceberg, DeltaLake, Hive and more.
pub trait Table {
    fn to_logical_plan_builder(&self) -> error::Result<LogicalPlanBuilder>;
}
