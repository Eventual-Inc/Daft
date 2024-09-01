use super::SQLModule;
use crate::functions::SQLFunctions;

pub struct SQLModulePartitioning;

impl SQLModule for SQLModulePartitioning {
    fn register(_parent: &mut SQLFunctions) {
        // use FunctionExpr::Partitioning as f;
        // TODO
    }
}
