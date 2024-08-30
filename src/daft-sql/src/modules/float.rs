use super::SQLModule;
use crate::functions::SQLFunctions;

pub struct SQLModuleFloat;

impl SQLModule for SQLModuleFloat {
    fn register(_parent: &mut SQLFunctions) {
        // use FunctionExpr::Float as f;
        // TODO
    }
}
