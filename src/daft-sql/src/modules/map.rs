use super::SQLModule;
use crate::functions::SQLFunctions;

pub struct SQLModuleMap;

impl SQLModule for SQLModuleMap {
    fn register(_parent: &mut SQLFunctions) {
        // use FunctionExpr::Map as f;
        // TODO
    }
}
