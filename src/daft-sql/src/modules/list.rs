use super::SQLModule;
use crate::functions::SQLFunctions;

pub struct SQLModuleList;

impl SQLModule for SQLModuleList {
    fn register(_parent: &mut SQLFunctions) {
        // use FunctionExpr::List as f;
        // TODO
    }
}
