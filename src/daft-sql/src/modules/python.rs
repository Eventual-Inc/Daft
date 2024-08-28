use super::SQLModule;
use crate::functions::SQLFunctions;

pub struct SQLModulePython;

impl SQLModule for SQLModulePython {
    fn register(_parent: &mut SQLFunctions) {
        // use FunctionExpr::Python as f;
        // TODO
    }
}
