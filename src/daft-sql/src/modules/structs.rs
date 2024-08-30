use super::SQLModule;
use crate::functions::SQLFunctions;

pub struct SQLModuleStructs;

impl SQLModule for SQLModuleStructs {
    fn register(_parent: &mut SQLFunctions) {
        // use FunctionExpr::Struct as f;
        // TODO
    }
}
