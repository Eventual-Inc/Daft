use super::SQLModule;
use crate::functions::SQLFunctions;

pub struct SQLModuleJson;

impl SQLModule for SQLModuleJson {
    fn register(_parent: &mut SQLFunctions) {
        // use FunctionExpr::Json as f;
        // TODO
    }
}
