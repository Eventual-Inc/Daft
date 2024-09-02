use super::SQLModule;
use crate::functions::SQLFunctions;

pub struct SQLModuleTemporal;

impl SQLModule for SQLModuleTemporal {
    fn register(_parent: &mut SQLFunctions) {
        // use FunctionExpr::Temporal as f;
        // TODO
    }
}
