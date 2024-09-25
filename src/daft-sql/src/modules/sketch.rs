use super::SQLModule;
use crate::functions::SQLFunctions;

pub struct SQLModuleSketch;

impl SQLModule for SQLModuleSketch {
    fn register(_parent: &mut SQLFunctions) {
        // use FunctionExpr::Sketch as f;
        // TODO
    }
}
