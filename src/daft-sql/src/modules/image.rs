use super::SQLModule;
use crate::functions::SQLFunctions;

pub struct SQLModuleImage;

impl SQLModule for SQLModuleImage {
    fn register(_parent: &mut SQLFunctions) {
        // use FunctionExpr::Image as f;
        // TODO
    }
}
