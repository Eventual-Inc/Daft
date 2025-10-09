mod fill_nan;
mod is_inf;
mod is_nan;
mod not_nan;

use daft_dsl::functions::{FunctionModule, FunctionRegistry};
use fill_nan::FillNan;
pub use fill_nan::fill_nan;
pub use is_inf::{IsInf, is_inf};
pub use is_nan::{IsNan, is_nan};
pub use not_nan::{NotNan, not_nan};

pub struct FloatFunctions;

impl FunctionModule for FloatFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(FillNan);
        parent.add_fn(IsInf);
        parent.add_fn(IsNan);
        parent.add_fn(NotNan);
    }
}
