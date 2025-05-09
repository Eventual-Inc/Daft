mod fill_nan;
mod is_inf;
mod is_nan;
mod not_nan;

pub use fill_nan::fill_nan;
use fill_nan::FillNan;
pub use is_inf::{is_inf, IsInf};
pub use is_nan::{is_nan, IsNan};
pub use not_nan::{not_nan, NotNan};

use crate::FunctionModule;

pub struct FloatFunctions;

impl FunctionModule for FloatFunctions {
    fn register(parent: &mut crate::FunctionRegistry) {
        parent.add_fn(FillNan);
        parent.add_fn(IsInf);
        parent.add_fn(IsNan);
        parent.add_fn(NotNan);
    }
}
