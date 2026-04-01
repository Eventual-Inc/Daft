mod int;

use daft_dsl::functions::FunctionModule;
use int::RandomIntFunction;
pub use int::random_int_expr;

pub struct RandomFunctions;

impl FunctionModule for RandomFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(RandomIntFunction);
    }
}
