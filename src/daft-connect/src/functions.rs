use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use spark_connect::Expression;

use crate::{
    error::ConnectResult, invalid_argument_err, spark_analyzer::expr_analyzer::analyze_expr,
};
mod aggregate;
mod core;
mod datetime;

mod math;
mod partition_transform;
mod string;

pub(crate) static CONNECT_FUNCTIONS: LazyLock<SparkFunctions> = LazyLock::new(|| {
    let mut functions = SparkFunctions::new();
    functions.register::<aggregate::AggregateFunctions>();
    functions.register::<core::CoreFunctions>();
    functions.register::<datetime::DatetimeFunctions>();
    functions.register::<math::MathFunctions>();
    functions.register::<partition_transform::PartitionTransformFunctions>();
    functions.register::<string::StringFunctions>();
    functions
});

pub trait SparkFunction: Send + Sync {
    fn to_expr(&self, args: &[Expression]) -> ConnectResult<daft_dsl::ExprRef>;
}

pub struct SparkFunctions {
    pub(crate) map: HashMap<String, Arc<dyn SparkFunction>>,
}

impl SparkFunctions {
    /// Create a new [SparkFunction] instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Register the module to the [SparkFunctions] instance.
    pub fn register<M: FunctionModule>(&mut self) {
        M::register(self);
    }
    /// Add a [FunctionExpr] to the [SparkFunction] instance.
    pub fn add_fn<F: SparkFunction + 'static>(&mut self, name: &str, func: F) {
        self.map.insert(name.to_string(), Arc::new(func));
    }

    pub fn add_todo_fn(&mut self, name: &'static str) {
        self.map
            .insert(name.to_string(), Arc::new(TodoFunction(name)));
    }

    /// Get a function by name from the [SparkFunctions] instance.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Arc<dyn SparkFunction>> {
        self.map.get(name)
    }
}

pub trait FunctionModule {
    /// Register this module to the given [SparkFunctions] table.
    fn register(_parent: &mut SparkFunctions);
}

struct UnaryFunction(fn(ExprRef) -> ExprRef);
struct BinaryFunction(fn(ExprRef, ExprRef) -> ExprRef);

impl<T> SparkFunction for T
where
    T: ScalarUDF + 'static + Clone,
{
    fn to_expr(&self, args: &[Expression]) -> ConnectResult<daft_dsl::ExprRef> {
        let sf = ScalarFunction::new(
            self.clone(),
            args.iter()
                .map(analyze_expr)
                .collect::<ConnectResult<Vec<_>>>()?,
        );
        Ok(sf.into())
    }
}

impl SparkFunction for UnaryFunction {
    fn to_expr(&self, args: &[Expression]) -> ConnectResult<daft_dsl::ExprRef> {
        match args {
            [arg] => {
                let arg = analyze_expr(arg)?;
                Ok(self.0(arg))
            }
            _ => invalid_argument_err!("requires exactly one argument"),
        }
    }
}

impl SparkFunction for BinaryFunction {
    fn to_expr(&self, args: &[Expression]) -> ConnectResult<daft_dsl::ExprRef> {
        match args {
            [arg, arg2] => {
                let arg = analyze_expr(arg)?;
                let arg2 = analyze_expr(arg2)?;
                Ok(self.0(arg, arg2))
            }
            _ => invalid_argument_err!("requires exactly two arguments"),
        }
    }
}

struct TodoFunction(&'static str);

impl SparkFunction for TodoFunction {
    fn to_expr(&self, _args: &[Expression]) -> ConnectResult<daft_dsl::ExprRef> {
        invalid_argument_err!("Function '{}' not implemented", self.0)
    }
}
