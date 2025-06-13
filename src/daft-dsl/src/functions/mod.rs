pub mod agg;
pub mod function_args;
#[cfg(test)]
mod macro_tests;
pub mod map;
pub mod partitioning;
pub mod prelude;
pub mod python;
pub mod scalar;
pub mod sketch;
pub mod struct_;

use std::{
    collections::HashMap,
    fmt::{Display, Formatter, Result, Write},
    hash::Hash,
    sync::{Arc, LazyLock, RwLock},
};

use common_error::DaftResult;
use daft_core::prelude::*;
pub use function_args::{FunctionArg, FunctionArgs, UnaryArg};
use python::PythonUDF;
use scalar::DynamicScalarFunction;
pub use scalar::{ScalarFunction, ScalarFunctionFactory, ScalarUDF};
use serde::{Deserialize, Serialize};

use self::{map::MapExpr, partitioning::PartitioningExpr, sketch::SketchExpr, struct_::StructExpr};
use crate::ExprRef;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FunctionExpr {
    Map(MapExpr),
    Sketch(SketchExpr),
    Struct(StructExpr),
    Python(PythonUDF),
    Partitioning(PartitioningExpr),
}

pub trait FunctionEvaluator {
    fn fn_name(&self) -> &'static str;
    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        expr: &FunctionExpr,
    ) -> DaftResult<Field>;
    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series>;
}

impl FunctionExpr {
    #[inline]
    fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        match self {
            Self::Map(expr) => expr.get_evaluator(),
            Self::Sketch(expr) => expr.get_evaluator(),
            Self::Struct(expr) => expr.get_evaluator(),
            Self::Python(expr) => expr,
            Self::Partitioning(expr) => expr.get_evaluator(),
        }
    }
}

impl Display for FunctionExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.fn_name())
    }
}

impl FunctionEvaluator for FunctionExpr {
    fn fn_name(&self) -> &'static str {
        self.get_evaluator().fn_name()
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        expr: &FunctionExpr,
    ) -> DaftResult<Field> {
        self.get_evaluator().to_field(inputs, schema, expr)
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        self.get_evaluator().evaluate(inputs, expr)
    }
}

pub fn function_display(f: &mut Formatter, func: &FunctionExpr, inputs: &[ExprRef]) -> Result {
    write!(f, "{}(", func)?;
    for (i, input) in inputs.iter().enumerate() {
        if i != 0 {
            write!(f, ", ")?;
        }
        write!(f, "{input}")?;
    }
    write!(f, ")")?;
    Ok(())
}

pub fn function_display_without_formatter(
    func: &FunctionExpr,
    inputs: &[ExprRef],
) -> std::result::Result<String, std::fmt::Error> {
    let mut f = String::default();
    write!(&mut f, "{}(", func)?;
    for (i, input) in inputs.iter().enumerate() {
        if i != 0 {
            write!(&mut f, ", ")?;
        }
        write!(&mut f, "{input}")?;
    }
    write!(&mut f, ")")?;
    Ok(f)
}

pub fn function_semantic_id(func: &FunctionExpr, inputs: &[ExprRef], schema: &Schema) -> FieldID {
    let inputs = inputs
        .iter()
        .map(|expr| expr.semantic_id(schema).id.to_string())
        .collect::<Vec<String>>()
        .join(", ");
    // TODO: check for function idempotency here.
    FieldID::new(format!("Function_{func:?}({inputs})"))
}

/// FunctionRegistry is a lookup structure for scalar functions.
#[derive(Default)]
pub struct FunctionRegistry {
    // Todo: Use the Bindings object instead, so we can get aliases and case handling.
    map: HashMap<String, Arc<dyn ScalarFunctionFactory>>,
}

/// FunctionModule is a mechanism to group and register scalar functions.
pub trait FunctionModule {
    /// Register this module to the given [SQLFunctions] table.
    fn register(_parent: &mut FunctionRegistry);
}

impl FunctionRegistry {
    /// Creates an empty FunctionRegistry.
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Registers all functions defined in the FunctionModule to this registry.
    pub fn register<Mod: FunctionModule>(&mut self) {
        Mod::register(self);
    }

    /// Registers a scalar function factory without monomorphization.
    pub fn add_fn_factory(&mut self, function: impl ScalarFunctionFactory + 'static) {
        let function = Arc::new(function);
        for alias in function.aliases() {
            self.map.insert((*alias).to_string(), function.clone());
        }
        self.map.insert(function.name().to_string(), function);
    }

    // TODO: remove this monomorphizing version after migrating to `add_function`.
    pub fn add_fn<UDF: ScalarUDF + 'static>(&mut self, func: UDF) {
        // casting to dyn ScalarUDF so as to not modify the signature
        let udf = Arc::new(func) as Arc<dyn ScalarUDF>;
        let function = DynamicScalarFunction::from(udf);
        self.add_fn_factory(function);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn ScalarFunctionFactory>> {
        self.map.get(name).cloned()
    }

    pub fn entries(&self) -> impl Iterator<Item = (&String, &Arc<dyn ScalarFunctionFactory>)> {
        self.map.iter()
    }
}

pub static FUNCTION_REGISTRY: LazyLock<RwLock<FunctionRegistry>> =
    LazyLock::new(|| RwLock::new(FunctionRegistry::new()));
