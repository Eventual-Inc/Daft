pub mod binary;
pub mod coalesce;
pub mod count_matches;
pub mod distance;
pub mod float;
pub mod hash;
pub mod list;
pub mod minhash;
pub mod numeric;
#[cfg(feature = "python")]
pub mod python;
pub mod sequence;
pub mod temporal;
pub mod to_struct;
pub mod tokenize;
pub mod uri;
pub mod utf8;

use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, RwLock},
};

use common_error::DaftError;
use daft_dsl::functions::ScalarUDF;
#[cfg(feature = "python")]
pub use python::register as register_modules;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid Argument: {:?}", msg))]
    InvalidArgument { msg: String },
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        Self::other(err)
    }
}

impl From<Error> for DaftError {
    fn from(err: Error) -> Self {
        Self::External(err.into())
    }
}

/// TODO chore: cleanup function implementations using error macros
#[macro_export]
macro_rules! invalid_argument_err {
    ($($arg:tt)*)  => {{
        let msg = format!($($arg)*);
        return Err(common_error::DaftError::TypeError(msg).into());
    }};
}
#[derive(Default)]
pub struct FunctionRegistry {
    // Todo: Use the Bindings object instead, so we can get aliases and case handling.
    map: HashMap<String, Arc<dyn ScalarUDF>>,
}
pub trait FunctionModule {
    /// Register this module to the given [SQLFunctions] table.
    fn register(_parent: &mut FunctionRegistry);
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    pub fn register<Mod: FunctionModule>(&mut self) {
        Mod::register(self);
    }

    pub fn add_fn<UDF: ScalarUDF + 'static>(&mut self, func: UDF) {
        let func = Arc::new(func);
        // todo: use bindings instead of hashmap so we don't need duplicate entries.
        for alias in func.aliases() {
            self.map.insert((*alias).to_string(), func.clone());
        }
        self.map.insert(func.name().to_string(), func);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn ScalarUDF>> {
        self.map.get(name).cloned()
    }

    pub fn entries(&self) -> impl Iterator<Item = (&String, &Arc<dyn ScalarUDF>)> {
        self.map.iter()
    }
}

pub static FUNCTION_REGISTRY: LazyLock<RwLock<FunctionRegistry>> = LazyLock::new(|| {
    let mut registry = FunctionRegistry::new();
    registry.register::<numeric::NumericFunctions>();
    registry.register::<float::FloatFunctions>();
    RwLock::new(registry)
});
