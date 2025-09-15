use super::functions::{CreateKVConfig, KVBatchGetWithConfig, KVExistsWithConfig, KVGetWithConfig};
use crate::functions::{FunctionModule, FunctionRegistry};

/// KV Store function module for registering all KV-related functions
pub struct KVModule;

impl FunctionModule for KVModule {
    fn register(parent: &mut FunctionRegistry) {
        // Register KV functions with config support
        parent.add_fn(KVGetWithConfig);
        parent.add_fn(KVBatchGetWithConfig);
        parent.add_fn(KVExistsWithConfig);

        // Register KV config creation function
        parent.add_fn(CreateKVConfig);
    }
}

/// Initialize KV Store functions in the global function registry
pub fn register_kv_functions() {
    use crate::functions::FUNCTION_REGISTRY;

    let mut registry = FUNCTION_REGISTRY.write().unwrap();
    registry.register::<KVModule>();
}
