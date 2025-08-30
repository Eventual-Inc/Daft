use crate::functions::{FunctionModule, FunctionRegistry};

use super::functions::{CreateKVConfig, LanceKVExists, LanceKVGet, LanceKVTake};

/// KV Store function module for registering all KV-related functions
pub struct KVModule;

impl FunctionModule for KVModule {
    fn register(parent: &mut FunctionRegistry) {
        // Register Lance KV functions
        parent.add_fn(LanceKVGet);
        parent.add_fn(LanceKVTake);
        parent.add_fn(LanceKVExists);
        
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