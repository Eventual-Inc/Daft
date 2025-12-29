use super::kv_functions::{
    KVBatchGetWithStoreName, KVExistsWithStoreName, KVGetWithStoreName, KVPutWithStoreName,
};
use crate::functions::{FunctionModule, FunctionRegistry};

/// KV Store function module for registering all KV-related functions
pub struct KVModule;

impl FunctionModule for KVModule {
    fn register(parent: &mut FunctionRegistry) {
        // Register KV functions with store name support
        parent.add_fn(KVGetWithStoreName);
        parent.add_fn(KVBatchGetWithStoreName);
        parent.add_fn(KVExistsWithStoreName);
        parent.add_fn(KVPutWithStoreName);
    }
}
