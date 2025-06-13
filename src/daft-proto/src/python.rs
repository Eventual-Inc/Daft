use std::{env, str::FromStr, sync::{Arc, Mutex, OnceLock}};

use daft_ir::rel::PyLogicalPlanBuilder;
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use tonic::transport::Endpoint;

use crate::{
    v1::{client::TronClient, protos::tron::RunRequest},
    FromToProto,
};

/// Tron context for the process.
static TRON_CONTEXT: OnceLock<Option<TronContext>> = OnceLock::new();


/// Tron context holds the client for this process.
#[derive(Debug, Clone)]
pub struct TronContext {
    client: Arc<Mutex<TronClient>>,
}

impl TronContext {
    /// Creates a context.
    pub fn create(endpoint: &str) -> Self {
        let dst = Endpoint::from_str(endpoint).expect("could not parse TRON_ENDPOINT");
        let client = TronClient::connect(dst).expect("could not connect!");
        Self { client: Arc::new(Mutex::new(client)) }
    }
}

/// Returns or initializes the tron context.
fn get_tron_context() -> Option<TronContext> {
    // return if already exists
    if let Some(ctx) = TRON_CONTEXT.get() {
        return ctx.clone();
    }
    // setup the tron context only if the endpoint is set
    if let Ok(endpoint) = env::var("TRON_ENDPOINT") {
        let ctx = TronContext::create(&endpoint);
        TRON_CONTEXT
            .set(Some(ctx.clone()))
            .expect("Failed to set TronContext");
        return Some(ctx)
    }
    // do nothing at this point
    None
}

#[pyfunction]
pub fn tron_run(plan: PyLogicalPlanBuilder) -> PyResult<()> {
    if let Some(ctx) = get_tron_context() {
        let schema = plan.schema()?;
        let schema = schema.schema;
        let schema = schema.as_ref().to_proto().expect("Error converting proto!");
        let mut client = ctx.client.lock().expect("Failed to lock client");
        client.run(RunRequest {
            schema: Some(schema)
        }).expect("some tron failure");
        return Ok(())
    }
    Err(PyRuntimeError::new_err("no tron context"))
}

pub fn register_modules(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(tron_run, parent)?)?;
    Ok(())
}
