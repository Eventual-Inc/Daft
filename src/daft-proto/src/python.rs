use std::str::FromStr;

use daft_ir::rel::PyLogicalPlanBuilder;
use pyo3::prelude::*;
use tonic::transport::Endpoint;

use crate::{protos::{echo::EchoRequest, FromToProto}, v1::client::EchoServiceClient};

#[pyclass]
pub struct PyEchoClient(EchoServiceClient);

#[pymethods]
impl PyEchoClient {
    #[staticmethod]
    pub fn connect(endpoint: String) -> PyResult<Self> {
        let dst = Endpoint::from_str(&endpoint).expect("could not parse endpoint");
        let client = EchoServiceClient::connect(dst).expect("could not connect!");
        Ok(Self(client))
    }

    pub fn describe(&mut self, plan: PyLogicalPlanBuilder) -> PyResult<String> {

        // for now, just pull out the schema!
        let schema = plan.schema()?;
        let schema = schema.schema;
        let schema = schema.as_ref().to_proto().expect("Error converting proto!");

        let res = self
            .0
            .get_echo(EchoRequest { schema: Some(schema) })
            .expect("failed to call service");
        let res = res.into_inner();
        Ok(res.message)
    }
}

pub fn register_modules(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<PyEchoClient>()?;
    Ok(())
}
