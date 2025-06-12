use std::str::FromStr;

use pyo3::prelude::*;
use tonic::transport::Endpoint;

use crate::{client::EchoServiceClient, echo::EchoRequest};

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

    pub fn echo(&mut self, message: String) -> PyResult<String> {
        let res = self
            .0
            .get_echo(EchoRequest { message })
            .expect("failed to call service");
        let res = res.into_inner();
        Ok(res.message)
    }
}

pub fn register_modules(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<PyEchoClient>()?;
    Ok(())
}
