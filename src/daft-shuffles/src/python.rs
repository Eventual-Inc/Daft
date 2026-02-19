use pyo3::{
    Bound, PyResult, pyclass, pyfunction, pymethods,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction,
};

use crate::server::flight_server::{FlightServerConnectionHandle, start_flight_server};

#[pyclass(module = "daft.daft", name = "FlightServerConnectionHandle")]
pub struct PyFlightServerConnectionHandle {
    handle: FlightServerConnectionHandle,
}

#[pymethods]
impl PyFlightServerConnectionHandle {
    pub fn shutdown(&mut self) -> PyResult<()> {
        self.handle.shutdown()?;
        Ok(())
    }

    pub fn port(&self) -> PyResult<u16> {
        Ok(self.handle.port())
    }
}

#[pyfunction(name = "start_flight_server")]
pub fn py_start_flight_server(ip: &str) -> PyResult<PyFlightServerConnectionHandle> {
    let handle = start_flight_server(ip);
    Ok(PyFlightServerConnectionHandle { handle })
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(py_start_flight_server, parent)?)?;
    Ok(())
}
