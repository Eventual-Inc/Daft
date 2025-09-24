use std::sync::Arc;

use common_io_config::python::IOConfig;
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    types::{PyBytes, PyString, PyTuple},
};

use crate::FileReference;

impl<'py> IntoPyObject<'py> for FileReference {
    type Target = PyTuple;

    type Output = Bound<'py, Self::Target>;

    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Reference(url, ioconfig) => {
                let io_config: Option<IOConfig> = ioconfig.map(|cfg| cfg.as_ref().clone().into());

                (url, io_config).into_pyobject(py)
            }
            Self::Data(data) => (data.as_ref(),).into_pyobject(py),
        }
    }
}

impl<'py> FromPyObject<'py> for FileReference {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let tuple = ob.extract::<Bound<'py, PyTuple>>()?;
        let first = tuple.get_item(0)?;
        if first.is_instance_of::<PyString>() {
            let url = first.extract::<String>()?;
            let io_config = tuple.get_item(1)?.extract::<Option<IOConfig>>()?;
            Ok(Self::Reference(
                url,
                io_config.map(|cfg| Arc::new(cfg.config)),
            ))
        } else if first.is_instance_of::<PyBytes>() {
            let data = first.extract::<Vec<u8>>()?;
            Ok(Self::Data(Arc::new(data)))
        } else {
            Err(PyErr::new::<PyTypeError, _>("Expected a string or bytes"))
        }
    }
}
