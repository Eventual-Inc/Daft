use common_io_config::python::IOConfig;
use daft_schema::file_format::FileFormat;
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    types::{PyBytes, PyString, PyTuple},
};

use crate::file::{DataOrReference, FileReference};

impl<'py> IntoPyObject<'py> for FileReference {
    type Target = PyTuple;

    type Output = Bound<'py, Self::Target>;

    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.inner {
            DataOrReference::Reference(url, ioconfig) => {
                let io_config: Option<IOConfig> = ioconfig.map(|cfg| cfg.as_ref().clone().into());

                (self.file_format, url, io_config).into_pyobject(py)
            }
            DataOrReference::Data(data) => (self.file_format, data.as_ref()).into_pyobject(py),
        }
    }
}

impl<'py> FromPyObject<'py> for FileReference {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let tuple = ob.extract::<Bound<'py, PyTuple>>()?;
        let file_format = tuple.get_item(0)?.extract::<FileFormat>()?;
        let first = tuple.get_item(1)?;
        if first.is_instance_of::<PyString>() {
            let url = first.extract::<String>()?;
            let io_config = tuple.get_item(2)?.extract::<Option<IOConfig>>()?;
            Ok(Self::new_from_reference(
                file_format,
                url,
                io_config.map(|cfg| cfg.config),
            ))
        } else if first.is_instance_of::<PyBytes>() {
            let data = first.extract::<Vec<u8>>()?;
            Ok(Self::new_from_data(file_format, data))
        } else {
            Err(PyErr::new::<PyTypeError, _>("Expected a string or bytes"))
        }
    }
}
