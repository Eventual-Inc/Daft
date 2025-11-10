use common_io_config::python::IOConfig;
use daft_schema::media_type::MediaType;
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

                (self.media_type, url, io_config).into_pyobject(py)
            }
            DataOrReference::Data(data) => (self.media_type, data.as_ref()).into_pyobject(py),
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for FileReference {
    type Error = PyErr;

    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let tuple = ob.extract::<Bound<'py, PyTuple>>()?;
        let media_type = tuple.get_item(0)?.extract::<MediaType>()?;
        let first = tuple.get_item(1)?;
        if first.is_instance_of::<PyString>() {
            let url = first.extract::<String>()?;
            let io_config = tuple.get_item(2)?.extract::<Option<IOConfig>>()?;
            Ok(Self::new_from_reference(
                media_type,
                url,
                io_config.map(|cfg| cfg.config),
            ))
        } else if first.is_instance_of::<PyBytes>() {
            let data = first.extract::<Vec<u8>>()?;
            Ok(Self::new_from_data(media_type, data))
        } else {
            Err(PyErr::new::<PyTypeError, _>("Expected a string or bytes"))
        }
    }
}
