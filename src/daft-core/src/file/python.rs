use common_io_config::python::IOConfig;
use daft_schema::media_type::MediaType;
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    types::{PyString, PyTuple},
};

use crate::file::FileReference;

impl<'py> IntoPyObject<'py> for FileReference {
    type Target = PyTuple;

    type Output = Bound<'py, Self::Target>;

    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let io_config: Option<IOConfig> = self.io_config.map(|cfg| cfg.as_ref().clone().into());

        (self.media_type, self.url, io_config).into_pyobject(py)
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
            Ok(Self::new(media_type, url, io_config.map(|cfg| cfg.config)))
        } else {
            Err(PyErr::new::<PyTypeError, _>("Expected a string or bytes"))
        }
    }
}
