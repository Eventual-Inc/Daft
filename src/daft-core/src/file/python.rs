use daft_common::io_config::python::IOConfig;
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

        (
            self.media_type,
            self.url,
            io_config,
            self.offset,
            self.length,
        )
            .into_pyobject(py)
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
            let (offset, length) = if tuple.len() >= 5 {
                let offset = tuple.get_item(3)?.extract::<Option<u64>>()?;
                let length = tuple.get_item(4)?.extract::<Option<u64>>()?;
                (offset, length)
            } else {
                (None, None)
            };
            Ok(Self::new_with_range(
                media_type,
                url,
                io_config.map(|cfg| cfg.config),
                offset,
                length,
            ))
        } else {
            Err(PyErr::new::<PyTypeError, _>("Expected a string or bytes"))
        }
    }
}
