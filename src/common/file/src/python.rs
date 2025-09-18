use common_io_config::python::IOConfig;
use pyo3::{intern, prelude::*};

use crate::DaftFile;

impl<'py> IntoPyObject<'py> for DaftFile {
    type Target = PyAny;

    type Output = Bound<'py, Self::Target>;

    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let py_file = py
            .import(intern!(py, "daft.file"))?
            .getattr(intern!(py, "File"))?;

        match self {
            Self::Reference(url, ioconfig) => {
                let io_config: Option<IOConfig> = ioconfig.map(|cfg| cfg.into());

                py_file
                    .getattr(intern!(py, "_from_path"))?
                    .call1((url, io_config))
            }
            Self::Data(data) => py_file.getattr(intern!(py, "_from_bytes"))?.call1((data,)),
        }
    }
}
