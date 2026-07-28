use pyo3::prelude::*;

use crate::{
    DaftFile,
    mcap::{
        BUFFER_SIZE_MCAP_METADATA, McapMetadata, ReadMetadataOptions, read_metadata_with_options,
    },
    python::PyFileReference,
};

#[pyfunction]
#[pyo3(signature = (file, include_schema_data=true, include_metadata_records=true, include_chunk_indexes=true))]
fn _read_mcap_metadata(
    py: Python<'_>,
    file: &PyFileReference,
    include_schema_data: bool,
    include_metadata_records: bool,
    include_chunk_indexes: bool,
) -> PyResult<McapMetadata> {
    let file_ref = file.file_reference();
    py.detach(move || {
        let mut file = DaftFile::load_blocking(file_ref, false, Some(BUFFER_SIZE_MCAP_METADATA))?;
        Ok(read_metadata_with_options(
            &mut file,
            ReadMetadataOptions {
                include_schema_data,
                include_metadata_records,
                include_chunk_indexes,
            },
        )?)
    })
}

pub(crate) fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(_read_mcap_metadata, parent)?)?;
    Ok(())
}
