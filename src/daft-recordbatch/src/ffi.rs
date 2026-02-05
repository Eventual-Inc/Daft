use common_arrow_ffi::FromPyArrow;
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use pyo3::prelude::*;

use crate::RecordBatch;

/// Converts Arrow RecordBatches to a Daft RecordBatch (Table)
pub fn record_batch_from_arrow(
    py: Python,
    batches: &[Bound<PyAny>],
    schema: SchemaRef,
) -> PyResult<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::empty(Some(schema)));
    }

    let num_batches = batches.len();
    // Extract all arrow RecordBatches while holding the GIL
    let mut arrow_batches = Vec::with_capacity(num_batches);
    for rb in batches {
        let arrow_batch = arrow::record_batch::RecordBatch::from_pyarrow_bound(rb)?;
        arrow_batches.push(arrow_batch);
    }

    // Now do the heavy lifting (casting and concats) without the GIL.
    py.detach(|| {
        let mut tables: Vec<RecordBatch> = Vec::with_capacity(num_batches);
        for rb in arrow_batches {
            let arrow_schema = rb.schema();
            let daft_schema = daft_core::prelude::Schema::from_arrow(arrow_schema.as_ref(), true)?;
            let target_arrow_schema = daft_schema.to_arrow()?;

            // Cast columns if the coerced schema differs from the input schema
            let arrays: Vec<_> = if target_arrow_schema != *arrow_schema.as_ref() {
                rb.columns()
                    .iter()
                    .zip(target_arrow_schema.fields())
                    .map(|(array, target_field)| {
                        if array.data_type() != target_field.data_type() {
                            arrow::compute::cast(array, target_field.data_type())
                                .map_err(common_error::DaftError::from)
                        } else {
                            Ok(array.clone())
                        }
                    })
                    .collect::<DaftResult<_>>()?
            } else {
                rb.columns().to_vec()
            };

            tables.push(RecordBatch::from_arrow(daft_schema, arrays)?);
        }
        Ok(RecordBatch::concat(tables.as_slice())?)
    })
}

/// Converts a Daft RecordBatch (Table) to an Arrow RecordBatch
pub fn record_batch_to_arrow(
    py: Python,
    table: &RecordBatch,
    pyarrow: Bound<PyModule>,
) -> PyResult<pyo3::Py<pyo3::PyAny>> {
    let mut arrays = Vec::with_capacity(table.num_columns());
    let mut names: Vec<String> = Vec::with_capacity(table.num_columns());

    for i in 0..table.num_columns() {
        let s = table.get_column(i);
        let arrow_field = s.field().to_arrow()?;
        let arrow_array = s.to_arrow()?;
        let arrow_array = if arrow_array.data_type() != arrow_field.data_type() {
            arrow::compute::cast(&arrow_array, arrow_field.data_type())
                .map_err(common_error::DaftError::from)?
        } else {
            arrow_array
        };
        let py_array = common_arrow_ffi::to_py_array_v2(py, arrow_array, &arrow_field)?;
        arrays.push(py_array);
        names.push(s.name().to_string());
    }

    let record = pyarrow
        .getattr(pyo3::intern!(py, "RecordBatch"))?
        .call_method1(pyo3::intern!(py, "from_arrays"), (arrays, names.clone()))?;

    Ok(record.into())
}
