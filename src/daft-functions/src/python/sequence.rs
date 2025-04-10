use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

simple_python_wrapper!(
    monotonically_increasing_id,
    crate::sequence::monotonically_increasing_id,
    []
);

simple_python_wrapper!(row_number, crate::sequence::row_number, []);
