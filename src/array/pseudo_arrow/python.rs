use crate::array::pseudo_arrow::PseudoArrowArray;
use arrow2::array::Array;

use pyo3::prelude::*;

impl PseudoArrowArray<PyObject> {
    pub fn from_pyobj_vec(pyobj_vec: Vec<PyObject>) -> Self {
        // Converts this Vec<PyObject> into a PseudoArrowArray<PyObject>.
        // PyNones will be marked as invalid bits in the validity bitmap.

        let validity: arrow2::bitmap::Bitmap = Python::with_gil(|py| {
            arrow2::bitmap::Bitmap::from_iter(pyobj_vec.iter().map(|pyobj| !pyobj.is_none(py)))
        });
        PseudoArrowArray::new(pyobj_vec.into(), Some(validity))
    }

    pub fn to_pyobj_vec(&self) -> Vec<PyObject> {
        // Converts this PseudoArrowArray<PyObject> into a Vec<PyObject>,
        // taking into account the validity bitmap.
        // Invalid slots will be set to py.None().

        Python::with_gil(|py| {
            if self.validity().is_some() {
                self.iter()
                    .map(|opt_val| match opt_val {
                        Some(pyobj) => pyobj.clone_ref(py),
                        None => py.None(),
                    })
                    .collect()
            } else {
                self.values().to_vec()
            }
        })
    }
}
