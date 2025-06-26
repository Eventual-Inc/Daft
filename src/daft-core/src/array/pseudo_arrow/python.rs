use std::sync::Arc;

use arrow2::{array::Array, bitmap::Bitmap};
use pyo3::prelude::*;

use crate::array::pseudo_arrow::PseudoArrowArray;

impl PseudoArrowArray<Arc<PyObject>> {
    pub fn from_pyobj_vec(pyobj_vec: Vec<Arc<PyObject>>) -> Self {
        // Converts this Vec<PyObject> into a PseudoArrowArray<Arc<PyObject>>.
        // PyNones will be marked as invalid bits in the validity bitmap.

        let validity: arrow2::bitmap::Bitmap = Python::with_gil(|py| {
            arrow2::bitmap::Bitmap::from_iter(pyobj_vec.iter().map(|pyobj| !pyobj.is_none(py)))
        });
        Self::new(pyobj_vec.into(), Some(validity))
    }

    pub fn to_pyobj_vec(&self) -> Vec<Arc<PyObject>> {
        // Converts this PseudoArrowArray<Arc<PyObject>> into a Vec<Arc<PyObject>>,
        // taking into account the validity bitmap.
        // Invalid slots will be set to py.None().

        if self.validity().is_some() {
            Python::with_gil(|py| {
                self.iter()
                    .map(|opt_val| {
                        Arc::new(match opt_val {
                            Some(pyobj) => pyobj.clone_ref(py),
                            None => py.None(),
                        })
                    })
                    .collect()
            })
        } else {
            self.values().to_vec()
        }
    }

    pub fn if_then_else(
        predicate: &arrow2::array::BooleanArray,
        lhs: &dyn Array,
        rhs: &dyn Array,
    ) -> Self {
        let pynone = Python::with_gil(|py| Arc::new(py.None()));

        let (new_values, new_validity): (Vec<Arc<PyObject>>, Vec<bool>) = {
            lhs.as_any()
                .downcast_ref::<Self>()
                .unwrap()
                .iter()
                .zip(rhs.as_any().downcast_ref::<Self>().unwrap().iter())
                .zip(predicate.iter())
                .map(|((self_val, other_val), pred_val)| match pred_val {
                    None => None,
                    Some(true) => self_val,
                    Some(false) => other_val,
                })
                .map(|result_val| match result_val {
                    Some(pyobj) => (pyobj.clone(), true),
                    None => (pynone.clone(), false),
                })
                .unzip()
        };

        let new_validity: Option<Bitmap> = Some(Bitmap::from_iter(new_validity));

        Self::new(new_values.into(), new_validity)
    }
}
