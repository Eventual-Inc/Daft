use crate::{
    array::{
        growable::{Growable, GrowableArray},
        ListArray,
    },
    with_match_daft_types,
};
use arrow2::offset::OffsetsBuffer;
use common_error::DaftResult;

use super::{as_arrow::AsArrow, DaftConcatAggable};

#[cfg(feature = "python")]
impl DaftConcatAggable for crate::datatypes::PythonArray {
    type Output = DaftResult<Self>;
    fn concat(&self) -> Self::Output {
        use crate::array::pseudo_arrow::PseudoArrowArray;
        use pyo3::prelude::*;
        use pyo3::types::PyList;

        let pyobj_vec = self.as_arrow().to_pyobj_vec();

        let pylist: Py<PyList> = Python::with_gil(|py| -> PyResult<Py<PyList>> {
            let pylist: Py<PyList> = PyList::empty(py).into();
            for pyobj in pyobj_vec {
                if !pyobj.is_none(py) {
                    pylist.call_method1(py, pyo3::intern!(py, "extend"), (pyobj,))?;
                }
            }
            Ok(pylist)
        })?;
        let arrow_array = PseudoArrowArray::<PyObject>::from_pyobj_vec(vec![pylist.into()]);
        Self::new(self.field().clone().into(), Box::new(arrow_array))
    }
    fn grouped_concat(&self, groups: &super::GroupIndices) -> Self::Output {
        use crate::array::pseudo_arrow::PseudoArrowArray;
        use pyo3::prelude::*;
        use pyo3::types::PyList;

        let mut result_pylists: Vec<PyObject> = Vec::with_capacity(groups.len());

        Python::with_gil(|py| -> DaftResult<()> {
            for group in groups {
                let indices_as_array = crate::datatypes::UInt64Array::from(("", group.clone()));
                let group_pyobjs = self.take(&indices_as_array)?.as_arrow().to_pyobj_vec();
                let pylist: Py<PyList> = PyList::empty(py).into();
                for pyobj in group_pyobjs {
                    if !pyobj.is_none(py) {
                        pylist.call_method1(py, pyo3::intern!(py, "extend"), (pyobj,))?;
                    }
                }
                result_pylists.push(pylist.into());
            }
            Ok(())
        })?;

        let arrow_array = PseudoArrowArray::<PyObject>::from_pyobj_vec(result_pylists);
        Self::new(self.field().clone().into(), Box::new(arrow_array))
    }
}

impl DaftConcatAggable for ListArray {
    type Output = DaftResult<Self>;
    fn concat(&self) -> Self::Output {
        let new_offsets = OffsetsBuffer::<i64>::try_from(vec![0, self.flat_child.len() as i64])?;
        Ok(ListArray::new(
            self.field.clone(),
            self.flat_child.clone(),
            new_offsets,
            None,
        ))
    }

    fn grouped_concat(&self, groups: &super::GroupIndices) -> Self::Output {
        let mut child_array_growable: Box<dyn Growable> = with_match_daft_types!(self.child_data_type(), |$T| {
            Box::new(<<$T as DaftDataType>::ArrayType as GrowableArray>::make_growable(
                self.flat_child.name().to_string(),
                self.child_data_type(),
                vec![self.flat_child.downcast::<<$T as DaftDataType>::ArrayType>().unwrap()],
                false,
                self.flat_child.len(),
            ))
        });
        let new_offsets = arrow2::offset::Offsets::try_from_lengths(
            std::iter::once(0).chain(groups.iter().map(|g| g.len())),
        )?;

        for group in groups {
            for idx in group {
                let (start, end) = self.offsets.start_end(*idx as usize);
                let len = end - start;
                child_array_growable.extend(0, start, len);
            }
        }

        Ok(ListArray::new(
            self.field.clone(),
            child_array_growable.build()?,
            new_offsets.into(),
            None,
        ))
    }
}
