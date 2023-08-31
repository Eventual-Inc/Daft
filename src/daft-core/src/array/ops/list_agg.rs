use crate::{
    array::{
        growable::{Growable, GrowableArray},
        DataArray, FixedSizeListArray, ListArray, StructArray,
    },
    datatypes::DaftArrowBackedType,
    IntoSeries,
};
use common_error::DaftResult;

use super::{as_arrow::AsArrow, DaftListAggable, GroupIndices};

impl<T> DaftListAggable for DataArray<T>
where
    T: DaftArrowBackedType,
    DataArray<T>: IntoSeries,
    DataArray<T>: GrowableArray,
{
    type Output = DaftResult<ListArray>;
    fn list(&self) -> Self::Output {
        let child_series = self.clone().into_series();
        let offsets = arrow2::offset::OffsetsBuffer::try_from(vec![0, child_series.len() as i64])?;
        let list_field = self.field.to_list_field()?;
        Ok(ListArray::new(list_field, child_series, offsets, None))
    }

    fn grouped_list(&self, groups: &GroupIndices) -> Self::Output {
        let mut offsets = Vec::with_capacity(groups.len() + 1);

        offsets.push(0);
        for g in groups {
            offsets.push(offsets.last().unwrap() + g.len() as i64);
        }

        let total_capacity = *offsets.last().unwrap();

        let mut growable: Box<dyn Growable> = Box::new(Self::make_growable(
            self.name(),
            self.data_type(),
            vec![self],
            self.data.null_count() > 0,
            total_capacity as usize,
        ));

        for g in groups {
            for idx in g {
                growable.extend(0, *idx as usize, 1);
            }
        }
        let list_field = self.field.to_list_field()?;

        Ok(ListArray::new(
            list_field,
            growable.build()?,
            arrow2::offset::OffsetsBuffer::try_from(offsets)?,
            None,
        ))
    }
}

#[cfg(feature = "python")]
impl DaftListAggable for crate::datatypes::PythonArray {
    type Output = DaftResult<crate::datatypes::PythonArray>;

    fn list(&self) -> Self::Output {
        use crate::array::pseudo_arrow::PseudoArrowArray;
        use pyo3::prelude::*;
        use pyo3::types::PyList;

        let pyobj_vec = self.as_arrow().to_pyobj_vec();

        let pylist: Py<PyList> = Python::with_gil(|py| PyList::new(py, pyobj_vec).into());

        let arrow_array = PseudoArrowArray::<PyObject>::from_pyobj_vec(vec![pylist.into()]);
        Self::new(self.field().clone().into(), Box::new(arrow_array))
    }

    fn grouped_list(&self, groups: &GroupIndices) -> Self::Output {
        use crate::array::pseudo_arrow::PseudoArrowArray;
        use pyo3::prelude::*;
        use pyo3::types::PyList;

        let mut result_pylists: Vec<PyObject> = Vec::with_capacity(groups.len());

        Python::with_gil(|py| -> DaftResult<()> {
            for group in groups {
                let indices_as_array = crate::datatypes::UInt64Array::from(("", group.clone()));
                let group_pyobjs = self.take(&indices_as_array)?.as_arrow().to_pyobj_vec();
                result_pylists.push(PyList::new(py, group_pyobjs).into());
            }
            Ok(())
        })?;

        let arrow_array = PseudoArrowArray::<PyObject>::from_pyobj_vec(result_pylists);
        Self::new(self.field().clone().into(), Box::new(arrow_array))
    }
}

impl DaftListAggable for ListArray {
    type Output = DaftResult<ListArray>;

    fn list(&self) -> Self::Output {
        // TODO(FixedSizeList)
        todo!("Requires new ListArrays for implementation")
    }

    fn grouped_list(&self, _groups: &GroupIndices) -> Self::Output {
        // TODO(FixedSizeList)
        todo!("Requires new ListArrays for implementation")
    }
}

impl DaftListAggable for FixedSizeListArray {
    type Output = DaftResult<ListArray>;

    fn list(&self) -> Self::Output {
        // TODO(FixedSizeList)
        todo!("Requires new ListArrays for implementation")
    }

    fn grouped_list(&self, _groups: &GroupIndices) -> Self::Output {
        // TODO(FixedSizeList)
        todo!("Requires new ListArrays for implementation")
    }
}

impl DaftListAggable for StructArray {
    type Output = DaftResult<ListArray>;

    fn list(&self) -> Self::Output {
        // TODO(FixedSizeList)
        todo!("Requires new ListArrays for implementation")
    }

    fn grouped_list(&self, _groups: &GroupIndices) -> Self::Output {
        // TODO(FixedSizeList)
        todo!("Requires new ListArrays for implementation")
    }
}
