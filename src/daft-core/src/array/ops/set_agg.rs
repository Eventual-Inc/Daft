use std::sync::Arc;

use common_error::DaftResult;

use super::{as_arrow::AsArrow, DaftSetAggable, GroupIndices};
use crate::{
    array::{
        growable::{Growable, GrowableArray},
        DataArray, FixedSizeListArray, ListArray, StructArray,
    },
    datatypes::DaftArrowBackedType,
    series::IntoSeries,
};

macro_rules! impl_daft_set_agg {
    () => {
        type Output = DaftResult<ListArray>;

        fn distinct(&self, _include_nulls: bool) -> Self::Output {
            let child_series = self.clone().into_series();
            let offsets =
                arrow2::offset::OffsetsBuffer::try_from(vec![0, child_series.len() as i64])?;
            let list_field = self.field.to_list_field()?;
            Ok(ListArray::new(list_field, child_series, offsets, None))
        }

        fn grouped_distinct(&self, groups: &GroupIndices, _include_nulls: bool) -> Self::Output {
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
                self.null_count() > 0,
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
    };
}

impl<T> DaftSetAggable for DataArray<T>
where
    T: DaftArrowBackedType,
    Self: IntoSeries,
    Self: GrowableArray,
{
    impl_daft_set_agg!();
}

impl DaftSetAggable for ListArray {
    impl_daft_set_agg!();
}

impl DaftSetAggable for FixedSizeListArray {
    impl_daft_set_agg!();
}

impl DaftSetAggable for StructArray {
    impl_daft_set_agg!();
}

#[cfg(feature = "python")]
impl DaftSetAggable for crate::datatypes::PythonArray {
    type Output = DaftResult<Self>;

    fn distinct(&self, _include_nulls: bool) -> Self::Output {
        use pyo3::{prelude::*, types::PyList};

        use crate::array::pseudo_arrow::PseudoArrowArray;

        let pyobj_vec = self.as_arrow().to_pyobj_vec();

        let pylist: Py<PyList> = Python::with_gil(|py| {
            let pyobj_vec_cloned = pyobj_vec
                .into_iter()
                .map(|pyobj| pyobj.clone_ref(py))
                .collect::<Vec<_>>();

            PyList::new(py, pyobj_vec_cloned).map(Into::into)
        })?;

        let arrow_array = PseudoArrowArray::from_pyobj_vec(vec![Arc::new(pylist.into())]);
        Self::new(self.field().clone().into(), Box::new(arrow_array))
    }

    fn grouped_distinct(&self, groups: &GroupIndices, _include_nulls: bool) -> Self::Output {
        use pyo3::{prelude::*, types::PyList};

        use crate::array::pseudo_arrow::PseudoArrowArray;

        let mut result_pylists: Vec<Arc<PyObject>> = Vec::with_capacity(groups.len());

        Python::with_gil(|py| -> DaftResult<()> {
            for group in groups {
                let indices_as_array = crate::datatypes::UInt64Array::from(("", group.clone()));
                let group_pyobjs = self.take(&indices_as_array)?.as_arrow().to_pyobj_vec();
                let group_pyobjs_cloned = group_pyobjs
                    .into_iter()
                    .map(|pyobj| pyobj.clone_ref(py))
                    .collect::<Vec<_>>();
                result_pylists.push(Arc::new(PyList::new(py, group_pyobjs_cloned)?.into()));
            }
            Ok(())
        })?;

        let arrow_array = PseudoArrowArray::from_pyobj_vec(result_pylists);
        Self::new(self.field().clone().into(), Box::new(arrow_array))
    }
}
