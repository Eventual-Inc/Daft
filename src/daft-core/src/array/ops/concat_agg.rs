use crate::array::{
    growable::{make_growable, Growable},
    ListArray,
};
use arrow2::{bitmap::utils::SlicesIterator, offset::OffsetsBuffer, types::Index};
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
        if self.null_count() == 0 {
            let new_offsets = OffsetsBuffer::<i64>::try_from(vec![0, *self.offsets().last()])?;
            return Ok(ListArray::new(
                self.field.clone(),
                self.flat_child.clone(),
                new_offsets,
                None,
            ));
        }

        // Only the all-null case leads to a null result. If any single element is non-null (e.g. an empty list []),
        // The concat will successfully return a single non-null element.
        let new_validity = match self.validity() {
            Some(validity) if validity.unset_bits() == self.len() => {
                Some(arrow2::bitmap::Bitmap::from(vec![false]))
            }
            _ => None,
        };

        // Re-grow the child, dropping elements where the parent is null
        let mut child_growable: Box<dyn Growable> = make_growable(
            self.flat_child.name(),
            self.flat_child.data_type(),
            vec![&self.flat_child],
            true,
            self.flat_child.len(), // Conservatively reserve a capacity == full size of the child
        );
        for (start_valid, len_valid) in SlicesIterator::new(self.validity().unwrap()) {
            let child_start = self.offsets().start_end(start_valid).0;
            let child_end = self.offsets().start_end(start_valid + len_valid - 1).1;
            child_growable.extend(0, child_start, child_end - child_start);
        }
        let new_child = child_growable.build()?;
        let new_offsets = OffsetsBuffer::<i64>::try_from(vec![0, new_child.len() as i64])?;

        Ok(ListArray::new(
            self.field.clone(),
            new_child,
            new_offsets,
            new_validity,
        ))
    }

    fn grouped_concat(&self, groups: &super::GroupIndices) -> Self::Output {
        let all_valid = self.null_count() == 0;

        let mut child_array_growable: Box<dyn Growable> = make_growable(
            self.flat_child.name(),
            self.child_data_type(),
            vec![&self.flat_child],
            false,
            self.flat_child.len(),
        );

        let mut group_lens: Vec<usize> = vec![];
        let mut group_valids: Vec<bool> = vec![];
        for group in groups {
            let mut group_valid = false;
            let mut group_len: usize = 0;
            for idx in group {
                if all_valid || self.is_valid(idx.to_usize()) {
                    let (start, end) = self.offsets().start_end(*idx as usize);
                    let len = end - start;
                    child_array_growable.extend(0, start, len);
                    group_len += len;
                    group_valid = true;
                }
            }
            group_valids.push(group_valid);
            group_lens.push(if group_valid { group_len } else { 0 });
        }
        let new_offsets = arrow2::offset::Offsets::try_from_lengths(group_lens.iter().copied())?;
        let new_validities = if all_valid {
            None
        } else {
            Some(arrow2::bitmap::Bitmap::from(group_valids))
        };

        Ok(ListArray::new(
            self.field.clone(),
            child_array_growable.build()?,
            new_offsets.into(),
            new_validities,
        ))
    }
}

#[cfg(test)]
mod test {
    use std::iter::repeat;

    use common_error::DaftResult;

    use crate::{
        array::{ops::DaftConcatAggable, ListArray},
        datatypes::{Field, Int64Array},
        DataType, IntoSeries,
    };

    #[test]
    fn test_list_concat_agg_all_null() -> DaftResult<()> {
        // [None, None, None]
        let list_array = ListArray::new(
            Field::new("foo", DataType::List(Box::new(DataType::Int64))),
            Int64Array::from((
                "item",
                Box::new(arrow2::array::Int64Array::from_iter([].iter())),
            ))
            .into_series(),
            arrow2::offset::OffsetsBuffer::<i64>::try_from(vec![0, 0, 0, 0])?,
            Some(arrow2::bitmap::Bitmap::from_iter(repeat(false).take(3))),
        );

        // Expected: [None]
        let concatted = list_array.concat()?;
        assert_eq!(concatted.len(), 1);
        assert_eq!(
            concatted.validity(),
            Some(&arrow2::bitmap::Bitmap::from_iter(repeat(false).take(1)))
        );
        Ok(())
    }

    #[test]
    fn test_list_concat_agg_with_nulls() -> DaftResult<()> {
        // [[0], [1, 1], [2, None], [None], [], None, None]
        let list_array = ListArray::new(
            Field::new("foo", DataType::List(Box::new(DataType::Int64))),
            Int64Array::from((
                "item",
                Box::new(arrow2::array::Int64Array::from_iter(
                    [Some(0), Some(1), Some(1), Some(2), None, None, Some(10000)].iter(),
                )),
            ))
            .into_series(),
            arrow2::offset::OffsetsBuffer::<i64>::try_from(vec![0, 1, 3, 5, 6, 6, 6, 7])?,
            Some(arrow2::bitmap::Bitmap::from(vec![
                true, true, true, true, true, false, false,
            ])),
        );

        // Expected: [[0, 1, 1, 2, None, None]]
        let concatted = list_array.concat()?;
        assert_eq!(concatted.len(), 1);
        assert_eq!(concatted.validity(), None);
        let element = concatted.get(0).unwrap();
        assert_eq!(
            element
                .downcast::<Int64Array>()
                .unwrap()
                .into_iter()
                .collect::<Vec<Option<&i64>>>(),
            vec![Some(&0), Some(&1), Some(&1), Some(&2), None, None]
        );
        Ok(())
    }

    #[test]
    fn test_grouped_list_concat_agg() -> DaftResult<()> {
        // [[0], [0, 0], [1, None], [None], [2, None], None, None, None]
        //  |  group0 |  |     group1    |  | group 2     |  group 3   |
        let list_array = ListArray::new(
            Field::new("foo", DataType::List(Box::new(DataType::Int64))),
            Int64Array::from((
                "item",
                Box::new(arrow2::array::Int64Array::from_iter(
                    [
                        Some(0),
                        Some(0),
                        Some(0),
                        Some(1),
                        None,
                        None,
                        Some(2),
                        None,
                        Some(1000),
                    ]
                    .iter(),
                )),
            ))
            .into_series(),
            arrow2::offset::OffsetsBuffer::<i64>::try_from(vec![0, 1, 3, 5, 6, 8, 8, 8, 9])?,
            Some(arrow2::bitmap::Bitmap::from(vec![
                true, true, true, true, true, false, false, false,
            ])),
        );

        let concatted =
            list_array.grouped_concat(&vec![vec![0, 1], vec![2, 3], vec![4, 5], vec![6, 7]])?;

        // Expected: [[0, 0, 0], [1, None, None], [2, None], None]
        assert_eq!(concatted.len(), 4);
        assert_eq!(
            concatted.validity(),
            Some(&arrow2::bitmap::Bitmap::from(vec![true, true, true, false]))
        );

        let element_0 = concatted.get(0).unwrap();
        assert_eq!(
            element_0
                .downcast::<Int64Array>()
                .unwrap()
                .into_iter()
                .collect::<Vec<Option<&i64>>>(),
            vec![Some(&0), Some(&0), Some(&0)]
        );

        let element_1 = concatted.get(1).unwrap();
        assert_eq!(
            element_1
                .downcast::<Int64Array>()
                .unwrap()
                .into_iter()
                .collect::<Vec<Option<&i64>>>(),
            vec![Some(&1), None, None]
        );

        let element_2 = concatted.get(2).unwrap();
        assert_eq!(
            element_2
                .downcast::<Int64Array>()
                .unwrap()
                .into_iter()
                .collect::<Vec<Option<&i64>>>(),
            vec![Some(&2), None]
        );

        let element_3 = concatted.get(3);
        assert!(element_3.is_none());
        Ok(())
    }
}
