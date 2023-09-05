use arrow2::types::Index;
use common_error::DaftResult;

use crate::{
    array::{growable::make_growable, ListArray},
    datatypes::Field,
    DataType, IntoSeries, Series,
};

use super::{bitmap_growable::ArrowBitmapGrowable, Growable};

pub struct ListGrowable<'a> {
    name: String,
    dtype: DataType,
    child_growable: Box<dyn Growable + 'a>,
    child_arrays_offsets: Vec<&'a arrow2::offset::OffsetsBuffer<i64>>,
    growable_validity: Option<ArrowBitmapGrowable<'a>>,
    growable_offsets: arrow2::offset::Offsets<i64>,
}

impl<'a> ListGrowable<'a> {
    pub fn new(
        name: &str,
        dtype: &DataType,
        arrays: Vec<&'a ListArray>,
        use_validity: bool,
        capacity: usize,
        child_capacity: usize,
    ) -> Self {
        match dtype {
            DataType::List(child_dtype) => {
                let child_growable = make_growable(
                    "list",
                    child_dtype.as_ref(),
                    arrays.iter().map(|a| &a.flat_child).collect::<Vec<_>>(),
                    use_validity,
                    child_capacity,
                );
                let growable_validity =
                    if use_validity || arrays.iter().any(|arr| arr.validity().is_some()) {
                        Some(ArrowBitmapGrowable::new(
                            arrays.iter().map(|a| a.validity()).collect(),
                            capacity,
                        ))
                    } else {
                        None
                    };
                let child_arrays_offsets =
                    arrays.iter().map(|arr| arr.offsets()).collect::<Vec<_>>();
                Self {
                    name: name.to_string(),
                    dtype: dtype.clone(),
                    child_growable,
                    child_arrays_offsets,
                    growable_validity,
                    growable_offsets: arrow2::offset::Offsets::<i64>::default(),
                }
            }
            _ => panic!("Cannot create ListGrowable from dtype: {}", dtype),
        }
    }
}

impl<'a> Growable for ListGrowable<'a> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        let offsets = self.child_arrays_offsets.get(index).unwrap();
        let start_offset = &offsets.buffer()[start];
        let end_offset = &offsets.buffer()[start + len];
        self.child_growable.extend(
            index,
            start_offset.to_usize(),
            (end_offset - start_offset).to_usize(),
        );

        match &mut self.growable_validity {
            Some(growable_validity) => growable_validity.extend(index, start, len),
            None => (),
        }

        self.growable_offsets
            .try_extend_from_slice(offsets, start, len)
            .unwrap();
    }

    fn add_nulls(&mut self, additional: usize) {
        match &mut self.growable_validity {
            Some(growable_validity) => growable_validity.add_nulls(additional),
            None => (),
        }
        self.growable_offsets.extend_constant(additional);
    }

    fn build(&mut self) -> DaftResult<Series> {
        let grown_offsets = std::mem::take(&mut self.growable_offsets);
        let grown_validity = std::mem::take(&mut self.growable_validity);

        let built_child = self.child_growable.build()?;
        let built_validity = grown_validity.map(|v| v.build());
        let built_offsets = grown_offsets.into();
        Ok(ListArray::new(
            Field::new(self.name.clone(), self.dtype.clone()),
            built_child,
            built_offsets,
            built_validity,
        )
        .into_series())
    }
}
