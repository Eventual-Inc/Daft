use std::iter::repeat_n;

use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use common_error::DaftResult;

use super::{Growable, bitmap_growable::ArrowBitmapGrowable};
use crate::{
    array::{ListArray, growable::make_growable},
    datatypes::{DataType, Field},
    series::{IntoSeries, Series},
};

pub struct ListGrowable<'a> {
    name: String,
    dtype: DataType,
    child_growable: Box<dyn Growable + 'a>,
    child_arrays_offsets: Vec<&'a OffsetBuffer<i64>>,
    growable_validity: Option<ArrowBitmapGrowable<'a>>,
    growable_offsets: Vec<i64>,
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
                    if use_validity || arrays.iter().any(|arr| arr.nulls().is_some()) {
                        Some(ArrowBitmapGrowable::new(
                            arrays.iter().map(|a| a.nulls()).collect(),
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
                    growable_offsets: vec![0i64],
                }
            }
            _ => panic!("Cannot create ListGrowable from dtype: {}", dtype),
        }
    }
}

impl Growable for ListGrowable<'_> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        let offsets = self.child_arrays_offsets.get(index).unwrap();
        let offsets = offsets.inner();
        let base = offsets[start];
        self.child_growable
            .extend(index, base as usize, (offsets[start + len] - base) as usize);

        if let Some(growable_validity) = &mut self.growable_validity {
            growable_validity.extend(index, start, len);
        }

        let last = *self.growable_offsets.last().unwrap();
        self.growable_offsets.extend(
            offsets[start + 1..=start + len]
                .iter()
                .map(|&o| last + (o - base)),
        );
    }

    fn add_nulls(&mut self, additional: usize) {
        if let Some(growable_validity) = &mut self.growable_validity {
            growable_validity.add_nulls(additional);
        }
        let last = *self.growable_offsets.last().unwrap();
        self.growable_offsets.extend(repeat_n(last, additional));
    }

    fn build(&mut self) -> DaftResult<Series> {
        let grown_offsets = std::mem::replace(&mut self.growable_offsets, vec![0i64]);
        let grown_validity = std::mem::take(&mut self.growable_validity);

        let built_child = self.child_growable.build()?;
        let built_validity = grown_validity.and_then(|v| v.build());
        let offsets = OffsetBuffer::new(ScalarBuffer::from(grown_offsets));
        Ok(ListArray::new(
            Field::new(self.name.clone(), self.dtype.clone()),
            built_child,
            offsets,
            built_validity,
        )
        .into_series())
    }
}
