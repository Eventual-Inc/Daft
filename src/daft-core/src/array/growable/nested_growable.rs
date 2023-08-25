use std::mem::swap;

use common_error::DaftResult;

use crate::{
    datatypes::{nested_arrays::FixedSizeListArray, Field},
    with_match_daft_types, DataType, IntoSeries, Series,
};

use super::{Growable, GrowableArray};

pub struct ArrowBitmapGrowable<'a> {
    bitmap_refs: Vec<Option<&'a arrow2::bitmap::Bitmap>>,
    mutable_bitmap: arrow2::bitmap::MutableBitmap,
}

impl<'a> ArrowBitmapGrowable<'a> {
    pub fn new(bitmap_refs: Vec<Option<&'a arrow2::bitmap::Bitmap>>, capacity: usize) -> Self {
        Self {
            bitmap_refs,
            mutable_bitmap: arrow2::bitmap::MutableBitmap::with_capacity(capacity),
        }
    }

    pub fn extend(&mut self, index: usize, start: usize, len: usize) {
        let bm = self.bitmap_refs.get(index).unwrap();
        match bm {
            None => self.mutable_bitmap.extend_constant(len, true),
            Some(bm) => {
                let (bm_data, bm_start, _bm_len) = bm.as_slice();
                self.mutable_bitmap
                    .extend_from_slice(bm_data, bm_start + start, len)
            }
        }
    }

    fn add_nulls(&mut self, additional: usize) {
        self.mutable_bitmap.extend_constant(additional, false)
    }

    fn build(self) -> arrow2::bitmap::Bitmap {
        self.mutable_bitmap.clone().into()
    }
}

pub struct FixedSizeListGrowable<'a> {
    name: String,
    dtype: DataType,
    element_fixed_len: usize,
    child_growable: Box<dyn Growable<'a> + 'a>,
    growable_validity: ArrowBitmapGrowable<'a>,
}

impl<'a> FixedSizeListGrowable<'a> {
    pub fn new(
        name: String,
        dtype: &DataType,
        arrays: Vec<&'a FixedSizeListArray>,
        use_validity: bool,
        capacity: usize,
    ) -> Self {
        match dtype {
            DataType::FixedSizeList(child_field, element_fixed_len) => {
                with_match_daft_types!(&child_field.dtype, |$T| {
                    let child_growable = <<$T as DaftDataType>::ArrayType as GrowableArray>::make_growable(
                        name.clone(),
                        &child_field.dtype,
                        arrays.iter().map(|a| a.flat_child.downcast::<<$T as DaftDataType>::ArrayType>().unwrap()).collect::<Vec<_>>(),
                        use_validity,
                        capacity * element_fixed_len,
                    );
                    let growable_validity = ArrowBitmapGrowable::new(
                        arrays.iter().map(|a| a.validity.as_ref()).collect(),
                        capacity,
                    );
                    Self {
                        name,
                        dtype: dtype.clone(),
                        element_fixed_len: *element_fixed_len,
                        child_growable: Box::new(child_growable),
                        growable_validity,
                    }
                })
            }
            _ => panic!("Cannot create FixedSizeListGrowable from dtype: {}", dtype),
        }
    }
}

impl<'a> Growable<'a> for FixedSizeListGrowable<'a> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.child_growable.extend(
            index,
            start * self.element_fixed_len,
            len * self.element_fixed_len,
        );
        self.growable_validity.extend(index, start, len);
    }

    fn add_nulls(&mut self, additional: usize) {
        self.child_growable
            .add_nulls(additional * self.element_fixed_len);
        self.growable_validity.add_nulls(additional);
    }

    fn build(&mut self) -> DaftResult<Series> {
        // Swap out self.growable_validity so we can use the values and move it
        let mut grown_validity = ArrowBitmapGrowable::new(vec![], 0);
        swap(&mut self.growable_validity, &mut grown_validity);

        let built_child = self.child_growable.build()?;
        let built_validity = grown_validity.build();
        Ok(FixedSizeListArray::new(
            Field::new(self.name.clone(), self.dtype.clone()),
            built_child,
            Some(built_validity),
        )
        .into_series())
    }
}
