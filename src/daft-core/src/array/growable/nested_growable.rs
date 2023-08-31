use arrow2::types::Index;
use common_error::DaftResult;

use crate::{
    array::{
        fixed_size_list_array::FixedSizeListArray, growable::make_growable, ListArray, StructArray,
    },
    datatypes::Field,
    DataType, IntoSeries, Series,
};

use super::Growable;

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

impl<'a> Default for ArrowBitmapGrowable<'a> {
    fn default() -> Self {
        ArrowBitmapGrowable::new(vec![], 0)
    }
}

pub struct FixedSizeListGrowable<'a> {
    name: String,
    dtype: DataType,
    element_fixed_len: usize,
    child_growable: Box<dyn Growable + 'a>,
    growable_validity: ArrowBitmapGrowable<'a>,
}

impl<'a> FixedSizeListGrowable<'a> {
    pub fn new(
        name: &str,
        dtype: &DataType,
        arrays: Vec<&'a FixedSizeListArray>,
        use_validity: bool,
        capacity: usize,
    ) -> Self {
        match dtype {
            DataType::FixedSizeList(child_field, element_fixed_len) => {
                let child_growable = make_growable(
                    child_field.name.as_str(),
                    &child_field.dtype,
                    arrays.iter().map(|a| &a.flat_child).collect::<Vec<_>>(),
                    use_validity,
                    capacity * element_fixed_len,
                );
                let growable_validity = ArrowBitmapGrowable::new(
                    arrays.iter().map(|a| a.validity()).collect(),
                    capacity,
                );
                Self {
                    name: name.to_string(),
                    dtype: dtype.clone(),
                    element_fixed_len: *element_fixed_len,
                    child_growable,
                    growable_validity,
                }
            }
            _ => panic!("Cannot create FixedSizeListGrowable from dtype: {}", dtype),
        }
    }
}

impl<'a> Growable for FixedSizeListGrowable<'a> {
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
        let grown_validity = std::mem::take(&mut self.growable_validity);

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

pub struct StructGrowable<'a> {
    name: String,
    dtype: DataType,
    children_growables: Vec<Box<dyn Growable + 'a>>,
    growable_validity: ArrowBitmapGrowable<'a>,
}

impl<'a> StructGrowable<'a> {
    pub fn new(
        name: &str,
        dtype: &DataType,
        arrays: Vec<&'a StructArray>,
        use_validity: bool,
        capacity: usize,
    ) -> Self {
        match dtype {
            DataType::Struct(fields) => {
                let children_growables: Vec<Box<dyn Growable>> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        make_growable(
                            f.name.as_str(),
                            &f.dtype,
                            arrays
                                .iter()
                                .map(|a| a.children.get(i).unwrap())
                                .collect::<Vec<_>>(),
                            use_validity,
                            capacity,
                        )
                    })
                    .collect::<Vec<_>>();
                let growable_validity = ArrowBitmapGrowable::new(
                    arrays.iter().map(|a| a.validity()).collect(),
                    capacity,
                );
                Self {
                    name: name.to_string(),
                    dtype: dtype.clone(),
                    children_growables,
                    growable_validity,
                }
            }
            _ => panic!("Cannot create StructGrowable from dtype: {}", dtype),
        }
    }
}

impl<'a> Growable for StructGrowable<'a> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        for child_growable in &mut self.children_growables {
            child_growable.extend(index, start, len)
        }
        self.growable_validity.extend(index, start, len);
    }

    fn add_nulls(&mut self, additional: usize) {
        for child_growable in &mut self.children_growables {
            child_growable.add_nulls(additional);
        }
        self.growable_validity.add_nulls(additional);
    }

    fn build(&mut self) -> DaftResult<Series> {
        let grown_validity = std::mem::take(&mut self.growable_validity);

        let built_children = self
            .children_growables
            .iter_mut()
            .map(|cg| cg.build())
            .collect::<DaftResult<Vec<_>>>()?;
        let built_validity = grown_validity.build();
        Ok(StructArray::new(
            Field::new(self.name.clone(), self.dtype.clone()),
            built_children,
            Some(built_validity),
        )
        .into_series())
    }
}

pub struct ListGrowable<'a> {
    name: String,
    dtype: DataType,
    child_growable: Box<dyn Growable + 'a>,
    child_arrays_offsets: Vec<&'a arrow2::offset::OffsetsBuffer<i64>>,
    growable_validity: ArrowBitmapGrowable<'a>,
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
            DataType::List(child_field) => {
                let child_growable = make_growable(
                    child_field.name.as_str(),
                    &child_field.dtype,
                    arrays.iter().map(|a| &a.flat_child).collect::<Vec<_>>(),
                    use_validity,
                    child_capacity,
                );
                let growable_validity = ArrowBitmapGrowable::new(
                    arrays.iter().map(|a| a.validity()).collect(),
                    capacity,
                );
                let child_arrays_offsets = arrays.iter().map(|arr| arr.offsets()).collect::<Vec<_>>();
                Self {
                    name: name.to_string(),
                    dtype: dtype.clone(),
                    child_growable: child_growable,
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
        self.growable_validity.extend(index, start, len);

        self.growable_offsets
            .try_extend_from_slice(offsets, start, len)
            .unwrap();
    }

    fn add_nulls(&mut self, additional: usize) {
        self.growable_validity.add_nulls(additional);
        self.growable_offsets.extend_constant(additional);
    }

    fn build(&mut self) -> DaftResult<Series> {
        let grown_offsets = std::mem::take(&mut self.growable_offsets);
        let grown_validity = std::mem::take(&mut self.growable_validity);

        let built_child = self.child_growable.build()?;
        let built_validity = grown_validity.build();
        let built_offsets = grown_offsets.into();
        Ok(ListArray::new(
            Field::new(self.name.clone(), self.dtype.clone()),
            built_child,
            built_offsets,
            Some(built_validity),
        )
        .into_series())
    }
}
