use std::marker::PhantomData;

use common_error::DaftResult;

use crate::{
    array::{ops::from_arrow::FromArrow, DataArray},
    datatypes::{DaftArrowBackedType, DaftDataType, ExtensionArray, Field},
    DataType, IntoSeries, Series,
};

use super::Growable;

pub struct ArrowGrowable<'a, T: DaftDataType, G: arrow2::array::growable::Growable<'a>>
where
    T: DaftArrowBackedType,
    DataArray<T>: IntoSeries,
{
    name: String,
    dtype: DataType,
    arrow2_growable: G,
    _phantom: PhantomData<&'a T>,
}

impl<'a, T: DaftDataType, G: arrow2::array::growable::Growable<'a>> ArrowGrowable<'a, T, G>
where
    T: DaftArrowBackedType,
    DataArray<T>: IntoSeries,
{
    pub fn new(name: String, dtype: &DataType, arrow2_growable: G) -> Self {
        Self {
            name,
            dtype: dtype.clone(),
            arrow2_growable,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T: DaftDataType, G: arrow2::array::growable::Growable<'a>> Growable
    for ArrowGrowable<'a, T, G>
where
    T: DaftArrowBackedType,
    DataArray<T>: IntoSeries,
{
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.arrow2_growable.extend(index, start, len);
    }

    fn add_nulls(&mut self, additional: usize) {
        self.arrow2_growable.extend_validity(additional)
    }

    fn build(&mut self) -> DaftResult<Series> {
        let arrow_array = self.arrow2_growable.as_box();
        let field = Field::new(self.name.clone(), self.dtype.clone());
        Ok(DataArray::<T>::from_arrow(&field, arrow_array)?.into_series())
    }
}

pub struct ArrowExtensionGrowable<'a> {
    name: String,
    dtype: DataType,
    child_growable: Box<dyn arrow2::array::growable::Growable<'a> + 'a>,
}

impl<'a> ArrowExtensionGrowable<'a> {
    pub fn new(
        name: String,
        dtype: &DataType,
        child_growable: Box<dyn arrow2::array::growable::Growable<'a> + 'a>,
    ) -> Self {
        assert!(matches!(dtype, DataType::Extension(..)));
        Self {
            name,
            dtype: dtype.clone(),
            child_growable,
        }
    }
}

impl<'a> Growable for ArrowExtensionGrowable<'a> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.child_growable.extend(index, start, len)
    }

    fn add_nulls(&mut self, additional: usize) {
        self.child_growable.extend_validity(additional)
    }

    fn build(&mut self) -> DaftResult<Series> {
        let arr = self.child_growable.as_box();
        let field = Field::new(self.name.clone(), self.dtype.clone());
        Ok(ExtensionArray::from_arrow(&field, arr)?.into_series())
    }
}
