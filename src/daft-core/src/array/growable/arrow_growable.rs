use std::{marker::PhantomData, sync::Arc};

use common_error::DaftResult;

use crate::{
    array::{
        ops::{as_arrow::AsArrow, from_arrow::FromArrow},
        DataArray,
    },
    datatypes::{DaftArrowBackedType, DaftDataType, Field},
    with_match_arrow_backed_physical_types, DataType, IntoSeries, Series,
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
    child_growable: Box<dyn Growable + 'a>,
}

impl<'a> ArrowExtensionGrowable<'a> {
    pub fn new(name: String, dtype: &DataType, child_growable: Box<dyn Growable + 'a>) -> Self {
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
        self.child_growable.add_nulls(additional)
    }

    fn build(&mut self) -> DaftResult<Series> {
        let child_series = self.child_growable.build()?;

        // Wrap the child series with the appropriate extension type
        match &self.dtype {
            DataType::Extension(s1, child_dtype, s2) => {
                let arrow_extension_type = arrow2::datatypes::DataType::Extension(
                    s1.clone(),
                    Box::new(child_series.data_type().to_arrow()?),
                    s2.clone(),
                );
                with_match_arrow_backed_physical_types!(child_dtype.as_ref(), |$T| {
                    // TODO: Can we downcast and move here?
                    let child_arrow_array = child_series.downcast::<<$T as DaftDataType>::ArrayType>()?.as_arrow().clone();
                    let child_arrow_array = child_arrow_array.to(arrow_extension_type);
                    Ok(DataArray::<ExtensionType>::new(
                        Arc::new(Field::new(self.name.clone(), self.dtype.clone())),
                        Box::new(child_arrow_array),
                    )?.into_series())
                })
            }
            _ => unreachable!("ArrowExtensionGrowable must have Extension dtype"),
        }
    }
}
