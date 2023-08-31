use common_error::DaftResult;

use crate::{
    array::{growable::make_growable, FixedSizeListArray},
    datatypes::Field,
    DataType, IntoSeries, Series,
};

use super::{bitmap_growable::ArrowBitmapGrowable, Growable};

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