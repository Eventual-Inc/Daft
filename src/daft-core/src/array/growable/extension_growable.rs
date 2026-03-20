use std::sync::Arc;

use common_error::DaftResult;
use daft_schema::{dtype::DataType, field::Field};

use super::Growable;
use crate::{
    array::extension_array::ExtensionArray,
    series::{IntoSeries, Series},
};

pub struct ExtensionGrowable<'a> {
    name: String,
    dtype: DataType,
    physical_growable: Box<dyn Growable + 'a>,
}

impl<'a> ExtensionGrowable<'a> {
    pub fn new(
        name: &str,
        dtype: &DataType,
        arrays: Vec<&'a ExtensionArray>,
        use_validity: bool,
        capacity: usize,
    ) -> Self {
        let DataType::Extension(_, storage_type, _) = dtype else {
            panic!("Expected Extension dtype for ExtensionGrowable, got {dtype}");
        };
        let physical_series: Vec<&Series> = arrays.iter().map(|a| &a.physical).collect();
        let physical_growable =
            super::make_growable(name, storage_type, physical_series, use_validity, capacity);
        Self {
            name: name.to_string(),
            dtype: dtype.clone(),
            physical_growable,
        }
    }
}

impl Growable for ExtensionGrowable<'_> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.physical_growable.extend(index, start, len);
    }

    fn add_nulls(&mut self, additional: usize) {
        self.physical_growable.add_nulls(additional);
    }

    fn build(&mut self) -> DaftResult<Series> {
        let physical = self.physical_growable.build()?;
        let field = Arc::new(Field::new(self.name.as_str(), self.dtype.clone()));
        Ok(ExtensionArray::new(field, physical).into_series())
    }
}
