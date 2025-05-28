use common_error::DaftResult;

use super::{list_growable::ListGrowable, Growable};
use crate::{
    datatypes::{logical::MapArray, DataType, Field},
    series::{IntoSeries, Series},
};

pub struct MapGrowable<'a> {
    name: String,
    dtype: DataType,
    list_growable: ListGrowable<'a>,
}

impl<'a> MapGrowable<'a> {
    pub fn new(
        name: &str,
        dtype: &DataType,
        arrays: Vec<&'a MapArray>,
        use_validity: bool,
        capacity: usize,
    ) -> Self {
        match dtype {
            DataType::Map { key, value } => {
                let physical_arrays: Vec<&crate::array::ListArray> =
                    arrays.iter().map(|a| &a.physical).collect();

                let list_growable = ListGrowable::new(
                    name,
                    // instead of doing dtype.to_physical(), which will recursively convert all children to physical types,
                    // we just want the top level physical type, which is a list of structs, and the inner dtypes should remain
                    // untouched and dealt with by the struct growable.
                    &DataType::List(Box::new(DataType::Struct(vec![
                        Field::new("key", *key.clone()),
                        Field::new("value", *value.clone()),
                    ]))),
                    physical_arrays,
                    use_validity,
                    capacity,
                    0, // child_capacity - use default for now
                );

                Self {
                    name: name.to_string(),
                    dtype: dtype.clone(),
                    list_growable,
                }
            }
            _ => panic!("Cannot create MapGrowable from dtype: {}", dtype),
        }
    }
}

impl Growable for MapGrowable<'_> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.list_growable.extend(index, start, len);
    }

    fn add_nulls(&mut self, additional: usize) {
        self.list_growable.add_nulls(additional);
    }

    fn build(&mut self) -> DaftResult<Series> {
        let physical_series = self.list_growable.build()?;
        let physical_list = physical_series.list()?;

        let map_array = MapArray::new(
            Field::new(self.name.clone(), self.dtype.clone()),
            physical_list.clone(),
        );
        Ok(map_array.into_series())
    }
}
