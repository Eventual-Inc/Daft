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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        array::{ListArray, StructArray},
        datatypes::{DaftArrayType, DataType, Field, Int64Array, Utf8Array},
        series::IntoSeries,
    };

    fn create_test_map_array(name: &str, keys: Vec<&str>, values: Vec<i64>) -> MapArray {
        assert_eq!(keys.len(), values.len());

        let num_entries = keys.len();

        // Create key and value series using proper from_iter methods
        let key_array = Utf8Array::from_iter("key", keys.into_iter().map(|s| Some(s.to_string())));
        let value_array = Int64Array::from_iter(
            Field::new("value", DataType::Int64),
            values.into_iter().map(Some),
        );

        // Create struct array with key-value pairs
        let struct_array = StructArray::new(
            Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("key", DataType::Utf8),
                    Field::new("value", DataType::Int64),
                ]),
            ),
            vec![key_array.into_series(), value_array.into_series()],
            None,
        );

        // Create list array with one entry containing all key-value pairs
        let list_array = ListArray::new(
            Field::new(
                name,
                DataType::List(Box::new(DataType::Struct(vec![
                    Field::new("key", DataType::Utf8),
                    Field::new("value", DataType::Int64),
                ]))),
            ),
            struct_array.into_series(),
            arrow2::offset::OffsetsBuffer::try_from(vec![0i64, num_entries as i64]).unwrap(),
            None,
        );

        // Create map array
        MapArray::new(
            Field::new(
                name,
                DataType::Map {
                    key: Box::new(DataType::Utf8),
                    value: Box::new(DataType::Int64),
                },
            ),
            list_array,
        )
    }

    fn create_nested_map_array(
        name: &str,
        outer_key: &str,
        inner_key: &str,
        inner_value: i64,
    ) -> MapArray {
        // Create inner map
        let inner_map = create_test_map_array("inner_map", vec![inner_key], vec![inner_value]);

        // Create outer map
        let outer_key_array =
            Utf8Array::from_iter("key", std::iter::once(Some(outer_key.to_string())));
        let outer_struct_array = StructArray::new(
            Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("key", DataType::Utf8),
                    Field::new(
                        "value",
                        DataType::Map {
                            key: Box::new(DataType::Utf8),
                            value: Box::new(DataType::Int64),
                        },
                    ),
                ]),
            ),
            vec![
                outer_key_array.into_series(),
                inner_map.into_series().rename("value"),
            ],
            None,
        );

        let outer_list_array = ListArray::new(
            Field::new(
                name,
                DataType::List(Box::new(DataType::Struct(vec![
                    Field::new("key", DataType::Utf8),
                    Field::new(
                        "value",
                        DataType::Map {
                            key: Box::new(DataType::Utf8),
                            value: Box::new(DataType::Int64),
                        },
                    ),
                ]))),
            ),
            outer_struct_array.into_series(),
            arrow2::offset::OffsetsBuffer::try_from(vec![0i64, 1i64]).unwrap(),
            None,
        );

        MapArray::new(
            Field::new(
                name,
                DataType::Map {
                    key: Box::new(DataType::Utf8),
                    value: Box::new(DataType::Map {
                        key: Box::new(DataType::Utf8),
                        value: Box::new(DataType::Int64),
                    }),
                },
            ),
            outer_list_array,
        )
    }

    fn verify_map_entry(
        map: &crate::datatypes::logical::MapArray,
        index: usize,
        expected_keys: &[&str],
        expected_values: &[i64],
    ) -> DaftResult<()> {
        let entry = map.get(index).unwrap();
        let struct_array = entry.struct_()?;
        let keys_series = struct_array.get("key")?;
        let values_series = struct_array.get("value")?;
        let keys = keys_series.utf8()?;
        let values = values_series.i64()?;

        assert_eq!(keys.len(), expected_keys.len());
        assert_eq!(values.len(), expected_values.len());

        for (i, (expected_key, expected_value)) in
            expected_keys.iter().zip(expected_values.iter()).enumerate()
        {
            assert_eq!(keys.get(i), Some(*expected_key));
            assert_eq!(values.get(i), Some(*expected_value));
        }
        Ok(())
    }

    fn verify_nested_map_entry(
        map: &crate::datatypes::logical::MapArray,
        index: usize,
        outer_key: &str,
        inner_key: &str,
        inner_value: i64,
    ) -> DaftResult<()> {
        let entry = map.get(index).unwrap();
        let struct_array = entry.struct_()?;
        let keys_series = struct_array.get("key")?;
        let values_series = struct_array.get("value")?;
        let keys = keys_series.utf8()?;
        let values_map = values_series.map()?;

        assert_eq!(keys.get(0), Some(outer_key));

        let nested_entry = values_map.get(0).unwrap();
        let nested_struct = nested_entry.struct_()?;
        let nested_keys_series = nested_struct.get("key")?;
        let nested_values_series = nested_struct.get("value")?;
        let nested_keys = nested_keys_series.utf8()?;
        let nested_values = nested_values_series.i64()?;

        assert_eq!(nested_keys.get(0), Some(inner_key));
        assert_eq!(nested_values.get(0), Some(inner_value));
        Ok(())
    }

    #[test]
    fn test_map_growable_basic() -> DaftResult<()> {
        let map1 = create_test_map_array("test_map", vec!["a", "b"], vec![1, 2]);
        let map2 = create_test_map_array("test_map", vec!["c", "d"], vec![3, 4]);

        let map_dtype = DataType::Map {
            key: Box::new(DataType::Utf8),
            value: Box::new(DataType::Int64),
        };

        let mut growable = MapGrowable::new("result", &map_dtype, vec![&map1, &map2], false, 2);
        growable.extend(0, 0, 1);
        growable.extend(1, 0, 1);

        let result = growable.build()?;
        let result_map = result.map()?;

        assert_eq!(result_map.len(), 2);
        assert_eq!(result_map.data_type(), &map_dtype);

        verify_map_entry(result_map, 0, &["a", "b"], &[1, 2])?;
        verify_map_entry(result_map, 1, &["c", "d"], &[3, 4])?;

        Ok(())
    }

    #[test]
    fn test_map_growable_with_nulls() -> DaftResult<()> {
        let map1 = create_test_map_array("test_map", vec!["a"], vec![1]);
        let map2 = create_test_map_array("test_map", vec!["b"], vec![2]);

        let map_dtype = DataType::Map {
            key: Box::new(DataType::Utf8),
            value: Box::new(DataType::Int64),
        };

        let mut growable = MapGrowable::new("result", &map_dtype, vec![&map1, &map2], true, 3);
        growable.extend(0, 0, 1);
        growable.add_nulls(1);
        growable.extend(1, 0, 1);

        let result = growable.build()?;
        let result_map = result.map()?;

        assert_eq!(result_map.len(), 3);
        verify_map_entry(result_map, 0, &["a"], &[1])?;
        assert!(result_map.get(1).is_none());
        verify_map_entry(result_map, 2, &["b"], &[2])?;

        Ok(())
    }

    #[test]
    fn test_map_growable_multiple_extends() -> DaftResult<()> {
        let map1 = create_test_map_array("test_map", vec!["a", "b", "c"], vec![1, 2, 3]);
        let map2 = create_test_map_array("test_map", vec!["d", "e"], vec![4, 5]);

        let map_dtype = DataType::Map {
            key: Box::new(DataType::Utf8),
            value: Box::new(DataType::Int64),
        };

        let mut growable = MapGrowable::new("result", &map_dtype, vec![&map1, &map2], false, 5);
        for _ in 0..3 {
            growable.extend(0, 0, 1);
            growable.extend(1, 0, 1);
        }

        let result = growable.build()?;
        let result_map = result.map()?;

        assert_eq!(result_map.len(), 6);
        for i in [0, 2, 4] {
            verify_map_entry(result_map, i, &["a", "b", "c"], &[1, 2, 3])?;
        }
        for i in [1, 3, 5] {
            verify_map_entry(result_map, i, &["d", "e"], &[4, 5])?;
        }

        Ok(())
    }

    #[test]
    fn test_map_growable_empty() -> DaftResult<()> {
        let map1 = create_test_map_array("test_map", vec![], vec![]);
        let map_dtype = DataType::Map {
            key: Box::new(DataType::Utf8),
            value: Box::new(DataType::Int64),
        };

        let mut growable = MapGrowable::new("result", &map_dtype, vec![&map1], false, 1);
        growable.extend(0, 0, 1);

        let result = growable.build()?;
        let result_map = result.map()?;

        assert_eq!(result_map.len(), 1);
        verify_map_entry(result_map, 0, &[], &[])?;

        Ok(())
    }

    #[test]
    fn test_map_growable_nested_maps() -> DaftResult<()> {
        let nested_map = create_nested_map_array("test_map", "outer_key", "inner_key", 42);

        let map_dtype = DataType::Map {
            key: Box::new(DataType::Utf8),
            value: Box::new(DataType::Map {
                key: Box::new(DataType::Utf8),
                value: Box::new(DataType::Int64),
            }),
        };

        let mut growable = MapGrowable::new("result", &map_dtype, vec![&nested_map], false, 1);
        growable.extend(0, 0, 1);

        let result = growable.build()?;
        let result_map = result.map()?;

        assert_eq!(result_map.len(), 1);
        verify_nested_map_entry(result_map, 0, "outer_key", "inner_key", 42)?;

        Ok(())
    }
}
