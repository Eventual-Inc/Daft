use std::collections::HashSet;

use common_error::{DaftError, DaftResult};

use crate::{datatypes::BooleanArray, IntoSeries, Series};

macro_rules! do_is_in {
    ($name:expr, $data:expr, $items:expr) => {{
        let mut set = HashSet::new();
        for i in 0..$items.len() {
            set.insert($items.get(i));
        }

        let mut result = Vec::new();
        for i in 0..$data.len() {
            result.push(set.contains(&$data.get(i)));
        }

        Ok(BooleanArray::from(($name, result.as_slice())).into_series())
    }};
}

impl Series {
    pub fn is_in(&self, items: &Self) -> DaftResult<Series> {
        let default =
            BooleanArray::from((self.name(), vec![false; self.len()].as_slice())).into_series();
        if items.is_empty() {
            return Ok(default);
        }
        match items.data_type() {
            crate::datatypes::DataType::Null => self.is_null(),
            crate::datatypes::DataType::Boolean => {
                match self.cast(&crate::datatypes::DataType::Boolean) {
                    Ok(data) => {
                        do_is_in!(self.name(), data.bool()?, items.bool()?)
                    }
                    Err(_) => Ok(default),
                }
            }
            crate::datatypes::DataType::Utf8 => {
                match self.cast(&crate::datatypes::DataType::Utf8) {
                    Ok(data) => {
                        do_is_in!(self.name(), data.utf8()?, items.utf8()?)
                    }
                    Err(_) => Ok(default),
                }
            }
            crate::datatypes::DataType::Binary => {
                match self.cast(&crate::datatypes::DataType::Binary) {
                    Ok(data) => {
                        do_is_in!(self.name(), data.binary()?, items.binary()?)
                    }
                    Err(_) => Ok(default),
                }
            }
            crate::datatypes::DataType::Int32 => {
                match self.cast(&crate::datatypes::DataType::Int32) {
                    Ok(data) => {
                        do_is_in!(self.name(), data.i32()?, items.i32()?)
                    }
                    Err(_) => Ok(default),
                }
            }
            crate::datatypes::DataType::UInt32 => {
                match self.cast(&crate::datatypes::DataType::UInt32) {
                    Ok(data) => {
                        do_is_in!(self.name(), data.u32()?, items.u32()?)
                    }
                    Err(_) => Ok(default),
                }
            }
            crate::datatypes::DataType::Int64 => {
                match self.cast(&crate::datatypes::DataType::Int64) {
                    Ok(data) => {
                        do_is_in!(self.name(), data.i64()?, items.i64()?)
                    }
                    Err(_) => Ok(default),
                }
            }
            crate::datatypes::DataType::UInt64 => {
                match self.cast(&crate::datatypes::DataType::UInt64) {
                    Ok(data) => {
                        do_is_in!(self.name(), data.u64()?, items.u64()?)
                    }
                    Err(_) => Ok(default),
                }
            }
            crate::datatypes::DataType::Float64 => {
                match self.cast(&crate::datatypes::DataType::Float64) {
                    Ok(data) => {
                        let data = data.f64()?;
                        let items = items.f64()?;

                        // Do a O(m*n) search for floats because of the precision issues
                        let mut result = Vec::with_capacity(data.len());
                        for i in 0..data.len() {
                            let mut found = false;
                            for j in 0..items.len() {
                                if let (Some(data_value), Some(items_value)) =
                                    (data.get(i), items.get(j))
                                {
                                    if (data_value - items_value).abs() < f64::EPSILON {
                                        found = true;
                                        break;
                                    }
                                } else if data.get(i).is_none() && items.get(j).is_none() {
                                    found = true;
                                    break;
                                }
                            }
                            result.push(found);
                        }
                        Ok(BooleanArray::from((self.name(), result.as_slice())).into_series())
                    }
                    Err(_) => Ok(default),
                }
            }
            // TODO: Implement these once lists of Date/Timestamp can be parsed into Literals
            // crate::datatypes::DataType::Date => {
            //     let data = self.date()?;
            //     let items = items.date()?;
            //     check_is_in!(data, items)
            // }
            // crate::datatypes::DataType::Timestamp(..) => {
            //     let data = self.timestamp()?;
            //     let items = items.timestamp()?;
            //     check_is_in!(data, items)
            // }
            #[cfg(feature = "python")]
            crate::datatypes::DataType::Python => Err(DaftError::TypeError(
                "Python types are not supported for `is_in`".to_string(),
            )),
            _ => Err(DaftError::TypeError(format!(
                "Unsupported data type for `is_in`: {:?}",
                items.data_type()
            ))),
        }
    }
}
