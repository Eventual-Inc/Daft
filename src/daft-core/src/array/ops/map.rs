use common_error::{DaftError, DaftResult};

use crate::{
    array::ops::DaftCompare,
    datatypes::{logical::MapArray, DaftArrayType},
    DataType, Series,
};

fn single_map_get(structs: &Series, key_to_get: &Series) -> DaftResult<Series> {
    let (keys, values) = {
        let struct_array = structs.struct_()?;
        (struct_array.get("key")?, struct_array.get("value")?)
    };
    let mask = keys.equal(key_to_get)?;
    let filtered = values.filter(&mask)?;
    if filtered.is_empty() {
        Ok(Series::full_null("value", values.data_type(), 1))
    } else if filtered.len() == 1 {
        Ok(filtered)
    } else {
        filtered.head(1)
    }
}

impl MapArray {
    pub fn map_get(&self, key_to_get: &Series) -> DaftResult<Series> {
        let value_type = if let DataType::Map(inner_dtype) = self.data_type() {
            match *inner_dtype.clone() {
                DataType::Struct(fields) if fields.len() == 2 => {
                    fields[1].dtype.clone()
                }
                _ => {
                    return Err(DaftError::TypeError(format!(
                        "Expected inner type to be a struct type with two fields: key and value, got {:?}",
                        inner_dtype
                    )))
                }
            }
        } else {
            return Err(DaftError::TypeError(format!(
                "Expected input to be a map type, got {:?}",
                self.data_type()
            )));
        };

        match key_to_get.len() {
            1 => {
                let mut result = Vec::with_capacity(self.len());
                for series in self.physical.into_iter() {
                    match series {
                        Some(s) if !s.is_empty() => result.push(single_map_get(&s, key_to_get)?),
                        _ => result.push(Series::full_null("value", &value_type, 1)),
                    }
                }
                Series::concat(&result.iter().collect::<Vec<_>>())
            }
            len if len == self.len() => {
                let mut result = Vec::with_capacity(len);
                for (i, series) in self.physical.into_iter().enumerate() {
                    match (series, key_to_get.slice(i, i + 1)?) {
                        (Some(s), k) if !s.is_empty() => result.push(single_map_get(&s, &k)?),
                        _ => result.push(Series::full_null("value", &value_type, 1)),
                    }
                }
                Series::concat(&result.iter().collect::<Vec<_>>())
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected key to have length 1 or length equal to the map length, got {}",
                key_to_get.len()
            ))),
        }
    }
}
