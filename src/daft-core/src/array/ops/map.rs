use common_error::{DaftError, DaftResult};
use itertools::Itertools;

use crate::{
    array::{ops::DaftCompare, prelude::*},
    datatypes::prelude::*,
    series::Series,
};

fn single_map_get(
    structs: &Series,
    key_to_get: &Series,
    coerce_value: &DataType,
) -> DaftResult<Series> {
    let (keys, values) = {
        let struct_array = structs.struct_()?;
        (struct_array.get("key")?, struct_array.get("value")?)
    };

    let mask = keys.equal(key_to_get)?;
    let filtered = values.filter(&mask)?;

    let filtered = filtered.cast(coerce_value)?;

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
        let DataType::Map {
            value: value_type, ..
        } = self.data_type()
        else {
            return Err(DaftError::TypeError(format!(
                "Expected input to be a map type, got {:?}",
                self.data_type()
            )));
        };

        match key_to_get.len() {
            1 => self.get_single_key(key_to_get, value_type),
            len if len == self.len() => self.get_multiple_keys(key_to_get, value_type),
            _ => Err(DaftError::ValueError(format!(
                "Expected key to have length 1 or length equal to the map length, got {}",
                key_to_get.len()
            ))),
        }
    }

    fn get_single_key(&self, key_to_get: &Series, coerce_value: &DataType) -> DaftResult<Series> {
        let result: Vec<_> = self
            .physical
            .iter()
            .map(|series| match series {
                Some(s) if !s.is_empty() => single_map_get(&s, key_to_get, coerce_value),
                _ => Ok(Series::full_null("value", coerce_value, 1)),
            })
            .try_collect()?;

        let result: Vec<_> = result.iter().collect();

        Series::concat(&result)
    }

    fn get_multiple_keys(
        &self,
        key_to_get: &Series,
        coerce_value: &DataType,
    ) -> DaftResult<Series> {
        let result: Vec<_> = self
            .physical
            .iter()
            .enumerate()
            .map(|(i, series)| match series {
                Some(s) if !s.is_empty() => {
                    single_map_get(&s, &key_to_get.slice(i, i + 1)?, coerce_value)
                }
                _ => Ok(Series::full_null("value", coerce_value, 1)),
            })
            .try_collect()?;

        let result: Vec<_> = result.iter().collect();

        Series::concat(&result)
    }
}
