use crate::{
    array::{
        growable::{Growable, GrowableArray},
        DataArray,
    },
    datatypes::{DaftArrayType, DaftPhysicalType, DataType},
};

use common_error::{DaftError, DaftResult};

use super::full::FullNull;

pub trait Broadcastable<'a> {
    fn broadcast(&'a self, num: usize) -> DaftResult<Self>
    where
        Self: Sized;
}

fn generic_growable_broadcast<'a, Arr>(
    arr: &'a Arr,
    num: usize,
    name: &'a str,
    dtype: &'a DataType,
) -> DaftResult<Arr>
where
    Arr: DaftArrayType + GrowableArray<'a> + 'static,
{
    let mut growable = Arr::make_growable(name.to_string(), dtype, vec![arr], false, num);
    for _ in 0..num {
        growable.extend(0, 0, 1);
    }
    let series = growable.build()?;
    Ok(series.downcast::<Arr>()?.clone())
}

impl<'a, T> Broadcastable<'a> for DataArray<T>
where
    T: DaftPhysicalType + 'static,
    DataArray<T>: GrowableArray<'a>,
{
    fn broadcast(&'a self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }

        if self.is_valid(0) {
            generic_growable_broadcast(self, num, self.name(), self.data_type())
        } else {
            Ok(DataArray::full_null(self.name(), self.data_type(), num))
        }
    }
}
