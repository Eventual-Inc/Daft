use std::{marker::PhantomData, sync::Arc};

use crate::{
    datatypes::{DaftLogicalType, DaftPhysicalType, DateType, Field, Int32Type},
    error::DaftResult,
};

use super::{DaftArrowBackedType, DaftIntegerType, DaftNumericType, DataArray, DataType};

pub struct LogicalArray<L: DaftLogicalType> {
    pub field: Arc<Field>,
    pub physical: DataArray<L::PhysicalType>,
    marker_: PhantomData<L>,
}

impl<L: DaftLogicalType + 'static> LogicalArray<L> {
    pub fn new(field: Arc<Field>, physical: DataArray<L::PhysicalType>) -> Self {
        LogicalArray {
            physical: physical,
            field: field,
            marker_: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        self.field.name.as_ref()
    }

    pub fn rename(&self, name: &str) -> Self {
        let new_field = self.field.rename(name);
        let new_array = self.physical.rename(name);
        Self::new(new_field.into(), new_array)
    }

    pub fn field(&self) -> &Field {
        &self.field
    }

    pub fn logical_type(&self) -> &DataType {
        &self.field.dtype
    }

    pub fn physical_type(&self) -> &DataType {
        &self.physical.data_type()
    }

    pub fn len(&self) -> usize {
        self.physical.len()
    }

    pub fn size_bytes(&self) -> DaftResult<usize> {
        self.physical.size_bytes()
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        let new_array = self.physical.slice(start, end)?;
        Ok(Self::new(self.field.clone(), new_array))
    }

    pub fn head(&self, num: usize) -> DaftResult<Self> {
        self.slice(0, num)
    }

    pub fn concat(arrays: &[&Self]) -> DaftResult<Self> {
        if arrays.len() == 0 {
            return Err(crate::error::DaftError::ValueError(format!(
                "Need at least 1 logical array to concat"
            )));
        }
        let physicals: Vec<_> = arrays.iter().map(|a| &a.physical).collect();
        let concatd = DataArray::<L::PhysicalType>::concat(physicals.as_slice())?;
        Ok(Self::new(arrays.first().unwrap().field.clone(), concatd))
    }
}

pub type DateArray = LogicalArray<DateType>;
