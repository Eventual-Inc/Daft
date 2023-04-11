use crate::{
    array::{BaseArray, DataArray},
    datatypes::{
        BinaryArray, BooleanArray, DaftIntegerType, DaftNumericType, NullArray, PythonArray,
        Utf8Array,
    },
    error::DaftResult,
};

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    #[inline]
    pub fn get(&self, idx: usize) -> Option<T::Native> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.downcast();
        let is_valid = match arrow_array.validity() {
            Some(validity) => validity.get_bit(idx),
            None => true,
        };
        if is_valid {
            Some(unsafe { arrow_array.value_unchecked(idx) })
        } else {
            None
        }
    }

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.downcast())?;
        Self::try_from((self.name(), result))
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v}")),
        }
    }
}

impl Utf8Array {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&str> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.downcast();
        let is_valid = match arrow_array.validity() {
            Some(validity) => validity.get_bit(idx),
            None => true,
        };
        if is_valid {
            Some(unsafe { arrow_array.value_unchecked(idx) })
        } else {
            None
        }
    }

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.downcast())?;
        Self::try_from((self.name(), result))
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("\"{v}\"")),
        }
    }
}

impl BooleanArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.downcast();
        let is_valid = match arrow_array.validity() {
            Some(validity) => validity.get_bit(idx),
            None => true,
        };
        if is_valid {
            Some(unsafe { arrow_array.value_unchecked(idx) })
        } else {
            None
        }
    }

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.downcast())?;
        Self::try_from((self.name(), result))
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v}")),
        }
    }
}

impl NullArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<()> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        None
    }

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.downcast())?;
        Self::try_from((self.name(), result))
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        Ok("None".to_string())
    }
}

impl BinaryArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&[u8]> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.downcast();
        let is_valid = match arrow_array.validity() {
            Some(validity) => validity.get_bit(idx),
            None => true,
        };
        if is_valid {
            Some(unsafe { arrow_array.value_unchecked(idx) })
        } else {
            None
        }
    }

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.downcast())?;
        Self::try_from((self.name(), result))
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            // TODO: [RUST-INT] proper display of bytes as string here, preferably similar to how Python displays it
            // See discussion: https://stackoverflow.com/questions/54358833/how-does-bytes-repr-representation-work
            Some(v) => Ok(format!("b\"{:?}\"", v)),
        }
    }
}

impl PythonArray {
    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.downcast())?;
        Self::try_from((self.name(), result))
    }
}
