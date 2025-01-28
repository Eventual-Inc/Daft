use arrow2::bitmap::Bitmap;
use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::{DataType, Int32Array, UInt64Array},
    series::Series,
    with_match_hashable_daft_types,
};

impl Series {
    fn is_hashable(dtype: &DataType) -> bool {
        matches!(
            dtype,
            DataType::Null
                | DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Decimal128(..)
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::Binary
                | DataType::FixedSizeBinary(_)
                | DataType::List(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Struct(_)
        )
    }

    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let s = self.as_physical()?;
        if !Self::is_hashable(s.data_type()) {
            return Err(DaftError::ValueError(
                "Elements of this type cannot be hashed".to_string(),
            ));
        }
        with_match_hashable_daft_types!(s.data_type(), |$T| {
            let downcasted = s.downcast::<<$T as DaftDataType>::ArrayType>()?;
            downcasted.hash(seed)
        })
    }

    pub fn hash_with_validity(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let hash = self.hash(seed)?;
        let validity = if matches!(self.data_type(), DataType::Null) {
            Some(Bitmap::new_zeroed(self.len()))
        } else {
            self.validity().cloned()
        };
        hash.with_validity(validity)
    }

    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Int8 => self.i8()?.murmur3_32(),
            Int16 => self.i16()?.murmur3_32(),
            Int32 => self.i32()?.murmur3_32(),
            Int64 => self.i64()?.murmur3_32(),
            UInt8 => self.u8()?.murmur3_32(),
            UInt16 => self.u16()?.murmur3_32(),
            UInt32 => self.u32()?.murmur3_32(),
            UInt64 => self.u64()?.murmur3_32(),
            Utf8 => self.utf8()?.murmur3_32(),
            Binary => self.binary()?.murmur3_32(),
            FixedSizeBinary(_) => self.fixed_size_binary()?.murmur3_32(),
            Date => self.date()?.murmur3_32(),
            Time(..) => self.time()?.murmur3_32(),
            Timestamp(..) => self.timestamp()?.murmur3_32(),
            Decimal128(..) => self.decimal128()?.murmur3_32(),
            v => panic!("murmur3 hash not implemented for datatype: {v}"),
        }
    }
}
