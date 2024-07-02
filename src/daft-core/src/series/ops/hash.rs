use crate::{
    datatypes::{Int32Array, UInt64Array},
    series::Series,
    with_match_hashable_daft_types,
};
use common_error::DaftResult;

impl Series {
    pub fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array> {
        let s = self.as_physical()?;
        with_match_hashable_daft_types!(s.data_type(), |$T| {
            let downcasted = s.downcast::<<$T as DaftDataType>::ArrayType>()?;
            downcasted.hash(seed)
        })
    }

    pub fn murmur3_32(&self) -> DaftResult<Int32Array> {
        use crate::DataType::*;
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
