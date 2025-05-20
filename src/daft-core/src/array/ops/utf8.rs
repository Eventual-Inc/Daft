use arrow2::{
    array::BinaryArray as ArrowBinaryArray, datatypes::DataType as ArrowType, offset::Offsets,
};
use common_error::DaftResult;

use crate::prelude::{AsArrow, BinaryArray, Utf8Array};

impl Utf8Array {
    /// For text-to-binary encoding.
    pub fn encode<Encoder>(&self, encoder: Encoder) -> DaftResult<BinaryArray>
    where
        Encoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = self.as_arrow();
        let buffer = input.values();
        let validity = input.validity().cloned();
        //
        let mut values = Vec::<u8>::new();
        let mut offsets = Offsets::<i64>::new();
        for span in input.offsets().windows(2) {
            let s = span[0] as usize;
            let e = span[1] as usize;
            let bytes = encoder(&buffer[s..e])?;
            //
            offsets.try_push(bytes.len() as i64)?;
            values.extend(bytes);
        }
        //
        let array = ArrowBinaryArray::new(
            ArrowType::LargeBinary,
            offsets.into(),
            values.into(),
            validity,
        );
        let array = Box::new(array);
        Ok(BinaryArray::from((self.name(), array)))
    }

    /// For text-to-binary encoding, but inserts nulls on failures.
    pub fn try_encode<Encoder>(&self, _: Encoder) -> DaftResult<BinaryArray>
    where
        Encoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        todo!("try_encode")
    }
}
