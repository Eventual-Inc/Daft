use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BufferBuilder, LargeBinaryArray, OffsetBufferBuilder};
use common_error::DaftResult;

use crate::{
    datatypes::{DataType, Field},
    prelude::{AsArrow, BinaryArray, Utf8Array},
};

impl Utf8Array {
    /// For text-to-binary encoding.
    pub fn encode<Encoder>(&self, encoder: Encoder) -> DaftResult<BinaryArray>
    where
        Encoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = self.as_arrow()?;
        let buffer = input.values();
        let nulls = input.nulls().cloned();

        let mut values = BufferBuilder::new(buffer.capacity());

        let mut offsets = OffsetBufferBuilder::new(input.len());
        for span in input.offsets().windows(2) {
            let s = span[0] as usize;
            let e = span[1] as usize;
            let bytes = encoder(&buffer[s..e])?;

            offsets.push_length(bytes.len());
            values.extend(bytes);
        }

        let array = LargeBinaryArray::new(offsets.finish(), values.into(), nulls);
        let array: ArrayRef = Arc::new(array);

        let binary_field = Field::new(self.field().name.clone(), DataType::Binary);
        BinaryArray::from_arrow(binary_field, array.into())
    }

    /// For text-to-binary encoding, but inserts nulls on failures.
    pub fn try_encode<Encoder>(&self, _: Encoder) -> DaftResult<BinaryArray>
    where
        Encoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        todo!("try_encode")
    }
}
