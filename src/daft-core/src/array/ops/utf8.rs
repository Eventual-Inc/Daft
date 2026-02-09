use std::sync::Arc;

use arrow::array::{Array, LargeBinaryBuilder};
use common_error::DaftResult;

use crate::prelude::{AsArrow, BinaryArray, Utf8Array};

impl Utf8Array {
    /// For text-to-binary encoding.
    pub fn encode<Encoder>(&self, encoder: Encoder) -> DaftResult<BinaryArray>
    where
        Encoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = self.as_arrow()?;
        let buffer = input.values();
        let nulls = input.nulls().cloned();

        let mut builder = LargeBinaryBuilder::new();

        for span in input.offsets().windows(2) {
            let s = span[0] as usize;
            let e = span[1] as usize;
            let bytes = encoder(&buffer[s..e])?;
            builder.append_value(bytes);
        }

        let array = builder.finish();

        BinaryArray::from_arrow(self.field().clone(), Arc::new(array))?.with_nulls(nulls)
    }

    /// For text-to-binary encoding, but inserts nulls on failures.
    pub fn try_encode<Encoder>(&self, _: Encoder) -> DaftResult<BinaryArray>
    where
        Encoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        todo!("try_encode")
    }
}
