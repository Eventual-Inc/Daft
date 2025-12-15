use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanBufferBuilder, FixedSizeBinaryArray as ArrowFixedSizeBinaryArray,
    LargeBinaryArray, LargeStringArray, OffsetBufferBuilder,
};
use common_error::DaftResult;
use daft_core::{
    array::ops::from_arrow::FromArrow,
    datatypes::{BinaryArray, FixedSizeBinaryArray},
    prelude::{DataType, Field, Utf8Array},
};

pub trait BinaryArrayExtension: Sized {
    fn transform<Transform>(&self, transform: Transform) -> DaftResult<BinaryArray>
    where
        Transform: Fn(&[u8]) -> DaftResult<Vec<u8>>;
    fn try_transform<Transform>(&self, transform: Transform) -> DaftResult<BinaryArray>
    where
        Transform: Fn(&[u8]) -> DaftResult<Vec<u8>>;
    fn decode<Decoder>(&self, decoder: Decoder) -> DaftResult<Utf8Array>
    where
        Decoder: Fn(&[u8]) -> DaftResult<Vec<u8>>;
    fn try_decode<Decoder>(&self, decoder: Decoder) -> DaftResult<Utf8Array>
    where
        Decoder: Fn(&[u8]) -> DaftResult<Vec<u8>>;
}

impl BinaryArrayExtension for BinaryArray {
    fn transform<Transform>(&self, transform: Transform) -> DaftResult<Self>
    where
        Transform: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = LargeBinaryArray::from(self.to_data());
        let buffer = input.values();
        let validity = input.nulls().cloned();

        //
        let mut values = Vec::<u8>::new();
        let mut offsets = OffsetBufferBuilder::new(input.len() + 1);
        for span in input.offsets().windows(2) {
            let s = span[0] as usize;
            let e = span[1] as usize;
            let bytes = transform(&buffer[s..e])?;
            //

            offsets.push_length(bytes.len());
            values.extend(bytes);
        }
        // create daft BinaryArray from the arrow BinaryArray<i64>
        let array = LargeBinaryArray::new(offsets.finish(), values.into(), validity);
        let array: ArrayRef = Arc::new(array);

        Self::from_arrow(self.field.clone(), array.into())
    }

    /// For binary-to-binary transformations, but inserts null on failures.
    fn try_transform<Transform>(&self, transform: Transform) -> DaftResult<Self>
    where
        Transform: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = LargeBinaryArray::from(self.to_data());

        let buffer = input.values();
        let validity = input.nulls().cloned();

        let mut validity = match validity {
            Some(bitmap) => {
                let mut builder = BooleanBufferBuilder::new(input.len());
                for b in &bitmap {
                    builder.append(b);
                }
                builder
            }
            None => {
                let mut builder = BooleanBufferBuilder::new(input.len());
                builder.append_n(input.len(), true);
                builder
            }
        };
        //
        let mut values = Vec::<u8>::new();
        let mut offsets = OffsetBufferBuilder::new(input.len() + 1);
        for (i, span) in input.offsets().windows(2).enumerate() {
            let s = span[0] as usize;
            let e = span[1] as usize;
            match transform(&buffer[s..e]) {
                Ok(bytes) => {
                    offsets.push_length(bytes.len());
                    values.extend(bytes);
                }
                Err(_) => {
                    offsets.push_length(0);
                    validity.set_bit(i, false);
                }
            }
        }
        let array = LargeBinaryArray::new(
            offsets.finish(),
            values.into(),
            Some(validity.finish().into()),
        );
        let array: ArrayRef = Arc::new(array);

        Self::from_arrow(self.field.clone(), array.into())
    }

    /// For binary-to-text decoding.
    fn decode<Decoder>(&self, decoder: Decoder) -> DaftResult<Utf8Array>
    where
        Decoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = LargeBinaryArray::from(self.to_data());

        let buffer = input.values();
        let validity = input.nulls().cloned();

        let mut values = Vec::<u8>::new();
        let mut offsets = OffsetBufferBuilder::new(input.len() + 1);

        for span in input.offsets().windows(2) {
            let s = span[0] as usize;
            let e = span[1] as usize;
            let bytes = decoder(&buffer[s..e])?;
            offsets.push_length(bytes.len());
            values.extend(bytes);
        }

        let array = LargeStringArray::new(offsets.into(), values.into(), validity);
        let array: ArrayRef = Arc::new(array);

        Utf8Array::from_arrow(
            Arc::new(Field::new(self.name(), DataType::Utf8)),
            array.into(),
        )
    }

    /// For binary-to-text decoding, but inserts null on failures.
    fn try_decode<Decoder>(&self, decoder: Decoder) -> DaftResult<Utf8Array>
    where
        Decoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = LargeBinaryArray::from(self.to_data());

        let buffer = input.values();
        let validity = input.nulls().cloned();

        let mut validity = match validity {
            Some(bitmap) => {
                let mut builder = BooleanBufferBuilder::new(input.len());
                for b in &bitmap {
                    builder.append(b);
                }
                builder
            }
            None => {
                let mut builder = BooleanBufferBuilder::new(input.len());
                builder.append_n(input.len(), true);
                builder
            }
        };
        //
        let mut values = Vec::<u8>::new();

        let mut offsets = OffsetBufferBuilder::new(input.len() + 1);
        for (i, span) in input.offsets().windows(2).enumerate() {
            let s = span[0] as usize;
            let e = span[1] as usize;
            match decoder(&buffer[s..e]) {
                Ok(bytes) => {
                    offsets.push_length(bytes.len());
                    values.extend(bytes);
                }
                Err(_) => {
                    offsets.push_length(0);
                    validity.set_bit(i, false);
                }
            }
        }
        let array = LargeStringArray::new(
            offsets.into(),
            values.into(),
            Some(validity.finish().into()),
        );
        let array: ArrayRef = Arc::new(array);

        Utf8Array::from_arrow(
            Arc::new(Field::new(self.name(), DataType::Utf8)),
            array.into(),
        )
    }
}

impl BinaryArrayExtension for FixedSizeBinaryArray {
    /// For binary-to-binary transforms (both encode & decode).
    fn transform<Transform>(&self, transform: Transform) -> DaftResult<BinaryArray>
    where
        Transform: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = ArrowFixedSizeBinaryArray::from(self.to_data());
        let size = input.value_length() as usize;

        let buffer = input.values();
        let chunks = buffer.len() / size;
        let validity = input.nulls().cloned();
        //
        let mut values = Vec::<u8>::new();
        let mut offsets = OffsetBufferBuilder::new(input.len() + 1);

        for i in 0..chunks {
            let s = i * size;
            let e = s + size;
            let bytes = transform(&buffer[s..e])?;
            //
            offsets.push_length(bytes.len());
            values.extend(bytes);
        }
        //
        let array = LargeBinaryArray::new(offsets.finish(), values.into(), validity);
        let array: ArrayRef = Arc::new(array);

        BinaryArray::from_arrow(self.field.clone(), array.into())
    }

    /// For binary-to-binary transformations, but inserts null on failures.
    fn try_transform<Transform>(&self, transform: Transform) -> DaftResult<BinaryArray>
    where
        Transform: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = ArrowFixedSizeBinaryArray::from(self.to_data());
        let size = input.value_length() as usize;
        let buffer = input.values();
        let chunks = buffer.len() / size;
        let validity = input.nulls().cloned();

        let mut validity = match validity {
            Some(bitmap) => {
                let mut builder = BooleanBufferBuilder::new(input.len());
                for b in &bitmap {
                    builder.append(b);
                }
                builder
            }
            None => {
                let mut builder = BooleanBufferBuilder::new(input.len());
                builder.append_n(input.len(), true);
                builder
            }
        };
        //
        let mut values = Vec::<u8>::new();

        let mut offsets = OffsetBufferBuilder::new(input.len() + 1);
        for i in 0..chunks {
            let s = i * size;
            let e = s + size;
            match transform(&buffer[s..e]) {
                Ok(bytes) => {
                    offsets.push_length(bytes.len());
                    values.extend(bytes);
                }
                Err(_) => {
                    offsets.push_length(0);
                    validity.set_bit(i, false);
                }
            }
        }
        let array = LargeBinaryArray::new(
            offsets.finish(),
            values.into(),
            Some(validity.finish().into()),
        );
        let array: ArrayRef = Arc::new(array);

        BinaryArray::from_arrow(self.field.clone(), array.into())
    }

    /// For binary-to-text decoding.
    fn decode<Decoder>(&self, decoder: Decoder) -> DaftResult<Utf8Array>
    where
        Decoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = ArrowFixedSizeBinaryArray::from(self.to_data());
        let size = input.value_length() as usize;
        let buffer = input.values();
        let chunks = buffer.len() / size;
        let validity = input.nulls().cloned();

        let mut values = Vec::<u8>::new();
        let mut offsets = OffsetBufferBuilder::new(input.len() + 1);

        for i in 0..chunks {
            let s = i * size;
            let e = s + size;
            let bytes = decoder(&buffer[s..e])?;
            offsets.push_length(bytes.len());
            values.extend(bytes);
        }
        let array = LargeStringArray::new(offsets.into(), values.into(), validity);
        let array: ArrayRef = Arc::new(array);

        Utf8Array::from_arrow(
            Arc::new(Field::new(self.name(), DataType::Utf8)),
            array.into(),
        )
    }

    /// For binary-to-text decoding, but inserts null on failures.
    fn try_decode<Decoder>(&self, decoder: Decoder) -> DaftResult<Utf8Array>
    where
        Decoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = ArrowFixedSizeBinaryArray::from(self.to_data());
        let size = input.value_length() as usize;
        let buffer = input.values();
        let chunks = buffer.len() / size;
        let validity = input.nulls().cloned();

        let mut validity = match validity {
            Some(bitmap) => {
                let mut builder = BooleanBufferBuilder::new(input.len());
                for b in &bitmap {
                    builder.append(b);
                }
                builder
            }
            None => {
                let mut builder = BooleanBufferBuilder::new(input.len());
                builder.append_n(input.len(), true);
                builder
            }
        };

        let mut values = Vec::<u8>::new();
        let mut offsets = OffsetBufferBuilder::new(input.len() + 1);

        for i in 0..chunks {
            let s = i * size;
            let e = s + size;
            match decoder(&buffer[s..e]) {
                Ok(bytes) => {
                    offsets.push_length(bytes.len());
                    values.extend(bytes);
                }
                Err(_) => {
                    offsets.push_length(0);
                    validity.set_bit(i, false);
                }
            }
        }

        let array = LargeStringArray::new(
            offsets.into(),
            values.into(),
            Some(validity.finish().into()),
        );
        let array: ArrayRef = Arc::new(array);

        Utf8Array::from_arrow(
            Arc::new(Field::new(self.name(), DataType::Utf8)),
            array.into(),
        )
    }
}
