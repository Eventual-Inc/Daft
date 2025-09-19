use arrow2::{
    array::{BinaryArray as ArrowBinaryArray, Utf8Array as ArrowUtf8Array},
    bitmap::MutableBitmap,
    datatypes::DataType as ArrowType,
    offset::Offsets,
};
use common_error::DaftResult;
use daft_core::{
    array::ops::as_arrow::AsArrow,
    datatypes::{BinaryArray, FixedSizeBinaryArray},
    prelude::Utf8Array,
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
        let input = self.as_arrow();
        let buffer = input.values();
        let validity = input.validity().cloned();
        //
        let mut values = Vec::<u8>::new();
        let mut offsets = Offsets::<i64>::new();
        for span in input.offsets().windows(2) {
            let s = span[0] as usize;
            let e = span[1] as usize;
            let bytes = transform(&buffer[s..e])?;
            //
            offsets.try_push(bytes.len() as i64)?;
            values.extend(bytes);
        }
        // create daft BinaryArray from the arrow BinaryArray<i64>
        let array = ArrowBinaryArray::new(
            ArrowType::LargeBinary,
            offsets.into(),
            values.into(),
            validity,
        );
        let array = Box::new(array);
        Ok(Self::from((self.name(), array)))
    }

    /// For binary-to-binary transformations, but inserts null on failures.
    fn try_transform<Transform>(&self, transform: Transform) -> DaftResult<Self>
    where
        Transform: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = self.as_arrow();
        let buffer = input.values();
        let mut validity = match input.validity() {
            Some(bitmap) => bitmap.clone().make_mut(),
            None => MutableBitmap::from_len_set(input.len()),
        };
        //
        let mut values = Vec::<u8>::new();
        let mut offsets = Offsets::<i64>::new();
        for (i, span) in input.offsets().windows(2).enumerate() {
            let s = span[0] as usize;
            let e = span[1] as usize;
            match transform(&buffer[s..e]) {
                Ok(bytes) => {
                    offsets.try_push(bytes.len() as i64)?;
                    values.extend(bytes);
                }
                Err(_) => {
                    offsets.try_push(0)?;
                    validity.set(i, false);
                }
            }
        }
        //
        let array = ArrowBinaryArray::new(
            ArrowType::LargeBinary,
            offsets.into(),
            values.into(),
            Some(validity.into()),
        );
        let array = Box::new(array);
        Ok(Self::from((self.name(), array)))
    }

    /// For binary-to-text decoding.
    fn decode<Decoder>(&self, decoder: Decoder) -> DaftResult<Utf8Array>
    where
        Decoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = self.as_arrow();
        let buffer = input.values();
        let validity = input.validity().cloned();

        let mut values = Vec::<u8>::new();
        let mut offsets = Offsets::<i64>::new();

        for span in input.offsets().windows(2) {
            let s = span[0] as usize;
            let e = span[1] as usize;
            let bytes = decoder(&buffer[s..e])?;
            offsets.try_push(bytes.len() as i64)?;
            values.extend(bytes);
        }

        let array = ArrowUtf8Array::new(
            ArrowType::LargeUtf8,
            offsets.into(),
            values.into(),
            validity,
        );
        let array = Box::new(array);
        Ok(Utf8Array::from((self.name(), array)))
    }

    /// For binary-to-text decoding, but inserts null on failures.
    fn try_decode<Decoder>(&self, decoder: Decoder) -> DaftResult<Utf8Array>
    where
        Decoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = self.as_arrow();
        let buffer = input.values();
        let mut validity = match input.validity() {
            Some(bitmap) => bitmap.clone().make_mut(),
            None => MutableBitmap::from_len_set(input.len()),
        };
        //
        let mut values = Vec::<u8>::new();
        let mut offsets = Offsets::<i64>::new();
        for (i, span) in input.offsets().windows(2).enumerate() {
            let s = span[0] as usize;
            let e = span[1] as usize;
            match decoder(&buffer[s..e]) {
                Ok(bytes) => {
                    offsets.try_push(bytes.len() as i64)?;
                    values.extend(bytes);
                }
                Err(_) => {
                    offsets.try_push(0)?;
                    validity.set(i, false);
                }
            }
        }
        //
        let array = ArrowUtf8Array::new(
            ArrowType::LargeUtf8,
            offsets.into(),
            values.into(),
            Some(validity.into()),
        );
        let array = Box::new(array);
        Ok(Utf8Array::from((self.name(), array)))
    }
}

impl BinaryArrayExtension for FixedSizeBinaryArray {
    /// For binary-to-binary transforms (both encode & decode).
    fn transform<Transform>(&self, transform: Transform) -> DaftResult<BinaryArray>
    where
        Transform: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = self.as_arrow();
        let size = input.size();
        let buffer = input.values();
        let chunks = buffer.len() / size;
        let validity = input.validity().cloned();
        //
        let mut values = Vec::<u8>::new();
        let mut offsets = Offsets::<i64>::new();
        for i in 0..chunks {
            let s = i * size;
            let e = s + size;
            let bytes = transform(&buffer[s..e])?;
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

    /// For binary-to-binary transformations, but inserts null on failures.
    fn try_transform<Transform>(&self, transform: Transform) -> DaftResult<BinaryArray>
    where
        Transform: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = self.as_arrow();
        let size = input.size();
        let buffer = input.values();
        let chunks = buffer.len() / size;
        let mut validity = match input.validity() {
            Some(bitmap) => bitmap.clone().make_mut(),
            None => MutableBitmap::from_len_set(input.len()),
        };
        //
        let mut values = Vec::<u8>::new();
        let mut offsets = Offsets::<i64>::new();
        for i in 0..chunks {
            let s = i * size;
            let e = s + size;
            match transform(&buffer[s..e]) {
                Ok(bytes) => {
                    offsets.try_push(bytes.len() as i64)?;
                    values.extend(bytes);
                }
                Err(_) => {
                    offsets.try_push(0)?;
                    validity.set(i, false);
                }
            }
        }
        //
        let array = ArrowBinaryArray::new(
            ArrowType::LargeBinary,
            offsets.into(),
            values.into(),
            Some(validity.into()),
        );
        let array = Box::new(array);
        Ok(BinaryArray::from((self.name(), array)))
    }

    /// For binary-to-text decoding.
    fn decode<Decoder>(&self, decoder: Decoder) -> DaftResult<Utf8Array>
    where
        Decoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = self.as_arrow();
        let size = input.size();
        let buffer = input.values();
        let chunks = buffer.len() / size;
        let validity = input.validity().cloned();

        let mut values = Vec::<u8>::new();
        let mut offsets = Offsets::<i64>::new();

        for i in 0..chunks {
            let s = i * size;
            let e = s + size;
            let bytes = decoder(&buffer[s..e])?;
            offsets.try_push(bytes.len() as i64)?;
            values.extend(bytes);
        }

        let array = ArrowUtf8Array::new(
            ArrowType::LargeUtf8,
            offsets.into(),
            values.into(),
            validity,
        );
        let array = Box::new(array);
        Ok(Utf8Array::from((self.name(), array)))
    }

    /// For binary-to-text decoding, but inserts null on failures.
    fn try_decode<Decoder>(&self, decoder: Decoder) -> DaftResult<Utf8Array>
    where
        Decoder: Fn(&[u8]) -> DaftResult<Vec<u8>>,
    {
        let input = self.as_arrow();
        let size = input.size();
        let buffer = input.values();
        let chunks = buffer.len() / size;
        let mut validity = match input.validity() {
            Some(bitmap) => bitmap.clone().make_mut(),
            None => MutableBitmap::from_len_set(input.len()),
        };

        let mut values = Vec::<u8>::new();
        let mut offsets = Offsets::<i64>::new();
        for i in 0..chunks {
            let s = i * size;
            let e = s + size;
            match decoder(&buffer[s..e]) {
                Ok(bytes) => {
                    offsets.try_push(bytes.len() as i64)?;
                    values.extend(bytes);
                }
                Err(_) => {
                    offsets.try_push(0)?;
                    validity.set(i, false);
                }
            }
        }

        let array = ArrowUtf8Array::new(
            ArrowType::LargeUtf8,
            offsets.into(),
            values.into(),
            Some(validity.into()),
        );
        let array = Box::new(array);
        Ok(Utf8Array::from((self.name(), array)))
    }
}
