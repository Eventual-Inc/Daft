use std::iter;

use arrow2::{
    array::{BinaryArray as ArrowBinaryArray, Utf8Array as ArrowUtf8Array},
    bitmap::MutableBitmap,
    datatypes::DataType as ArrowType,
    offset::Offsets,
};
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::as_arrow::AsArrow,
    datatypes::{
        BinaryArray, DaftIntegerType, DaftNumericType, DataArray, FixedSizeBinaryArray, UInt64Array,
    },
    prelude::{DataType, Utf8Array},
};

use crate::utils::*;

pub trait BinaryArrayExtension: Sized {
    fn length(&self) -> DaftResult<UInt64Array>;
    fn binary_concat(&self, other: &Self) -> DaftResult<Self>;
    fn binary_slice<I, J>(
        &self,
        start: &DataArray<I>,
        length: Option<&DataArray<J>>,
    ) -> DaftResult<BinaryArray>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: Ord + TryInto<usize>,
        J: DaftIntegerType,
        <J as DaftNumericType>::Native: Ord + TryInto<usize>;
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
    fn length(&self) -> DaftResult<UInt64Array> {
        let self_arrow = self.as_arrow();
        let offsets = self_arrow.offsets();
        let arrow_result = arrow2::array::UInt64Array::from_iter(
            offsets.windows(2).map(|w| Some((w[1] - w[0]) as u64)),
        )
        .with_validity(self_arrow.validity().cloned());
        Ok(UInt64Array::from((self.name(), Box::new(arrow_result))))
    }

    fn binary_concat(&self, other: &Self) -> DaftResult<Self> {
        let self_arrow = self.as_arrow();
        let other_arrow = other.as_arrow();

        if self_arrow.len() == 0 || other_arrow.len() == 0 {
            return Ok(Self::from((
                self.name(),
                Box::new(arrow2::array::BinaryArray::<i64>::new_empty(
                    self_arrow.data_type().clone(),
                )),
            )));
        }

        let output_len = if self_arrow.len() == 1 || other_arrow.len() == 1 {
            std::cmp::max(self_arrow.len(), other_arrow.len())
        } else {
            self_arrow.len()
        };

        let self_iter = create_broadcasted_binary_iter(self, output_len);
        let other_iter = create_broadcasted_binary_iter(other, output_len);

        let arrow_result = self_iter
            .zip(other_iter)
            .map(|(left_val, right_val)| match (left_val, right_val) {
                (Some(left), Some(right)) => Some([left, right].concat()),
                _ => None,
            })
            .collect::<arrow2::array::BinaryArray<i64>>();

        Ok(Self::from((self.name(), Box::new(arrow_result))))
    }

    fn binary_slice<I, J>(
        &self,
        start: &DataArray<I>,
        length: Option<&DataArray<J>>,
    ) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: Ord + TryInto<usize>,
        J: DaftIntegerType,
        <J as DaftNumericType>::Native: Ord + TryInto<usize>,
    {
        let self_arrow = self.as_arrow();
        let output_len = if self_arrow.len() == 1 {
            std::cmp::max(start.len(), length.map_or(1, |l| l.len()))
        } else {
            self_arrow.len()
        };

        let self_iter = create_broadcasted_binary_iter(self, output_len);
        let start_iter = create_broadcasted_numeric_iter::<I, usize>(start, output_len);

        let mut builder = arrow2::array::MutableBinaryArray::<i64>::new();

        let arrow_result = match length {
            Some(length) => {
                let length_iter = create_broadcasted_numeric_iter::<J, usize>(length, output_len);

                for ((val, start), length) in self_iter.zip(start_iter).zip(length_iter) {
                    match (val, start?, length?) {
                        (Some(val), Some(start), Some(length)) => {
                            if start >= val.len() || length == 0 {
                                builder.push(Some(&[]));
                            } else {
                                let end = (start + length).min(val.len());
                                let slice = &val[start..end];
                                builder.push(Some(slice));
                            }
                        }
                        _ => {
                            builder.push::<&[u8]>(None);
                        }
                    }
                }
                builder.into()
            }
            None => {
                for (val, start) in self_iter.zip(start_iter) {
                    match (val, start?) {
                        (Some(val), Some(start)) => {
                            if start >= val.len() {
                                builder.push(Some(&[]));
                            } else {
                                let slice = &val[start..];
                                builder.push(Some(slice));
                            }
                        }
                        _ => {
                            builder.push::<&[u8]>(None);
                        }
                    }
                }
                builder.into()
            }
        };

        Ok(Self::from((self.name(), Box::new(arrow_result))))
    }

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
    fn length(&self) -> DaftResult<UInt64Array> {
        let self_arrow = self.as_arrow();
        let size = self_arrow.size();
        let arrow_result = arrow2::array::UInt64Array::from_iter(iter::repeat_n(
            Some(size as u64),
            self_arrow.len(),
        ))
        .with_validity(self_arrow.validity().cloned());
        Ok(UInt64Array::from((self.name(), Box::new(arrow_result))))
    }

    fn binary_concat(&self, other: &Self) -> std::result::Result<Self, DaftError> {
        let self_arrow = self.as_arrow();
        let other_arrow = other.as_arrow();
        let self_size = self_arrow.size();
        let other_size = other_arrow.size();
        let combined_size = self_size + other_size;

        // Create a new FixedSizeBinaryArray with the combined size
        let mut values = Vec::with_capacity(self_arrow.len() * combined_size);
        let mut validity = arrow2::bitmap::MutableBitmap::new();

        let output_len = if self_arrow.len() == 1 || other_arrow.len() == 1 {
            std::cmp::max(self_arrow.len(), other_arrow.len())
        } else {
            self_arrow.len()
        };

        let self_iter = create_broadcasted_fixed_size_binary_iter(self, output_len);
        let other_iter = create_broadcasted_fixed_size_binary_iter(other, output_len);

        for (val1, val2) in self_iter.zip(other_iter) {
            match (val1, val2) {
                (Some(val1), Some(val2)) => {
                    values.extend_from_slice(val1);
                    values.extend_from_slice(val2);
                    validity.push(true);
                }
                _ => {
                    values.extend(std::iter::repeat_n(0u8, combined_size));
                    validity.push(false);
                }
            }
        }

        // Create a new FixedSizeBinaryArray with the combined size
        let result = arrow2::array::FixedSizeBinaryArray::try_new(
            arrow2::datatypes::DataType::FixedSizeBinary(combined_size),
            values.into(),
            Some(validity.into()),
        )?;

        Ok(Self::from((self.name(), Box::new(result))))
    }

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

    fn binary_slice<I, J>(
        &self,
        start: &DataArray<I>,
        length: Option<&DataArray<J>>,
    ) -> DaftResult<BinaryArray>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: Ord + TryInto<usize>,
        J: DaftIntegerType,
        <J as DaftNumericType>::Native: Ord + TryInto<usize>,
    {
        self.cast(&DataType::Binary)?
            .binary()?
            .binary_slice(start, length)
    }
}
