use std::iter;

use common_error::{DaftError, DaftResult};

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{BinaryArray, DaftIntegerType, DaftNumericType, DataArray, UInt64Array},
};

enum BroadcastedBinaryIter<'a> {
    Repeat(std::iter::Take<std::iter::Repeat<Option<&'a [u8]>>>),
    NonRepeat(
        arrow2::bitmap::utils::ZipValidity<
            &'a [u8],
            arrow2::array::ArrayValuesIter<'a, arrow2::array::BinaryArray<i64>>,
            arrow2::bitmap::utils::BitmapIter<'a>,
        >,
    ),
}

impl<'a> Iterator for BroadcastedBinaryIter<'a> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastedBinaryIter::Repeat(iter) => iter.next(),
            BroadcastedBinaryIter::NonRepeat(iter) => iter.next(),
        }
    }
}

fn create_broadcasted_binary_iter(arr: &BinaryArray, len: usize) -> BroadcastedBinaryIter<'_> {
    if arr.len() == 1 {
        BroadcastedBinaryIter::Repeat(std::iter::repeat(arr.as_arrow().get(0)).take(len))
    } else {
        BroadcastedBinaryIter::NonRepeat(arr.as_arrow().iter())
    }
}

fn create_broadcasted_numeric_iter<'a, T, I>(
    arr: &'a DataArray<T>,
    len: usize,
) -> Box<dyn Iterator<Item = DaftResult<Option<I>>> + 'a>
where
    T: DaftNumericType,
    T::Native: TryInto<I>,
{
    if arr.len() == 1 {
        let val = arr.as_arrow().iter().next().unwrap();
        Box::new(
            iter::repeat(val)
                .take(len)
                .map(|x| -> DaftResult<Option<I>> {
                    x.map(|x| {
                        (*x).try_into().map_err(|_| {
                            DaftError::ComputeError(
                                "Error in slice: failed to cast value".to_string(),
                            )
                        })
                    })
                    .transpose()
                }),
        )
    } else {
        Box::new(arr.as_arrow().iter().map(|x| -> DaftResult<Option<I>> {
            x.map(|x| {
                (*x).try_into().map_err(|_| {
                    DaftError::ComputeError("Error in slice: failed to cast value".to_string())
                })
            })
            .transpose()
        }))
    }
}

impl BinaryArray {
    pub fn length(&self) -> DaftResult<UInt64Array> {
        let self_arrow = self.as_arrow();
        let offsets = self_arrow.offsets();
        let arrow_result = arrow2::array::UInt64Array::from_iter(
            offsets.windows(2).map(|w| Some((w[1] - w[0]) as u64)),
        )
        .with_validity(self_arrow.validity().cloned());
        Ok(UInt64Array::from((self.name(), Box::new(arrow_result))))
    }

    pub fn binary_concat(&self, other: &Self) -> DaftResult<Self> {
        let self_arrow = self.as_arrow();
        let other_arrow = other.as_arrow();

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

    pub fn binary_slice<I, J>(
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
        let length_iter = match length {
            Some(length) => create_broadcasted_numeric_iter::<J, usize>(length, output_len),
            None => Box::new(iter::repeat_with(|| Ok(None))),
        };

        let mut builder = arrow2::array::MutableBinaryArray::<i64>::new();
        let mut validity = arrow2::bitmap::MutableBitmap::new();

        for ((val, start), length) in self_iter.zip(start_iter).zip(length_iter) {
            match (val, start?, length?) {
                (Some(val), Some(start), Some(length)) => {
                    if start >= val.len() || length == 0 {
                        builder.push::<&[u8]>(None);
                        validity.push(false);
                    } else {
                        let end = (start + length).min(val.len());
                        let slice = &val[start..end];
                        builder.push(Some(slice));
                        validity.push(true);
                    }
                }
                (Some(val), Some(start), None) => {
                    if start >= val.len() {
                        builder.push::<&[u8]>(None);
                        validity.push(false);
                    } else {
                        let slice = &val[start..];
                        builder.push(Some(slice));
                        validity.push(true);
                    }
                }
                _ => {
                    builder.push::<&[u8]>(None);
                    validity.push(false);
                }
            }
        }

        Ok(Self::from((self.name(), Box::new(builder.into()))))
    }
}
