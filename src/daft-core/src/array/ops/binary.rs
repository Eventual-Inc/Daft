use std::iter;

use common_error::{DaftError, DaftResult};

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{BinaryArray, DaftIntegerType, DaftNumericType, DataArray, UInt64Array},
};

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

        let arrow_result = if self_arrow.len() == 1 || other_arrow.len() == 1 {
            // Handle broadcasting case
            let (longer_arr, shorter_arr, is_self_longer) = if self_arrow.len() > other_arrow.len()
            {
                (self_arrow, other_arrow, true)
            } else {
                (other_arrow, self_arrow, false)
            };
            let shorter_val = shorter_arr.value(0);
            longer_arr
                .iter()
                .map(|val| {
                    val.map(|val| {
                        if is_self_longer {
                            [val, shorter_val].concat()
                        } else {
                            [shorter_val, val].concat()
                        }
                    })
                })
                .collect::<arrow2::array::BinaryArray<i64>>()
        } else {
            // Regular case - element-wise concatenation
            self_arrow
                .iter()
                .zip(other_arrow.iter())
                .map(|(left_val, right_val)| match (left_val, right_val) {
                    (Some(left), Some(right)) => Some([left, right].concat()),
                    _ => None,
                })
                .collect::<arrow2::array::BinaryArray<i64>>()
        };

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

        // Handle broadcasting for start
        let start_iter = if start.len() == 1 {
            let start_val = start.as_arrow().iter().next().unwrap();
            Box::new(iter::repeat(start_val).take(self_arrow.len()).map(
                |x| -> DaftResult<Option<usize>> {
                    x.map(|x| {
                        (*x).try_into().map_err(|_| {
                            DaftError::ComputeError(
                                "Error in slice: failed to cast length as usize".to_string(),
                            )
                        })
                    })
                    .transpose()
                },
            )) as Box<dyn Iterator<Item = DaftResult<Option<usize>>>>
        } else {
            Box::new(
                start
                    .as_arrow()
                    .iter()
                    .map(|x| -> DaftResult<Option<usize>> {
                        x.map(|x| {
                            (*x).try_into().map_err(|_| {
                                DaftError::ComputeError(
                                    "Error in slice: failed to cast length as usize".to_string(),
                                )
                            })
                        })
                        .transpose()
                    }),
            ) as Box<dyn Iterator<Item = DaftResult<Option<usize>>>>
        };

        // Handle broadcasting for length
        let length_iter = match length {
            Some(length) => {
                if length.len() == 1 {
                    let length_val = length.as_arrow().iter().next().unwrap();
                    Box::new(iter::repeat(length_val).take(self_arrow.len()).map(
                        |x| -> DaftResult<Option<usize>> {
                            x.map(|x| {
                                (*x).try_into().map_err(|_| {
                                    DaftError::ComputeError(
                                        "Error in slice: failed to cast length as usize"
                                            .to_string(),
                                    )
                                })
                            })
                            .transpose()
                        },
                    )) as Box<dyn Iterator<Item = DaftResult<Option<usize>>>>
                } else {
                    Box::new(
                        length
                            .as_arrow()
                            .iter()
                            .map(|x| -> DaftResult<Option<usize>> {
                                x.map(|x| {
                                    (*x).try_into().map_err(|_| {
                                        DaftError::ComputeError(
                                            "Error in slice: failed to cast length as usize"
                                                .to_string(),
                                        )
                                    })
                                })
                                .transpose()
                            }),
                    ) as Box<dyn Iterator<Item = DaftResult<Option<usize>>>>
                }
            }
            None => Box::new(iter::repeat_with(|| Ok(None)))
                as Box<dyn Iterator<Item = DaftResult<Option<usize>>>>,
        };

        let mut builder = arrow2::array::MutableBinaryArray::<i64>::new();
        let mut validity = arrow2::bitmap::MutableBitmap::new();

        for ((val, start), length) in self_arrow.iter().zip(start_iter).zip(length_iter) {
            match (val, start?, length?) {
                (Some(val), Some(start), Some(length)) => {
                    if start >= val.len() || length == 0 || val.is_empty() {
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
                    if start >= val.len() || val.is_empty() {
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

        let arrow_array = builder.into();
        Ok(Self::from((self.name(), Box::new(arrow_array))))
    }
}
