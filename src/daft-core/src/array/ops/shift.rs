use common_error::{DaftError, DaftResult};
use num_traits::{NumCast, PrimInt};

use crate::{
    array::DataArray,
    datatypes::{DaftIntegerType, UInt64Type},
};

use super::as_arrow::AsArrow;

impl<T> DataArray<T>
where
    T: DaftIntegerType,
    T::Native: PrimInt,
{
    pub fn shift_left(&self, bits: &DataArray<UInt64Type>) -> DaftResult<Self> {
        let result = match (self.len(), bits.len()) {
            (x, y) if x == y => self
                .as_arrow()
                .iter()
                .zip(bits.as_arrow().iter())
                .map(|(a, b)| {
                    a.zip(b)
                        .map(|(a, b)| {
                            let b: usize = NumCast::from(*b).ok_or_else(|| {
                                DaftError::ValueError("Failed to cast bits to usize".to_string())
                            })?;
                            Ok(*a << b)
                        })
                        .transpose()
                })
                .collect::<DaftResult<Vec<_>>>()?,
            (1, _) => {
                let data = self.get(0).unwrap();
                bits.as_arrow()
                    .iter()
                    .map(|b| match b {
                        Some(b) => {
                            let b: usize = NumCast::from(*b).ok_or_else(|| {
                                DaftError::ValueError("Failed to cast bits to usize".to_string())
                            })?;
                            Ok(Some(data << b))
                        }
                        None => Ok(None),
                    })
                    .collect::<DaftResult<Vec<_>>>()?
            }
            (_, 1) => {
                let bits = bits.get(0).unwrap();
                let b = NumCast::from(bits).ok_or_else(|| {
                    DaftError::ValueError("Failed to cast bits to usize".to_string())
                })?;
                self.as_arrow()
                    .iter()
                    .map(|a| {
                        let a = a?;
                        Some(*a << b)
                    })
                    .collect::<Vec<_>>()
            }
            _ => {
                return Err(DaftError::ValueError(format!(
                    "Expected input and bits to shift to be of length 1, got {} and {}",
                    self.len(),
                    bits.len()
                )))
            }
        };
        Ok(Self::from_iter(self.name(), result.into_iter()))
    }

    pub fn shift_right(&self, bits: &DataArray<UInt64Type>) -> DaftResult<Self> {
        let result = match (self.len(), bits.len()) {
            (x, y) if x == y => self
                .as_arrow()
                .iter()
                .zip(bits.as_arrow().iter())
                .map(|(a, b)| {
                    a.zip(b)
                        .map(|(a, b)| {
                            let b: usize = NumCast::from(*b).ok_or_else(|| {
                                DaftError::ValueError("Failed to cast bits to usize".to_string())
                            })?;
                            Ok(*a >> b)
                        })
                        .transpose()
                })
                .collect::<DaftResult<Vec<_>>>()?,
            (1, _) => {
                let data = self.get(0).unwrap();
                bits.as_arrow()
                    .iter()
                    .map(|b| match b {
                        Some(b) => {
                            let b: usize = NumCast::from(*b).ok_or_else(|| {
                                DaftError::ValueError("Failed to cast bits to usize".to_string())
                            })?;
                            Ok(Some(data >> b))
                        }
                        None => Ok(None),
                    })
                    .collect::<DaftResult<Vec<_>>>()?
            }
            (_, 1) => {
                let bits = bits.get(0).unwrap();
                let b = NumCast::from(bits).ok_or_else(|| {
                    DaftError::ValueError("Failed to cast bits to usize".to_string())
                })?;
                self.as_arrow()
                    .iter()
                    .map(|a| {
                        let a = a?;
                        Some(*a >> b)
                    })
                    .collect::<Vec<_>>()
            }
            _ => {
                return Err(DaftError::ValueError(format!(
                    "Expected input and bits to shift to be of length 1, got {} and {}",
                    self.len(),
                    bits.len()
                )))
            }
        };
        Ok(Self::from_iter(self.name(), result.into_iter()))
    }
}
