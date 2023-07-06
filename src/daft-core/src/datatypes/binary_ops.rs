use std::ops::{Add, Div, Mul, Rem, Sub};

use common_error::{DaftError, DaftResult};

use crate::impl_binary_trait_by_reference;

use super::DataType;

impl DataType {
    pub fn logical_op(&self, other: &Self) -> DaftResult<DataType> {
        // Whether a logical op (and, or, xor) is supported between the two types.
        use DataType::*;
        match (self, other) {
            #[cfg(feature = "python")]
            (Python, _) | (_, Python) => Ok(()),
            (Boolean, Boolean) | (Boolean, Null) | (Null, Boolean) => Ok(()),
            _ => Err(()),
        }
        .map(|()| Boolean)
        .map_err(|()| {
            DaftError::TypeError(format!(
                "Cannot perform logic on types: {}, {}",
                self, other
            ))
        })
    }
    pub fn comparison_op(&self, other: &Self) -> DaftResult<(DataType, DataType)> {
        // Whether a comparison op is supported between the two types.
        // Returns:
        // - the output type,
        // - the type at which the comparison should be performed.
        use DataType::*;
        match (self, other) {
            // TODO: [ISSUE-688] Make Binary type comparable
            (Binary, _) | (_, Binary) => Err(()),
            (s, o) if s == o => Ok(s.to_physical()),
            (s, o) if s.is_physical() && o.is_physical() => {
                try_physical_supertype(s, o).map_err(|_| ())
            }
            // To maintain existing behaviour. TODO: cleanup
            (Date, o) | (o, Date) if o.is_physical() && o.clone() != Boolean => {
                try_physical_supertype(&Date.to_physical(), o).map_err(|_| ())
            }
            _ => Err(()),
        }
        .map(|comp_type| (Boolean, comp_type))
        .map_err(|()| {
            DaftError::TypeError(format!(
                "Cannot perform comparison on types: {}, {}",
                self, other
            ))
        })
    }
}

impl Add for &DataType {
    type Output = DaftResult<DataType>;

    fn add(self, other: Self) -> Self::Output {
        use DataType::*;
        try_numeric_supertype(self, other).or(
            match (self, other) {
                #[cfg(feature = "python")]
                (Python, _) | (_, Python) => Ok(Python),
                (Timestamp(t_unit, tz), Duration(d_unit))
                | (Duration(d_unit), Timestamp(t_unit, tz))
                    if t_unit == d_unit => Ok(Timestamp(*t_unit, tz.clone())),
                (ts @ Timestamp(..), du @ Duration(..))
                | (du @ Duration(..), ts @ Timestamp(..)) => Err(DaftError::TypeError(
                    format!("Cannot add due to differing precision: {}, {}. Please explicitly cast to the precision you wish to add in.", ts, du)
                )),
                (Null, other) | (other, Null) => {
                    match other {
                        // Condition is for backwards compatibility. TODO: remove
                        Binary | Date => Err(DaftError::TypeError(
                            format!("Cannot add types: {}, {}", self, other)
                        )),
                        other if other.is_physical() => Ok(other.clone()),
                        _ => Err(DaftError::TypeError(
                            format!("Cannot add types: {}, {}", self, other)
                        )),
                    }
                }
                (Utf8, other) | (other, Utf8) => {
                    match other {
                        // Date condition is for backwards compatibility. TODO: remove
                        Binary | Date => Err(DaftError::TypeError(
                            format!("Cannot add types: {}, {}", self, other)
                        )),
                        other if other.is_physical() => Ok(Utf8),
                        _ => Err(DaftError::TypeError(
                            format!("Cannot add types: {}, {}", self, other)
                        )),
                    }
                }
                (Boolean, other) | (other, Boolean)
                    if other.is_numeric() => Ok(other.clone()),
                _ => Err(DaftError::TypeError(
                    format!("Cannot add types: {}, {}", self, other)
                ))
            }
        )
    }
}

impl Sub for &DataType {
    type Output = DaftResult<DataType>;

    fn sub(self, other: Self) -> Self::Output {
        use DataType::*;
        try_numeric_supertype(self, other).or(
            match (self, other) {
                #[cfg(feature = "python")]
                (Python, _) | (_, Python) => Ok(Python),
                (Timestamp(t_unit, tz), Duration(d_unit))
                    if t_unit == d_unit => Ok(Timestamp(*t_unit, tz.clone())),
                (ts @ Timestamp(..), du @ Duration(..)) => Err(DaftError::TypeError(
                    format!("Cannot subtract due to differing precision: {}, {}. Please explicitly cast to the precision you wish to add in.", ts, du)
                )),
                _ => Err(DaftError::TypeError(
                    format!("Cannot subtract types: {}, {}", self, other)
                ))
            }
        )
    }
}

impl Div for &DataType {
    type Output = DaftResult<DataType>;

    fn div(self, other: Self) -> Self::Output {
        use DataType::*;
        match (self, other) {
            #[cfg(feature = "python")]
            (Python, _) | (_, Python) => Ok(Python),
            (s, o) if s.is_numeric() && o.is_numeric() => Ok(Float64),
            _ => Err(DaftError::TypeError(format!(
                "Cannot divide types: {}, {}",
                self, other
            ))),
        }
    }
}

impl Mul for &DataType {
    type Output = DaftResult<DataType>;

    fn mul(self, other: Self) -> Self::Output {
        use DataType::*;
        try_numeric_supertype(self, other).or(match (self, other) {
            #[cfg(feature = "python")]
            (Python, _) | (_, Python) => Ok(Python),
            _ => Err(DaftError::TypeError(format!(
                "Cannot multiply types: {}, {}",
                self, other
            ))),
        })
    }
}

impl Rem for &DataType {
    type Output = DaftResult<DataType>;

    fn rem(self, other: Self) -> Self::Output {
        use DataType::*;
        try_numeric_supertype(self, other).or(match (self, other) {
            #[cfg(feature = "python")]
            (Python, _) | (_, Python) => Ok(Python),
            _ => Err(DaftError::TypeError(format!(
                "Cannot multiply types: {}, {}",
                self, other
            ))),
        })
    }
}

impl_binary_trait_by_reference!(DataType, Add, add);
impl_binary_trait_by_reference!(DataType, Sub, sub);
impl_binary_trait_by_reference!(DataType, Mul, mul);
impl_binary_trait_by_reference!(DataType, Div, div);
impl_binary_trait_by_reference!(DataType, Rem, rem);

pub fn try_physical_supertype(l: &DataType, r: &DataType) -> DaftResult<DataType> {
    // Given two physical data types,
    // get the physical data type that they can both be casted to.

    use DataType::*;
    try_numeric_supertype(l, r).or(match (l, r) {
        (Null, other) | (other, Null) if other.is_physical() => Ok(other.clone()),
        (Boolean, other) | (other, Boolean) if other.is_physical() => Ok(other.clone()),
        #[cfg(feature = "python")]
        (Python, _) | (_, Python) => Ok(Python),
        (Utf8, o) | (o, Utf8) if o.is_physical() => Ok(Utf8),
        _ => Err(DaftError::TypeError(format!(
            "Invalid arguments to try_physical_supertype: {}, {}",
            l, r
        ))),
    })
}

pub fn try_numeric_supertype(l: &DataType, r: &DataType) -> DaftResult<DataType> {
    // If given two numeric data types,
    // get the numeric type that they should both be casted to
    // for the purpose of performing numeric operations.

    fn inner(l: &DataType, r: &DataType) -> Option<DataType> {
        use DataType::*;

        match (l, r) {
            (Int8, Int8) => Some(Int8),
            (Int8, Int16) => Some(Int16),
            (Int8, Int32) => Some(Int32),
            (Int8, Int64) => Some(Int64),
            (Int8, UInt8) => Some(Int16),
            (Int8, UInt16) => Some(Int32),
            (Int8, UInt32) => Some(Int64),
            (Int8, UInt64) => Some(Float64), // Follow numpy
            (Int8, Float32) => Some(Float32),
            (Int8, Float64) => Some(Float64),

            (Int16, Int16) => Some(Int16),
            (Int16, Int32) => Some(Int32),
            (Int16, Int64) => Some(Int64),
            (Int16, UInt8) => Some(Int16),
            (Int16, UInt16) => Some(Int32),
            (Int16, UInt32) => Some(Int64),
            (Int16, UInt64) => Some(Float64), // Follow numpy
            (Int16, Float32) => Some(Float32),
            (Int16, Float64) => Some(Float64),

            (Int32, Int32) => Some(Int32),
            (Int32, Int64) => Some(Int64),
            (Int32, UInt8) => Some(Int32),
            (Int32, UInt16) => Some(Int32),
            (Int32, UInt32) => Some(Int64),
            (Int32, UInt64) => Some(Float64),  // Follow numpy
            (Int32, Float32) => Some(Float64), // Follow numpy
            (Int32, Float64) => Some(Float64),

            (Int64, Int64) => Some(Int64),
            (Int64, UInt8) => Some(Int64),
            (Int64, UInt16) => Some(Int64),
            (Int64, UInt32) => Some(Int64),
            (Int64, UInt64) => Some(Float64),  // Follow numpy
            (Int64, Float32) => Some(Float64), // Follow numpy
            (Int64, Float64) => Some(Float64),

            (UInt8, UInt8) => Some(UInt8),
            (UInt8, UInt16) => Some(UInt16),
            (UInt8, UInt32) => Some(UInt32),
            (UInt8, UInt64) => Some(UInt64),
            (UInt8, Float32) => Some(Float32),
            (UInt8, Float64) => Some(Float64),

            (UInt16, UInt16) => Some(UInt16),
            (UInt16, UInt32) => Some(UInt32),
            (UInt16, UInt64) => Some(UInt64),
            (UInt16, Float32) => Some(Float32),
            (UInt16, Float64) => Some(Float64),

            (UInt32, UInt32) => Some(UInt32),
            (UInt32, UInt64) => Some(UInt64),
            (UInt32, Float32) => Some(Float64),
            (UInt32, Float64) => Some(Float64),

            (UInt64, UInt64) => Some(UInt64),
            (UInt64, Float32) => Some(Float64),
            (UInt64, Float64) => Some(Float64),

            (Float32, Float32) => Some(Float32),
            (Float32, Float64) => Some(Float64),

            (Float64, Float64) => Some(Float64),

            _ => None,
        }
    }

    inner(l, r)
        .or(inner(r, l))
        .ok_or(DaftError::TypeError(format!(
            "Invalid arguments to numeric supertype: {}, {}",
            l, r
        )))
}
