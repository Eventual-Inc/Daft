use std::{
    f64::consts::LOG10_2,
    fmt::Display,
    ops::{Add, Div, Mul, Rem, Shl, Shr, Sub},
};

use common_error::{DaftError, DaftResult};

use super::DataType;
use crate::utils::supertype::try_get_supertype;

// This is a stopgap to keep this logic separated from the DataTypes themselves
// Once we convert daft-dsl to a root level crate, this logic should move there
pub struct InferDataType<'a>(&'a DataType);

impl<'a> Display for InferDataType<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl<'a> From<&'a DataType> for InferDataType<'a> {
    fn from(value: &'a DataType) -> Self {
        InferDataType(value)
    }
}

impl<'a> AsRef<DataType> for InferDataType<'a> {
    fn as_ref(&self) -> &DataType {
        self.0
    }
}

impl<'a> InferDataType<'a> {
    pub fn logical_op(&self, other: &Self) -> DaftResult<DataType> {
        // Whether a logical op (and, or, xor) is supported between the two types.
        let left = self.0;
        let other = other.0;
        match (left, other) {
            #[cfg(feature = "python")]
            (DataType::Python, _) | (_, DataType::Python) => Ok(DataType::Boolean),
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Boolean, DataType::Null)
            | (DataType::Null, DataType::Boolean) => Ok(DataType::Boolean),
            (s, o) if s.is_integer() && o.is_integer() => {
                let dtype = try_numeric_supertype(s, o)?;
                if dtype.is_floating() {
                    Err(DaftError::TypeError(format!(
                        "Cannot perform logic on types: {}, {}",
                        left, other
                    )))
                } else {
                    Ok(dtype)
                }
            }
            (s, o) if (s.is_integer() && o.is_null()) => Ok(s.clone()),
            (s, o) if (s.is_null() && o.is_integer()) => Ok(o.clone()),
            _ => Err(DaftError::TypeError(format!(
                "Cannot perform logic on types: {}, {}",
                left, other
            ))),
        }
    }

    pub fn comparison_op(
        &self,
        other: &Self,
    ) -> DaftResult<(DataType, Option<DataType>, DataType)> {
        // Whether a comparison op is supported between the two types.
        // Returns:
        // - the output type,
        // - an optional intermediate type
        // - the type at which the comparison should be performed.

        let left = &self.0;
        let other = &other.0;
        let evaluator = || match (left, other) {
            (s, o) if s == o => Ok((DataType::Boolean, None, s.to_physical())),
            (DataType::Utf8, o) | (o, DataType::Utf8) if o.is_numeric() => {
                Err(DaftError::TypeError(format!(
                    "Cannot perform comparison on DataType::Utf8 and numeric type.\ntypes: {}, {}",
                    left, other
                )))
            }
            (DataType::Timestamp(..), DataType::Timestamp(..)) => {
                let intermediate_type = try_get_supertype(left, other)?;
                let pt = intermediate_type.to_physical();
                Ok((DataType::Boolean, Some(intermediate_type), pt))
            }
            (DataType::Timestamp(..), DataType::Date)
            | (DataType::Date, DataType::Timestamp(..)) => {
                let intermediate_type = DataType::Date;
                let pt = intermediate_type.to_physical();
                Ok((DataType::Boolean, Some(intermediate_type), pt))
            }

            (DataType::Decimal128(..), other) if other.is_integer() => {
                self.comparison_op(&InferDataType::from(&integer_to_decimal128(other)?))
            }
            (left, DataType::Decimal128(..)) if left.is_integer() => {
                InferDataType::from(&integer_to_decimal128(left)?)
                    .comparison_op(&InferDataType::from(*other))
            }
            (DataType::Decimal128(..), DataType::Float32 | DataType::Float64)
            | (DataType::Float32 | DataType::Float64, DataType::Decimal128(..)) => Ok((
                DataType::Boolean,
                Some(DataType::Float64),
                DataType::Float64,
            )),
            (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
                let s_max = *std::cmp::max(s1, s2);
                let p_prime = std::cmp::max(p1 - s1, p2 - s2) + s_max;

                let d_type = if !(1..=38).contains(&p_prime) {
                    Err(DaftError::TypeError(
                        format!("Cannot infer supertypes for comparison on types: {}, {} result precision: {p_prime} exceed bounds of [1, 38]", self, other)
                    ))
                } else if s_max > 38 {
                    Err(DaftError::TypeError(
                        format!("Cannot infer supertypes for comparison on types: {}, {} result scale: {s_max} exceed bounds of [0, 38]", self, other)
                    ))
                } else if s_max > p_prime {
                    Err(DaftError::TypeError(
                        format!("Cannot infer supertypes for comparison on types: {}, {} result scale: {s_max} exceed precision {p_prime}", self, other)
                    ))
                } else {
                    Ok(DataType::Decimal128(p_prime, s_max))
                }?;

                Ok((DataType::Boolean, Some(d_type.clone()), d_type))
            }

            (DataType::Utf8, DataType::Date) | (DataType::Date, DataType::Utf8) => {
                // Date is logical, so we cast to intermediate type (date), then compare on the physical type (i32)
                Ok((DataType::Boolean, Some(DataType::Date), DataType::Int32))
            }
            (s, o) if s.is_physical() && o.is_physical() => {
                Ok((DataType::Boolean, None, try_physical_supertype(s, o)?))
            }
            _ => Err(DaftError::TypeError(format!(
                "Cannot perform comparison on types: {}, {}",
                left, other
            ))),
        };

        evaluator().map_err(|err| {
            DaftError::TypeError(format!(
                "Cannot perform comparison on types: {}, {}\nDetails:\n{err}",
                left, other
            ))
        })
    }
    pub fn membership_op(
        &self,
        other: &Self,
    ) -> DaftResult<(DataType, Option<DataType>, DataType)> {
        // membership checks (is_in) use equality checks, so we can use the same logic as comparison ops.
        self.comparison_op(other)
    }

    pub fn floor_div(&self, other: &Self) -> DaftResult<DataType> {
        try_numeric_supertype(self.0, other.0).or(match (self.0, other.0) {
            #[cfg(feature = "python")]
            (DataType::Python, _) | (_, DataType::Python) => Ok(DataType::Python),
            _ => Err(DaftError::TypeError(format!(
                "Cannot perform floor divide on types: {}, {}",
                self, other
            ))),
        })
    }
}

impl<'a> Add for InferDataType<'a> {
    type Output = DaftResult<DataType>;

    fn add(self, other: Self) -> Self::Output {
        try_numeric_supertype(self.0, other.0).or_else(|_| try_fixed_shape_numeric_datatype(self.0, other.0, |l, r| {InferDataType::from(l) + InferDataType::from(r)})).or(
            match (self.0, other.0) {
                // --- Python + Python = Python ---
                #[cfg(feature = "python")]
                (DataType::Python, _) | (_, DataType::Python) => Ok(DataType::Python),
                // --- Timestamp + Duration = Timestamp ---
                (DataType::Timestamp(t_unit, tz), DataType::Duration(d_unit))
                | (DataType::Duration(d_unit), DataType::Timestamp(t_unit, tz))
                    if t_unit == d_unit => Ok(DataType::Timestamp(*t_unit, tz.clone())),
                    (ts @ DataType::Timestamp(..), du @ DataType::Duration(..))
                    | (du @ DataType::Duration(..), ts @ DataType::Timestamp(..)) => Err(DaftError::TypeError(
                    format!("Cannot add due to differing precision: {}, {}. Please explicitly cast to the precision you wish to add in.", ts, du)
                )),
                // --- Date & Duration = Date ---
                (DataType::Date, DataType::Duration(..)) | (DataType::Duration(..), DataType::Date) => Ok(DataType::Date),
                // --- Duration + Duration = Duration ---
                (DataType::Duration(d_unit_self), DataType::Duration(d_unit_other)) if d_unit_self == d_unit_other => {
                    Ok(DataType::Duration(*d_unit_self))
                },
                // --------
                // Duration + other
                // --------
                (du_self @ &DataType::Duration(..), du_other @ &DataType::Duration(..)) => Err(DaftError::TypeError(
                    format!("Cannot add due to differing precision: {}, {}. Please explicitly cast to the precision you wish to add in.", du_self, du_other)
                )),
                // --------
                // Nulls + other
                // --------
                (dtype @ DataType::Null, other) | (other, dtype @ DataType::Null) => {
                    match other {
                        // Condition is for backwards compatibility. TODO: remove
                        DataType::Binary | DataType::FixedSizeBinary(..) | DataType::Date => Err(DaftError::TypeError(
                            format!("Cannot add types: {}, {}", dtype, other)
                        )),
                        other if other.is_physical() => Ok(other.clone()),
                        _ => Err(DaftError::TypeError(
                            format!("Cannot add types: {}, {}", dtype, other)
                        )),
                    }
                }
                // --------
                // Utf8 + other
                // --------
                (dtype @ DataType::Utf8, other) | (other, dtype @ DataType::Utf8) => {
                    match other {
                        // DataType::Date condition is for backwards compatibility. TODO: remove
                        DataType::Binary | DataType::FixedSizeBinary(..) | DataType::Date => Err(DaftError::TypeError(
                            format!("Cannot add types: {}, {}", dtype, other)
                        )),
                        other if other.is_physical() => Ok(DataType::Utf8),
                        _ => Err(DaftError::TypeError(
                            format!("Cannot add types: {}, {}", dtype, other)
                        )),
                    }
                },
                // ---- Interval + temporal ----
                (DataType::Interval, dtype) | (dtype, DataType::Interval) if dtype.is_temporal() => Ok(dtype.clone()),
                // ---- Boolean + other ----
                (DataType::Boolean, other) | (other, DataType::Boolean)
                    if other.is_numeric() => Ok(other.clone()),

                (DataType::Decimal128(..), other) if other.is_integer() => self.add(InferDataType::from(&integer_to_decimal128(other)?)),
                (left, DataType::Decimal128(..)) if left.is_integer() => InferDataType::from(&integer_to_decimal128(left)?).add(other),
                (DataType::Decimal128(..), DataType::Float32 | DataType::Float64 ) | (DataType::Float32 | DataType::Float64, DataType::Decimal128(..)) => Ok(DataType::Float64),
                (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
                    let s_max = *std::cmp::max(s1, s2);
                    let p_prime = std::cmp::max(p1 - s1, p2 - s2) + s_max + 1;


                    if !(1..=38).contains(&p_prime) {
                        Err(DaftError::TypeError(
                            format!("Cannot infer supertypes for addition on types: {}, {} result precision: {p_prime} exceed bounds of [1, 38]", self, other)
                        ))
                    } else if s_max > 38 {
                        Err(DaftError::TypeError(
                            format!("Cannot infer supertypes for addition on types: {}, {} result scale: {s_max} exceed bounds of [0, 38]", self, other)
                        ))
                    } else if s_max > p_prime {
                        Err(DaftError::TypeError(
                            format!("Cannot infer supertypes for addition on types: {}, {} result scale: {s_max} exceed precision {p_prime}", self, other)
                        ))
                    } else {
                        Ok(DataType::Decimal128(p_prime, s_max))
                    }
                }
                _ => Err(DaftError::TypeError(
                    format!("Cannot infer supertypes for addition on types: {}, {}", self, other)
                ))
            }
        )
    }
}

impl<'a> Sub for InferDataType<'a> {
    type Output = DaftResult<DataType>;

    fn sub(self, other: Self) -> Self::Output {
        try_numeric_supertype(self.0, other.0).or_else(|_| try_fixed_shape_numeric_datatype(self.0, other.0, |l, r| {InferDataType::from(l) - InferDataType::from(r)})).or(
            match (self.0, other.0) {
                #[cfg(feature = "python")]
                (DataType::Python, _) | (_, DataType::Python) => Ok(DataType::Python),
                (DataType::Timestamp(t_unit, tz), DataType::Duration(d_unit))
                    if t_unit == d_unit => Ok(DataType::Timestamp(*t_unit, tz.clone())),
                    (ts @ DataType::Timestamp(..), du @ DataType::Duration(..)) => Err(DaftError::TypeError(
                    format!("Cannot subtract due to differing precision: {}, {}. Please explicitly cast to the precision you wish to add in.", ts, du)
                )),
                    (DataType::Timestamp(t_unit_self, tz_self), DataType::Timestamp(t_unit_other, tz_other))
                    if t_unit_self == t_unit_other && tz_self == tz_other => Ok(DataType::Duration(*t_unit_self)),
                (ts @ DataType::Timestamp(..), ts_other @ DataType::Timestamp(..)) => Err(DaftError::TypeError(
                    format!("Cannot subtract due to differing precision or timezone: {}, {}. Please explicitly cast to the precision or timezone you wish to add in.", ts, ts_other)
                )),
                (DataType::Date, DataType::Duration(..)) => Ok(DataType::Date),
                (DataType::Date, DataType::Date) => Ok(DataType::Duration(crate::datatypes::TimeUnit::Seconds)),
                (DataType::Duration(d_unit_self), DataType::Duration(d_unit_other)) if d_unit_self == d_unit_other => {
                    Ok(DataType::Duration(*d_unit_self))
                },
                (du_self @ &DataType::Duration(..), du_other @ &DataType::Duration(..)) => Err(DaftError::TypeError(
                    format!("Cannot subtract due to differing precision: {}, {}. Please explicitly cast to the precision you wish to add in.", du_self, du_other)
                )),
                (DataType::Decimal128(..), other) if other.is_integer() => self.sub(InferDataType::from(&integer_to_decimal128(other)?)),
                (left, DataType::Decimal128(..)) if left.is_integer() => InferDataType::from(&integer_to_decimal128(left)?).sub(other),
                (DataType::Decimal128(..), DataType::Float32 | DataType::Float64 ) | (DataType::Float32 | DataType::Float64, DataType::Decimal128(..)) => Ok(DataType::Float64),
                (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
                    let s_max = *std::cmp::max(s1, s2);
                    let p_prime = std::cmp::max(p1 - s1, p2 - s2) + s_max + 1;
                    if !(1..=38).contains(&p_prime) {
                        Err(DaftError::TypeError(
                            format!("Cannot infer supertypes for subtraction on types: {}, {} result precision: {p_prime} exceed bounds of [1, 38]", self, other)
                        ))
                    } else if s_max > 38 {
                        Err(DaftError::TypeError(
                            format!("Cannot infer supertypes for subtraction on types: {}, {} result scale: {s_max} exceed bounds of [0, 38]", self, other)
                        ))
                    } else if s_max > p_prime {
                        Err(DaftError::TypeError(
                            format!("Cannot infer supertypes for subtraction on types: {}, {} result scale: {s_max} exceed precision {p_prime}", self, other)
                        ))
                    } else {
                        Ok(DataType::Decimal128(p_prime, s_max))
                    }
                }
                (DataType::Interval, dtype) | (dtype, DataType::Interval) if dtype.is_temporal() => Ok(dtype.clone()),
                _ => Err(DaftError::TypeError(
                    format!("Cannot subtract types: {}, {}", self, other)
                ))
            }
        )
    }
}

impl<'a> Div for InferDataType<'a> {
    type Output = DaftResult<DataType>;

    fn div(self, other: Self) -> Self::Output {
        try_fixed_shape_numeric_datatype(self.0, other.0, |l, r| {
            InferDataType::from(l) / InferDataType::from(r)
        }).or_else(|_| {
            match (&self.0, &other.0) {
                #[cfg(feature = "python")]
                (DataType::Python, _) | (_, DataType::Python) => Ok(DataType::Python),
                (DataType::Decimal128(..), right) if right.is_integer() => self.div(InferDataType::from(&integer_to_decimal128(right)?)),
                (left, DataType::Decimal128(..)) if left.is_integer() => InferDataType::from(&integer_to_decimal128(left)?).div(other),
                (DataType::Decimal128(..), DataType::Float32 | DataType::Float64 ) | (DataType::Float32 | DataType::Float64, DataType::Decimal128(..)) => Ok(DataType::Float64),
                (DataType::Decimal128(p1, s1), DataType::Decimal128(_, s2)) => {
                    let s1 = *s1 as i64;
                    let s2 = *s2 as i64;
                    let p1 = *p1 as i64;
                    let s_prime = s1 - s2 + std::cmp::max(6, p1+s2+1);
                    let p_prime = p1 - s1 + s_prime;
                    if !(1..=38).contains(&p_prime) {
                        Err(DaftError::TypeError(
                            format!("Cannot infer supertypes for divide on types: {}, {} result precision: {p_prime} exceed bounds of [1, 38]. scale: {s_prime}", self, other)
                        ))
                    } else if !(0..=38).contains(&s_prime){
                        Err(DaftError::TypeError(
                            format!("Cannot infer supertypes for divide on types: {}, {} result scale: {s_prime} exceed bounds of [0, 38]. precision: {p_prime}", self, other)
                        ))
                    } else if s_prime > p_prime {
                        Err(DaftError::TypeError(
                            format!("Cannot infer supertypes for divide on types: {}, {} result scale: {s_prime} exceed precision {p_prime}", self, other)
                        ))
                    } else {
                        Ok(DataType::Decimal128(p_prime as usize, s_prime as usize))
                    }
                }
                (s, o) if s.is_numeric() && o.is_numeric() => Ok(DataType::Float64),
                (l, r) => Err(DaftError::TypeError(format!(
                    "Cannot divide types: {}, {}",
                    l, r
                ))),
            }

        })
    }
}

impl<'a> Mul for InferDataType<'a> {
    type Output = DaftResult<DataType>;

    fn mul(self, other: Self) -> Self::Output {
        try_numeric_supertype(self.0, other.0)
            .or_else(|_| {
                try_fixed_shape_numeric_datatype(self.0, other.0, |l, r| {
                    InferDataType::from(l) * InferDataType::from(r)
                })
            })
            .or(match (self.0, other.0) {
                #[cfg(feature = "python")]
                (DataType::Python, _) | (_, DataType::Python) => Ok(DataType::Python),
                (DataType::Decimal128(..), other) if other.is_integer() => self.mul(InferDataType::from(&integer_to_decimal128(other)?)),
                (left, DataType::Decimal128(..)) if left.is_integer() => InferDataType::from(&integer_to_decimal128(left)?).mul(other),
                (DataType::Decimal128(..), DataType::Float32) | (DataType::Float32, DataType::Decimal128(..)) => Ok(DataType::Float32),
                (DataType::Decimal128(..), DataType::Float64) | (DataType::Float64, DataType::Decimal128(..)) => Ok(DataType::Float64),
                (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
                    let s_prime = s1 + s2;
                    let p_prime = p1 + p2;
                    if !(1..=38).contains(&p_prime) {
                        Err(DaftError::TypeError(
                            format!("Cannot infer supertypes for multiply on types: {}, {} result precision: {p_prime} exceed bounds of [1, 38]", self, other)
                        ))
                    } else if s_prime > 34 {
                        Err(DaftError::TypeError(
                            format!("Cannot infer supertypes for multiply on types: {}, {} result scale: {s_prime} exceed bounds of [0, 38]", self, other)
                        ))
                    } else if s_prime > p_prime {
                        Err(DaftError::TypeError(
                            format!("Cannot infer supertypes for multiply on types: {}, {} result scale: {s_prime} exceed precision {p_prime}", self, other)
                        ))
                    } else {
                        Ok(DataType::Decimal128(p_prime, s_prime))
                    }
                }
                _ => Err(DaftError::TypeError(format!(
                    "Cannot multiply types: {}, {}",
                    self, other
                ))),
            })
    }
}

impl<'a> Rem for InferDataType<'a> {
    type Output = DaftResult<DataType>;

    fn rem(self, other: Self) -> Self::Output {
        try_integer_widen_for_rem(self.0, other.0)
            .or_else(|_| {
                try_fixed_shape_numeric_datatype(self.0, other.0, |l, r| {
                    InferDataType::from(l) % InferDataType::from(r)
                })
            })
            .or(match (self.0, other.0) {
                #[cfg(feature = "python")]
                (DataType::Python, _) | (_, DataType::Python) => Ok(DataType::Python),
                _ => Err(DaftError::TypeError(format!(
                    "Cannot modulo types: {}, {}",
                    self, other
                ))),
            })
    }
}

impl<'a> Shl for InferDataType<'a> {
    type Output = DaftResult<DataType>;

    fn shl(self, rhs: Self) -> Self::Output {
        match (self.0, rhs.0) {
            (s, o) if s.is_integer() && o.is_integer() => Ok(s.clone()),
            _ => Err(DaftError::TypeError(format!(
                "Cannot operate shift left on types: {}, {}",
                self, rhs
            ))),
        }
    }
}

impl<'a> Shr for InferDataType<'a> {
    type Output = DaftResult<DataType>;

    fn shr(self, rhs: Self) -> Self::Output {
        match (self.0, rhs.0) {
            (s, o) if s.is_integer() && o.is_integer() => Ok(s.clone()),
            _ => Err(DaftError::TypeError(format!(
                "Cannot operate shift right on types: {}, {}",
                self, rhs
            ))),
        }
    }
}

pub fn integer_to_decimal128(dtype: &DataType) -> DaftResult<DataType> {
    let constant = LOG10_2;

    let num_bits = match dtype {
        DataType::Int8 | DataType::UInt8 => Ok(8),
        DataType::Int16 | DataType::UInt16 => Ok(16),
        DataType::Int32 | DataType::UInt32 => Ok(32),
        DataType::Int64 | DataType::UInt64 => Ok(64),
        _ => Err(DaftError::TypeError(format!(
            "We can't infer the number of digits for a decimal from a non integer: {}",
            dtype
        ))),
    }?;
    let num_digits = ((num_bits as f64) * constant).ceil() as usize;

    Ok(DataType::Decimal128(num_digits, 0))
}

pub fn try_physical_supertype(l: &DataType, r: &DataType) -> DaftResult<DataType> {
    // Given two physical data types,
    // get the physical data type that they can both be casted to.

    try_numeric_supertype(l, r).or(match (l, r) {
        (DataType::Null, other) | (other, DataType::Null) if other.is_physical() => {
            Ok(other.clone())
        }
        (DataType::Boolean, other) | (other, DataType::Boolean) if other.is_numeric() => {
            Ok(other.clone())
        }
        #[cfg(feature = "python")]
        (DataType::Python, _) | (_, DataType::Python) => Ok(DataType::Python),
        (DataType::Utf8, o) | (o, DataType::Utf8)
            if o.is_physical()
                && !matches!(o, DataType::Binary | DataType::FixedSizeBinary(..)) =>
        {
            Ok(DataType::Utf8)
        }
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
        match (l, r) {
            (DataType::Int8, DataType::Int8) => Some(DataType::Int8),
            (DataType::Int8, DataType::Int16) => Some(DataType::Int16),
            (DataType::Int8, DataType::Int32) => Some(DataType::Int32),
            (DataType::Int8, DataType::Int64) => Some(DataType::Int64),
            (DataType::Int8, DataType::UInt8) => Some(DataType::Int16),
            (DataType::Int8, DataType::UInt16) => Some(DataType::Int32),
            (DataType::Int8, DataType::UInt32) => Some(DataType::Int64),
            (DataType::Int8, DataType::UInt64) => Some(DataType::Float64), // Follow numpy
            (DataType::Int8, DataType::Float32) => Some(DataType::Float32),
            (DataType::Int8, DataType::Float64) => Some(DataType::Float64),

            (DataType::Int16, DataType::Int16) => Some(DataType::Int16),
            (DataType::Int16, DataType::Int32) => Some(DataType::Int32),
            (DataType::Int16, DataType::Int64) => Some(DataType::Int64),
            (DataType::Int16, DataType::UInt8) => Some(DataType::Int16),
            (DataType::Int16, DataType::UInt16) => Some(DataType::Int32),
            (DataType::Int16, DataType::UInt32) => Some(DataType::Int64),
            (DataType::Int16, DataType::UInt64) => Some(DataType::Float64), // Follow numpy
            (DataType::Int16, DataType::Float32) => Some(DataType::Float32),
            (DataType::Int16, DataType::Float64) => Some(DataType::Float64),

            (DataType::Int32, DataType::Int32) => Some(DataType::Int32),
            (DataType::Int32, DataType::Int64) => Some(DataType::Int64),
            (DataType::Int32, DataType::UInt8) => Some(DataType::Int32),
            (DataType::Int32, DataType::UInt16) => Some(DataType::Int32),
            (DataType::Int32, DataType::UInt32) => Some(DataType::Int64),
            (DataType::Int32, DataType::UInt64) => Some(DataType::Float64), // Follow numpy
            (DataType::Int32, DataType::Float32) => Some(DataType::Float64), // Follow numpy
            (DataType::Int32, DataType::Float64) => Some(DataType::Float64),

            (DataType::Int64, DataType::Int64) => Some(DataType::Int64),
            (DataType::Int64, DataType::UInt8) => Some(DataType::Int64),
            (DataType::Int64, DataType::UInt16) => Some(DataType::Int64),
            (DataType::Int64, DataType::UInt32) => Some(DataType::Int64),
            (DataType::Int64, DataType::UInt64) => Some(DataType::Float64), // Follow numpy
            (DataType::Int64, DataType::Float32) => Some(DataType::Float64), // Follow numpy
            (DataType::Int64, DataType::Float64) => Some(DataType::Float64),

            (DataType::UInt8, DataType::UInt8) => Some(DataType::UInt8),
            (DataType::UInt8, DataType::UInt16) => Some(DataType::UInt16),
            (DataType::UInt8, DataType::UInt32) => Some(DataType::UInt32),
            (DataType::UInt8, DataType::UInt64) => Some(DataType::UInt64),
            (DataType::UInt8, DataType::Float32) => Some(DataType::Float32),
            (DataType::UInt8, DataType::Float64) => Some(DataType::Float64),

            (DataType::UInt16, DataType::UInt16) => Some(DataType::UInt16),
            (DataType::UInt16, DataType::UInt32) => Some(DataType::UInt32),
            (DataType::UInt16, DataType::UInt64) => Some(DataType::UInt64),
            (DataType::UInt16, DataType::Float32) => Some(DataType::Float32),
            (DataType::UInt16, DataType::Float64) => Some(DataType::Float64),

            (DataType::UInt32, DataType::UInt32) => Some(DataType::UInt32),
            (DataType::UInt32, DataType::UInt64) => Some(DataType::UInt64),
            (DataType::UInt32, DataType::Float32) => Some(DataType::Float64),
            (DataType::UInt32, DataType::Float64) => Some(DataType::Float64),

            (DataType::UInt64, DataType::UInt64) => Some(DataType::UInt64),
            (DataType::UInt64, DataType::Float32) => Some(DataType::Float64),
            (DataType::UInt64, DataType::Float64) => Some(DataType::Float64),

            (DataType::Float32, DataType::Float32) => Some(DataType::Float32),
            (DataType::Float32, DataType::Float64) => Some(DataType::Float64),

            (DataType::Float64, DataType::Float64) => Some(DataType::Float64),

            _ => None,
        }
    }

    inner(l, r)
        .or_else(|| inner(r, l))
        .ok_or(DaftError::TypeError(format!(
            "Invalid arguments to numeric supertype: {}, {}",
            l, r
        )))
}

pub fn try_integer_widen_for_rem(l: &DataType, r: &DataType) -> DaftResult<DataType> {
    // If given two integer data types,
    // get the integer type that they should both be casted to
    // for the purpose of performing widening.

    fn inner(l: &DataType, r: &DataType) -> Option<DataType> {
        match (l, r) {
            (DataType::Float64, other) | (other, DataType::Float64) if other.is_numeric() => {
                Some(DataType::Float64)
            }
            (DataType::Float32, other) | (other, DataType::Float32) if other.is_numeric() => {
                Some(DataType::Float32)
            }

            (DataType::Int8, DataType::Int8) => Some(DataType::Int8),
            (DataType::Int8, DataType::Int16) => Some(DataType::Int16),
            (DataType::Int8, DataType::Int32) => Some(DataType::Int32),
            (DataType::Int8, DataType::Int64) => Some(DataType::Int64),
            (DataType::Int8, DataType::UInt8) => Some(DataType::UInt8),
            (DataType::Int8, DataType::UInt16) => Some(DataType::UInt16),
            (DataType::Int8, DataType::UInt32) => Some(DataType::UInt32),
            (DataType::Int8, DataType::UInt64) => Some(DataType::UInt64),

            (DataType::Int16, DataType::Int8) => Some(DataType::Int16),
            (DataType::Int16, DataType::Int16) => Some(DataType::Int16),
            (DataType::Int16, DataType::Int32) => Some(DataType::Int32),
            (DataType::Int16, DataType::Int64) => Some(DataType::Int64),
            (DataType::Int16, DataType::UInt8) => Some(DataType::UInt16),
            (DataType::Int16, DataType::UInt16) => Some(DataType::UInt16),
            (DataType::Int16, DataType::UInt32) => Some(DataType::UInt32),
            (DataType::Int16, DataType::UInt64) => Some(DataType::UInt64),

            (DataType::Int32, DataType::Int8) => Some(DataType::Int32),
            (DataType::Int32, DataType::Int16) => Some(DataType::Int32),
            (DataType::Int32, DataType::Int32) => Some(DataType::Int32),
            (DataType::Int32, DataType::Int64) => Some(DataType::Int64),
            (DataType::Int32, DataType::UInt8) => Some(DataType::UInt32),
            (DataType::Int32, DataType::UInt16) => Some(DataType::UInt32),
            (DataType::Int32, DataType::UInt32) => Some(DataType::UInt32),
            (DataType::Int32, DataType::UInt64) => Some(DataType::UInt64),

            (DataType::Int64, DataType::Int8) => Some(DataType::Int64),
            (DataType::Int64, DataType::Int16) => Some(DataType::Int64),
            (DataType::Int64, DataType::Int32) => Some(DataType::Int64),
            (DataType::Int64, DataType::Int64) => Some(DataType::Int64),
            (DataType::Int64, DataType::UInt8) => Some(DataType::UInt64),
            (DataType::Int64, DataType::UInt16) => Some(DataType::UInt64),
            (DataType::Int64, DataType::UInt32) => Some(DataType::UInt64),
            (DataType::Int64, DataType::UInt64) => Some(DataType::UInt64),

            (DataType::UInt8, DataType::UInt8) => Some(DataType::UInt8),
            (DataType::UInt8, DataType::UInt16) => Some(DataType::UInt16),
            (DataType::UInt8, DataType::UInt32) => Some(DataType::UInt32),
            (DataType::UInt8, DataType::UInt64) => Some(DataType::UInt64),

            (DataType::UInt16, DataType::UInt8) => Some(DataType::UInt16),
            (DataType::UInt16, DataType::UInt16) => Some(DataType::UInt16),
            (DataType::UInt16, DataType::UInt32) => Some(DataType::UInt32),
            (DataType::UInt16, DataType::UInt64) => Some(DataType::UInt64),

            (DataType::UInt32, DataType::UInt8) => Some(DataType::UInt32),
            (DataType::UInt32, DataType::UInt16) => Some(DataType::UInt32),
            (DataType::UInt32, DataType::UInt32) => Some(DataType::UInt32),
            (DataType::UInt32, DataType::UInt64) => Some(DataType::UInt64),

            (DataType::UInt64, DataType::UInt8) => Some(DataType::UInt64),
            (DataType::UInt64, DataType::UInt16) => Some(DataType::UInt64),
            (DataType::UInt64, DataType::UInt32) => Some(DataType::UInt64),
            (DataType::UInt64, DataType::UInt64) => Some(DataType::UInt64),
            _ => None,
        }
    }

    inner(l, r)
        .or_else(|| inner(r, l))
        .ok_or(DaftError::TypeError(format!(
            "Invalid arguments to integer widening: {}, {}",
            l, r
        )))
}

pub fn try_fixed_shape_numeric_datatype<F>(
    l: &DataType,
    r: &DataType,
    inner_f: F,
) -> DaftResult<DataType>
where
    F: Fn(&DataType, &DataType) -> DaftResult<DataType>,
{
    match (l, r) {
        (
            DataType::FixedShapeTensor(ldtype, lshape),
            DataType::FixedShapeTensor(rdtype, rshape),
        ) => {
            if lshape != rshape {
                Err(DaftError::TypeError(format!(
                    "Cannot add types: {}, {} due to shape mismatch",
                    l, r
                )))
            } else if let Ok(result_type) = inner_f(ldtype.as_ref(), rdtype.as_ref())
                && result_type.is_numeric()
            {
                Ok(DataType::FixedShapeTensor(
                    Box::new(result_type),
                    lshape.clone(),
                ))
            } else {
                Err(DaftError::TypeError(format!(
                    "Cannot add types: {}, {}",
                    l, r
                )))
            }
        }
        (DataType::FixedSizeList(ldtype, lsize), DataType::FixedSizeList(rdtype, rsize)) => {
            if lsize != rsize {
                Err(DaftError::TypeError(format!(
                    "Cannot add types: {}, {} due to shape mismatch",
                    l, r
                )))
            } else if let Ok(result_type) = inner_f(ldtype.as_ref(), rdtype.as_ref()) {
                Ok(DataType::FixedSizeList(Box::new(result_type), *lsize))
            } else {
                Err(DaftError::TypeError(format!(
                    "Cannot add types: {}, {}",
                    l, r
                )))
            }
        }
        (DataType::Embedding(ldtype, lsize), DataType::Embedding(rdtype, rsize)) => {
            if lsize != rsize {
                Err(DaftError::TypeError(format!(
                    "Cannot add types: {}, {} due to shape mismatch",
                    l, r
                )))
            } else if let Ok(result_type) = inner_f(ldtype.as_ref(), rdtype.as_ref())
                && result_type.is_numeric()
            {
                Ok(DataType::Embedding(Box::new(result_type), *lsize))
            } else {
                Err(DaftError::TypeError(format!(
                    "Cannot add types: {}, {}",
                    l, r
                )))
            }
        }
        _ => Err(DaftError::TypeError(format!(
            "Invalid arguments to numeric supertype: {}, {}",
            l, r
        ))),
    }
}
