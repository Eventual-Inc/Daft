use std::{
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
            (s, o) if s.is_physical() && o.is_physical() => {
                Ok((DataType::Boolean, None, try_physical_supertype(s, o)?))
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
}

impl<'a> Add for InferDataType<'a> {
    type Output = DaftResult<DataType>;

    fn add(self, other: Self) -> Self::Output {
        try_numeric_supertype(self.0, other.0).or_else(|_| try_fixed_shape_numeric_datatype(self.0, other.0, |l, r| {InferDataType::from(l) + InferDataType::from(r)})).or(
            match (self.0, other.0) {
                #[cfg(feature = "python")]
                (DataType::Python, _) | (_, DataType::Python) => Ok(DataType::Python),
                (DataType::Timestamp(t_unit, tz), DataType::Duration(d_unit))
                | (DataType::Duration(d_unit), DataType::Timestamp(t_unit, tz))
                    if t_unit == d_unit => Ok(DataType::Timestamp(*t_unit, tz.clone())),
                    (ts @ DataType::Timestamp(..), du @ DataType::Duration(..))
                    | (du @ DataType::Duration(..), ts @ DataType::Timestamp(..)) => Err(DaftError::TypeError(
                    format!("Cannot add due to differing precision: {}, {}. Please explicitly cast to the precision you wish to add in.", ts, du)
                )),
                (DataType::Date, DataType::Duration(..)) | (DataType::Duration(..), DataType::Date) => Ok(DataType::Date),
                (DataType::Duration(d_unit_self), DataType::Duration(d_unit_other)) if d_unit_self == d_unit_other => {
                    Ok(DataType::Duration(*d_unit_self))
                },
                (du_self @ &DataType::Duration(..), du_other @ &DataType::Duration(..)) => Err(DaftError::TypeError(
                    format!("Cannot add due to differing precision: {}, {}. Please explicitly cast to the precision you wish to add in.", du_self, du_other)
                )),
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
                }
                (DataType::Boolean, other) | (other, DataType::Boolean)
                    if other.is_numeric() => Ok(other.clone()),
                _ => Err(DaftError::TypeError(
                    format!("Cannot add types: {}, {}", self, other)
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
        match (&self.0, &other.0) {
            #[cfg(feature = "python")]
            (DataType::Python, _) | (_, DataType::Python) => Ok(DataType::Python),
            (s, o) if s.is_numeric() && o.is_numeric() => Ok(DataType::Float64),
            _ => Err(DaftError::TypeError(format!(
                "Cannot divide types: {}, {}",
                self, other
            ))),
        }
        .or_else(|_| {
            try_fixed_shape_numeric_datatype(self.0, other.0, |l, r| {
                InferDataType::from(l) / InferDataType::from(r)
            })
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
        try_numeric_supertype(self.0, other.0)
            .or_else(|_| {
                try_fixed_shape_numeric_datatype(self.0, other.0, |l, r| {
                    InferDataType::from(l) % InferDataType::from(r)
                })
            })
            .or(match (self.0, other.0) {
                #[cfg(feature = "python")]
                (DataType::Python, _) | (_, DataType::Python) => Ok(DataType::Python),
                _ => Err(DaftError::TypeError(format!(
                    "Cannot multiply types: {}, {}",
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
