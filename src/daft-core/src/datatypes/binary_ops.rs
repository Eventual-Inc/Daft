use std::{
    fmt::{write, Display},
    ops::{Add, Div, Mul, Rem, Shl, Shr, Sub},
};

use common_error::{DaftError, DaftResult};

use crate::{impl_binary_trait_by_reference, utils::supertype::try_get_supertype};

use super::DataType;

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
        use DataType::*;
        let left = self.0;
        let other = other.0;
        match (left, other) {
            #[cfg(feature = "python")]
            (Python, _) | (_, Python) => Ok(Boolean),
            (Boolean, Boolean) | (Boolean, Null) | (Null, Boolean) => Ok(Boolean),
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
        let evaluator = || {
            use DataType::*;
            match (left, other) {
                (s, o) if s == o => Ok((Boolean, None, s.to_physical())),
                (Utf8, o) | (o, Utf8) if o.is_numeric() => Err(DaftError::TypeError(format!(
                    "Cannot perform comparison on Utf8 and numeric type.\ntypes: {}, {}",
                    left, other
                ))),
                (s, o) if s.is_physical() && o.is_physical() => {
                    Ok((Boolean, None, try_physical_supertype(s, o)?))
                }
                (Timestamp(..), Timestamp(..)) => {
                    let intermediate_type = try_get_supertype(left, other)?;
                    let pt = intermediate_type.to_physical();
                    Ok((Boolean, Some(intermediate_type), pt))
                }
                (Timestamp(..), Date) | (Date, Timestamp(..)) => {
                    let intermediate_type = Date;
                    let pt = intermediate_type.to_physical();
                    Ok((Boolean, Some(intermediate_type), pt))
                }
                _ => Err(DaftError::TypeError(format!(
                    "Cannot perform comparison on types: {}, {}",
                    left, other
                ))),
            }
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
        use DataType::*;

        try_numeric_supertype(self.0, other.0).or(try_fixed_shape_numeric_datatype(self.0, other.0, |l, r| {InferDataType::from(l) + InferDataType::from(r)})).or(
            match (self.0, other.0) {
                #[cfg(feature = "python")]
                (Python, _) | (_, Python) => Ok(Python),
                (Timestamp(t_unit, tz), Duration(d_unit))
                | (Duration(d_unit), Timestamp(t_unit, tz))
                    if t_unit == d_unit => Ok(Timestamp(*t_unit, tz.clone())),
                (ts @ Timestamp(..), du @ Duration(..))
                | (du @ Duration(..), ts @ Timestamp(..)) => Err(DaftError::TypeError(
                    format!("Cannot add due to differing precision: {}, {}. Please explicitly cast to the precision you wish to add in.", ts, du)
                )),
                (Date, Duration(..)) | (Duration(..), Date) => Ok(Date),
                (Duration(d_unit_self), Duration(d_unit_other)) if d_unit_self == d_unit_other => {
                    Ok(Duration(*d_unit_self))
                },
                (du_self @ &Duration(..), du_other @ &Duration(..)) => Err(DaftError::TypeError(
                    format!("Cannot add due to differing precision: {}, {}. Please explicitly cast to the precision you wish to add in.", du_self, du_other)
                )),
                (Null, other) | (other, Null) => {
                    match other {
                        // Condition is for backwards compatibility. TODO: remove
                        Binary | FixedSizeBinary(..) | Date => Err(DaftError::TypeError(
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
                        Binary | FixedSizeBinary(..) | Date => Err(DaftError::TypeError(
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

impl<'a> Sub for InferDataType<'a> {
    type Output = DaftResult<DataType>;

    fn sub(self, other: Self) -> Self::Output {
        use DataType::*;
        try_numeric_supertype(self.0, other.0).or(try_fixed_shape_numeric_datatype(self.0, other.0, |l, r| {InferDataType::from(l) - InferDataType::from(r)})).or(
            match (self.0, other.0) {
                #[cfg(feature = "python")]
                (Python, _) | (_, Python) => Ok(Python),
                (Timestamp(t_unit, tz), Duration(d_unit))
                    if t_unit == d_unit => Ok(Timestamp(*t_unit, tz.clone())),
                (ts @ Timestamp(..), du @ Duration(..)) => Err(DaftError::TypeError(
                    format!("Cannot subtract due to differing precision: {}, {}. Please explicitly cast to the precision you wish to add in.", ts, du)
                )),
                (Timestamp(t_unit_self, tz_self), Timestamp(t_unit_other, tz_other))
                    if t_unit_self == t_unit_other && tz_self == tz_other => Ok(Duration(*t_unit_self)),
                (ts @ Timestamp(..), ts_other @ Timestamp(..)) => Err(DaftError::TypeError(
                    format!("Cannot subtract due to differing precision or timezone: {}, {}. Please explicitly cast to the precision or timezone you wish to add in.", ts, ts_other)
                )),
                (Date, Duration(..)) => Ok(Date),
                (Date, Date) => Ok(Duration(crate::datatypes::TimeUnit::Seconds)),
                (Duration(d_unit_self), Duration(d_unit_other)) if d_unit_self == d_unit_other => {
                    Ok(Duration(*d_unit_self))
                },
                (du_self @ &Duration(..), du_other @ &Duration(..)) => Err(DaftError::TypeError(
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
        use DataType::*;
        match (&self.0, &other.0) {
            #[cfg(feature = "python")]
            (Python, _) | (_, Python) => Ok(Python),
            (s, o) if s.is_numeric() && o.is_numeric() => Ok(Float64),
            _ => Err(DaftError::TypeError(format!(
                "Cannot divide types: {}, {}",
                self, other
            ))),
        }
        .or(try_fixed_shape_numeric_datatype(self.0, other.0, |l, r| {
            InferDataType::from(l) / InferDataType::from(r)
        }))
    }
}

impl<'a> Mul for InferDataType<'a> {
    type Output = DaftResult<DataType>;

    fn mul(self, other: Self) -> Self::Output {
        use DataType::*;
        try_numeric_supertype(self.0, other.0)
            .or(try_fixed_shape_numeric_datatype(self.0, other.0, |l, r| {
                InferDataType::from(l) * InferDataType::from(r)
            }))
            .or(match (self.0, other.0) {
                #[cfg(feature = "python")]
                (Python, _) | (_, Python) => Ok(Python),
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
        use DataType::*;
        try_numeric_supertype(self.0, other.0)
            .or(try_fixed_shape_numeric_datatype(self.0, other.0, |l, r| {
                InferDataType::from(l) % InferDataType::from(r)
            }))
            .or(match (self.0, other.0) {
                #[cfg(feature = "python")]
                (Python, _) | (_, Python) => Ok(Python),
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

    use DataType::*;
    try_numeric_supertype(l, r).or(match (l, r) {
        (Null, other) | (other, Null) if other.is_physical() => Ok(other.clone()),
        (Boolean, other) | (other, Boolean) if other.is_numeric() => Ok(other.clone()),
        #[cfg(feature = "python")]
        (Python, _) | (_, Python) => Ok(Python),
        (Utf8, o) | (o, Utf8) if o.is_physical() && !matches!(o, Binary | FixedSizeBinary(..)) => {
            Ok(Utf8)
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

pub fn try_fixed_shape_numeric_datatype<F>(
    l: &DataType,
    r: &DataType,
    inner_f: F,
) -> DaftResult<DataType>
where
    F: Fn(&DataType, &DataType) -> DaftResult<DataType>,
{
    use DataType::*;

    match (l, r) {
        (FixedShapeTensor(ldtype, lshape), FixedShapeTensor(rdtype, rshape)) => {
            if lshape != rshape {
                Err(DaftError::TypeError(format!(
                    "Cannot add types: {}, {} due to shape mismatch",
                    l, r
                )))
            } else if let Ok(result_type) = inner_f(ldtype.as_ref(), rdtype.as_ref())
                && result_type.is_numeric()
            {
                Ok(FixedShapeTensor(Box::new(result_type), lshape.clone()))
            } else {
                Err(DaftError::TypeError(format!(
                    "Cannot add types: {}, {}",
                    l, r
                )))
            }
        }
        (FixedSizeList(ldtype, lsize), FixedSizeList(rdtype, rsize)) => {
            if lsize != rsize {
                Err(DaftError::TypeError(format!(
                    "Cannot add types: {}, {} due to shape mismatch",
                    l, r
                )))
            } else if let Ok(result_type) = inner_f(ldtype.as_ref(), rdtype.as_ref()) {
                Ok(FixedSizeList(Box::new(result_type), *lsize))
            } else {
                Err(DaftError::TypeError(format!(
                    "Cannot add types: {}, {}",
                    l, r
                )))
            }
        }
        (Embedding(ldtype, lsize), Embedding(rdtype, rsize)) => {
            if lsize != rsize {
                Err(DaftError::TypeError(format!(
                    "Cannot add types: {}, {} due to shape mismatch",
                    l, r
                )))
            } else if let Ok(result_type) = inner_f(ldtype.as_ref(), rdtype.as_ref())
                && result_type.is_numeric()
            {
                Ok(Embedding(Box::new(result_type), *lsize))
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
