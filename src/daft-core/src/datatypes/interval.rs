use std::marker::PhantomData;

use daft_schema::{dtype::DataType, prelude::IntervalUnit};

use super::{
    logical, DaftArrowBackedType, DaftDataType, DaftLogicalType, DaftPhysicalType, DataArray,
    Int32Type,
};

#[derive(Clone, Debug)]
pub struct IntervalType<T>(PhantomData<T>);

#[derive(Clone, Debug)]

/// Value of an IntervalDayTime array
///
/// ## Representation
///
/// This type is stored as a single 64 bit integer, interpreted as two i32
/// fields:
///
/// 1. the number of elapsed days
/// 2. The number of milliseconds (no leap seconds),
///
/// ```text
/// ┌──────────────┬──────────────┐
/// │     Days     │ Milliseconds │
/// │  (32 bits)   │  (32 bits)   │
/// └──────────────┴──────────────┘
/// 0              31            63 bit offset
/// ```
///
/// Please see the [Arrow Spec](https://github.com/apache/arrow/blob/081b4022fe6f659d8765efc82b3f4787c5039e3c/format/Schema.fbs#L406-L408) for more details
///
/// ## Note on Comparing and Ordering for Calendar Types
///
/// Values of `IntervalDayTime` are compared using their binary representation,
/// which can lead to surprising results. Please see the description of ordering on
/// [`IntervalMonthDayNano`] for more details
pub struct DayTimeType {}

impl DaftDataType for DayTimeType {
    #[inline]
    fn get_dtype() -> DataType {
        DataType::Interval(IntervalUnit::DayTime)
    }
    type ArrayType = DataArray<DayTimeType>;
}

impl DaftArrowBackedType for DayTimeType {}
impl DaftPhysicalType for DayTimeType {}

impl DaftDataType for IntervalType<DayTimeType> {
    #[inline]
    fn get_dtype() -> DataType {
        DayTimeType::get_dtype()
    }
    type ArrayType = logical::LogicalArray<IntervalType<DayTimeType>>;
}

impl DaftLogicalType for IntervalType<DayTimeType> {
    type PhysicalType = DayTimeType;
}

/// Value of an IntervalMonthDayNano array
///
///  ## Representation
///
/// This type is stored as a single 128 bit integer, interpreted as three
/// different signed integral fields:
///
/// 1. The number of months (32 bits)
/// 2. The number days (32 bits)
/// 2. The number of nanoseconds (64 bits).
///
/// Nanoseconds does not allow for leap seconds.
///
/// Each field is independent (e.g. there is no constraint that the quantity of
/// nanoseconds represents less than a day's worth of time).
///
/// ```text
/// ┌───────────────┬─────────────┬─────────────────────────────┐
/// │     Months    │     Days    │            Nanos            │
/// │   (32 bits)   │  (32 bits)  │          (64 bits)          │
/// └───────────────┴─────────────┴─────────────────────────────┘
/// 0            32             64                           128 bit offset
/// ```
/// Please see the [Arrow Spec](https://github.com/apache/arrow/blob/081b4022fe6f659d8765efc82b3f4787c5039e3c/format/Schema.fbs#L409-L415) for more details
#[derive(Clone, Debug)]
pub struct MonthDayNanoType {}

impl DaftDataType for MonthDayNanoType {
    #[inline]
    fn get_dtype() -> DataType {
        DataType::Interval(IntervalUnit::MonthDayNano)
    }
    type ArrayType = DataArray<MonthDayNanoType>;
}

impl DaftArrowBackedType for MonthDayNanoType {}
impl DaftPhysicalType for MonthDayNanoType {}

impl DaftDataType for IntervalType<MonthDayNanoType> {
    #[inline]
    fn get_dtype() -> DataType {
        MonthDayNanoType::get_dtype()
    }
    type ArrayType = logical::LogicalArray<IntervalType<MonthDayNanoType>>;
}

impl DaftLogicalType for IntervalType<MonthDayNanoType> {
    type PhysicalType = MonthDayNanoType;
}

/// YearMonth is represented as a single 32 bit signed integer
impl DaftDataType for IntervalType<i32> {
    #[inline]
    fn get_dtype() -> DataType {
        DataType::Interval(IntervalUnit::YearMonth)
    }
    type ArrayType = logical::LogicalArray<IntervalType<i32>>;
}

impl DaftLogicalType for IntervalType<i32> {
    type PhysicalType = Int32Type;
}

pub type IntervalMonthDayNanoType = IntervalType<MonthDayNanoType>;
pub type IntervalDayTimeType = IntervalType<DayTimeType>;
pub type IntervalYearMonthType = IntervalType<i32>;
