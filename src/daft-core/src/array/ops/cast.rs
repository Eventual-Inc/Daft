use std::{
    ops::{Div, Mul},
    sync::Arc,
};

use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use common_error::{DaftError, DaftResult};
use indexmap::IndexMap;
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
use crate::lit::Literal;
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{
        DataArray, FixedSizeListArray, ListArray, StructArray,
        growable::make_growable,
        image_array::ImageArraySidecarData,
        ops::{DaftCompare, full::FullNull},
    },
    datatypes::{
        DaftArrayType, DaftArrowBackedType, DataType, Field, FileArray, ImageMode, Int64Array,
        NullArray, TimeUnit, UInt64Array, Utf8Array,
        logical::{
            DateArray, DurationArray, EmbeddingArray, FixedShapeImageArray,
            FixedShapeSparseTensorArray, FixedShapeTensorArray, ImageArray, MapArray,
            SparseTensorArray, TensorArray, TimeArray, TimestampArray,
        },
    },
    file::{DaftMediaType, MediaTypeAudio, MediaTypeUnknown, MediaTypeVideo},
    series::{IntoSeries, Series},
    utils::display::display_time64,
    with_match_numeric_daft_types,
};

#[cfg(feature = "python")]
impl Series {
    fn cast_to_python(&self) -> DaftResult<Self> {
        let py_values = Python::attach(|py| {
            use pyo3::IntoPyObjectExt;

            self.to_literals()
                .map(|lit| {
                    use crate::lit::Literal;

                    if matches!(lit, Literal::Null) {
                        Ok(None)
                    } else {
                        Ok(Some(Arc::new(lit.into_py_any(py)?)))
                    }
                })
                .collect::<PyResult<Vec<_>>>()
        })?;

        Ok(PythonArray::from_iter(self.name(), py_values.into_iter()).into_series())
    }
}

impl<T> DataArray<T>
where
    T: DaftArrowBackedType,
{
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        if self.data_type().is_null() || dtype.is_null() {
            return Ok(Series::full_null(self.name(), dtype, self.len()));
        }

        match dtype {
            #[cfg(feature = "python")]
            DataType::Python => {
                Series::try_from((self.name(), self.data.clone()))?.cast_to_python()
            }
            _ => {
                // Cast from DataArray to the target DataType
                // by using Arrow's casting mechanisms.

                if !dtype.is_arrow() || !self.data_type().is_arrow() {
                    return Err(DaftError::TypeError(format!(
                        "Can not cast {:?} to type: {:?}: not convertible to Arrow",
                        self.data_type(),
                        dtype
                    )));
                }

                // Arrow-rs doesn't support numeric/float ↔ binary casts directly.
                // Route through Utf8 as an intermediate to preserve string representation.
                let src = self.data_type();
                let is_binary_target = matches!(dtype, DataType::Binary);
                let is_binary_source = matches!(src, DataType::Binary);

                if src.is_boolean() && is_binary_target {
                    // bool → binary: cast to UInt8 first to get 1/0, then to Utf8, then binary
                    let int_series = self.cast(&DataType::UInt8)?;
                    let utf8_series = int_series.cast(&DataType::Utf8)?;
                    return utf8_series.cast(dtype);
                }
                if src.is_numeric() && is_binary_target {
                    // numeric → binary: cast to Utf8 first, then to binary
                    let utf8_series = self.cast(&DataType::Utf8)?;
                    return utf8_series.cast(dtype);
                }
                if is_binary_source && (dtype.is_numeric() || dtype.is_boolean()) {
                    // binary → numeric/bool: cast to Utf8 first, then to target
                    let utf8_series = self.cast(&DataType::Utf8)?;
                    return utf8_series.cast(dtype);
                }

                // Binary → FixedSizeBinary: validate all non-null values match the target width
                if let DataType::FixedSizeBinary(target_size) = dtype
                    && is_binary_source
                {
                    use arrow::array::Array as ArrowArray;
                    let arrow_arr = self.to_arrow();
                    let binary_arr = arrow_arr
                        .as_any()
                        .downcast_ref::<arrow::array::LargeBinaryArray>()
                        .expect("Expected LargeBinary array for Binary DataType");
                    for i in 0..binary_arr.len() {
                        if !binary_arr.is_null(i) && binary_arr.value(i).len() != *target_size {
                            return Err(DaftError::ComputeError(format!(
                                "Cannot cast Binary to FixedSizeBinary({}): \
                                 element at index {} has length {}",
                                target_size,
                                i,
                                binary_arr.value(i).len()
                            )));
                        }
                    }
                }

                let target_physical_type = dtype.to_physical();
                let target_arrow_type = dtype.to_arrow()?;
                let target_arrow_physical_type = target_physical_type.to_arrow()?;
                let self_physical_type = self.data_type().to_physical();
                let self_arrow_type = self.data_type().to_arrow()?;
                let self_physical_arrow_type = self_physical_type.to_arrow()?;

                // Special case: Utf8 -> numeric/temporal needs whitespace trimming
                // This matches Python/Pandas/NumPy behavior
                let trimmed_utf8: Option<Utf8Array>;
                let data_to_cast: arrow::array::ArrayRef = if self.data_type() == &DataType::Utf8
                    && (dtype.is_numeric() || dtype.is_temporal())
                {
                    let arrow_arr = self.to_arrow();
                    let utf8_array = arrow_arr
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                        .expect("Expected LargeUtf8 array for Utf8 DataType");
                    trimmed_utf8 = Some(Utf8Array::from_iter(
                        self.name(),
                        utf8_array.iter().map(|opt_s| opt_s.map(|s| s.trim())),
                    ));
                    trimmed_utf8.as_ref().unwrap().to_arrow()
                } else {
                    self.to_arrow()
                };

                let result_array =
                    if arrow::compute::can_cast_types(&self_arrow_type, &target_arrow_type) {
                        arrow::compute::cast(data_to_cast.as_ref(), &target_arrow_type)?
                    } else if target_arrow_physical_type != target_arrow_type
                        && arrow::compute::can_cast_types(
                            &self_physical_arrow_type,
                            &target_arrow_physical_type,
                        )
                    {
                        arrow::compute::cast(data_to_cast.as_ref(), &target_arrow_physical_type)?
                    } else {
                        return Err(DaftError::TypeError(format!(
                            "can not cast {:?} to type: {:?}: Arrow types not castable, {:?}, {:?}",
                            self.data_type(),
                            dtype,
                            self_arrow_type,
                            target_arrow_type,
                        )));
                    };

                let new_field = Arc::new(Field::new(self.name(), dtype.clone()));
                Series::from_arrow(new_field, result_array)
            }
        }
    }
}

impl DateArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            DataType::Null => {
                Ok(NullArray::full_null(self.name(), dtype, self.len()).into_series())
            }
            DataType::Date => Ok(self.clone().into_series()),
            DataType::Utf8 => {
                use chrono::Datelike;
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                Ok(Utf8Array::from_iter(
                    self.name(),
                    self.physical.into_iter().map(|opt_days| {
                        opt_days.map(|days| {
                            let date = epoch + chrono::TimeDelta::days(days as i64);
                            format!("{}-{}-{}", date.year(), date.month(), date.day())
                        })
                    }),
                )
                .into_series())
            }
            DataType::Timestamp(tu, _) => {
                let days_to_unit: i64 = match tu {
                    TimeUnit::Nanoseconds => 24 * 3_600_000_000_000,
                    TimeUnit::Microseconds => 24 * 3_600_000_000,
                    TimeUnit::Milliseconds => 24 * 3_600_000,
                    TimeUnit::Seconds => 24 * 3_600,
                };

                let units_per_day = Int64Array::from_slice("units", &[days_to_unit]).into_series();
                let unit_since_epoch = ((&self.physical.clone().into_series()) * &units_per_day)?;
                unit_since_epoch.cast(dtype)
            }
            dtype if dtype.is_numeric() => self.physical.cast(dtype),
            #[cfg(feature = "python")]
            DataType::Python => self.clone().into_series().cast_to_python(),
            _ => Err(DaftError::TypeError(format!(
                "Cannot cast Date to {}",
                dtype
            ))),
        }
    }
}

/// Formats a naive timestamp to a string in the format "%Y-%m-%d %H:%M:%S%.f".
/// Example: 2021-01-01 00:00:00
/// See https://docs.rs/chrono/latest/chrono/format/strftime/index.html for format string options.
pub fn timestamp_to_str_naive(val: i64, unit: &TimeUnit) -> String {
    let chrono_ts = daft_schema::time_unit::timestamp_to_naive_datetime(val, *unit);
    let format_str = "%Y-%m-%d %H:%M:%S%.f";
    chrono_ts.format(format_str).to_string()
}

/// Formats a timestamp with an offset to a string in the format "%Y-%m-%d %H:%M:%S%.f %:z".
/// Example: 2021-01-01 00:00:00 -07:00
/// See https://docs.rs/chrono/latest/chrono/format/strftime/index.html for format string options.
pub fn timestamp_to_str_offset(val: i64, unit: &TimeUnit, offset: &chrono::FixedOffset) -> String {
    let chrono_ts = daft_schema::time_unit::timestamp_to_datetime(val, *unit, offset);
    let format_str = "%Y-%m-%d %H:%M:%S%.f %:z";
    chrono_ts.format(format_str).to_string()
}

/// Formats a timestamp with a timezone to a string in the format "%Y-%m-%d %H:%M:%S%.f %Z".
/// Example: 2021-01-01 00:00:00 PST
/// See https://docs.rs/chrono/latest/chrono/format/strftime/index.html for format string options.
pub fn timestamp_to_str_tz(val: i64, unit: &TimeUnit, tz: &chrono_tz::Tz) -> String {
    let chrono_ts = daft_schema::time_unit::timestamp_to_datetime(val, *unit, tz);
    let format_str = "%Y-%m-%d %H:%M:%S%.f %Z";
    chrono_ts.format(format_str).to_string()
}

impl TimestampArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            DataType::Null => {
                Ok(NullArray::full_null(self.name(), dtype, self.len()).into_series())
            }
            DataType::Timestamp(tu, _) => {
                let self_tu = match self.data_type() {
                    DataType::Timestamp(tu, _) => tu,
                    _ => panic!("Wrong dtype for TimestampArray: {}", self.data_type()),
                };
                let physical = match self_tu.cmp(tu) {
                    std::cmp::Ordering::Equal => self.physical.clone(),
                    std::cmp::Ordering::Greater => {
                        let factor = tu.to_scale_factor() / self_tu.to_scale_factor();
                        self.physical
                            .mul(&Int64Array::from_slice("factor", &[factor]))?
                    }
                    std::cmp::Ordering::Less => {
                        let factor = self_tu.to_scale_factor() / tu.to_scale_factor();
                        self.physical
                            .div(&Int64Array::from_slice("factor", &[factor]))?
                    }
                };
                Ok(
                    TimestampArray::new(Field::new(self.name(), dtype.clone()), physical)
                        .into_series(),
                )
            }
            DataType::Date => Ok(self.date()?.into_series()),
            DataType::Time(tu) => Ok(self.time(tu)?.into_series()),
            DataType::Utf8 => {
                let DataType::Timestamp(unit, timezone) = self.data_type() else {
                    panic!("Wrong dtype for TimestampArray: {}", self.data_type())
                };

                let str_iter: Box<dyn Iterator<Item = Option<String>>> = match timezone.as_ref() {
                    None => Box::new(
                        self.physical
                            .into_iter()
                            .map(|val| val.map(|val| timestamp_to_str_naive(val, unit))),
                    ),
                    Some(timezone) => {
                        if let Ok(offset) = daft_schema::time_unit::parse_offset(timezone) {
                            Box::new(self.physical.into_iter().map(move |val| {
                                val.map(|val| timestamp_to_str_offset(val, unit, &offset))
                            }))
                        } else if let Ok(tz) = daft_schema::time_unit::parse_offset_tz(timezone) {
                            Box::new(
                                self.physical.into_iter().map(move |val| {
                                    val.map(|val| timestamp_to_str_tz(val, unit, &tz))
                                }),
                            )
                        } else {
                            panic!("Unable to parse timezone string {}", timezone)
                        }
                    }
                };

                Ok(Utf8Array::from_iter(self.name(), str_iter).into_series())
            }
            dtype if dtype.is_numeric() => self.physical.cast(dtype),
            #[cfg(feature = "python")]
            DataType::Python => self.clone().into_series().cast_to_python(),
            _ => Err(DaftError::TypeError(format!(
                "Cannot cast Timestamp to {}",
                dtype
            ))),
        }
    }
}

impl TimeArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            DataType::Null => {
                Ok(NullArray::full_null(self.name(), dtype, self.len()).into_series())
            }
            DataType::Time(tu) => {
                let self_tu = match self.data_type() {
                    DataType::Time(tu) => tu,
                    _ => panic!("Wrong dtype for TimeArray: {}", self.data_type()),
                };
                let physical = match self_tu.cmp(tu) {
                    std::cmp::Ordering::Equal => self.physical.clone(),
                    std::cmp::Ordering::Greater => {
                        let factor = tu.to_scale_factor() / self_tu.to_scale_factor();
                        self.physical
                            .mul(&Int64Array::from_slice("factor", &[factor]))?
                    }
                    std::cmp::Ordering::Less => {
                        let factor = self_tu.to_scale_factor() / tu.to_scale_factor();
                        self.physical
                            .div(&Int64Array::from_slice("factor", &[factor]))?
                    }
                };
                Ok(TimeArray::new(Field::new(self.name(), dtype.clone()), physical).into_series())
            }
            DataType::Utf8 => Ok(Utf8Array::from_iter(
                self.name(),
                self.physical.into_iter().map(|val| {
                    val.map(|val| {
                        let DataType::Time(unit) = &self.field.dtype else {
                            panic!("Wrong dtype for TimeArray: {}", self.field.dtype)
                        };
                        display_time64(val, unit)
                    })
                }),
            )
            .into_series()),
            dtype if dtype.is_numeric() => self.physical.cast(dtype),
            #[cfg(feature = "python")]
            DataType::Python => self.clone().into_series().cast_to_python(),
            _ => Err(DaftError::TypeError(format!(
                "Cannot cast Time to {}",
                dtype
            ))),
        }
    }
}

impl DurationArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            DataType::Null => {
                Ok(NullArray::full_null(self.name(), dtype, self.len()).into_series())
            }
            dtype if dtype == self.data_type() => Ok(self.clone().into_series()),
            dtype if dtype.is_numeric() => self.physical.cast(dtype),
            DataType::Int64 => Ok(self.physical.clone().into_series()),
            #[cfg(feature = "python")]
            DataType::Python => self.clone().into_series().cast_to_python(),
            _ => Err(DaftError::TypeError(format!(
                "Cannot cast Duration to {}",
                dtype
            ))),
        }
    }

    pub fn cast_to_seconds(&self) -> DaftResult<Int64Array> {
        let tu = match self.data_type() {
            DataType::Duration(tu) => tu,
            _ => panic!("Wrong dtype for DurationArray: {}", self.data_type()),
        };
        let seconds = match tu {
            TimeUnit::Seconds => self.physical.clone(),
            TimeUnit::Milliseconds => self
                .physical
                .div(&Int64Array::from_slice("MillisecondsPerSecond", &[1000]))?,
            TimeUnit::Microseconds => self.physical.div(&Int64Array::from_slice(
                "MicrosecondsPerSecond",
                &[1_000_000],
            ))?,
            TimeUnit::Nanoseconds => self.physical.div(&Int64Array::from_slice(
                "NanosecondsPerSecond",
                &[1_000_000_000],
            ))?,
        };
        Ok(seconds)
    }

    pub fn cast_to_milliseconds(&self) -> DaftResult<Int64Array> {
        let tu = match self.data_type() {
            DataType::Duration(tu) => tu,
            _ => panic!("Wrong dtype for DurationArray: {}", self.data_type()),
        };
        let milliseconds = match tu {
            TimeUnit::Seconds => self
                .physical
                .mul(&Int64Array::from_slice("MillisecondsPerSecond", &[1000]))?,
            TimeUnit::Milliseconds => self.physical.clone(),
            TimeUnit::Microseconds => self.physical.div(&Int64Array::from_slice(
                "MicrosecondsPerMillisecond",
                &[1_000],
            ))?,
            TimeUnit::Nanoseconds => self.physical.div(&Int64Array::from_slice(
                "NanosecondsPerMillisecond",
                &[1_000_000],
            ))?,
        };
        Ok(milliseconds)
    }

    pub fn cast_to_microseconds(&self) -> DaftResult<Int64Array> {
        let tu = match self.data_type() {
            DataType::Duration(tu) => tu,
            _ => panic!("Wrong dtype for DurationArray: {}", self.data_type()),
        };
        let microseconds = match tu {
            TimeUnit::Seconds => self.physical.mul(&Int64Array::from_slice(
                "MicrosecondsPerSecond",
                &[1_000_000],
            ))?,
            TimeUnit::Milliseconds => self.physical.mul(&Int64Array::from_slice(
                "MicrosecondsPerMillisecond",
                &[1_000],
            ))?,
            TimeUnit::Microseconds => self.physical.clone(),
            TimeUnit::Nanoseconds => self.physical.div(&Int64Array::from_slice(
                "NanosecondsPerMicrosecond",
                &[1_000],
            ))?,
        };
        Ok(microseconds)
    }

    pub fn cast_to_nanoseconds(&self) -> DaftResult<Int64Array> {
        {
            let tu = match self.data_type() {
                DataType::Duration(tu) => tu,
                _ => panic!("Wrong dtype for DurationArray: {}", self.data_type()),
            };
            let nanoseconds = match tu {
                TimeUnit::Seconds => self.physical.mul(&Int64Array::from_slice(
                    "NanosecondsPerSecond",
                    &[1_000_000_000],
                ))?,
                TimeUnit::Milliseconds => self.physical.mul(&Int64Array::from_slice(
                    "NanosecondsPerMillisecond",
                    &[1_000_000],
                ))?,
                TimeUnit::Microseconds => self.physical.mul(&Int64Array::from_slice(
                    "NanosecondsPerMicrosecond",
                    &[1_000],
                ))?,
                TimeUnit::Nanoseconds => self.physical.clone(),
            };
            Ok(nanoseconds)
        }
    }
    pub fn cast_to_minutes(&self) -> DaftResult<Int64Array> {
        let tu = match self.data_type() {
            DataType::Duration(tu) => tu,
            _ => panic!("Wrong dtype for DurationArray: {}", self.data_type()),
        };
        let minutes = match tu {
            TimeUnit::Seconds => self
                .physical
                .div(&Int64Array::from_slice("SecondsInMinute", &[60]))?,
            TimeUnit::Milliseconds => self.physical.div(&Int64Array::from_slice(
                "MillisecondsInMinute",
                &[60 * 1000],
            ))?,
            TimeUnit::Microseconds => self.physical.div(&Int64Array::from_slice(
                "MicrosecondsInMinute",
                &[60 * 1_000_000],
            ))?,
            TimeUnit::Nanoseconds => self.physical.div(&Int64Array::from_slice(
                "NanosecondsInMinute",
                &[60 * 1_000_000_000],
            ))?,
        };
        Ok(minutes)
    }

    pub fn cast_to_hours(&self) -> DaftResult<Int64Array> {
        let tu = match self.data_type() {
            DataType::Duration(tu) => tu,
            _ => panic!("Wrong dtype for DurationArray: {}", self.data_type()),
        };
        let hours = match tu {
            TimeUnit::Seconds => self
                .physical
                .div(&Int64Array::from_slice("SecondsInHour", &[60 * 60]))?,
            TimeUnit::Milliseconds => self.physical.div(&Int64Array::from_slice(
                "MillisecondsInHour",
                &[60 * 60 * 1000],
            ))?,
            TimeUnit::Microseconds => self.physical.div(&Int64Array::from_slice(
                "MicrosecondsInHour",
                &[60 * 60 * 1_000_000],
            ))?,
            TimeUnit::Nanoseconds => self.physical.div(&Int64Array::from_slice(
                "NanosecondsInHour",
                &[60 * 60 * 1_000_000_000],
            ))?,
        };
        Ok(hours)
    }

    pub fn cast_to_days(&self) -> DaftResult<Int64Array> {
        let tu = match self.data_type() {
            DataType::Duration(tu) => tu,
            _ => panic!("Wrong dtype for DurationArray: {}", self.data_type()),
        };
        let days = match tu {
            TimeUnit::Seconds => self
                .physical
                .div(&Int64Array::from_slice("SecondsPerDay", &[60 * 60 * 24]))?,
            TimeUnit::Milliseconds => self.physical.div(&Int64Array::from_slice(
                "MillisecondsPerDay",
                &[1_000 * 60 * 60 * 24],
            ))?,
            TimeUnit::Microseconds => self.physical.div(&Int64Array::from_slice(
                "MicrosecondsPerDay",
                &[1_000_000 * 60 * 60 * 24],
            ))?,
            TimeUnit::Nanoseconds => self.physical.div(&Int64Array::from_slice(
                "NanosecondsPerDay",
                &[1_000_000_000 * 60 * 60 * 24],
            ))?,
        };
        Ok(days)
    }
}

impl<T> FileArray<T>
where
    T: DaftMediaType,
{
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        use daft_schema::media_type::MediaType::*;
        match dtype {
            DataType::File(media_type) => match (media_type, T::get_type()) {
                (Unknown, Unknown) | (Video, Video) | (Audio, Audio) => {
                    Ok(self.clone().into_series())
                }
                (Unknown, Video) => Ok(self.clone().change_type::<MediaTypeVideo>().into_series()),
                (Unknown, Audio) => Ok(self.clone().change_type::<MediaTypeAudio>().into_series()),
                (Video, Unknown) | (Audio, Unknown) => {
                    Ok(self.clone().change_type::<MediaTypeUnknown>().into_series())
                }
                (Video, Audio) | (Audio, Video) => {
                    Err(DaftError::TypeError("invalid cast".to_string()))
                }
            },
            DataType::Null => {
                Ok(NullArray::full_null(self.name(), dtype, self.len()).into_series())
            }
            dtype if dtype == self.data_type() => Ok(self.clone().into_series()),
            _ => Err(DaftError::TypeError("invalid cast".to_string())),
        }
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        if dtype.is_python() {
            return Ok(self.clone().into_series());
        }

        // TODO: optimize this.
        // Currently this does PythonArray -> Vec<Literal> -> Series -> Series::cast
        let literals = Python::attach(|py| {
            self.values()
                .iter()
                .map(|ob| Literal::from_pyobj(ob.bind(py), Some(dtype)))
                .collect::<PyResult<Vec<Literal>>>()
        })?;

        Ok(Series::from_literals(literals)?
            .cast(dtype)?
            .rename(self.name()))
    }
}

impl EmbeddingArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match (dtype, self.data_type()) {
            #[cfg(feature = "python")]
            (DataType::Python, _) => self.clone().into_series().cast_to_python(),
            (DataType::Tensor(_), DataType::Embedding(inner_dtype, size)) => {
                let image_shape = vec![*size as u64];
                let fixed_shape_tensor_dtype =
                    DataType::FixedShapeTensor(Box::new(inner_dtype.as_ref().clone()), image_shape);
                let fixed_shape_tensor_array = self.cast(&fixed_shape_tensor_dtype)?;
                let fixed_shape_tensor_array =
                    fixed_shape_tensor_array.downcast::<FixedShapeTensorArray>()?;
                fixed_shape_tensor_array.cast(dtype)
            }
            // NOTE(Clark): Casting to FixedShapeTensor is supported by the physical array cast.
            (_, _) => self.physical.cast(dtype),
        }
    }
}

impl ImageArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            #[cfg(feature = "python")]
            DataType::Python => self.clone().into_series().cast_to_python(),
            DataType::FixedShapeImage(mode, height, width) => {
                let num_channels = mode.num_channels();
                let image_shape = vec![*height as u64, *width as u64, num_channels as u64];
                let fixed_shape_tensor_dtype =
                    DataType::FixedShapeTensor(Box::new(mode.get_dtype()), image_shape);
                let fixed_shape_tensor_array =
                    self.cast(&fixed_shape_tensor_dtype).or_else(|e| {
                        if num_channels == 1 {
                            // Fall back to height x width shape if unit channel.
                            self.cast(&DataType::FixedShapeTensor(
                                Box::new(mode.get_dtype()),
                                vec![*height as u64, *width as u64],
                            ))
                            .or(Err(e))
                        } else {
                            Err(e)
                        }
                    })?;
                let fixed_shape_tensor_array =
                    fixed_shape_tensor_array.downcast::<FixedShapeTensorArray>()?;
                fixed_shape_tensor_array.cast(dtype)
            }
            DataType::Tensor(inner_dtype) => {
                let ndim = 3;
                let mut shapes = Vec::with_capacity(ndim * self.len());
                let shape_offsets = (0..=ndim * self.len())
                    .step_by(ndim)
                    .map(|v| v as i64)
                    .collect::<ScalarBuffer<i64>>();
                let nulls = self.physical.nulls();
                let data_series = self
                    .data_array()
                    .clone()
                    .into_series()
                    .cast(&DataType::List(inner_dtype.clone()))?;
                let ha = self.heights().values();
                let wa = self.widths().values();
                let ca = self.channels().values();
                for i in 0..self.len() {
                    shapes.push(ha[i] as u64);
                    shapes.push(wa[i] as u64);
                    shapes.push(ca[i] as u64);
                }
                let shapes_dtype = DataType::List(Box::new(DataType::UInt64));
                let shape_offsets = OffsetBuffer::new(shape_offsets);
                let shapes_array = ListArray::new(
                    Field::new("shape", shapes_dtype),
                    UInt64Array::from_vec("shape", shapes).into_series(),
                    shape_offsets,
                    nulls.cloned(),
                );

                let physical_type = dtype.to_physical();

                let struct_array = StructArray::new(
                    Field::new(self.name(), physical_type),
                    vec![data_series, shapes_array.into_series()],
                    nulls.cloned(),
                );
                Ok(
                    TensorArray::new(Field::new(self.name(), dtype.clone()), struct_array)
                        .into_series(),
                )
            }
            DataType::FixedShapeTensor(inner_dtype, _) => {
                let tensor_dtype = DataType::Tensor(inner_dtype.clone());
                let tensor_array = self.cast(&tensor_dtype)?;
                let tensor_array = tensor_array.downcast::<TensorArray>()?;
                tensor_array.cast(dtype)
            }
            _ => self.physical.cast(dtype),
        }
    }
}

impl FixedShapeImageArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match (dtype, self.data_type()) {
            #[cfg(feature = "python")]
            (DataType::Python, _) => self.clone().into_series().cast_to_python(),
            (DataType::Tensor(_), DataType::FixedShapeImage(mode, height, width)) => {
                let num_channels = mode.num_channels();
                let image_shape = vec![*height as u64, *width as u64, num_channels as u64];
                let fixed_shape_tensor_dtype =
                    DataType::FixedShapeTensor(Box::new(mode.get_dtype()), image_shape);
                let fixed_shape_tensor_array =
                    self.cast(&fixed_shape_tensor_dtype).or_else(|e| {
                        if num_channels == 1 {
                            // Fall back to height x width shape if unit channel.
                            self.cast(&DataType::FixedShapeTensor(
                                Box::new(mode.get_dtype()),
                                vec![*height as u64, *width as u64],
                            ))
                            .or(Err(e))
                        } else {
                            Err(e)
                        }
                    })?;
                let fixed_shape_tensor_array =
                    fixed_shape_tensor_array.downcast::<FixedShapeTensorArray>()?;
                fixed_shape_tensor_array.cast(dtype)
            }
            (DataType::Image(_), DataType::FixedShapeImage(mode, _, _)) => {
                let tensor_dtype = DataType::Tensor(Box::new(mode.get_dtype()));
                let tensor_array = self.cast(&tensor_dtype)?;
                let tensor_array = tensor_array.downcast::<TensorArray>()?;
                tensor_array.cast(dtype)
            }
            // NOTE(Clark): Casting to FixedShapeTensor is supported by the physical array cast.
            (_, _) => self.physical.cast(dtype),
        }
    }
}

impl TensorArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            #[cfg(feature = "python")]
            DataType::Python => self.clone().into_series().cast_to_python(),
            DataType::FixedShapeTensor(inner_dtype, shape) => {
                let da = self.data_array();
                let sa = self.shape_array();
                if !(0..self.len()).map(|i| sa.get(i)).all(|s| {
                    s.is_none_or(|s| {
                        s.u64()
                            .unwrap()
                            .into_iter()
                            .eq(shape.iter().copied().map(Some))
                    })
                }) {
                    return Err(DaftError::TypeError(format!(
                        "Can not cast Tensor array to FixedShapeTensor array with type {:?}: Tensor array has shapes different than {:?};",
                        dtype, shape,
                    )));
                }
                let size = shape.iter().product::<u64>() as usize;

                let result = da
                    .cast(&DataType::FixedSizeList(
                        Box::new(inner_dtype.as_ref().clone()),
                        size,
                    ))?
                    .rename(self.name());
                let tensor_array = FixedShapeTensorArray::new(
                    Field::new(self.name(), dtype.clone()),
                    result.fixed_size_list().unwrap().clone(),
                );
                Ok(tensor_array.into_series())
            }
            DataType::Embedding(inner_dtype, length) => self
                .cast(&DataType::FixedShapeTensor(
                    inner_dtype.clone(),
                    vec![*length as u64],
                ))?
                .cast(dtype),
            DataType::List(_) => self.data_array().cast(dtype),
            DataType::FixedSizeList(inner_dtype, length) => self
                .cast(&DataType::FixedShapeTensor(
                    inner_dtype.clone(),
                    vec![*length as u64],
                ))?
                .cast(dtype),
            DataType::SparseTensor(inner_dtype, use_offset_indices) => {
                let shape_iterator = self.shape_array().into_iter();
                let data_iterator = self.data_array().into_iter();
                let nulls = self.data_array().nulls();
                let shape_and_data_iter = shape_iterator.zip(data_iterator);
                let zero_series = Int64Array::from_slice("item", &[0]).into_series();
                let mut non_zero_values = Vec::new();
                let mut non_zero_indices = Vec::new();
                for (i, (shape_series, data_series)) in shape_and_data_iter.enumerate() {
                    let is_valid = nulls.is_none_or(|v| v.is_valid(i));
                    if !is_valid {
                        // Handle invalid row by populating dummy data.
                        non_zero_values.push(Series::empty("dummy", inner_dtype.as_ref()));
                        non_zero_indices.push(Series::empty("dummy", &DataType::UInt64));
                        continue;
                    }
                    let shape_series = shape_series.unwrap();
                    let data_series = data_series.unwrap();
                    let shape_array = shape_series.u64().unwrap();
                    assert!(
                        data_series.len()
                            == shape_array.into_iter().flatten().product::<u64>() as usize
                    );
                    let non_zero_mask = data_series.not_equal(&zero_series)?;
                    let data = data_series.filter(&non_zero_mask)?;
                    let indices = UInt64Array::arange("item", 0, data_series.len() as i64, 1)?
                        .into_series()
                        .filter(&non_zero_mask)?;
                    let indices_arr = match use_offset_indices {
                        true => {
                            let offsets_values = indices
                                .u64()?
                                .as_slice()
                                .iter()
                                .scan(0, |previous, &current| {
                                    let offset = current - *previous;
                                    *previous = current;
                                    Some(offset)
                                })
                                .collect::<Vec<_>>();
                            UInt64Array::from_slice("item", offsets_values.as_slice()).into_series()
                        }
                        false => indices,
                    };
                    non_zero_values.push(data);
                    non_zero_indices.push(indices_arr);
                }

                let offsets = OffsetBuffer::from_lengths(non_zero_values.iter().map(|s| s.len()));
                let non_zero_values_series =
                    Series::concat(&non_zero_values.iter().collect::<Vec<&Series>>())?;
                let non_zero_indices_series =
                    Series::concat(&non_zero_indices.iter().collect::<Vec<&Series>>())?;
                let offsets_cloned = offsets.clone();
                let data_list_arr = ListArray::new(
                    Field::new(
                        "values",
                        DataType::List(Box::new(non_zero_values_series.data_type().clone())),
                    ),
                    non_zero_values_series,
                    offsets.into(),
                    nulls.cloned(),
                );
                let indices_list_arr = ListArray::new(
                    Field::new(
                        "indices",
                        DataType::List(Box::new(non_zero_indices_series.data_type().clone())),
                    ),
                    non_zero_indices_series,
                    offsets_cloned.into(),
                    nulls.cloned(),
                );
                // Shapes must be all valid to reproduce dense tensor.
                let all_valid_shape_array = self.shape_array().with_nulls(None)?;
                let sparse_struct_array = StructArray::new(
                    Field::new(self.name(), dtype.to_physical()),
                    vec![
                        data_list_arr.into_series(),
                        indices_list_arr.into_series(),
                        all_valid_shape_array.into_series(),
                    ],
                    nulls.cloned(),
                );
                Ok(SparseTensorArray::new(
                    Field::new(sparse_struct_array.name(), dtype.clone()),
                    sparse_struct_array,
                )
                .into_series())
            }
            DataType::Image(mode) => {
                let DataType::Tensor(inner_dtype) = self.data_type() else {
                    unreachable!("TensorArray should have Tensor datatype")
                };

                if let Some(m) = mode
                    && **inner_dtype != m.get_dtype()
                {
                    return Err(DaftError::TypeError(format!(
                        "Images with mode {} can only be created from tensors of type {}, found {}",
                        m,
                        m.get_dtype(),
                        inner_dtype
                    )));
                }

                let sa = self.shape_array();
                if !(0..self.len()).map(|i| sa.get(i)).all(|s| {
                    s.is_none_or(|s| {
                        if s.len() != 3 && s.len() != 2 {
                            // Images must have 2 or 3 dimensions: height x width or height x width x channel.
                            // If image is 2 dimensions, 8-bit grayscale is assumed.
                            return false;
                        }
                        if let Some(mode) = mode
                            && s.u64().unwrap().get(s.len() - 1).unwrap()
                                != mode.num_channels() as u64
                        {
                            // If type-level mode is defined, each image must have the implied number of channels.
                            return false;
                        }
                        true
                    })
                }) {
                    return Err(DaftError::TypeError(format!(
                        "Can not cast Tensor array to Image array with type {:?}: Tensor array shapes are not compatible",
                        dtype,
                    )));
                }
                let num_rows = self.len();
                let mut channels = Vec::<u16>::with_capacity(num_rows);
                let mut heights = Vec::<u32>::with_capacity(num_rows);
                let mut widths = Vec::<u32>::with_capacity(num_rows);
                let mut modes = Vec::<u8>::with_capacity(num_rows);
                let da = self.data_array();
                let nulls = da.nulls();
                for i in 0..num_rows {
                    let is_valid = nulls.is_none_or(|v| v.is_valid(i));
                    if !is_valid {
                        // Handle invalid row by populating dummy data.
                        channels.push(1);
                        heights.push(1);
                        widths.push(1);
                        modes.push(mode.unwrap_or(ImageMode::L) as u8);
                        continue;
                    }
                    let shape = sa.get(i).unwrap();
                    let shape = shape.u64().unwrap();
                    assert!(shape.nulls().is_none_or(|v| v.iter().all(|b| b)));
                    let mut shape = shape.values().to_vec();
                    if shape.len() == 2 {
                        // Add unit channel dimension to grayscale height x width image.
                        shape.push(1);
                    }
                    if shape.len() != 3 {
                        return Err(DaftError::ValueError(format!(
                            "Image expected to have {} dimensions, but has {}. Image shape = {:?}",
                            3,
                            shape.len(),
                            shape,
                        )));
                    }
                    heights.push(
                        shape[0]
                            .try_into()
                            .expect("Image height should fit into a uint16"),
                    );
                    widths.push(
                        shape[1]
                            .try_into()
                            .expect("Image width should fit into a uint16"),
                    );
                    channels.push(
                        shape[2]
                            .try_into()
                            .expect("Number of channels should fit into a uint8"),
                    );

                    modes.push(mode.unwrap_or(ImageMode::try_from_num_channels(
                        shape[2].try_into().unwrap(),
                        &DataType::UInt8,
                    )?) as u8);
                }
                Ok(ImageArray::from_list_array(
                    self.name(),
                    dtype.clone(),
                    da.clone(),
                    ImageArraySidecarData {
                        channels,
                        heights,
                        widths,
                        modes,
                        nulls: nulls.cloned(),
                    },
                )?
                .into_series())
            }
            DataType::FixedShapeImage(mode, height, width) => {
                let num_channels = mode.num_channels();
                let image_shape = vec![*height as u64, *width as u64, num_channels as u64];
                let fixed_shape_tensor_dtype =
                    DataType::FixedShapeTensor(Box::new(mode.get_dtype()), image_shape);
                let fixed_shape_tensor_array =
                    self.cast(&fixed_shape_tensor_dtype).or_else(|e| {
                        if num_channels == 1 {
                            // Fall back to height x width shape if unit channel.
                            self.cast(&DataType::FixedShapeTensor(
                                Box::new(mode.get_dtype()),
                                vec![*height as u64, *width as u64],
                            ))
                            .or(Err(e))
                        } else {
                            Err(e)
                        }
                    })?;
                let fixed_shape_tensor_array =
                    fixed_shape_tensor_array.downcast::<FixedShapeTensorArray>()?;
                fixed_shape_tensor_array.cast(dtype)
            }
            _ => self.physical.cast(dtype),
        }
    }
}

fn cast_sparse_to_dense_for_inner_dtype(
    inner_dtype: &DataType,
    n_values: usize,
    non_zero_indices_array: &ListArray,
    non_zero_values_array: &ListArray,
    offsets: &OffsetBuffer<i64>,
    use_offset_indices: bool,
) -> DaftResult<Series> {
    let item: Series = with_match_numeric_daft_types!(inner_dtype, |$T| {
            let mut values = vec![0 as <$T as DaftNumericType>::Native; n_values];
            let nulls = non_zero_values_array.nulls();
            for i in 0..non_zero_values_array.len() {
                let is_valid = nulls.is_none_or(|v| v.is_valid(i));
                if !is_valid {
                    continue;
                }
                let index_series: Series = non_zero_indices_array.get(i).unwrap().cast(&DataType::UInt64)?;
                let index_array = index_series.u64().unwrap();
                let values_series: Series = non_zero_values_array.get(i).unwrap();
                let values_da = values_series.downcast::<<$T as DaftDataType>::ArrayType>()
                .unwrap();
                let list_start_offset = offsets.inner()[i] as usize;
                if use_offset_indices {
                    let mut old_idx: u64 = 0;
                    for (idx, val) in index_array.into_iter().zip(values_da.into_iter()) {
                        let current_idx = idx.unwrap() + old_idx;
                        old_idx = current_idx;
                        values[list_start_offset + current_idx as usize] = val.unwrap();
                    }
                } else {
                    for (idx, val) in index_array.into_iter().zip(values_da.into_iter()) {
                        values[list_start_offset + idx.unwrap() as usize] = val.unwrap();
                    }
                }
            }
            DataArray::<$T>::from_vec("item", values).into_series()
    });
    Ok(item)
}

fn minimal_uint_dtype(value: u64) -> DataType {
    if u8::try_from(value).is_ok() {
        DataType::UInt8
    } else if u16::try_from(value).is_ok() {
        DataType::UInt16
    } else if u32::try_from(value).is_ok() {
        DataType::UInt32
    } else {
        DataType::UInt64
    }
}

impl SparseTensorArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match (dtype, self.data_type()) {
            (DataType::Tensor(_), DataType::SparseTensor(inner_dtype, use_offset_indices)) => {
                let non_zero_values_array = self.values_array();
                let non_zero_indices_array = self.indices_array();
                let shape_array = self.shape_array();
                let sizes_vec: Vec<usize> = shape_array
                    .into_iter()
                    .map(|shape| {
                        shape.map_or(0, |shape| {
                            shape.u64().unwrap().into_iter().flatten().product::<u64>() as usize
                        })
                    })
                    .collect();
                let offsets = OffsetBuffer::from_lengths(sizes_vec.iter().copied());
                let n_values = sizes_vec.iter().sum::<usize>();
                let nulls = non_zero_indices_array.nulls();
                let item = cast_sparse_to_dense_for_inner_dtype(
                    inner_dtype,
                    n_values,
                    non_zero_indices_array,
                    non_zero_values_array,
                    &offsets,
                    *use_offset_indices,
                )?;
                let list_arr = ListArray::new(
                    Field::new(
                        "data",
                        DataType::List(Box::new(inner_dtype.as_ref().clone())),
                    ),
                    item,
                    offsets.into(),
                    nulls.cloned(),
                )
                .into_series();
                let physical_type = dtype.to_physical();
                let struct_array = StructArray::new(
                    Field::new(self.name(), physical_type),
                    vec![list_arr, shape_array.clone().into_series()],
                    nulls.cloned(),
                );
                Ok(
                    TensorArray::new(Field::new(self.name(), dtype.clone()), struct_array)
                        .into_series(),
                )
            }
            (
                DataType::FixedShapeSparseTensor(_, shape, _),
                DataType::SparseTensor(inner_dtype, _),
            ) => {
                let sa = self.shape_array();
                let va = self.values_array();
                let ia = self.indices_array();
                if !(0..self.len()).map(|i| sa.get(i)).all(|s| {
                    s.is_none_or(|s| {
                        s.u64()
                            .unwrap()
                            .into_iter()
                            .eq(shape.iter().copied().map(Some))
                    })
                }) {
                    return Err(DaftError::TypeError(format!(
                        "Can not cast SparseTensor array to FixedShapeSparseTensor array with type {:?}: Tensor array has shapes different than {:?};",
                        dtype, shape,
                    )));
                }

                let largest_index = std::cmp::max(shape.iter().product::<u64>(), 1) - 1;
                let indices_minimal_inner_dtype = minimal_uint_dtype(largest_index);
                let values_array =
                    va.cast(&DataType::List(Box::new(inner_dtype.as_ref().clone())))?;
                let indices_array =
                    ia.cast(&DataType::List(Box::new(indices_minimal_inner_dtype)))?;
                let struct_array = StructArray::new(
                    Field::new(self.name(), dtype.to_physical()),
                    vec![values_array, indices_array],
                    va.nulls().cloned(),
                );
                let sparse_tensor_array = FixedShapeSparseTensorArray::new(
                    Field::new(self.name(), dtype.clone()),
                    struct_array,
                );
                Ok(sparse_tensor_array.into_series())
            }
            #[cfg(feature = "python")]
            (DataType::Python, _) => self.clone().into_series().cast_to_python(),
            (_, _) => self.physical.cast(dtype),
        }
    }
}

impl FixedShapeSparseTensorArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match (dtype, self.data_type()) {
            (
                DataType::SparseTensor(_, _),
                DataType::FixedShapeSparseTensor(inner_dtype, tensor_shape, _),
            ) => {
                let ndim = tensor_shape.len();
                let shapes = tensor_shape
                    .iter()
                    .cycle()
                    .copied()
                    .take(ndim * self.len())
                    .collect();
                let shape_offsets = (0..=ndim * self.len())
                    .step_by(ndim)
                    .map(|v| v as i64)
                    .collect::<ScalarBuffer<i64>>();

                let nulls = self.physical.nulls();

                let va = self.values_array();
                let ia = self.indices_array();

                let values_arr =
                    va.cast(&DataType::List(Box::new(inner_dtype.as_ref().clone())))?;
                let indices_arr = ia.cast(&DataType::List(Box::new(DataType::UInt64)))?;

                // List -> Struct
                let shape_offsets = OffsetBuffer::new(shape_offsets);
                let shapes_array = ListArray::new(
                    Field::new("shape", DataType::List(Box::new(DataType::UInt64))),
                    UInt64Array::from_vec("shape", shapes).into_series(),
                    shape_offsets,
                    nulls.cloned(),
                );
                let physical_type = dtype.to_physical();
                let struct_array = StructArray::new(
                    Field::new(self.name(), physical_type),
                    vec![values_arr, indices_arr, shapes_array.into_series()],
                    nulls.cloned(),
                );
                Ok(
                    SparseTensorArray::new(Field::new(self.name(), dtype.clone()), struct_array)
                        .into_series(),
                )
            }
            (
                DataType::FixedShapeTensor(_, target_tensor_shape),
                DataType::FixedShapeSparseTensor(inner_dtype, tensor_shape, use_offset_indices),
            ) => {
                let non_zero_values_array = self.values_array();
                let non_zero_indices_array = self.indices_array();
                let size = tensor_shape.iter().product::<u64>() as usize;
                let target_size = target_tensor_shape.iter().product::<u64>() as usize;
                if size != target_size {
                    return Err(DaftError::TypeError(format!(
                        "Can not cast FixedShapeSparseTensor array to FixedShapeTensor array with type {:?}: FixedShapeSparseTensor array has shapes different than {:?};",
                        dtype, tensor_shape,
                    )));
                }
                let n_values = size * non_zero_values_array.len();
                let offsets = OffsetBuffer::from_repeated_length(target_size, self.len());

                let item = cast_sparse_to_dense_for_inner_dtype(
                    inner_dtype,
                    n_values,
                    non_zero_indices_array,
                    non_zero_values_array,
                    &offsets,
                    *use_offset_indices,
                )?;
                let nulls = non_zero_values_array.nulls();
                let physical = FixedSizeListArray::new(
                    Field::new(
                        self.name(),
                        DataType::FixedSizeList(Box::new(inner_dtype.as_ref().clone()), size),
                    ),
                    item,
                    nulls.cloned(),
                );
                let fixed_shape_tensor_array =
                    FixedShapeTensorArray::new(Field::new(self.name(), dtype.clone()), physical);
                Ok(fixed_shape_tensor_array.into_series())
            }
            #[cfg(feature = "python")]
            (DataType::Python, _) => self.clone().into_series().cast_to_python(),
            (_, _) => self.physical.cast(dtype),
        }
    }
}

impl FixedShapeTensorArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match (dtype, self.data_type()) {
            #[cfg(feature = "python")]
            (DataType::Python, _) => self.clone().into_series().cast_to_python(),
            (DataType::Tensor(_), DataType::FixedShapeTensor(inner_dtype, tensor_shape)) => {
                let ndim = tensor_shape.len();
                let shapes = tensor_shape
                    .iter()
                    .cycle()
                    .copied()
                    .take(ndim * self.len())
                    .collect();
                let shape_offsets = (0..=ndim * self.len())
                    .step_by(ndim)
                    .map(|v| v as i64)
                    .collect::<ScalarBuffer<i64>>();

                let physical_arr = &self.physical;
                let nulls = self.physical.nulls();

                // FixedSizeList -> List
                let list_arr = physical_arr
                    .cast(&DataType::List(Box::new(inner_dtype.as_ref().clone())))?
                    .rename("data");

                // List -> Struct
                let shape_offsets = OffsetBuffer::new(shape_offsets);
                let shapes_array = ListArray::new(
                    Field::new("shape", DataType::List(Box::new(DataType::UInt64))),
                    UInt64Array::from_vec("shape", shapes).into_series(),
                    shape_offsets,
                    nulls.cloned(),
                );
                let physical_type = dtype.to_physical();
                let struct_array = StructArray::new(
                    Field::new(self.name(), physical_type),
                    vec![list_arr, shapes_array.into_series()],
                    nulls.cloned(),
                );
                Ok(
                    TensorArray::new(Field::new(self.name(), dtype.clone()), struct_array)
                        .into_series(),
                )
            }
            (
                DataType::FixedShapeSparseTensor(_, _, use_offset_indices),
                DataType::FixedShapeTensor(inner_dtype, tensor_shape),
            ) => {
                let physical_arr = &self.physical;
                let nulls = self.physical.nulls();
                let zero_series = Int64Array::from_slice("item", &[0]).into_series();
                let mut non_zero_values = Vec::new();
                let mut non_zero_indices = Vec::new();
                for (i, data_series) in physical_arr.into_iter().enumerate() {
                    let is_valid = nulls.is_none_or(|v| v.is_valid(i));
                    if !is_valid {
                        // Handle invalid row by populating dummy data.
                        non_zero_values.push(Series::empty("dummy", inner_dtype.as_ref()));
                        non_zero_indices.push(Series::empty("dummy", &DataType::UInt64));
                        continue;
                    }
                    let data_series = data_series.unwrap();
                    assert_eq!(
                        data_series.len(),
                        tensor_shape.iter().product::<u64>() as usize
                    );
                    let non_zero_mask = data_series.not_equal(&zero_series)?;
                    let data = data_series.filter(&non_zero_mask)?;
                    let indices = UInt64Array::arange("item", 0, data_series.len() as i64, 1)?
                        .into_series()
                        .filter(&non_zero_mask)?;
                    let indices_arr = match use_offset_indices {
                        true => {
                            let offsets_values = indices
                                .u64()?
                                .as_slice()
                                .iter()
                                .scan(0, |previous, &current| {
                                    let offset = current - *previous;
                                    *previous = current;
                                    Some(offset)
                                })
                                .collect::<Vec<_>>();
                            UInt64Array::from_vec("item", offsets_values).into_series()
                        }
                        false => indices,
                    };
                    non_zero_values.push(data);
                    non_zero_indices.push(indices_arr);
                }
                let offsets = OffsetBuffer::from_lengths(non_zero_values.iter().map(|s| s.len()));
                let non_zero_values_series =
                    Series::concat(&non_zero_values.iter().collect::<Vec<&Series>>())?;
                let non_zero_indices_series =
                    Series::concat(&non_zero_indices.iter().collect::<Vec<&Series>>())?;
                let offsets_cloned = offsets.clone();
                let data_list_arr = ListArray::new(
                    Field::new(
                        "values",
                        DataType::List(Box::new(non_zero_values_series.data_type().clone())),
                    ),
                    non_zero_values_series,
                    offsets.into(),
                    nulls.cloned(),
                );
                let indices_list_arr = ListArray::new(
                    Field::new(
                        "indices",
                        DataType::List(Box::new(non_zero_indices_series.data_type().clone())),
                    ),
                    non_zero_indices_series,
                    offsets_cloned.into(),
                    nulls.cloned(),
                );

                let largest_index = std::cmp::max(tensor_shape.iter().product::<u64>(), 1) - 1;
                let indices_minimal_inner_dtype = minimal_uint_dtype(largest_index);
                let casted_indices = indices_list_arr
                    .cast(&DataType::List(Box::new(indices_minimal_inner_dtype)))?;

                let sparse_struct_array = StructArray::new(
                    Field::new(self.name(), dtype.to_physical()),
                    vec![data_list_arr.into_series(), casted_indices],
                    nulls.cloned(),
                );
                Ok(FixedShapeSparseTensorArray::new(
                    Field::new(sparse_struct_array.name(), dtype.clone()),
                    sparse_struct_array,
                )
                .into_series())
            }
            (_, _) => self.physical.cast(dtype),
        }
    }
}

impl FixedSizeListArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            DataType::FixedSizeList(child_dtype, size) => {
                if size != &self.fixed_element_len() {
                    return Err(DaftError::ValueError(format!(
                        "Cannot cast from FixedSizeListSeries with size {} to size: {}",
                        self.fixed_element_len(),
                        size
                    )));
                }
                let casted_child = self.flat_child.cast(child_dtype.as_ref())?;
                Ok(Self::new(
                    Field::new(self.name().to_string(), dtype.clone()),
                    casted_child,
                    self.nulls().cloned(),
                )
                .into_series())
            }
            DataType::List(child_dtype) => {
                let element_size = self.fixed_element_len();
                let casted_child = self.flat_child.cast(child_dtype.as_ref())?;
                let offsets = OffsetBuffer::from_repeated_length(element_size, self.len());
                Ok(ListArray::new(
                    Field::new(self.name().to_string(), dtype.clone()),
                    casted_child,
                    offsets.into(),
                    self.nulls().cloned(),
                )
                .into_series())
            }
            DataType::FixedShapeTensor(child_datatype, shape) => {
                if child_datatype.as_ref() != self.child_data_type() {
                    return Err(DaftError::TypeError(format!(
                        "Cannot cast {} to {}: mismatched child type",
                        self.data_type(),
                        dtype
                    )));
                }
                if shape.iter().product::<u64>() != (self.fixed_element_len() as u64) {
                    return Err(DaftError::TypeError(format!(
                        "Cannot cast {} to {}: mismatch in element sizes",
                        self.data_type(),
                        dtype
                    )));
                }
                // TODO [mg]: I recall in testing that this shape is wrong. It flattens everything.
                //            It needs to preserve the shape! Otherwise it is *strictly wrong*.
                //            Why is this occurring when it deserializes??
                //            Need to figure out how to trace the data flow / debug so I can
                //            see where this incorrect decision occurs. Maybe because Parquet
                //            saves it as a big ol' fixed list? We need to recognize this
                //            by saving the type + shape in the Arrow schema, recognizing this, and
                //            deserializing into a real n-dimensional tensor!
                Ok(FixedShapeTensorArray::new(
                    Field::new(self.name().to_string(), dtype.clone()),
                    self.clone(),
                )
                .into_series())
            }
            DataType::FixedShapeImage(mode, h, w) => {
                if (h * w * mode.num_channels() as u32) as u64 != self.fixed_element_len() as u64 {
                    return Err(DaftError::TypeError(format!(
                        "Cannot cast {} to {}: mismatch in element sizes",
                        self.data_type(),
                        dtype
                    )));
                }
                Ok(FixedShapeImageArray::new(
                    Field::new(self.name().to_string(), dtype.clone()),
                    self.clone(),
                )
                .into_series())
            }
            DataType::Embedding(child_dtype, dimensionality) => {
                if **child_dtype != *self.child_data_type() {
                    return Err(DaftError::TypeError(format!(
                        "Cannot cast {} to {}: mismatched child type. Found {} but expected {}",
                        self.data_type(),
                        dtype,
                        child_dtype.as_ref(),
                        self.child_data_type(),
                    )));
                }
                if *dimensionality != self.fixed_element_len() {
                    return Err(DaftError::TypeError(format!(
                        "Cannot cast {} to {}: mismatched sizes. Found {} but expected {}",
                        self.data_type(),
                        dtype,
                        dimensionality,
                        self.fixed_element_len(),
                    )));
                }
                Ok(EmbeddingArray::new(
                    Field::new(self.name().to_string(), dtype.clone()),
                    self.clone(),
                )
                .into_series())
            }
            _ => unimplemented!("FixedSizeList casting not implemented for dtype: {}", dtype),
        }
    }
}

impl ListArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match dtype {
            DataType::List(child_dtype) => Ok(Self::new(
                Field::new(self.name(), dtype.clone()),
                self.flat_child.cast(child_dtype.as_ref())?,
                self.offsets().clone(),
                self.nulls().cloned(),
            )
            .into_series()),
            DataType::FixedSizeList(child_dtype, size) => {
                // Validate lengths of elements are equal to `size`
                let lengths_ok = match self.nulls() {
                    None => self.offsets().lengths().all(|l| l == *size),
                    Some(nulls) => self
                        .offsets()
                        .lengths()
                        .zip(nulls)
                        .all(|(l, valid)| (l == 0 && !valid) || l == *size),
                };
                if !lengths_ok {
                    return Err(DaftError::ComputeError(format!(
                        "Cannot cast List to FixedSizeList because not all elements have sizes: {}",
                        size
                    )));
                }

                // Cast child
                let mut casted_child = self.flat_child.cast(child_dtype.as_ref())?;
                // Build a FixedSizeListArray
                match self.nulls() {
                    // All valid, easy conversion -- everything is correctly sized and valid
                    None => {
                        // Slice child to match offsets if necessary
                        if casted_child.len() / size > self.len() {
                            casted_child = casted_child.slice(
                                *self.offsets().first().unwrap() as usize,
                                *self.offsets().last().unwrap() as usize,
                            )?;
                        }
                        Ok(FixedSizeListArray::new(
                            Field::new(self.name(), dtype.clone()),
                            casted_child,
                            None,
                        )
                        .into_series())
                    }
                    // Some invalids, we need to insert nulls into the child
                    // TODO(desmond): Migrate this to arrow-rs after migrating growable internals.
                    Some(nulls) => {
                        let mut child_growable = make_growable(
                            "item",
                            child_dtype.as_ref(),
                            vec![&casted_child],
                            true,
                            self.nulls().map_or(self.len() * size, |v| v.len() * size),
                        );

                        let mut invalid_ptr = 0;
                        for (start, end) in nulls.valid_slices() {
                            let len = end - start;
                            child_growable.add_nulls((start - invalid_ptr) * size);
                            let child_start = self.offsets().inner()[start] as usize;
                            child_growable.extend(0, child_start, len * size);
                            invalid_ptr = end;
                        }
                        child_growable.add_nulls((self.len() - invalid_ptr) * size);

                        Ok(FixedSizeListArray::new(
                            Field::new(self.name(), dtype.clone()),
                            child_growable.build()?,
                            self.nulls().cloned(),
                        )
                        .into_series())
                    }
                }
            }
            DataType::Map { .. } => Ok(MapArray::new(
                Field::new(self.name(), dtype.clone()),
                self.clone(),
            )
            .into_series()),
            DataType::Embedding(..) => {
                let result = self.cast(&dtype.to_physical())?;
                let embedding_array = EmbeddingArray::new(
                    Field::new(self.name(), dtype.clone()),
                    result.fixed_size_list()?.clone(),
                );
                Ok(embedding_array.into_series())
            }
            DataType::FixedShapeTensor(_, _) => {
                let result = self.cast(&dtype.to_physical())?;
                let result = result.fixed_size_list()?;
                result.cast(dtype)
            }
            _ => unimplemented!("List casting not implemented for dtype: {}", dtype),
        }
    }
}

impl MapArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        self.physical.cast(dtype)
    }
}

impl StructArray {
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        match (self.data_type(), dtype) {
            (DataType::Struct(self_fields), DataType::Struct(other_fields)) => {
                let self_field_names_to_idx: IndexMap<&str, usize> = IndexMap::from_iter(
                    self_fields
                        .iter()
                        .enumerate()
                        .map(|(i, f)| (f.name.as_str(), i)),
                );
                let casted_series = other_fields
                    .iter()
                    .map(
                        |field| match self_field_names_to_idx.get(field.name.as_str()) {
                            None => Ok(Series::full_null(
                                field.name.as_str(),
                                &field.dtype,
                                self.len(),
                            )),
                            Some(field_idx) => self.children[*field_idx].cast(&field.dtype),
                        },
                    )
                    .collect::<DaftResult<Vec<Series>>>();
                Ok(Self::new(
                    Field::new(self.name(), dtype.clone()),
                    casted_series?,
                    self.nulls().cloned(),
                )
                .into_series())
            }
            (DataType::Struct(..), DataType::Tensor(..)) => {
                let casted_struct_array =
                    self.cast(&dtype.to_physical())?.struct_().unwrap().clone();
                Ok(
                    TensorArray::new(Field::new(self.name(), dtype.clone()), casted_struct_array)
                        .into_series(),
                )
            }
            (DataType::Struct(..), DataType::SparseTensor(..)) => {
                let casted_struct_array =
                    self.cast(&dtype.to_physical())?.struct_().unwrap().clone();
                Ok(SparseTensorArray::new(
                    Field::new(self.name(), dtype.clone()),
                    casted_struct_array,
                )
                .into_series())
            }
            (DataType::Struct(..), DataType::FixedShapeSparseTensor(..)) => {
                let casted_struct_array =
                    self.cast(&dtype.to_physical())?.struct_().unwrap().clone();
                Ok(FixedShapeSparseTensorArray::new(
                    Field::new(self.name(), dtype.clone()),
                    casted_struct_array,
                )
                .into_series())
            }
            (DataType::Struct(..), DataType::Image(..)) => {
                let casted_struct_array =
                    self.cast(&dtype.to_physical())?.struct_().unwrap().clone();
                Ok(
                    ImageArray::new(Field::new(self.name(), dtype.clone()), casted_struct_array)
                        .into_series(),
                )
            }
            (DataType::Struct(..), DataType::List(child_dtype)) => {
                let casted_children = self
                    .children
                    .iter()
                    .map(|c| c.cast(child_dtype))
                    .collect::<DaftResult<Vec<_>>>()?;

                let lists = (0..self.len())
                    .map(|i| {
                        if self.is_valid(i) {
                            let slices_at_index = casted_children
                                .iter()
                                .map(|c| c.slice(i, i + 1))
                                .collect::<DaftResult<Vec<_>>>()?;

                            Some(Series::concat(&slices_at_index.iter().collect::<Vec<_>>()))
                                .transpose()
                        } else {
                            Ok(None)
                        }
                    })
                    .collect::<DaftResult<Vec<_>>>()?;

                Ok(ListArray::from_series(self.name(), lists)?.into_series())
            }
            (DataType::Struct(..), DataType::FixedSizeList(child_dtype, length))
                if *length == self.children.len() =>
            {
                let casted_children = self
                    .children
                    .iter()
                    .map(|c| c.cast(child_dtype))
                    .collect::<DaftResult<Vec<_>>>()?;

                let flat_slices = (0..self.len())
                    .flat_map(|i| casted_children.iter().map(move |c| c.slice(i, i + 1)))
                    .collect::<DaftResult<Vec<_>>>()?;

                let flat_child = Series::concat(&flat_slices.iter().collect::<Vec<_>>())?;

                Ok(FixedSizeListArray::new(
                    Field::new(self.name(), dtype.clone()),
                    flat_child,
                    self.nulls().cloned(),
                )
                .into_series())
            }
            (DataType::Struct(..), DataType::Embedding(_, length))
                if *length == self.children.len() =>
            {
                self.cast(&dtype.to_physical())?.cast(dtype)
            }
            _ => unimplemented!(
                "Daft casting from {} to {} not implemented",
                self.data_type(),
                dtype
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::{Rng, rng};

    use super::*;
    use crate::{
        datatypes::DataArray,
        prelude::{Decimal128Type, Float64Array},
    };

    fn create_test_decimal_array(
        values: Vec<i128>,
        precision: usize,
        scale: usize,
    ) -> DataArray<Decimal128Type> {
        let arrow_array = arrow::array::Decimal128Array::from(values)
            .with_precision_and_scale(precision as u8, scale as i8)
            .unwrap();
        let field = Arc::new(Field::new(
            "test_decimal",
            DataType::Decimal128(precision, scale),
        ));
        DataArray::<Decimal128Type>::from_arrow(field, Arc::new(arrow_array))
            .expect("Failed to create test decimal array")
    }

    fn create_test_f64_array(values: Vec<f64>) -> Float64Array {
        Float64Array::from_vec("test_float", values)
    }

    fn create_test_i64_array(values: Vec<i64>) -> Int64Array {
        Int64Array::from_vec("test_int", values)
    }

    // For a Decimal(p, s) to be valid, p, s, and max_val must satisfy:
    //   p > ceil(log_9(max_val * 10^s)) - 1
    // So with a max_val of 10^10, we get:
    //   p > ceil(log_9(10^(10+s))) - 1
    // Since p <= 32, for this inequality to hold, we need s <= 20.
    const MAX_VAL: f64 = 1e10;
    const MAX_SCALE: usize = 20;
    const MIN_DIFF_FOR_PRECISION: usize = 12;
    #[test]
    fn test_decimal_to_decimal_cast() {
        let mut rng = rng();
        let mut values: Vec<f64> = (0..100)
            .map(|_| rng.random_range(-MAX_VAL..MAX_VAL))
            .collect();
        values.extend_from_slice(&[0.0, -0.0]);

        let initial_scale: usize = rng.random_range(0..=MAX_SCALE);
        let initial_precision: usize =
            rng.random_range(initial_scale + MIN_DIFF_FOR_PRECISION..=32);
        let min_integral_comp = initial_precision - initial_scale;
        let i128_values: Vec<i128> = values
            .iter()
            .map(|&x| (x * 10_f64.powi(initial_scale as i32) as f64) as i128)
            .collect();
        let original = create_test_decimal_array(i128_values, initial_precision, initial_scale);

        // We always widen the Decimal, otherwise we lose information and can no longer compare with the original Decimal values.
        let intermediate_scale: usize = rng.random_range(initial_scale..=32 - min_integral_comp);
        let intermediate_precision: usize =
            rng.random_range(intermediate_scale + min_integral_comp..=32);

        let result = original
            .cast(&DataType::Decimal128(
                intermediate_precision,
                intermediate_scale,
            ))
            .expect("Failed to cast to intermediate decimal")
            .cast(&DataType::Decimal128(initial_precision, initial_scale))
            .expect("Failed to cast back to original decimal");

        assert!(
            original.into_series() == result,
            "Failed with intermediate decimal({}, {})",
            intermediate_precision,
            intermediate_scale,
        );
    }

    // We do fuzzy equality when comparing floats converted to and from decimals. This test is
    // primarily sanity checking that we don't repeat the mistake of shifting the scale and precision
    // of floats during casting, while avoiding flakiness due small differences in floats.
    #[test]
    fn test_decimal_to_float() {
        let mut rng = rng();
        let mut values: Vec<f64> = (0..100)
            .map(|_| rng.random_range(-MAX_VAL..MAX_VAL))
            .collect();
        values.extend_from_slice(&[0.0, -0.0]);
        let num_values = values.len();

        let scale: usize = rng.random_range(0..=MAX_SCALE);
        let precision: usize = rng.random_range(scale + MIN_DIFF_FOR_PRECISION..=32);
        // when the scale is 0, the created decimal values are integers, the epsilon should be 1
        let epsilon = if scale == 0 { 1f64 } else { 0.1f64 };

        let i128_values: Vec<i128> = values
            .iter()
            .map(|&x| (x * 10_f64.powi(scale as i32) as f64) as i128)
            .collect();
        let original = create_test_decimal_array(i128_values, precision, scale);

        let result = original
            .cast(&DataType::Float64)
            .expect("Failed to cast to float");
        let original = create_test_f64_array(values);

        let epsilon_series = create_test_f64_array(vec![epsilon; num_values]).into_series();

        assert!(
            result.fuzzy_eq(&original.into_series(), &epsilon_series),
            "Failed with decimal({}, {})",
            precision,
            scale,
        );
    }

    // 2^63 gives us 18 unrestricted digits. So precision - scale has to be <= 18.
    const MAX_DIFF_FOR_PRECISION: usize = 18;
    #[test]
    fn test_decimal_to_int() {
        let mut rng = rng();
        let mut values: Vec<f64> = (0..100)
            .map(|_| rng.random_range(-MAX_VAL..MAX_VAL))
            .collect();
        values.extend_from_slice(&[0.0, -0.0]);

        let scale: usize = rng.random_range(0..=MAX_SCALE);
        let precision: usize =
            rng.random_range(scale + MIN_DIFF_FOR_PRECISION..=scale + MAX_DIFF_FOR_PRECISION);
        let i128_values: Vec<i128> = values
            .iter()
            .map(|&x| (x * 10_f64.powi(scale as i32) as f64) as i128)
            .collect();
        let original = create_test_decimal_array(i128_values, precision, scale);

        let result = original
            .cast(&DataType::Int64)
            .expect("Failed to cast to int64");

        // Convert the original floats directly to integers.
        let values = values.into_iter().map(|f| f as i64).collect();
        let original = create_test_i64_array(values);

        assert!(
            original.into_series() == result,
            "Failed with decimal({}, {})",
            precision,
            scale,
        );
    }

    #[test]
    fn cast_fsl_into_embedding() {
        let fsl = FixedSizeListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::FixedSizeList(Box::new(DataType::Int64), 0),
            )),
            Series::empty("item", &DataType::Int64),
            None,
        );

        assert!(
            fsl.cast(&DataType::Embedding(Box::new(DataType::Int64), 0))
                .is_ok(),
            "Expected to be able to cast FixedSizeList into Embedding of same datatype & size."
        );
        assert!(
            fsl.cast(&DataType::Embedding(Box::new(DataType::Int64), 1))
                .is_err(),
            "Not expected to be able to cast FixedSizeList into Embedding with different size."
        );

        assert!(
            fsl.cast(&DataType::Embedding(Box::new(DataType::Float32), 0))
                .is_err(),
            "Not expected to be able to cast FixedSizeList into Embedding with different element type."
        );
    }

    // Tests for Utf8 to numeric casting with whitespace handling
    // These tests verify that leading/trailing whitespace is trimmed before parsing,
    // matching Python/Pandas/NumPy behavior.

    #[test]
    fn test_utf8_to_int32_with_whitespace() {
        let utf8_array = Utf8Array::from_iter(
            "test",
            vec![Some("  42  "), Some("-1"), Some("  100  "), None].into_iter(),
        );
        let result = utf8_array
            .cast(&DataType::Int32)
            .expect("Failed to cast Utf8 to Int32");

        let values: Vec<Option<i32>> = result.i32().unwrap().into_iter().collect();
        assert_eq!(values, vec![Some(42), Some(-1), Some(100), None]);
    }

    #[test]
    fn test_utf8_to_int64_with_whitespace() {
        let utf8_array = Utf8Array::from_iter(
            "test",
            vec![Some("  42  "), Some("  -9999999999  "), Some("\t123\n")].into_iter(),
        );
        let result = utf8_array
            .cast(&DataType::Int64)
            .expect("Failed to cast Utf8 to Int64");

        let values: Vec<Option<i64>> = result.i64().unwrap().into_iter().collect();
        assert_eq!(values, vec![Some(42), Some(-9999999999), Some(123)]);
    }

    #[test]
    fn test_utf8_to_float64_with_whitespace() {
        let utf8_array = Utf8Array::from_iter(
            "test",
            vec![Some("  3.14  "), Some("-2.5"), Some("  1e10  "), None].into_iter(),
        );
        let result = utf8_array
            .cast(&DataType::Float64)
            .expect("Failed to cast Utf8 to Float64");

        let values: Vec<Option<f64>> = result.f64().unwrap().into_iter().collect();
        assert_eq!(values, vec![Some(3.14), Some(-2.5), Some(1e10), None]);
    }

    #[test]
    fn test_utf8_to_float32_with_whitespace() {
        let utf8_array =
            Utf8Array::from_iter("test", vec![Some("  3.14  "), Some("  -2.5  ")].into_iter());
        let result = utf8_array
            .cast(&DataType::Float32)
            .expect("Failed to cast Utf8 to Float32");

        let values: Vec<Option<f32>> = result.f32().unwrap().into_iter().collect();
        assert_eq!(values, vec![Some(3.14_f32), Some(-2.5_f32)]);
    }

    #[test]
    fn test_utf8_to_int_invalid_returns_none() {
        // Invalid strings should return None, not error
        let utf8_array = Utf8Array::from_iter(
            "test",
            vec![Some("  42  "), Some("not_a_number"), Some("  ")].into_iter(),
        );
        let result = utf8_array
            .cast(&DataType::Int32)
            .expect("Failed to cast Utf8 to Int32");

        let values: Vec<Option<i32>> = result.i32().unwrap().into_iter().collect();
        assert_eq!(values, vec![Some(42), None, None]);
    }

    // Tests for Utf8 to Date casting with whitespace handling
    // Note: Date parsing already handles whitespace (this test documents existing behavior)

    #[test]
    fn test_utf8_to_date_with_whitespace() {
        let utf8_array = Utf8Array::from_iter(
            "test",
            vec![
                Some("  2024-01-01  "),
                Some("2024-06-15"),
                Some("  2024-12-31  "),
                None,
            ]
            .into_iter(),
        );
        let result = utf8_array
            .cast(&DataType::Date)
            .expect("Failed to cast Utf8 to Date");

        // Date is stored as days since epoch (1970-01-01)
        // 2024-01-01 = 19723 days, 2024-06-15 = 19889 days, 2024-12-31 = 20088 days
        let date_array = result.date().unwrap();
        let values: Vec<Option<i32>> = date_array.physical.into_iter().collect();
        // Check that we got valid dates (not None from failed parse)
        assert!(values[0].is_some(), "First date should parse successfully");
        assert!(values[1].is_some(), "Second date should parse successfully");
        assert!(values[2].is_some(), "Third date should parse successfully");
    }
}
