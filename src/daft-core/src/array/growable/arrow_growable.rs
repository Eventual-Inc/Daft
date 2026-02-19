use std::{marker::PhantomData, sync::Arc};

use arrow::{
    array::{ArrayData, BooleanBufferBuilder, NullBufferBuilder, make_array},
    buffer::{Buffer, MutableBuffer, ScalarBuffer},
};
use common_error::DaftResult;

use super::Growable;
use crate::{
    array::prelude::*,
    datatypes::prelude::*,
    series::{IntoSeries, Series},
};

/// Handles three physical buffer layouts for direct buffer manipulation.
/// Selected at construction time based on Daft `DataType`.
enum ValueGrower {
    /// Fixed-width types: primitives, decimal, interval, temporals, and fixed-size binary.
    /// Single contiguous buffer of elements with known byte width.
    FixedWidth {
        buffer: MutableBuffer,
        byte_width: usize,
    },
    /// Bit-packed boolean values.
    Boolean { builder: BooleanBufferBuilder },
    /// Variable-length types (LargeUtf8, LargeBinary): i64 offsets + value bytes.
    VarLen {
        offsets: Vec<i64>,
        values: MutableBuffer,
    },
}

/// Select the appropriate ValueGrower variant based on the Daft DataType.
/// Temporal types (Date, Timestamp, etc.) are included for the Extension recursive case.
fn grower_from_dtype(dtype: &DataType, capacity: usize) -> ValueGrower {
    let fixed_width = |bw: usize| ValueGrower::FixedWidth {
        buffer: MutableBuffer::new(capacity * bw),
        byte_width: bw,
    };
    match dtype {
        DataType::Boolean => ValueGrower::Boolean {
            builder: BooleanBufferBuilder::new(capacity),
        },
        DataType::Int8 | DataType::UInt8 => fixed_width(1),
        DataType::Int16 | DataType::UInt16 => fixed_width(2),
        DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date => fixed_width(4),
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Timestamp(..)
        | DataType::Duration(..)
        | DataType::Time(..) => fixed_width(8),
        DataType::Decimal128(..) | DataType::Interval => fixed_width(16),
        DataType::Utf8 | DataType::Binary => ValueGrower::VarLen {
            offsets: {
                let mut v = Vec::with_capacity(capacity + 1);
                v.push(0i64);
                v
            },
            values: MutableBuffer::new(0),
        },
        DataType::FixedSizeBinary(n) => fixed_width(*n),
        DataType::Extension(_, inner, _) => grower_from_dtype(inner, capacity),
        other => panic!("Unsupported DataType for ArrowGrowable: {other:?}"),
    }
}

/// High-performance growable for all `DaftArrowBackedType` variants.
///
/// Instead of deferring operations to `MutableArrayData` (which uses internal
/// function-pointer dispatch), this copies directly into raw buffers using
/// `extend_from_slice` - about 2x faster than arrow2's growable.
pub struct ArrowGrowable<'a, T: DaftArrowBackedType> {
    name: String,
    dtype: DataType,
    arrow_dtype: arrow::datatypes::DataType,
    source_data: Vec<ArrayData>,
    grower: ValueGrower,
    validity: Option<NullBufferBuilder>,
    len: usize,
    _phantom: PhantomData<&'a T>,
}

impl<'a, T: DaftArrowBackedType> ArrowGrowable<'a, T> {
    pub fn new(
        name: &str,
        dtype: &DataType,
        arrays: Vec<&'a DataArray<T>>,
        use_validity: bool,
        capacity: usize,
    ) -> Self {
        let source_data: Vec<ArrayData> = arrays.iter().map(|s| s.to_data()).collect();

        // Get arrow dtype from first source (handles extension types correctly,
        // since Extension dtype cannot go through DataType::to_arrow()).
        let arrow_dtype = source_data
            .first()
            .map(|d| d.data_type().clone())
            .unwrap_or_else(|| dtype.to_arrow().unwrap_or(arrow::datatypes::DataType::Null));

        let grower = grower_from_dtype(dtype, capacity);

        let needs_validity = use_validity || source_data.iter().any(|d| d.nulls().is_some());
        let validity = if needs_validity {
            Some(NullBufferBuilder::new(capacity))
        } else {
            None
        };

        Self {
            name: name.to_string(),
            dtype: dtype.clone(),
            arrow_dtype,
            source_data,
            grower,
            validity,
            len: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T: DaftArrowBackedType> Growable for ArrowGrowable<'_, T>
where
    DataArray<T>: IntoSeries,
{
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        let source = &self.source_data[index];

        // Extend validity bitmap.
        // NullBuffer from ArrayData::nulls() has offset baked in, so use logical indices.
        if let Some(ref mut validity) = self.validity {
            if let Some(nulls) = source.nulls() {
                validity.append_buffer(&nulls.slice(start, len));
            } else {
                validity.append_n_non_nulls(len);
            }
        }

        // Extend value buffer(s).
        // Raw buffers from ArrayData::buffers() are NOT offset-adjusted,
        // so we must add source.offset() for physical byte access.
        let offset = source.offset();
        match &mut self.grower {
            ValueGrower::FixedWidth { buffer, byte_width } => {
                let bw = *byte_width;
                let src = source.buffers()[0].as_slice();
                let byte_start = (offset + start) * bw;
                buffer.extend_from_slice(&src[byte_start..byte_start + len * bw]);
            }
            ValueGrower::Boolean { builder } => {
                let src = source.buffers()[0].as_slice();
                let base = offset + start;
                for i in 0..len {
                    let bit_idx = base + i;
                    let is_set = (src[bit_idx / 8] >> (bit_idx % 8)) & 1 == 1;
                    builder.append(is_set);
                }
            }
            ValueGrower::VarLen { offsets, values } => {
                debug_assert!(
                    matches!(
                        source.data_type(),
                        arrow::datatypes::DataType::LargeUtf8
                            | arrow::datatypes::DataType::LargeBinary
                    ),
                    "VarLen grower expects LargeUtf8/LargeBinary but got {:?}",
                    source.data_type()
                );
                let src_offsets: &[i64] = source.buffers()[0].typed_data();
                let src_values = source.buffers()[1].as_slice();
                let base = offset + start;
                for i in 0..len {
                    let idx = base + i;
                    let val_start = src_offsets[idx] as usize;
                    let val_end = src_offsets[idx + 1] as usize;
                    values.extend_from_slice(&src_values[val_start..val_end]);
                    offsets.push(offsets.last().unwrap() + (val_end - val_start) as i64);
                }
            }
        }

        self.len += len;
    }

    #[inline]
    fn add_nulls(&mut self, additional: usize) {
        if let Some(ref mut validity) = self.validity {
            validity.append_n_nulls(additional);
        }

        match &mut self.grower {
            ValueGrower::FixedWidth { buffer, byte_width } => {
                buffer.resize(buffer.len() + additional * *byte_width, 0);
            }
            ValueGrower::Boolean { builder } => {
                builder.append_n(additional, false);
            }
            ValueGrower::VarLen { offsets, .. } => {
                let last = *offsets.last().unwrap();
                offsets.resize(offsets.len() + additional, last);
            }
        }

        self.len += additional;
    }

    fn build(&mut self) -> DaftResult<Series> {
        let null_buffer = self.validity.as_mut().and_then(|v| v.finish());

        // Collect buffers from the grower, then build ArrayData.
        let buffers: Vec<Buffer> = match &mut self.grower {
            ValueGrower::FixedWidth { buffer, .. } => {
                vec![std::mem::replace(buffer, MutableBuffer::new(0)).into()]
            }
            ValueGrower::Boolean { builder } => {
                vec![builder.finish().into_inner()]
            }
            ValueGrower::VarLen { offsets, values } => {
                let offsets_vec = std::mem::replace(offsets, vec![0i64]);
                vec![
                    ScalarBuffer::from(offsets_vec).into_inner(),
                    std::mem::replace(values, MutableBuffer::new(0)).into(),
                ]
            }
        };

        // SAFETY: buffers are constructed correctly by extend/add_nulls.
        let mut builder = ArrayData::builder(self.arrow_dtype.clone())
            .len(self.len)
            .nulls(null_buffer);
        for buf in buffers {
            builder = builder.add_buffer(buf);
        }
        let data = unsafe { builder.build_unchecked() };

        self.len = 0;

        let arrow_array = make_array(data);
        let field = Arc::new(Field::new(self.name.clone(), self.dtype.clone()));
        Ok(DataArray::<T>::from_arrow(field, arrow_array)?.into_series())
    }
}

/// Simplified null growable — just tracks a length counter.
/// No sources needed since every element is null.
pub struct ArrowNullGrowable {
    name: String,
    dtype: DataType,
    len: usize,
}

impl ArrowNullGrowable {
    pub fn new(name: &str, dtype: &DataType) -> Self {
        Self {
            name: name.to_string(),
            dtype: dtype.clone(),
            len: 0,
        }
    }
}

impl Growable for ArrowNullGrowable {
    #[inline]
    fn extend(&mut self, _index: usize, _start: usize, len: usize) {
        self.len += len;
    }

    #[inline]
    fn add_nulls(&mut self, additional: usize) {
        self.len += additional;
    }

    #[inline]
    fn build(&mut self) -> DaftResult<Series> {
        let len = self.len;
        self.len = 0;
        Ok(NullArray::full_null(&self.name, &self.dtype, len).into_series())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies that extend with a non-zero start correctly offsets into the source buffer.
    #[test]
    fn test_extend_from_nonzero_start() {
        let field = Field::new("test", DataType::Int32);
        let src = Int32Array::from_iter(
            field.clone(),
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        );
        let mut growable =
            ArrowGrowable::<Int32Type>::new("test", &DataType::Int32, vec![&src], false, 0);
        // Take elements at indices 2..4 → [30, 40]
        growable.extend(0, 2, 2);
        let result = growable.build().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.i32().unwrap().get(0), Some(30));
        assert_eq!(result.i32().unwrap().get(1), Some(40));
    }
}
