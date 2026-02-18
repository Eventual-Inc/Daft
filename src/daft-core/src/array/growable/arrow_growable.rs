use std::{marker::PhantomData, sync::Arc};

use arrow::{
    array::{ArrayData, make_array},
    buffer::{Buffer, MutableBuffer, ScalarBuffer},
};
use common_error::DaftResult;
use daft_arrow::{
    array::to_data,
    buffer::{BooleanBufferBuilder, NullBufferBuilder},
};

use super::Growable;
use crate::{
    array::prelude::*,
    datatypes::prelude::*,
    series::{IntoSeries, Series},
};

/// Interpret a Buffer's raw bytes as `&[i64]` for offset access.
///
/// # Safety
/// Caller must ensure the buffer was originally written as aligned i64 values.
/// This is safe for arrow offset buffers, which are guaranteed 64-byte aligned.
#[allow(clippy::cast_ptr_alignment)]
unsafe fn buffer_as_i64_slice(buf: &Buffer) -> &[i64] {
    unsafe { std::slice::from_raw_parts(buf.as_ptr().cast::<i64>(), buf.len() / 8) }
}

/// Handles four physical buffer layouts for direct buffer manipulation.
/// Selected at construction time based on Daft `DataType`.
enum ValueGrower {
    /// Fixed-width primitives (Int8-64, UInt8-64, Float32/64, Decimal128, Interval, temporals).
    Primitive {
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
    /// Fixed-size binary: contiguous buffer with known element size.
    FixedBinary {
        buffer: MutableBuffer,
        element_size: usize,
    },
}

/// Select the appropriate ValueGrower variant based on the Daft DataType.
fn grower_from_dtype(dtype: &DataType) -> ValueGrower {
    match dtype {
        DataType::Boolean => ValueGrower::Boolean {
            builder: BooleanBufferBuilder::new(0),
        },
        DataType::Int8 | DataType::UInt8 => ValueGrower::Primitive {
            buffer: MutableBuffer::new(0),
            byte_width: 1,
        },
        DataType::Int16 | DataType::UInt16 => ValueGrower::Primitive {
            buffer: MutableBuffer::new(0),
            byte_width: 2,
        },
        DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date => {
            ValueGrower::Primitive {
                buffer: MutableBuffer::new(0),
                byte_width: 4,
            }
        }
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Timestamp(..)
        | DataType::Duration(..)
        | DataType::Time(..) => ValueGrower::Primitive {
            buffer: MutableBuffer::new(0),
            byte_width: 8,
        },
        DataType::Decimal128(..) | DataType::Interval => ValueGrower::Primitive {
            buffer: MutableBuffer::new(0),
            byte_width: 16,
        },
        DataType::Utf8 | DataType::Binary => ValueGrower::VarLen {
            offsets: vec![0i64],
            values: MutableBuffer::new(0),
        },
        DataType::FixedSizeBinary(n) => ValueGrower::FixedBinary {
            buffer: MutableBuffer::new(0),
            element_size: *n,
        },
        DataType::Extension(_, inner, _) => grower_from_dtype(inner),
        other => panic!("Unsupported DataType for ArrowGrowable: {other:?}"),
    }
}

/// High-performance growable for all `DaftArrowBackedType` variants.
///
/// Instead of deferring operations to `MutableArrayData` (which uses internal
/// function-pointer dispatch), this copies directly into raw buffers using
/// `extend_from_slice` — ~2x faster than arrow2's growable.
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
        _capacity: usize,
    ) -> Self {
        let source_data: Vec<ArrayData> = arrays.iter().map(|s| to_data(s.data())).collect();

        // Get arrow dtype from first source (handles extension types correctly,
        // since Extension dtype cannot go through DataType::to_arrow()).
        let arrow_dtype = source_data
            .first()
            .map(|d| d.data_type().clone())
            .unwrap_or_else(|| dtype.to_arrow().unwrap_or(arrow::datatypes::DataType::Null));

        let grower = grower_from_dtype(dtype);

        let needs_validity = use_validity || source_data.iter().any(|d| d.nulls().is_some());
        let validity = if needs_validity {
            Some(NullBufferBuilder::new(0))
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
                for i in 0..len {
                    validity.append(nulls.is_valid(start + i));
                }
            } else {
                validity.append_n_non_nulls(len);
            }
        }

        // Extend value buffer(s).
        // Raw buffers from ArrayData::buffers() are NOT offset-adjusted,
        // so we must add source.offset() for physical byte access.
        let offset = source.offset();
        match &mut self.grower {
            ValueGrower::Primitive { buffer, byte_width } => {
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
                // SAFETY: arrow offset buffers are 64-byte aligned; i64 requires 8.
                let src_offsets = unsafe { buffer_as_i64_slice(&source.buffers()[0]) };
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
            ValueGrower::FixedBinary {
                buffer,
                element_size,
            } => {
                let es = *element_size;
                let src = source.buffers()[0].as_slice();
                let byte_start = (offset + start) * es;
                buffer.extend_from_slice(&src[byte_start..byte_start + len * es]);
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
            ValueGrower::Primitive { buffer, byte_width } => {
                buffer.resize(buffer.len() + additional * *byte_width, 0);
            }
            ValueGrower::Boolean { builder } => {
                builder.append_n(additional, false);
            }
            ValueGrower::VarLen { offsets, .. } => {
                let last = *offsets.last().unwrap();
                offsets.resize(offsets.len() + additional, last);
            }
            ValueGrower::FixedBinary {
                buffer,
                element_size,
            } => {
                buffer.resize(buffer.len() + additional * *element_size, 0);
            }
        }

        self.len += additional;
    }

    fn build(&mut self) -> DaftResult<Series> {
        let null_buffer = self.validity.as_mut().and_then(|v| v.finish());

        let data = match &mut self.grower {
            ValueGrower::Primitive { buffer, .. } => {
                let buf: Buffer = std::mem::replace(buffer, MutableBuffer::new(0)).into();
                // SAFETY: buffers are constructed correctly by extend/add_nulls.
                unsafe {
                    ArrayData::builder(self.arrow_dtype.clone())
                        .len(self.len)
                        .add_buffer(buf)
                        .nulls(null_buffer)
                        .build_unchecked()
                }
            }
            ValueGrower::Boolean { builder } => {
                let bool_buf = builder.finish();
                // SAFETY: boolean buffer is constructed correctly by extend/add_nulls.
                unsafe {
                    ArrayData::builder(self.arrow_dtype.clone())
                        .len(self.len)
                        .add_buffer(bool_buf.into_inner())
                        .nulls(null_buffer)
                        .build_unchecked()
                }
            }
            ValueGrower::VarLen { offsets, values } => {
                let offsets_vec = std::mem::replace(offsets, vec![0i64]);
                let offsets_buf: Buffer = ScalarBuffer::from(offsets_vec).into_inner();
                let values_buf: Buffer = std::mem::replace(values, MutableBuffer::new(0)).into();
                // SAFETY: offsets and values are constructed correctly by extend/add_nulls.
                unsafe {
                    ArrayData::builder(self.arrow_dtype.clone())
                        .len(self.len)
                        .add_buffer(offsets_buf)
                        .add_buffer(values_buf)
                        .nulls(null_buffer)
                        .build_unchecked()
                }
            }
            ValueGrower::FixedBinary { buffer, .. } => {
                let buf: Buffer = std::mem::replace(buffer, MutableBuffer::new(0)).into();
                // SAFETY: buffer is constructed correctly by extend/add_nulls.
                unsafe {
                    ArrayData::builder(self.arrow_dtype.clone())
                        .len(self.len)
                        .add_buffer(buf)
                        .nulls(null_buffer)
                        .build_unchecked()
                }
            }
        };

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

    /// Extends from a non-zero start to catch off-by-one errors in physical buffer access.
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
