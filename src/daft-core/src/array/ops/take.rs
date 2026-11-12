use arrow::{
    array::NullBufferBuilder,
    buffer::{NullBuffer, OffsetBuffer},
    datatypes::ArrowNativeType,
};
use common_error::{DaftError, DaftResult};

use crate::{
    array::{
        growable::{Growable, GrowableArray},
        prelude::*,
    },
    datatypes::{FileArray, prelude::*},
    file::DaftMediaType,
};

impl<T> DataArray<T>
where
    T: DaftPhysicalType,
{
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let result = arrow::compute::take(self.to_arrow().as_ref(), idx.to_arrow().as_ref(), None)?;
        Self::from_arrow(self.field.clone(), result)
    }
}

macro_rules! impl_logicalarray_take {
    ($ArrayT:ty) => {
        impl $ArrayT {
            pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
                let new_array = self.physical.take(idx)?;
                Ok(Self::new(self.field.clone(), new_array))
            }
        }
    };
}

impl_logicalarray_take!(DateArray);
impl_logicalarray_take!(TimeArray);
impl_logicalarray_take!(DurationArray);
impl_logicalarray_take!(TimestampArray);
impl_logicalarray_take!(UuidArray);
impl_logicalarray_take!(EmbeddingArray);
impl_logicalarray_take!(ImageArray);
impl_logicalarray_take!(FixedShapeImageArray);
impl_logicalarray_take!(TensorArray);
impl_logicalarray_take!(SparseTensorArray);
impl_logicalarray_take!(FixedShapeSparseTensorArray);
impl_logicalarray_take!(FixedShapeTensorArray);
impl_logicalarray_take!(MapArray);

impl FixedSizeListArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let fixed_size = self.fixed_element_len();
        if idx.is_empty() {
            return Ok(Self::new(
                self.field.clone(),
                self.flat_child.take(idx)?,
                None,
            ));
        }
        let source_len = self
            .flat_child
            .len()
            .checked_div(fixed_size)
            .unwrap_or_else(|| self.nulls().map_or(0, NullBuffer::len));
        let out_of_bounds = if idx.null_count() == 0 {
            idx.as_slice()
                .iter()
                .max()
                .is_some_and(|&i| i >= source_len as u64)
        } else {
            idx.into_iter().flatten().any(|i| i >= source_len as u64)
        };
        if out_of_bounds {
            return Err(DaftError::ValueError(format!(
                "take index out of bounds for FixedSizeListArray of length {source_len}"
            )));
        }

        // Keep short lists and other child types on element take until benchmarks justify expanding this path.
        let use_growable = fixed_size >= 32
            && matches!(
                self.child_data_type(),
                DataType::Float16 | DataType::Float32
            );
        if !use_growable {
            if source_len == 0 {
                let child_idx =
                    UInt64Array::full_null("", &DataType::UInt64, idx.len() * fixed_size);
                return Ok(Self::new(
                    self.field.clone(),
                    self.flat_child.take(&child_idx)?,
                    Some(NullBuffer::new_null(idx.len())),
                ));
            }
            let mut child_indices = Vec::with_capacity(idx.len() * fixed_size);
            let mut nulls_builder = NullBufferBuilder::new(idx.len());
            for i in idx {
                match i {
                    None => {
                        nulls_builder.append_null();
                        child_indices.extend(std::iter::repeat_n(0, fixed_size));
                    }
                    Some(i) => {
                        let i = i.to_usize().unwrap();
                        nulls_builder.append(self.is_valid(i));
                        let start = i as u64 * fixed_size as u64;
                        child_indices.extend(start..start + fixed_size as u64);
                    }
                }
            }
            let child_idx = UInt64Array::from_vec("", child_indices);
            return Ok(Self::new(
                self.field.clone(),
                self.flat_child.take(&child_idx)?,
                nulls_builder.finish(),
            ));
        }
        let mut growable = Self::make_growable(
            self.name(),
            self.data_type(),
            vec![self],
            idx.null_count() > 0,
            idx.len(),
        );

        for i in idx {
            match i {
                None => growable.add_nulls(1),
                Some(i) => {
                    let i = i.to_usize().unwrap();
                    growable.extend(0, i, 1);
                }
            }
        }

        let mut result = growable.build()?.downcast::<Self>()?.clone();
        // Growables rebuild fields; rewrap the primitive child without copying its buffers.
        result.field = self.field.clone();
        result.flat_child = crate::series::Series::from_arrow(
            self.flat_child.field().clone(),
            result.flat_child.to_arrow()?,
        )?;
        Ok(result)
    }
}

impl ListArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let mut new_offsets = Vec::with_capacity(idx.len() + 1);
        new_offsets.push(0i64);

        let mut child_indices = Vec::new();
        let mut nulls_builder = NullBufferBuilder::new(idx.len());

        for i in idx {
            match i {
                None => {
                    nulls_builder.append_null();
                    new_offsets.push(*new_offsets.last().unwrap());
                }
                Some(i) => {
                    let start = self.offsets()[i as usize] as usize;
                    let end = self.offsets()[i as usize + 1] as usize;
                    child_indices.extend(start..end);
                    new_offsets.push(*new_offsets.last().unwrap() + (end - start) as i64);
                    nulls_builder.append(self.is_valid(i.to_usize().unwrap()));
                }
            }
        }
        let nulls = nulls_builder.finish();

        let child_idx = UInt64Array::from_values("", child_indices.into_iter().map(|i| i as u64));
        let new_child = self.flat_child.take(&child_idx)?;

        Ok(Self::new(
            self.field.clone(),
            new_child,
            OffsetBuffer::new(new_offsets.into()),
            nulls,
        ))
    }
}
impl StructArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let nulls = self.nulls().map(|v| {
            NullBuffer::from_iter(idx.into_iter().map(|i| match i {
                None => false,
                Some(i) => v.is_valid(i.to_usize().unwrap()),
            }))
        });
        Ok(Self::new(
            self.field.clone(),
            self.children
                .iter()
                .map(|c| c.take(idx))
                .collect::<DaftResult<Vec<_>>>()?,
            nulls,
        ))
    }
}
impl UnionArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let mut growable = Self::make_growable(
            self.name(),
            self.data_type(),
            vec![self],
            idx.null_count() > 0,
            idx.len(),
        );

        for i in idx {
            match i {
                None => {
                    growable.add_nulls(1);
                }
                Some(i) => {
                    growable.extend(0, i.to_usize().unwrap(), 1);
                }
            }
        }

        Ok(growable.build()?.downcast::<Self>()?.clone())
    }
}
impl<T> FileArray<T>
where
    T: DaftMediaType,
{
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let new_array = self.physical.take(idx)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

// TODO(desmond): Migrate this to arrow-rs after migrating growable internals.
#[cfg(feature = "python")]
impl PythonArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let mut growable = Self::make_growable(
            self.name(),
            self.data_type(),
            vec![self],
            idx.null_count() > 0,
            idx.len(),
        );

        for i in idx {
            match i {
                None => {
                    growable.add_nulls(1);
                }
                Some(i) => {
                    growable.extend(0, i.to_usize().unwrap(), 1);
                }
            }
        }

        Ok(growable.build()?.downcast::<Self>()?.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use super::*;
    use crate::series::IntoSeries;

    #[test]
    fn test_fixed_size_list_take_zero_size() -> DaftResult<()> {
        let array = FixedSizeListArray::new(
            Field::new(
                "vectors",
                DataType::FixedSizeList(Box::new(DataType::Float32), 0),
            ),
            Float32Array::from_values("components", std::iter::empty::<f32>()).into_series(),
            None,
        );
        for indices in [vec![], vec![None, None]] {
            let null_count = indices.len();
            let indices = UInt64Array::from_iter(Field::new("", DataType::UInt64), indices);
            let result = array.take(&indices)?;
            assert_eq!(result.field(), array.field());
            assert_eq!(result.fixed_element_len(), 0);
            assert_eq!(result.flat_child.len(), 0);
            assert_eq!(result.null_count(), null_count);
        }
        assert!(matches!(
            array.take(&UInt64Array::from_vec("", vec![0])),
            Err(DaftError::ValueError(_))
        ));
        let nullable =
            FixedSizeListArray::new(array.field, array.flat_child, Some(NullBuffer::new_null(3)));
        assert_eq!(
            nullable
                .take(&UInt64Array::from_vec("", vec![2]))?
                .null_count(),
            1
        );
        assert!(matches!(
            nullable.take(&UInt64Array::from_vec("", vec![3])),
            Err(DaftError::ValueError(_))
        ));
        Ok(())
    }

    #[test]
    fn test_fixed_size_list_take_preserves_field_metadata() -> DaftResult<()> {
        for dtype in [DataType::Float16, DataType::Float32] {
            for size in [2, 16, 17, 31, 32, 768] {
                let field = Arc::new(
                    Field::new(
                        "vectors",
                        DataType::FixedSizeList(Box::new(dtype.clone()), size),
                    )
                    .with_metadata(BTreeMap::from([("source".into(), "queries".into())])),
                );
                let child_field = Arc::new(
                    Field::new("components", dtype.clone())
                        .with_metadata(BTreeMap::from([("units".into(), "normalized".into())])),
                );
                let values = (0..size * 4).map(|i| (i % 7 != 0).then_some(i as f32));
                let child = match dtype {
                    DataType::Float16 => Float16Array::from_iter(
                        child_field.clone(),
                        values.map(|v| v.map(half::f16::from_f32)),
                    )
                    .into_series(),
                    DataType::Float32 => {
                        Float32Array::from_iter(child_field.clone(), values).into_series()
                    }
                    _ => unreachable!(),
                };
                let array = FixedSizeListArray::new(
                    field.clone(),
                    child,
                    Some(NullBuffer::from(vec![true, false, true, true])),
                )
                .slice(1, 4)?;

                for (source, indices) in [
                    (array.clone(), vec![]),
                    (
                        array.clone(),
                        vec![Some(2), None, Some(0), Some(1), Some(2)],
                    ),
                    (array.slice(0, 0)?, vec![None, None]),
                ] {
                    let indices = UInt64Array::from_iter(Field::new("", DataType::UInt64), indices);
                    let result = source.take(&indices)?;
                    assert_eq!(result.field(), field.as_ref());
                    assert_eq!(result.field().metadata, field.metadata);
                    assert_eq!(result.flat_child.field(), child_field.as_ref());
                    assert_eq!(result.flat_child.field().metadata, child_field.metadata);
                    let output = result.to_arrow()?;
                    let arrow::datatypes::DataType::FixedSizeList(arrow_child_field, _) =
                        output.data_type()
                    else {
                        unreachable!()
                    };
                    assert_eq!(
                        arrow_child_field.metadata(),
                        child_field.to_arrow()?.metadata()
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_fixed_size_list_take_preserves_nested_child_metadata() -> DaftResult<()> {
        let size = 768;
        let child_field = Arc::new(
            Field::new("components", DataType::List(Box::new(DataType::Float32)))
                .with_metadata(BTreeMap::from([("units".into(), "normalized".into())])),
        );
        let child = ListArray::new(
            child_field.clone(),
            Float32Array::from_values("scalars", (0..size * 4).map(|i| i as f32)).into_series(),
            OffsetBuffer::new((0..=size * 4).map(|i| i as i64).collect()),
            None,
        );
        let field = Arc::new(
            Field::new(
                "vectors",
                DataType::FixedSizeList(Box::new(child_field.dtype.clone()), size),
            )
            .with_metadata(BTreeMap::from([("source".into(), "queries".into())])),
        );
        let array =
            FixedSizeListArray::new(field.clone(), child.into_series(), None).slice(1, 4)?;

        for (source, indices) in [
            (array.clone(), vec![]),
            (
                array.clone(),
                vec![Some(2), None, Some(0), Some(1), Some(2)],
            ),
            (array.slice(0, 0)?, vec![None, None]),
        ] {
            let indices = UInt64Array::from_iter(Field::new("", DataType::UInt64), indices);
            let result = source.take(&indices)?;
            assert_eq!(result.field(), field.as_ref());
            assert_eq!(result.field().metadata, field.metadata);
            assert_eq!(result.flat_child.field(), child_field.as_ref());
            assert_eq!(result.flat_child.field().metadata, child_field.metadata);
            assert_eq!(result.flat_child.list()?.flat_child.name(), "scalars");
            let output = result.to_arrow()?;
            let arrow::datatypes::DataType::FixedSizeList(arrow_child_field, _) =
                output.data_type()
            else {
                unreachable!()
            };
            assert_eq!(
                arrow_child_field.metadata(),
                child_field.to_arrow()?.metadata()
            );
        }
        Ok(())
    }
}
