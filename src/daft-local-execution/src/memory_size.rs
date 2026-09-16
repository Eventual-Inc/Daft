use arrow_array::{
    Array, BinaryArray, FixedSizeListArray, LargeBinaryArray, LargeListArray, LargeStringArray,
    ListArray, MapArray, OffsetSizeTrait, StringArray, StructArray,
};
use arrow_schema::DataType;
use common_error::{DaftError, DaftResult};
use daft_core::{prelude::DataType as DaftType, series::Series};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

/// Size of the selected values materialized by an operator. Python objects are shared:
/// account their reference slots, not a pickle representation of those objects.
/// This measures native materialization, not serialization or Python heap size.
pub(crate) fn batch_bytes(batch: &RecordBatch) -> DaftResult<u64> {
    batch
        .as_materialized_series()
        .into_iter()
        .try_fold(0_u64, |bytes, series| {
            Ok(bytes.saturating_add(SeriesSize::new(series)?.bytes(0, series.len())?))
        })
}

pub(crate) fn partition_bytes(partition: &MicroPartition) -> DaftResult<u64> {
    partition
        .record_batches()
        .iter()
        .try_fold(0_u64, |bytes, batch| {
            Ok(bytes.saturating_add(batch_bytes(batch)?))
        })
}

/// In-memory Python payloads are reference-counted objects. Sorting copies references, and
/// must not require those objects (including nested payloads) to support pickling.
/// Maps also use native sizing: Daft retains i64 offsets, unlike Arrow Map's i32 offsets.
pub(crate) enum SeriesSize {
    Arrow(arrow_array::ArrayRef),
    Native(Series),
}

fn needs_native_size(dtype: &DaftType) -> bool {
    if dtype.is_python() {
        return true;
    }
    match dtype {
        DaftType::List(child)
        | DaftType::FixedSizeList(child, _)
        | DaftType::Extension(_, child, _) => needs_native_size(child),
        DaftType::Struct(fields) => fields.iter().any(|field| needs_native_size(&field.dtype)),
        DaftType::Map { .. } => true,
        _ => false,
    }
}

impl SeriesSize {
    pub fn new(series: &Series) -> DaftResult<Self> {
        if needs_native_size(series.data_type()) {
            Ok(Self::Native(series.clone()))
        } else {
            Ok(Self::Arrow(series.to_arrow()?))
        }
    }

    pub fn bytes(&self, start: usize, len: usize) -> DaftResult<u64> {
        match self {
            Self::Arrow(array) => selected_array_bytes(array.as_ref(), start, len),
            Self::Native(series) => native_bytes(&series.slice(start, start + len)?),
        }
    }

    /// Additional scratch space used by Daft's nested take kernels. Top-level
    /// row indices and output buffers are accounted by the caller separately.
    pub fn take_workspace_bytes(&self, start: usize, len: usize) -> DaftResult<u64> {
        match self {
            Self::Arrow(array) => Ok(take_array_workspace(array.as_ref(), start, len)),
            Self::Native(series) => native_take_workspace(&series.slice(start, start + len)?),
        }
    }

    /// A fixed one-row materialization bound, including validity when present.
    /// Such columns do not need to be visited row by row to find the largest row.
    pub fn constant_row_bytes(&self) -> Option<u64> {
        match self {
            Self::Arrow(array) => {
                let data = match array.data_type() {
                    DataType::Null => 0,
                    DataType::Boolean => 1,
                    DataType::FixedSizeBinary(width) => *width as u64,
                    dtype => dtype.primitive_width()? as u64,
                };
                Some(data + u64::from(array.nulls().is_some()))
            }
            Self::Native(series) if series.data_type().is_python() => {
                Some(std::mem::size_of::<usize>() as u64 + 1)
            }
            Self::Native(_) => None,
        }
    }
}

fn list_index_bytes(children: usize) -> u64 {
    // ListArray::take grows Vec<usize> and converts it to UInt64 indices. Allow
    // both buffers to coexist, including Vec's geometric growth/minimum capacity.
    (children as u64).saturating_mul(24).saturating_add(32)
}

fn take_array_workspace(array: &dyn Array, start: usize, len: usize) -> u64 {
    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let offsets = list.value_offsets();
            let first = offsets[start] as usize;
            let children = offsets[start + len] as usize - first;
            list_index_bytes(children).saturating_add(take_array_workspace(
                list.values().as_ref(),
                first,
                children,
            ))
        }
        DataType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let offsets = list.value_offsets();
            let first = offsets[start] as usize;
            let children = offsets[start + len] as usize - first;
            list_index_bytes(children).saturating_add(take_array_workspace(
                list.values().as_ref(),
                first,
                children,
            ))
        }
        DataType::FixedSizeList(_, width) => {
            let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let children = len * *width as usize;
            (children as u64)
                .saturating_mul(8)
                .saturating_add(take_array_workspace(
                    list.values().as_ref(),
                    start * *width as usize,
                    children,
                ))
        }
        DataType::Struct(_) => array
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .columns()
            .iter()
            .fold(0_u64, |bytes, child| {
                bytes.saturating_add(take_array_workspace(child.as_ref(), start, len))
            }),
        DataType::Map(_, _) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            let offsets = map.value_offsets();
            let first = offsets[start] as usize;
            let children = offsets[start + len] as usize - first;
            list_index_bytes(children).saturating_add(take_array_workspace(
                map.entries(),
                first,
                children,
            ))
        }
        _ => 0,
    }
}

fn native_take_workspace(series: &Series) -> DaftResult<u64> {
    if series.data_type().is_python() {
        return Ok(0);
    }
    match series.data_type() {
        DaftType::List(_) | DaftType::Map { .. } => {
            let list = if matches!(series.data_type(), DaftType::Map { .. }) {
                &series.map()?.physical
            } else {
                series.list()?
            };
            let first = list.offsets()[0] as usize;
            let end = list.offsets()[series.len()] as usize;
            Ok(list_index_bytes(end - first)
                .saturating_add(native_take_workspace(&list.flat_child.slice(first, end)?)?))
        }
        DaftType::FixedSizeList(_, _) => {
            let child = &series.fixed_size_list()?.flat_child;
            Ok((child.len() as u64)
                .saturating_mul(8)
                .saturating_add(native_take_workspace(child)?))
        }
        DaftType::Struct(_) => series
            .struct_()?
            .children
            .iter()
            .try_fold(0_u64, |bytes, child| {
                Ok(bytes.saturating_add(native_take_workspace(child)?))
            }),
        DaftType::Extension(..) => native_take_workspace(&series.as_physical()?),
        _ => Ok(take_array_workspace(
            series.to_arrow()?.as_ref(),
            0,
            series.len(),
        )),
    }
}

fn offset_bytes<T: OffsetSizeTrait>(offsets: &[T], start: usize, len: usize) -> u64 {
    (offsets[start + len].as_usize() - offsets[start].as_usize()) as u64
        + (len as u64 + 1) * std::mem::size_of::<T>() as u64
}

/// Read lengths and offsets directly for flat columns. In particular, finding a
/// row's size must not allocate an Arrow slice and ArrayData for every column.
fn selected_array_bytes(array: &dyn Array, start: usize, len: usize) -> DaftResult<u64> {
    // Keep a bitmap allowance even if this selection happens to contain no nulls.
    // Discovering that by scanning validity would make sizing linear in row count.
    let validity = array.nulls().map_or(0, |_| len.div_ceil(8) as u64);
    let data = match array.data_type() {
        DataType::Null => 0,
        DataType::Boolean => len.div_ceil(8) as u64,
        DataType::FixedSizeBinary(width) => (len as u64).saturating_mul(*width as u64),
        DataType::Utf8 => offset_bytes(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value_offsets(),
            start,
            len,
        ),
        DataType::LargeUtf8 => offset_bytes(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value_offsets(),
            start,
            len,
        ),
        DataType::Binary => offset_bytes(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value_offsets(),
            start,
            len,
        ),
        DataType::LargeBinary => offset_bytes(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value_offsets(),
            start,
            len,
        ),
        dtype => match dtype.primitive_width() {
            Some(width) => (len as u64).saturating_mul(width as u64),
            None => return array_bytes(array.slice(start, len).as_ref()),
        },
    };
    Ok(data.saturating_add(validity))
}

fn native_bytes(series: &Series) -> DaftResult<u64> {
    let validity = series.len().div_ceil(8) as u64;
    if series.data_type().is_python() {
        return Ok(series.len() as u64 * std::mem::size_of::<usize>() as u64 + validity);
    }
    match series.data_type() {
        DaftType::List(_) | DaftType::Map { .. } => {
            let list = if matches!(series.data_type(), DaftType::Map { .. }) {
                &series.map()?.physical
            } else {
                series.list()?
            };
            let offsets = list.offsets();
            let start = offsets[0] as usize;
            let end = offsets[series.len()] as usize;
            Ok(validity
                + (series.len() as u64 + 1) * 8
                + native_bytes(&list.flat_child.slice(start, end)?)?)
        }
        DaftType::FixedSizeList(_, _) => {
            Ok(validity + native_bytes(&series.fixed_size_list()?.flat_child)?)
        }
        DaftType::Struct(_) => series
            .struct_()?
            .children
            .iter()
            .try_fold(validity, |bytes, child| Ok(bytes + native_bytes(child)?)),
        DaftType::Extension(..) => native_bytes(&series.as_physical()?),
        _ => array_bytes(series.to_arrow()?.as_ref()),
    }
}

/// A conservative bound for materializing the selected values, excluding shared backing
/// buffers outside the selection. Offsets and validity are counted even for one-row slices.
pub(crate) fn array_bytes(array: &dyn Array) -> DaftResult<u64> {
    let validity = array.nulls().map_or(0, |_| array.len().div_ceil(8) as u64);
    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let offsets = list.value_offsets();
            let start = offsets[0] as usize;
            let end = offsets[array.len()] as usize;
            Ok(validity
                + (array.len() as u64 + 1) * 4
                + array_bytes(list.values().slice(start, end - start).as_ref())?)
        }
        DataType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let offsets = list.value_offsets();
            let start = offsets[0] as usize;
            let end = offsets[array.len()] as usize;
            Ok(validity
                + (array.len() as u64 + 1) * 8
                + array_bytes(list.values().slice(start, end - start).as_ref())?)
        }
        DataType::Map(_, _) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            let offsets = map.value_offsets();
            let start = offsets[0] as usize;
            let end = offsets[array.len()] as usize;
            Ok(validity
                + (array.len() as u64 + 1) * 4
                + array_bytes(&map.entries().slice(start, end - start))?)
        }
        DataType::FixedSizeList(_, _) => {
            let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            Ok(validity + array_bytes(list.values().as_ref())?)
        }
        DataType::Struct(_) => {
            let value = array.as_any().downcast_ref::<StructArray>().unwrap();
            value.columns().iter().try_fold(validity, |sum, child| {
                Ok(sum.saturating_add(array_bytes(child.as_ref())?))
            })
        }
        DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => {
            // Arrow's slice size counts N offsets; a materialized array needs N + 1.
            let offset = if matches!(array.data_type(), DataType::Utf8 | DataType::Binary) {
                4
            } else {
                8
            };
            Ok(array
                .to_data()
                .get_slice_memory_size()
                .map_err(|error| DaftError::ComputeError(error.to_string()))? as u64
                + offset)
        }
        // View, dictionary and union layouts can retain auxiliary buffers. Count their
        // full buffers conservatively rather than claiming an average row size is a bound.
        DataType::Utf8View
        | DataType::BinaryView
        | DataType::Dictionary(_, _)
        | DataType::Union(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::RunEndEncoded(_, _) => Ok(array.get_buffer_memory_size() as u64),
        _ => Ok(array
            .to_data()
            .get_slice_memory_size()
            .map_err(|error| DaftError::ComputeError(error.to_string()))? as u64),
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{BooleanArray, Int64Array, NullArray, types::Int64Type};

    use super::*;

    #[test]
    fn sliced_strings_use_selected_offsets() {
        let values = LargeStringArray::from(vec!["x".repeat(1_000_000), "abc".into()]);
        assert_eq!(array_bytes(&values.slice(1, 1)).unwrap(), 19);
        assert_eq!(array_bytes(&values.slice(0, 1)).unwrap(), 1_000_016);
    }

    #[test]
    fn sliced_lists_only_count_selected_children() {
        let values = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(1); 10_000]),
            Some(vec![Some(2)]),
        ]);
        assert!(array_bytes(&values.slice(1, 1)).unwrap() < 32);
    }

    #[test]
    fn map_slices_only_account_selected_entries_in_arrow_and_native_layouts() {
        use std::sync::Arc;

        use arrow_array::builder::{
            BooleanBuilder, LargeListBuilder, LargeStringBuilder, MapBuilder,
        };
        use daft_core::prelude::Field;

        let mut builder = MapBuilder::new(
            None,
            LargeStringBuilder::new(),
            LargeListBuilder::new(BooleanBuilder::new()),
        );
        builder.keys().append_value("large");
        builder.values().values().append_slice(&vec![true; 65_536]);
        builder.values().append(true);
        builder.append(true).unwrap();
        builder.keys().append_value("small");
        builder.values().values().append_slice(&[true, false, true]);
        builder.values().append(true);
        builder.append(true).unwrap();
        builder.append(true).unwrap(); // empty map
        builder.append(false).unwrap(); // null map
        let map = builder.finish();
        let dtype = DaftType::Map {
            key: Box::new(DaftType::Utf8),
            value: Box::new(DaftType::List(Box::new(DaftType::Boolean))),
        };
        let series =
            Series::from_arrow(Field::new("m", dtype.clone()), Arc::new(map.clone())).unwrap();
        let size = SeriesSize::new(&series).unwrap();
        assert!(matches!(size, SeriesSize::Native(_)));
        assert!(size.bytes(0, 1).unwrap() > 8192);
        for row in 1..4 {
            assert!(array_bytes(&map.slice(row, 1)).unwrap() < 128);
            assert!(size.bytes(row, 1).unwrap() < 128);
        }
        // Nonzero slice offsets must also propagate through Map's nested values.
        let sliced = SeriesSize::new(&series.slice(1, 4).unwrap()).unwrap();
        assert_eq!(sliced.bytes(0, 1).unwrap(), size.bytes(1, 1).unwrap());
        assert_eq!(
            sliced.take_workspace_bytes(0, 1).unwrap(),
            list_index_bytes(1) + list_index_bytes(3)
        );
        let arrow_small = map.slice(1, 1);
        let native_small = size.bytes(1, 1).unwrap();
        assert!(
            native_small >= array_bytes(&arrow_small).unwrap() + 8,
            "native Map needs two i64 offsets, not i32 offsets"
        );

        let field = Arc::new(arrow_schema::Field::new("m", map.data_type().clone(), true));
        let nested = StructArray::from(vec![(field, Arc::new(map) as arrow_array::ArrayRef)]);
        let nested = Series::from_arrow(
            Field::new("s", DaftType::Struct(vec![Field::new("m", dtype)])),
            Arc::new(nested),
        )
        .unwrap();
        let size = SeriesSize::new(&nested).unwrap();
        assert!(matches!(size, SeriesSize::Native(_)));
        assert!(size.bytes(1, 1).unwrap() < 128);
    }

    #[test]
    fn nested_take_workspace_counts_selected_children_at_each_level() {
        use std::sync::Arc;

        use daft_core::prelude::Field;

        let lists = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(1); 10_000]),
            Some(vec![Some(2); 3]),
            None,
            Some(vec![Some(3); 5]),
        ]);
        let inner_field = Arc::new(arrow_schema::Field::new(
            "item",
            lists.data_type().clone(),
            true,
        ));
        let fixed = FixedSizeListArray::new(inner_field, 2, Arc::new(lists), None);
        let dtype = fixed.data_type().clone();
        let array = Arc::new(StructArray::from(vec![(
            Arc::new(arrow_schema::Field::new("nested", dtype, true)),
            Arc::new(fixed) as arrow_array::ArrayRef,
        )])) as arrow_array::ArrayRef;
        // Slice first to exercise nonzero offsets in both fixed and variable lists.
        let array = array.slice(1, 1);
        let native = Series::from_arrow(
            Field::new(
                "s",
                DaftType::Struct(vec![Field::new(
                    "nested",
                    DaftType::FixedSizeList(Box::new(DaftType::List(Box::new(DaftType::Int64))), 2),
                )]),
            ),
            array.clone(),
        )
        .unwrap();
        for size in [SeriesSize::Arrow(array), SeriesSize::Native(native)] {
            let expected = 2 * 8 + list_index_bytes(5);
            assert_eq!(size.take_workspace_bytes(0, 1).unwrap(), expected);
        }
    }

    #[test]
    fn boolean_list_indices_are_not_estimated_from_payload_bytes() {
        use std::sync::Arc;

        let width = 65_536;
        let array = FixedSizeListArray::new(
            Arc::new(arrow_schema::Field::new("item", DataType::Boolean, false)),
            width,
            Arc::new(BooleanArray::from(vec![false; 2 * width as usize])),
            None,
        );
        let size = SeriesSize::Arrow(Arc::new(array));
        assert_eq!(size.bytes(1, 1).unwrap(), width as u64 / 8);
        assert_eq!(size.take_workspace_bytes(1, 1).unwrap(), width as u64 * 8);
    }

    #[test]
    fn flat_column_sizes_match_materialized_slices() {
        let arrays: Vec<arrow_array::ArrayRef> = vec![
            std::sync::Arc::new(Int64Array::from(vec![Some(7), None, Some(19)])),
            std::sync::Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            std::sync::Arc::new(NullArray::new(3)),
            std::sync::Arc::new(StringArray::from(vec![Some("longer"), None, Some("x")])),
            std::sync::Arc::new(LargeStringArray::from(vec![
                Some("longer"),
                None,
                Some("x"),
            ])),
            std::sync::Arc::new(BinaryArray::from(vec![
                Some(b"longer".as_slice()),
                None,
                Some(b"x".as_slice()),
            ])),
            std::sync::Arc::new(LargeBinaryArray::from(vec![
                Some(b"longer".as_slice()),
                None,
                Some(b"x".as_slice()),
            ])),
        ];
        for array in arrays {
            // Include nonzero offsets, null-only selections, and empty selections.
            for base in 0..3 {
                let array = array.slice(base, 3 - base);
                let size = SeriesSize::Arrow(array.clone());
                for start in 0..=array.len() {
                    for len in 0..=array.len() - start {
                        let bytes = size.bytes(start, len).unwrap();
                        let sliced = array_bytes(array.slice(start, len).as_ref()).unwrap();
                        // Arrow may omit an all-valid selection's bitmap in ArrayData.
                        assert!(
                            bytes >= sliced && bytes - sliced <= len.div_ceil(8) as u64,
                            "{:?}, base={base}, start={start}, len={len}",
                            array.data_type(),
                        );
                        if len == 1
                            && let Some(fixed) = size.constant_row_bytes()
                        {
                            assert_eq!(fixed, size.bytes(start, len).unwrap());
                        }
                    }
                }
            }
        }
    }
}
