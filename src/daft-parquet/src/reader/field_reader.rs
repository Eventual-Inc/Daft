use std::{collections::HashMap, sync::Arc};

use arrow::{array::ArrayRef, datatypes::Field as ArrowField};
use parquet::{
    arrow::{
        array_reader::{
            ArrayReader, FixedSizeListArrayReader, ListArrayReader, MapArrayReader,
            NullArrayReader, PrimitiveArrayReader, StructArrayReader, make_byte_array_reader,
            make_byte_view_array_reader, make_fixed_len_byte_array_reader,
        },
        arrow_reader::RowSelection,
    },
    basic::{ConvertedType, LogicalType, Repetition, Type as PhysicalType},
    column::page::{PageIterator, PageReader},
    data_type::{BoolType, DoubleType, FloatType, Int32Type, Int64Type, Int96Type},
    errors::{ParquetError, Result as ParquetResult},
    file::{metadata::ParquetMetaData, serialized_reader::SerializedPageReader},
    schema::types::{ColumnDescriptor, ColumnPath, Type as ParquetType},
};
use snafu::ResultExt;

use super::chunk_source::OffsetBytes;
use crate::ParquetColumnDecodeSnafu;

struct SinglePageIter {
    inner: Option<Box<dyn PageReader>>,
}

impl Iterator for SinglePageIter {
    type Item = ParquetResult<Box<dyn PageReader>>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.take().map(Ok)
    }
}

impl PageIterator for SinglePageIter {}

pub(super) fn leaves_for_top_fields(
    metadata: &ParquetMetaData,
    top_field_indices: &[usize],
) -> Vec<usize> {
    let root_fields = metadata
        .file_metadata()
        .schema_descr()
        .root_schema()
        .get_fields();
    // Prefix sums of leaf counts: starts[i]..starts[i+1] is the leaf range for top field i.
    let mut starts = Vec::with_capacity(root_fields.len() + 1);
    starts.push(0usize);
    for f in root_fields {
        starts.push(starts.last().unwrap() + count_primitive_leaves(f));
    }
    let mut out = Vec::new();
    for &i in top_field_indices {
        out.extend(starts[i]..starts[i + 1]);
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn build_primitive_leaf_reader(
    chunk_bytes: OffsetBytes,
    metadata: &ParquetMetaData,
    rg_idx: usize,
    leaf_idx: usize,
    arrow_type: arrow::datatypes::DataType,
    def_level: i16,
    rep_level: i16,
) -> ParquetResult<Box<dyn ArrayReader>> {
    let rg = metadata.row_group(rg_idx);
    let col_chunk = rg.column(leaf_idx);
    let total_rows = rg.num_rows() as usize;

    // Page locations come from the offset index when present.
    let page_locations = metadata.offset_index().and_then(|oi| {
        oi.get(rg_idx)
            .and_then(|cols| cols.get(leaf_idx))
            .map(|loc| loc.page_locations.clone())
    });

    let page_reader =
        SerializedPageReader::new(Arc::new(chunk_bytes), col_chunk, total_rows, page_locations)?;
    let pages: Box<dyn PageIterator> = Box::new(SinglePageIter {
        inner: Some(Box::new(page_reader)),
    });

    // File-level descriptor (per-column-chunk may carry stale logical-type
    // info after set_column_metadata).
    let file_col_descr = metadata.file_metadata().schema_descr().column(leaf_idx);
    let primitive_type = file_col_descr.self_type_ptr();
    let physical_type = file_col_descr.physical_type();

    // def/rep levels here are the leaf's levels within its wrapping context
    // (may differ from the file-level descriptor's when nested).
    let col_descr = Arc::new(ColumnDescriptor::new(
        primitive_type,
        def_level,
        rep_level,
        ColumnPath::new(vec![]),
    ));

    let reader: Box<dyn ArrayReader> = if matches!(arrow_type, arrow::datatypes::DataType::Null) {
        Box::new(NullArrayReader::<Int32Type>::new(pages, col_descr)?)
    } else {
        match physical_type {
            PhysicalType::BOOLEAN => Box::new(PrimitiveArrayReader::<BoolType>::new(
                pages,
                col_descr,
                Some(arrow_type),
            )?),
            PhysicalType::INT32 => Box::new(PrimitiveArrayReader::<Int32Type>::new(
                pages,
                col_descr,
                Some(arrow_type),
            )?),
            PhysicalType::INT64 => Box::new(PrimitiveArrayReader::<Int64Type>::new(
                pages,
                col_descr,
                Some(arrow_type),
            )?),
            PhysicalType::FLOAT => Box::new(PrimitiveArrayReader::<FloatType>::new(
                pages,
                col_descr,
                Some(arrow_type),
            )?),
            PhysicalType::DOUBLE => Box::new(PrimitiveArrayReader::<DoubleType>::new(
                pages,
                col_descr,
                Some(arrow_type),
            )?),
            PhysicalType::INT96 => Box::new(PrimitiveArrayReader::<Int96Type>::new(
                pages,
                col_descr,
                Some(arrow_type),
            )?),
            PhysicalType::BYTE_ARRAY => match arrow_type {
                arrow::datatypes::DataType::Utf8View | arrow::datatypes::DataType::BinaryView => {
                    make_byte_view_array_reader(pages, col_descr, Some(arrow_type))?
                }
                _ => make_byte_array_reader(pages, col_descr, Some(arrow_type))?,
            },
            PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                make_fixed_len_byte_array_reader(pages, col_descr, Some(arrow_type))?
            }
        }
    };
    Ok(reader)
}

fn parquet_type_is_list(t: &ParquetType) -> bool {
    if !t.is_group() {
        return false;
    }
    let info = t.get_basic_info();
    if let Some(lt) = info.logical_type_ref() {
        return lt == &LogicalType::List;
    }
    info.converted_type() == ConvertedType::LIST
}

fn parquet_type_has_single_repeated_child(t: &ParquetType) -> bool {
    if !t.is_group() {
        return false;
    }
    let children = t.get_fields();
    children.len() == 1
        && children[0].get_basic_info().has_repetition()
        && children[0].get_basic_info().repetition() == Repetition::REPEATED
}

/// Root schema has no repetition; treat as REQUIRED.
fn parquet_repetition(t: &ParquetType) -> Repetition {
    let info = t.get_basic_info();
    if info.has_repetition() {
        info.repetition()
    } else {
        Repetition::REQUIRED
    }
}

fn count_primitive_leaves(t: &ParquetType) -> usize {
    if t.is_primitive() {
        return 1;
    }
    t.get_fields()
        .iter()
        .map(|c| count_primitive_leaves(c))
        .sum()
}

fn leaf_index_for_top_field(metadata: &ParquetMetaData, top_field_idx: usize) -> usize {
    let root = metadata.file_metadata().schema_descr().root_schema();
    let root_fields = root.get_fields();
    let mut idx = 0usize;
    for (i, f) in root_fields.iter().enumerate() {
        if i == top_field_idx {
            break;
        }
        idx += count_primitive_leaves(f);
    }
    idx
}

fn apply_repetition(parent_def: i16, parent_rep: i16, repetition: Repetition) -> (i16, i16, bool) {
    match repetition {
        Repetition::OPTIONAL => (parent_def + 1, parent_rep, true),
        Repetition::REQUIRED => (parent_def, parent_rep, false),
        Repetition::REPEATED => (parent_def + 1, parent_rep + 1, false),
    }
}

/// `def_level` / `rep_level` are the LIST's own levels (not the item's).
fn wrap_in_list_like(
    item_reader: Box<dyn ArrayReader>,
    arrow_type: arrow::datatypes::DataType,
    def_level: i16,
    rep_level: i16,
    nullable: bool,
) -> Box<dyn ArrayReader> {
    match arrow_type {
        arrow::datatypes::DataType::LargeList(_) => Box::new(ListArrayReader::<i64>::new(
            item_reader,
            arrow_type,
            def_level,
            rep_level,
            nullable,
        )),
        arrow::datatypes::DataType::FixedSizeList(_, size) => {
            Box::new(FixedSizeListArrayReader::new(
                item_reader,
                size as usize,
                arrow_type,
                def_level,
                rep_level,
                nullable,
            ))
        }
        _ => Box::new(ListArrayReader::<i32>::new(
            item_reader,
            arrow_type,
            def_level,
            rep_level,
            nullable,
        )),
    }
}

fn build_top_field_reader(
    chunks: &HashMap<usize, OffsetBytes>,
    metadata: &ParquetMetaData,
    rg_idx: usize,
    top_field_idx: usize,
    arrow_field: &ArrowField,
) -> ParquetResult<(Box<dyn ArrayReader>, usize)> {
    let total_rows = metadata.row_group(rg_idx).num_rows() as usize;
    let mut leaf_idx = leaf_index_for_top_field(metadata, top_field_idx);
    let root_fields = metadata
        .file_metadata()
        .schema_descr()
        .root_schema()
        .get_fields();
    let parquet_type = &root_fields[top_field_idx];
    let builder = FieldReaderBuilder {
        chunks,
        metadata,
        rg_idx,
    };
    let reader = builder.build(parquet_type, arrow_field.data_type(), 0, 0, &mut leaf_idx)?;
    Ok((reader, total_rows))
}

/// Walks the parquet + arrow schemas in lockstep (mirrors arrow-rs's
/// private `complex::Visitor`) to build a tree of `Box<dyn ArrayReader>`.
/// `leaf_idx` is the depth-first cursor into parquet leaf columns.
/// `parent_def_level` / `parent_rep_level` are the PARENT-context level
/// counters (before this type's repetition is applied).
struct FieldReaderBuilder<'a> {
    chunks: &'a HashMap<usize, OffsetBytes>,
    metadata: &'a ParquetMetaData,
    rg_idx: usize,
}

impl FieldReaderBuilder<'_> {
    fn chunk_for(&self, leaf_idx: usize) -> ParquetResult<OffsetBytes> {
        self.chunks.get(&leaf_idx).cloned().ok_or_else(|| {
            ParquetError::General(format!(
                "FieldReaderBuilder: chunk for rg={} leaf={} not pre-fetched",
                self.rg_idx, leaf_idx
            ))
        })
    }

    fn build(
        &self,
        parquet_type: &ParquetType,
        arrow_type: &arrow::datatypes::DataType,
        parent_def_level: i16,
        parent_rep_level: i16,
        leaf_idx: &mut usize,
    ) -> ParquetResult<Box<dyn ArrayReader>> {
        if parquet_type.is_primitive() {
            return self.build_primitive(
                parquet_type,
                arrow_type,
                parent_def_level,
                parent_rep_level,
                leaf_idx,
            );
        }
        match parquet_type.get_basic_info().converted_type() {
            ConvertedType::LIST => self.build_list(
                parquet_type,
                arrow_type,
                parent_def_level,
                parent_rep_level,
                leaf_idx,
            ),
            ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE => self.build_map(
                parquet_type,
                arrow_type,
                parent_def_level,
                parent_rep_level,
                leaf_idx,
            ),
            _ => self.build_struct(
                parquet_type,
                arrow_type,
                parent_def_level,
                parent_rep_level,
                leaf_idx,
            ),
        }
    }

    fn build_primitive(
        &self,
        parquet_type: &ParquetType,
        arrow_type: &arrow::datatypes::DataType,
        parent_def_level: i16,
        parent_rep_level: i16,
        leaf_idx: &mut usize,
    ) -> ParquetResult<Box<dyn ArrayReader>> {
        let repetition = parquet_repetition(parquet_type);
        let (def_level, rep_level, _nullable) =
            apply_repetition(parent_def_level, parent_rep_level, repetition);

        // REPEATED primitive: arrow wraps as List<inner>. The inner element
        // drives the leaf decode and is non-nullable per parquet semantics.
        // `ListArrayReader` takes the ELEMENT's def/rep (matches arrow-rs's
        // `ParquetField::into_list`).
        if repetition == Repetition::REPEATED {
            let inner_arrow_type = match arrow_type {
                arrow::datatypes::DataType::List(f) => f.data_type().clone(),
                arrow::datatypes::DataType::LargeList(f) => f.data_type().clone(),
                arrow::datatypes::DataType::FixedSizeList(f, _) => f.data_type().clone(),
                other => other.clone(),
            };
            let inner_idx = *leaf_idx;
            *leaf_idx += 1;
            let chunk_bytes = self.chunk_for(inner_idx)?;
            let inner_reader = build_primitive_leaf_reader(
                chunk_bytes,
                self.metadata,
                self.rg_idx,
                inner_idx,
                inner_arrow_type,
                def_level,
                rep_level,
            )?;
            return Ok(wrap_in_list_like(
                inner_reader,
                arrow_type.clone(),
                def_level,
                rep_level,
                false,
            ));
        }

        let idx = *leaf_idx;
        *leaf_idx += 1;
        let chunk_bytes = self.chunk_for(idx)?;
        build_primitive_leaf_reader(
            chunk_bytes,
            self.metadata,
            self.rg_idx,
            idx,
            arrow_type.clone(),
            def_level,
            rep_level,
        )
    }

    fn build_struct(
        &self,
        parquet_type: &ParquetType,
        arrow_type: &arrow::datatypes::DataType,
        parent_def_level: i16,
        parent_rep_level: i16,
        leaf_idx: &mut usize,
    ) -> ParquetResult<Box<dyn ArrayReader>> {
        let repetition = parquet_repetition(parquet_type);
        let (def_level, rep_level, nullable) =
            apply_repetition(parent_def_level, parent_rep_level, repetition);

        let arrow_fields = match arrow_type {
            arrow::datatypes::DataType::Struct(fields) => fields,
            // A REPEATED struct surfaces as List<Struct<...>> in arrow.
            arrow::datatypes::DataType::List(f)
            | arrow::datatypes::DataType::LargeList(f)
            | arrow::datatypes::DataType::FixedSizeList(f, _)
                if repetition == Repetition::REPEATED =>
            {
                match f.data_type() {
                    arrow::datatypes::DataType::Struct(inner) => inner,
                    _ => {
                        return Err(ParquetError::General(format!(
                            "build_struct: expected List<Struct>, got {:?}",
                            arrow_type
                        )));
                    }
                }
            }
            other => {
                return Err(ParquetError::General(format!(
                    "build_struct: expected struct arrow type, got {:?}",
                    other
                )));
            }
        };

        let parquet_fields = parquet_type.get_fields();
        if arrow_fields.len() != parquet_fields.len() {
            return Err(ParquetError::General(format!(
                "build_struct: arrow has {} fields, parquet has {}",
                arrow_fields.len(),
                parquet_fields.len()
            )));
        }

        let mut child_readers = Vec::with_capacity(parquet_fields.len());
        let mut child_arrow_fields: Vec<Arc<ArrowField>> = Vec::with_capacity(parquet_fields.len());
        for (parquet_child, arrow_child) in parquet_fields.iter().zip(arrow_fields.iter()) {
            let reader = self.build(
                parquet_child,
                arrow_child.data_type(),
                def_level,
                rep_level,
                leaf_idx,
            )?;
            // Reflect reader's actual type back into the field so it matches
            // what StructArray expects.
            let actual_type = reader.get_data_type().clone();
            let child_field =
                ArrowField::new(arrow_child.name(), actual_type, arrow_child.is_nullable())
                    .with_metadata(arrow_child.metadata().clone());
            child_arrow_fields.push(Arc::new(child_field));
            child_readers.push(reader);
        }

        let struct_dt =
            arrow::datatypes::DataType::Struct(arrow::datatypes::Fields::from(child_arrow_fields));
        let struct_reader: Box<dyn ArrayReader> = Box::new(StructArrayReader::new(
            struct_dt,
            child_readers,
            def_level,
            rep_level,
            nullable,
        ));

        if repetition == Repetition::REPEATED {
            // REPEATED struct → List<Struct>. `ListArrayReader` takes the
            // element's (struct's) def/rep levels.
            Ok(wrap_in_list_like(
                struct_reader,
                arrow_type.clone(),
                def_level,
                rep_level,
                false,
            ))
        } else {
            Ok(struct_reader)
        }
    }

    fn build_list(
        &self,
        parquet_type: &ParquetType,
        arrow_type: &arrow::datatypes::DataType,
        parent_def_level: i16,
        parent_rep_level: i16,
        leaf_idx: &mut usize,
    ) -> ParquetResult<Box<dyn ArrayReader>> {
        if parquet_type.is_primitive() {
            return Err(ParquetError::General(
                "build_list: parquet type annotated as LIST is primitive".into(),
            ));
        }

        let fields = parquet_type.get_fields();
        if fields.len() != 1 {
            return Err(ParquetError::General(format!(
                "list type must have a single child, found {}",
                fields.len()
            )));
        }

        let repeated_field = &fields[0];
        if parquet_repetition(repeated_field) != Repetition::REPEATED {
            return Err(ParquetError::General("list child must be repeated".into()));
        }

        // List nullability + def level shift for the list itself.
        let (list_def_level, nullable) = match parquet_repetition(parquet_type) {
            Repetition::REQUIRED => (parent_def_level, false),
            Repetition::OPTIONAL => (parent_def_level + 1, true),
            Repetition::REPEATED => {
                return Err(ParquetError::General("list type cannot be REPEATED".into()));
            }
        };

        let inner_arrow_field = match arrow_type {
            arrow::datatypes::DataType::List(f)
            | arrow::datatypes::DataType::LargeList(f)
            | arrow::datatypes::DataType::FixedSizeList(f, _) => f.as_ref(),
            other => {
                return Err(ParquetError::General(format!(
                    "build_list: expected list arrow type, got {:?}",
                    other
                )));
            }
        };

        // Element-level def/rep.
        let elem_def_level = list_def_level + 1;
        let elem_rep_level = parent_rep_level + 1;

        // Primitive-element legacy 2-level list:
        // `required/optional group L (LIST) { repeated T element; }`
        if repeated_field.is_primitive() {
            let inner_idx = *leaf_idx;
            *leaf_idx += 1;
            let chunk_bytes = self.chunk_for(inner_idx)?;
            let inner_reader = build_primitive_leaf_reader(
                chunk_bytes,
                self.metadata,
                self.rg_idx,
                inner_idx,
                inner_arrow_field.data_type().clone(),
                elem_def_level,
                elem_rep_level,
            )?;
            return Ok(wrap_in_list_like(
                inner_reader,
                arrow_type.clone(),
                elem_def_level,
                elem_rep_level,
                nullable,
            ));
        }

        // 2-level legacy group element detection.
        let items = repeated_field.get_fields();
        let is_two_level_group = items.len() != 1
            || (!parquet_type_is_list(repeated_field)
                && !parquet_type_has_single_repeated_child(repeated_field)
                && (repeated_field.name() == "array"
                    || repeated_field.name() == format!("{}_tuple", parquet_type.name())));

        if is_two_level_group {
            // Element is the repeated group (treated as a struct);
            // build_struct will see REPEATED and wrap as List<Struct>.
            return self.build_struct(
                repeated_field,
                inner_arrow_field.data_type(),
                list_def_level,
                parent_rep_level,
                leaf_idx,
            );
        }

        // Standard 3-level LIST.
        let item_type = &items[0];
        let item_reader = self.build(
            item_type,
            inner_arrow_field.data_type(),
            elem_def_level,
            elem_rep_level,
            leaf_idx,
        )?;
        Ok(wrap_in_list_like(
            item_reader,
            arrow_type.clone(),
            elem_def_level,
            elem_rep_level,
            nullable,
        ))
    }

    fn build_map(
        &self,
        parquet_type: &ParquetType,
        arrow_type: &arrow::datatypes::DataType,
        parent_def_level: i16,
        parent_rep_level: i16,
        leaf_idx: &mut usize,
    ) -> ParquetResult<Box<dyn ArrayReader>> {
        let map_rep = parquet_repetition(parquet_type);
        let rep_level = parent_rep_level + 1;
        let (def_level, nullable) = match map_rep {
            Repetition::REQUIRED => (parent_def_level + 1, false),
            Repetition::OPTIONAL => (parent_def_level + 2, true),
            Repetition::REPEATED => {
                return Err(ParquetError::General("map cannot be repeated".into()));
            }
        };

        let map_fields = parquet_type.get_fields();
        if map_fields.len() != 1 {
            return Err(ParquetError::General(format!(
                "map field must have one key_value child, found {}",
                map_fields.len()
            )));
        }
        let key_value = &map_fields[0];
        if parquet_repetition(key_value) != Repetition::REPEATED {
            return Err(ParquetError::General(
                "map key_value child must be repeated".into(),
            ));
        }

        // Per spec, values may be omitted — degrades to a list of keys.
        if key_value.get_fields().len() == 1 {
            return self.build_list(
                parquet_type,
                arrow_type,
                parent_def_level,
                parent_rep_level,
                leaf_idx,
            );
        }
        if key_value.get_fields().len() != 2 {
            return Err(ParquetError::General(format!(
                "map key_value child must have two children, found {}",
                key_value.get_fields().len()
            )));
        }

        let map_key = &key_value.get_fields()[0];
        let map_value = &key_value.get_fields()[1];

        let (arrow_key_field, arrow_value_field) = match arrow_type {
            arrow::datatypes::DataType::Map(field, _sorted) => match field.data_type() {
                arrow::datatypes::DataType::Struct(fields) if fields.len() == 2 => {
                    (fields[0].as_ref(), fields[1].as_ref())
                }
                d => {
                    return Err(ParquetError::General(format!(
                        "map data type should contain struct with two children, got {:?}",
                        d
                    )));
                }
            },
            other => {
                return Err(ParquetError::General(format!(
                    "build_map: expected Map arrow type, got {:?}",
                    other
                )));
            }
        };

        let key_reader = self.build(
            map_key,
            arrow_key_field.data_type(),
            def_level,
            rep_level,
            leaf_idx,
        )?;
        let value_reader = self.build(
            map_value,
            arrow_value_field.data_type(),
            def_level,
            rep_level,
            leaf_idx,
        )?;

        Ok(Box::new(MapArrayReader::new(
            key_reader,
            value_reader,
            arrow_type.clone(),
            def_level,
            rep_level,
            nullable,
        )))
    }
}

/// Stream `ArrayRef`s of up to `chunk_size` rows each from one top-level
/// column of one RG via `sender`. Exits when the column chunk is fully
/// consumed or the receiver is dropped.
#[allow(clippy::too_many_arguments)]
pub(super) async fn decode_one_streaming(
    chunks: Arc<HashMap<usize, OffsetBytes>>,
    metadata: Arc<ParquetMetaData>,
    rg_idx: usize,
    top_field_idx: usize,
    arrow_field: ArrowField,
    selection: Option<RowSelection>,
    chunk_size: usize,
    path: Arc<str>,
    sender: tokio::sync::mpsc::Sender<common_error::DaftResult<ArrayRef>>,
) {
    let result: ParquetResult<()> = async {
        let (mut reader, total_rows) = build_top_field_reader(
            chunks.as_ref(),
            &metadata,
            rg_idx,
            top_field_idx,
            &arrow_field,
        )?;

        use parquet::arrow::arrow_reader::RowSelector;
        let selectors: Vec<RowSelector> = match &selection {
            Some(s) => s.iter().copied().collect(),
            None => vec![RowSelector::select(total_rows)],
        };

        let mut acc = 0usize;
        for sel in selectors {
            if sel.skip {
                reader.skip_records(sel.row_count)?;
                continue;
            }
            let mut remaining = sel.row_count;
            while remaining > 0 {
                let room = chunk_size - acc;
                let take = remaining.min(room);
                reader.read_records(take)?;
                acc += take;
                remaining -= take;
                if acc == chunk_size {
                    let arr = reader.consume_batch()?;
                    if sender.send(Ok(arr)).await.is_err() {
                        return Ok(());
                    }
                    acc = 0;
                }
            }
        }
        if acc > 0 {
            let arr = reader.consume_batch()?;
            let _ = sender.send(Ok(arr)).await;
        }
        Ok(())
    }
    .await;

    if let Err(e) = result {
        let wrapped: crate::Error = Err::<(), _>(e)
            .with_context(|_| ParquetColumnDecodeSnafu {
                path: path.to_string(),
            })
            .unwrap_err();
        let _ = sender.send(Err(wrapped.into())).await;
    }
}
