//! Daft-owned parquet metadata types wrapping arrow-rs `ParquetMetaData`.
//!
//! These adapter types provide a stable API with serde support and row group index
//! tracking that consuming crates (daft-scan, daft-micropartition) depend on.

use std::sync::Arc;

use indexmap::IndexMap;
use parquet::file::metadata::{
    FileMetaData as ArrowrsFileMetaData, ParquetMetaData,
    RowGroupMetaData as ArrowrsRowGroupMetaData,
};
use serde::{Deserialize, Serialize, de::Deserializer, ser::Serializer};

/// Row group list preserving original indices through filter/split operations.
pub type RowGroupList = IndexMap<usize, DaftRowGroupMetaData>;

/// Daft-owned parquet file metadata.
///
/// Wraps arrow-rs `ParquetMetaData` and tracks original row group indices
/// so that downstream code can correctly reference row groups after filtering.
#[derive(Debug, Clone)]
pub struct DaftParquetMetadata {
    inner: Arc<ParquetMetaData>,
    /// Maps position `i` in `inner.row_groups()` → original row group index in the file.
    original_indices: Vec<usize>,
}

impl DaftParquetMetadata {
    /// Construct from arrow-rs metadata, with sequential indices 0..N.
    pub fn from_arrowrs(metadata: Arc<ParquetMetaData>) -> Self {
        let n = metadata.row_groups().len();
        Self {
            inner: metadata,
            original_indices: (0..n).collect(),
        }
    }

    /// Total number of rows across all row groups.
    pub fn num_rows(&self) -> usize {
        self.inner.file_metadata().num_rows() as usize
    }

    /// Number of row groups in this file.
    pub fn num_row_groups(&self) -> usize {
        self.inner.row_groups().len()
    }

    /// Parquet file version.
    pub fn version(&self) -> i32 {
        self.inner.file_metadata().version()
    }

    /// Iterate over (original_index, row_group_metadata) pairs.
    pub fn row_groups(&self) -> impl Iterator<Item = (usize, DaftRowGroupMetaData)> + '_ {
        self.original_indices
            .iter()
            .zip(self.inner.row_groups().iter())
            .map(|(&idx, rg)| (idx, DaftRowGroupMetaData::from_arrowrs(rg.clone())))
    }

    /// Clone this metadata with a different set of row groups.
    pub fn clone_with_row_groups(&self, num_rows: usize, row_groups: RowGroupList) -> Self {
        let (indices, rgs): (Vec<usize>, Vec<ArrowrsRowGroupMetaData>) = row_groups
            .into_iter()
            .map(|(idx, drg)| (idx, drg.into_inner()))
            .unzip();

        let fm = self.inner.file_metadata();
        let new_file_metadata = ArrowrsFileMetaData::new(
            fm.version(),
            num_rows as i64,
            fm.created_by().map(|s| s.to_string()),
            fm.key_value_metadata().cloned(),
            fm.schema_descr_ptr(),
            fm.column_orders().cloned(),
        );

        Self {
            inner: Arc::new(ParquetMetaData::new(new_file_metadata, rgs)),
            original_indices: indices,
        }
    }

    /// Access the underlying arrow-rs `ParquetMetaData`.
    pub fn as_arrowrs(&self) -> &Arc<ParquetMetaData> {
        &self.inner
    }

    /// Convenience accessor for the parquet schema descriptor.
    pub fn schema_descriptor(&self) -> &parquet::schema::types::SchemaDescriptor {
        self.inner.file_metadata().schema_descr()
    }

    /// Return the value of the `"geo"` key from the file-level key-value metadata, if present.
    ///
    /// GeoParquet files embed a JSON document under the `"geo"` key in the Parquet footer.
    /// Returns `None` when the file has no key-value metadata or no `"geo"` entry.
    pub fn geo_metadata(&self) -> Option<String> {
        self.inner
            .file_metadata()
            .key_value_metadata()
            .and_then(|kv| {
                kv.iter()
                    .find(|entry| entry.key == "geo")
                    .and_then(|entry| entry.value.clone())
            })
    }
}

// Custom Serialize: write footer thrift bytes + original_indices + num_rows
impl Serialize for DaftParquetMetadata {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;

        // Write the full parquet footer via ParquetMetaDataWriter
        let mut buf = Vec::new();
        parquet::file::metadata::ParquetMetaDataWriter::new(&mut buf, &self.inner)
            .finish()
            .map_err(serde::ser::Error::custom)?;

        // Store num_rows separately because ParquetMetaDataWriter recalculates it
        // from row groups, which can differ from our FileMetaData.num_rows()
        // (e.g. after clone_with_row_groups with a subset).
        let num_rows = self.inner.file_metadata().num_rows();

        // Serialize as (footer_bytes, original_indices, num_rows)
        let mut tup = serializer.serialize_tuple(3)?;
        tup.serialize_element(&buf)?;
        tup.serialize_element(&self.original_indices)?;
        tup.serialize_element(&num_rows)?;
        tup.end()
    }
}

// Custom Deserialize: parse footer bytes → ParquetMetaData
impl<'de> Deserialize<'de> for DaftParquetMetadata {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (footer_bytes, original_indices, num_rows): (Vec<u8>, Vec<usize>, i64) =
            Deserialize::deserialize(deserializer)?;

        // The footer format written by ParquetMetaDataWriter is:
        //   [optional col/offset indexes] [thrift FileMetaData] [4-byte LE metadata length] [PAR1]
        // We extract thrift bytes using the metadata length stored at the end.
        let len = footer_bytes.len();
        if len < 8 {
            return Err(serde::de::Error::custom(
                "footer bytes too short for parquet footer",
            ));
        }
        let metadata_len = i32::from_le_bytes(
            footer_bytes[len - 8..len - 4]
                .try_into()
                .map_err(serde::de::Error::custom)?,
        ) as usize;
        if len < 8 + metadata_len {
            return Err(serde::de::Error::custom(
                "footer bytes shorter than declared metadata length",
            ));
        }
        let thrift_bytes = &footer_bytes[len - 8 - metadata_len..len - 8];

        let decoded = parquet::file::metadata::ParquetMetaDataReader::decode_metadata(thrift_bytes)
            .map_err(serde::de::Error::custom)?;

        // Restore the correct num_rows (may differ from sum of row groups)
        let fm = decoded.file_metadata();
        let corrected_fm = parquet::file::metadata::FileMetaData::new(
            fm.version(),
            num_rows,
            fm.created_by().map(|s| s.to_string()),
            fm.key_value_metadata().cloned(),
            fm.schema_descr_ptr(),
            fm.column_orders().cloned(),
        );
        let metadata = ParquetMetaData::new(corrected_fm, decoded.row_groups().to_vec());

        Ok(Self {
            inner: Arc::new(metadata),
            original_indices,
        })
    }
}

// Structural equality based on key fields (not deep byte-level comparison).
impl PartialEq for DaftParquetMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.num_rows() == other.num_rows()
            && self.version() == other.version()
            && self.num_row_groups() == other.num_row_groups()
            && self.original_indices == other.original_indices
    }
}

/// Daft-owned row group metadata.
#[derive(Debug, Clone)]
pub struct DaftRowGroupMetaData {
    inner: ArrowrsRowGroupMetaData,
}

impl DaftRowGroupMetaData {
    pub(crate) fn from_arrowrs(inner: ArrowrsRowGroupMetaData) -> Self {
        Self { inner }
    }

    pub(crate) fn into_inner(self) -> ArrowrsRowGroupMetaData {
        self.inner
    }

    /// Number of rows in this row group.
    pub fn num_rows(&self) -> usize {
        self.inner.num_rows() as usize
    }

    /// Total compressed size of this row group in bytes.
    pub fn compressed_size(&self) -> usize {
        self.inner.compressed_size() as usize
    }

    /// Estimated in-memory (materialized) size in bytes for each column chunk in this
    /// row group, keyed by the top-level (root) column name.
    ///
    /// For fixed-width physical types we use `num_values * physical_width`, which is the
    /// size of the decoded Arrow buffer and is *independent of the on-disk encoding*. This
    /// matters for dictionary/RLE-encoded data, where the Parquet "uncompressed" size is
    /// the encoded (dictionary index) size and can be many times smaller than the
    /// materialized buffer. For variable-width `BYTE_ARRAY` (e.g. Utf8/Binary) the byte
    /// count is not derivable from `num_values`, so we fall back to the uncompressed size.
    ///
    /// Nested columns are flattened to their root: every leaf chunk (e.g.
    /// `position_ids.list.element`) is attributed to its top-level field (`position_ids`).
    /// Offset/validity buffers are not added (they are negligible next to the value
    /// buffers). The caller is responsible for summing across row groups.
    pub fn column_materialized_sizes(&self) -> Vec<(String, u64)> {
        use parquet::basic::Type;

        self.inner
            .columns()
            .iter()
            .map(|col| {
                let descr = col.column_descr();
                let root = descr.path().parts().first().cloned().unwrap_or_default();
                // `num_values()` is an i64 and always non-negative in practice; clamp
                // defensively so a malformed (negative) value can't wrap to a huge u64.
                let num_values = col.num_values().max(0) as u64;
                let bytes = match descr.physical_type() {
                    Type::BOOLEAN => num_values.div_ceil(8), // 1 bit per value
                    Type::INT32 | Type::FLOAT => num_values * 4,
                    Type::INT64 | Type::DOUBLE => num_values * 8,
                    Type::INT96 => num_values * 12,
                    Type::FIXED_LEN_BYTE_ARRAY => num_values * descr.type_length().max(0) as u64,
                    // Variable-width: byte count isn't derivable from num_values, so fall
                    // back to the uncompressed (encoded) size as a proxy.
                    Type::BYTE_ARRAY => col.uncompressed_size().max(0) as u64,
                };
                (root, bytes)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::{
        file::metadata::{FileMetaData, ParquetMetaData},
        schema::types::Type,
    };

    use super::*;

    fn make_test_metadata(num_rows: i64) -> Arc<ParquetMetaData> {
        let schema = Arc::new(Type::group_type_builder("schema").build().unwrap());
        let schema_descr = Arc::new(parquet::schema::types::SchemaDescriptor::new(schema));
        let file_metadata = FileMetaData::new(
            2,
            num_rows,
            Some("test".to_string()),
            None,
            schema_descr,
            None,
        );
        Arc::new(ParquetMetaData::new(file_metadata, vec![]))
    }

    #[test]
    fn test_daft_metadata_serde_roundtrip() {
        let metadata = make_test_metadata(1000);
        let adapter = DaftParquetMetadata::from_arrowrs(metadata);

        let config = bincode::config::legacy();
        let adapter_bytes = bincode::serde::encode_to_vec(&adapter, config).unwrap();

        let (roundtripped, _): (DaftParquetMetadata, _) =
            bincode::serde::decode_from_slice(&adapter_bytes, config).unwrap();
        assert_eq!(roundtripped.num_rows(), 1000);
        assert_eq!(roundtripped.version(), 2);
    }

    #[test]
    fn test_partial_eq() {
        let m1 = DaftParquetMetadata::from_arrowrs(make_test_metadata(500));
        let m2 = DaftParquetMetadata::from_arrowrs(make_test_metadata(500));
        assert_eq!(m1, m2);

        let m3 = DaftParquetMetadata::from_arrowrs(make_test_metadata(1000));
        assert_ne!(m1, m3);
    }

    #[test]
    fn test_column_materialized_sizes() {
        use std::collections::BTreeMap;

        use parquet::{
            basic::Type as PhysicalType,
            file::metadata::{ColumnChunkMetaData, RowGroupMetaData},
            schema::types::{SchemaDescriptor, Type as SchemaType},
        };

        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    SchemaType::primitive_type_builder("ids", PhysicalType::INT64)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    SchemaType::primitive_type_builder("flag", PhysicalType::BOOLEAN)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    SchemaType::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
                        .build()
                        .unwrap(),
                ),
            ])
            .build()
            .unwrap();
        let descr = Arc::new(SchemaDescriptor::new(Arc::new(schema)));

        let rg = RowGroupMetaData::builder(descr.clone())
            .set_num_rows(100)
            .set_column_metadata(vec![
                // INT64: dictionary-encoded so its uncompressed size would be small, but
                // the materialized buffer is num_values * 8.
                ColumnChunkMetaData::builder(descr.column(0))
                    .set_num_values(100)
                    .set_total_uncompressed_size(40) // encoded; deliberately tiny
                    .build()
                    .unwrap(),
                ColumnChunkMetaData::builder(descr.column(1))
                    .set_num_values(65) // not a multiple of 8 -> exercises div_ceil rounding
                    .build()
                    .unwrap(),
                // BYTE_ARRAY: byte count isn't derivable from num_values, so fall back to
                // the uncompressed size.
                ColumnChunkMetaData::builder(descr.column(2))
                    .set_num_values(100)
                    .set_total_uncompressed_size(777)
                    .build()
                    .unwrap(),
            ])
            .build()
            .unwrap();

        let sizes: BTreeMap<String, u64> = DaftRowGroupMetaData::from_arrowrs(rg)
            .column_materialized_sizes()
            .into_iter()
            .collect();

        // INT64 uses num_values * 8, NOT the tiny encoded size.
        assert_eq!(sizes["ids"], 100 * 8);
        // BOOLEAN is 1 bit per value, rounded up to whole bytes (65 bits -> 9 bytes).
        assert_eq!(sizes["flag"], 9);
        // BYTE_ARRAY falls back to the uncompressed size.
        assert_eq!(sizes["name"], 777);
    }
}
