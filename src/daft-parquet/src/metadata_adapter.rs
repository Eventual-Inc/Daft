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

    /// Construct from arrow-rs metadata with explicit original indices.
    pub fn from_arrowrs_with_indices(
        metadata: Arc<ParquetMetaData>,
        original_indices: Vec<usize>,
    ) -> Self {
        debug_assert_eq!(metadata.row_groups().len(), original_indices.len());
        Self {
            inner: metadata,
            original_indices,
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

    /// Get a specific row group by its original index.
    pub fn get_row_group(&self, index: usize) -> Option<DaftRowGroupMetaData> {
        self.original_indices
            .iter()
            .position(|&i| i == index)
            .map(|pos| DaftRowGroupMetaData::from_arrowrs(self.inner.row_groups()[pos].clone()))
    }

    /// Check if a row group index exists.
    pub fn contains_row_group(&self, index: usize) -> bool {
        self.original_indices.contains(&index)
    }

    /// Get the set of row group indices.
    pub fn row_group_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.original_indices.iter().copied()
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
}
