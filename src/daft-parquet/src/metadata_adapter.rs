//! Daft-owned parquet metadata types that decouple consuming crates from parquet2.
//!
//! These adapter types wrap `parquet2::metadata::*` today and will be extended
//! with an `ArrowRs` variant when the core read pipeline migrates to arrow-rs.

use indexmap::IndexMap;
use parquet2::metadata::{
    ColumnChunkMetaData as Pq2ColumnChunkMetaData, FileMetaData as Pq2FileMetaData,
    RowGroupMetaData as Pq2RowGroupMetaData,
};
use serde::{Deserialize, Serialize};

/// Row group list preserving original indices through filter/split operations.
pub type RowGroupList = IndexMap<usize, DaftRowGroupMetaData>;

/// Daft-owned parquet file metadata.
///
/// Wraps the underlying parquet library's metadata and provides a stable API
/// that consuming crates (daft-scan, daft-micropartition) depend on.
///
/// Currently backed by `parquet2::metadata::FileMetaData`. When the arrow-rs
/// migration completes, an `ArrowRs` variant will be added.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct DaftParquetMetadata {
    inner: Pq2FileMetaData,
}

impl DaftParquetMetadata {
    /// Total number of rows across all row groups.
    pub fn num_rows(&self) -> usize {
        self.inner.num_rows
    }

    /// Number of row groups in this file.
    pub fn num_row_groups(&self) -> usize {
        self.inner.row_groups.len()
    }

    /// Parquet file version.
    pub fn version(&self) -> i32 {
        self.inner.version
    }

    /// Iterate over (index, row_group_metadata) pairs, preserving original indices.
    pub fn row_groups(&self) -> impl Iterator<Item = (usize, DaftRowGroupMetaData)> + '_ {
        self.inner
            .row_groups
            .iter()
            .map(|(&idx, rg)| (idx, DaftRowGroupMetaData::from_parquet2(rg.clone())))
    }

    /// Iterate over (index, row_group_metadata) pairs without cloning.
    #[allow(dead_code)]
    pub(crate) fn row_groups_ref(
        &self,
    ) -> impl Iterator<Item = (usize, &Pq2RowGroupMetaData)> + '_ {
        self.inner.row_groups.iter().map(|(&idx, rg)| (idx, rg))
    }

    /// Get a specific row group by its original index.
    pub fn get_row_group(&self, index: usize) -> Option<DaftRowGroupMetaData> {
        self.inner
            .row_groups
            .get(&index)
            .map(|rg| DaftRowGroupMetaData::from_parquet2(rg.clone()))
    }

    /// Get a specific row group by its original index without cloning.
    pub(crate) fn get_row_group_ref(&self, index: usize) -> Option<&Pq2RowGroupMetaData> {
        self.inner.row_groups.get(&index)
    }

    /// Check if a row group index exists.
    pub fn contains_row_group(&self, index: usize) -> bool {
        self.inner.row_groups.contains_key(&index)
    }

    /// Get the set of row group indices.
    pub fn row_group_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.inner.row_groups.keys().copied()
    }

    /// Clone this metadata with a different set of row groups.
    pub fn clone_with_row_groups(&self, num_rows: usize, row_groups: RowGroupList) -> Self {
        let pq2_row_groups: indexmap::IndexMap<usize, Pq2RowGroupMetaData> = row_groups
            .into_iter()
            .map(|(idx, rg)| (idx, rg.into_parquet2()))
            .collect();
        Self {
            inner: self.inner.clone_with_row_groups(num_rows, pq2_row_groups),
        }
    }

    /// Access the underlying parquet2 FileMetaData.
    ///
    /// This is a transitional API. Consuming code should prefer the adapter methods
    /// above. This will be removed once the arrow-rs migration is complete.
    pub fn as_parquet2(&self) -> &Pq2FileMetaData {
        &self.inner
    }

    /// Consume and return the underlying parquet2 FileMetaData.
    pub fn into_parquet2(self) -> Pq2FileMetaData {
        self.inner
    }
}

impl From<Pq2FileMetaData> for DaftParquetMetadata {
    fn from(inner: Pq2FileMetaData) -> Self {
        Self { inner }
    }
}

impl From<DaftParquetMetadata> for Pq2FileMetaData {
    fn from(adapter: DaftParquetMetadata) -> Self {
        adapter.inner
    }
}

/// Daft-owned row group metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct DaftRowGroupMetaData {
    inner: Pq2RowGroupMetaData,
}

impl DaftRowGroupMetaData {
    fn from_parquet2(inner: Pq2RowGroupMetaData) -> Self {
        Self { inner }
    }

    fn into_parquet2(self) -> Pq2RowGroupMetaData {
        self.inner
    }

    /// Number of rows in this row group.
    pub fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// Total compressed size of this row group in bytes.
    pub fn compressed_size(&self) -> usize {
        self.inner.compressed_size()
    }

    /// Column chunk metadata for this row group.
    #[allow(dead_code)]
    pub(crate) fn columns(&self) -> &[Pq2ColumnChunkMetaData] {
        self.inner.columns()
    }

    /// Access the underlying parquet2 RowGroupMetaData.
    pub fn as_parquet2(&self) -> &Pq2RowGroupMetaData {
        &self.inner
    }
}

impl From<Pq2RowGroupMetaData> for DaftRowGroupMetaData {
    fn from(inner: Pq2RowGroupMetaData) -> Self {
        Self { inner }
    }
}

impl From<DaftRowGroupMetaData> for Pq2RowGroupMetaData {
    fn from(adapter: DaftRowGroupMetaData) -> Self {
        adapter.inner
    }
}

#[cfg(test)]
mod tests {
    use parquet2::metadata::SchemaDescriptor;

    use super::*;

    fn make_test_schema(name: &str) -> SchemaDescriptor {
        SchemaDescriptor::new(name.to_string(), vec![])
    }

    #[test]
    fn test_daft_metadata_serde_roundtrip() {
        let file_meta = Pq2FileMetaData {
            version: 2,
            num_rows: 1000,
            created_by: Some("test".to_string()),
            row_groups: IndexMap::new(),
            key_value_metadata: None,
            schema_descr: make_test_schema("test"),
            column_orders: None,
        };

        let adapter = DaftParquetMetadata::from(file_meta.clone());

        // Serialize both and compare (bincode v2 API)
        let config = bincode::config::legacy();
        let adapter_bytes = bincode::serde::encode_to_vec(&adapter, config).unwrap();
        let file_meta_bytes = bincode::serde::encode_to_vec(&file_meta, config).unwrap();
        assert_eq!(adapter_bytes, file_meta_bytes);

        // Deserialize back
        let (roundtripped, _): (DaftParquetMetadata, _) =
            bincode::serde::decode_from_slice(&adapter_bytes, config).unwrap();
        assert_eq!(roundtripped.num_rows(), 1000);
        assert_eq!(roundtripped.version(), 2);
    }

    #[test]
    fn test_as_parquet2_file_metadata() {
        let file_meta = Pq2FileMetaData {
            version: 2,
            num_rows: 500,
            created_by: None,
            row_groups: IndexMap::new(),
            key_value_metadata: None,
            schema_descr: make_test_schema("test"),
            column_orders: None,
        };

        let adapter = DaftParquetMetadata::from(file_meta);
        let pq2 = adapter.as_parquet2();
        assert_eq!(pq2.num_rows, 500);
        assert_eq!(pq2.schema().name(), "test");
    }
}
