use std::collections::HashMap;

use common_scan_info::Pushdowns;
use daft_core::prelude::*;

/// An estimator that can be derived/inferred from reading a file (e.g. CSV, JSON or Parquet)
#[derive(Debug)]
pub struct FileInferredEstimator {
    /// Schema of the data
    schema: SchemaRef,

    /// Estimate how much large each row is at rest (compressed)
    estimated_row_size: usize,

    /// Fraction of total size taken up by each column at rest (compressed)
    column_size_fraction: HashMap<usize, f64>,

    /// How much we expect each column to inflate when decompressed and decoded into memory
    column_inflation: HashMap<usize, f64>,
}

impl FileInferredEstimator {
    /// Creates a FileInferredEstimator from Parquet metadata
    ///
    /// NOTE: Currently has strong assumptions about the names in the provided `daft_schema` matching names in the Parquet metadata.
    /// This might not be an accurate mapping, especially if field IDs are being used (for example in Apache Iceberg).
    pub fn from_parquet_metadata(
        daft_schema: SchemaRef,
        parquet_meta: &parquet2::metadata::FileMetaData,
    ) -> Option<Self> {
        let total_rg_compressed_size: i64 = parquet_meta
            .row_groups
            .iter()
            .map(|(_, rg)| (rg.compressed_size() as i64))
            .sum();
        let total_num_rows: usize = parquet_meta
            .row_groups
            .iter()
            .map(|(_, rg)| rg.num_rows())
            .sum();

        // If this is an empty Parquet file, we can't do any estimations
        // NOTE: Not handling this case here will cause divide-by-zero panics further down in the code!
        if (total_num_rows == 0) || (total_rg_compressed_size == 0) {
            return None;
        }

        // Accumulate statistics per-column
        // We only consider columns with names that match the field names in the provided `daft_schema`
        let mut total_compressed_size_per_column = HashMap::new();
        let mut total_uncompressed_size_per_column = HashMap::new();
        for (_, rg) in &parquet_meta.row_groups {
            for cc_meta in rg.columns() {
                match cc_meta.descriptor().path_in_schema.first() {
                    Some(topmost_schema_name) => {
                        if let Some(field_idx) = daft_schema
                            .as_ref()
                            .fields
                            .get_index_of(topmost_schema_name)
                        {
                            let total_column_compressed_size =
                                if let Some(existing_accumulated_size) =
                                    total_compressed_size_per_column.get(&field_idx)
                                {
                                    existing_accumulated_size + cc_meta.compressed_size()
                                } else {
                                    cc_meta.compressed_size()
                                };
                            let total_column_uncompressed_size =
                                if let Some(existing_accumulated_size) =
                                    total_uncompressed_size_per_column.get(&field_idx)
                                {
                                    existing_accumulated_size + cc_meta.uncompressed_size()
                                } else {
                                    cc_meta.uncompressed_size()
                                };
                            total_compressed_size_per_column
                                .insert(field_idx, total_column_compressed_size);
                            total_uncompressed_size_per_column
                                .insert(field_idx, total_column_uncompressed_size);
                        } else {
                            log::warn!(
                                "Parquet file contained column that was not in schema: {}",
                                topmost_schema_name
                            );
                            continue;
                        }
                    }
                    None => {
                        continue;
                    }
                }
            }
        }

        Some(Self {
            schema: daft_schema,
            estimated_row_size: (total_rg_compressed_size as usize) / total_num_rows,
            column_size_fraction: total_compressed_size_per_column
                .iter()
                .map(|(&field_idx, &column_compressed_size)| {
                    (
                        field_idx,
                        (column_compressed_size as f64) / (total_rg_compressed_size as f64),
                    )
                })
                .collect(),
            column_inflation: total_compressed_size_per_column
                .iter()
                .filter_map(|(&field_idx, &col_compressed_size)| {
                    if col_compressed_size == 0 {
                        None
                    } else {
                        Some((
                            field_idx,
                            (*total_uncompressed_size_per_column.get(&field_idx).unwrap() as f64)
                                / (col_compressed_size as f64),
                        ))
                    }
                })
                .collect(),
        })
    }
}

impl FileInferredEstimator {
    /// Runs the estimator based on the size of a file on disk
    pub fn estimate_from_size_on_disk(
        &self,
        size_bytes_on_disk: u64,
        pushdowns: &Pushdowns,
    ) -> usize {
        // If we have column pushdowns, only consider those columns
        let columns_to_consider: Vec<usize> =
            if let Some(column_pushdowns) = pushdowns.columns.as_ref() {
                column_pushdowns
                    .iter()
                    .map(|name| self.schema.as_ref().fields.get_index_of(name).unwrap())
                    .collect()
            } else {
                (0..self.schema.as_ref().len()).collect()
            };

        // Grab the uncompressed size of each column, and then inflate it to add to the total size
        let total_uncompressed_size: f64 = columns_to_consider
            .iter()
            .filter_map(|col_idx| {
                let fraction = self.column_size_fraction.get(col_idx)?;
                let inflation = self.column_inflation.get(col_idx)?;

                Some((size_bytes_on_disk as f64) * fraction * inflation)
            })
            .sum();

        // Apply limit pushdown if present
        let total_uncompressed_size = if let Some(limit) = pushdowns.limit {
            let estimated_num_rows = (size_bytes_on_disk as f64) / self.estimated_row_size as f64;
            let limit_fraction = (limit as f64 / estimated_num_rows).min(1.0);
            total_uncompressed_size * limit_fraction
        } else {
            total_uncompressed_size
        };

        total_uncompressed_size as usize
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::datatypes::Field;
    use indexmap::IndexMap;
    use parquet2::{
        metadata::{
            ColumnChunkMetaData, ColumnDescriptor, Descriptor, FileMetaData, RowGroupMetaData,
            SchemaDescriptor,
        },
        schema::types::{
            FieldInfo, ParquetType, PhysicalType, PrimitiveLogicalType, PrimitiveType,
        },
        thrift_format::{ColumnChunk, ColumnMetaData, CompressionCodec, Encoding, Type},
    };

    use super::*;

    fn make_simple_string_column(
        name: String,
        num_rows: usize,
    ) -> (ParquetType, ColumnChunkMetaData) {
        let primitive_type = PrimitiveType {
            field_info: FieldInfo {
                name: name.clone(),
                repetition: parquet2::schema::Repetition::Optional,
                id: None,
            },
            logical_type: Some(PrimitiveLogicalType::String),
            converted_type: None,
            physical_type: PhysicalType::ByteArray,
        };
        let parquet_type = ParquetType::PrimitiveType(primitive_type.clone());
        let total_byte_size = 1000;
        let compression_ratio = 1000;
        let column_metadata = ColumnMetaData::new(
            Type::BYTE_ARRAY,
            vec![Encoding::PLAIN],
            vec![name.clone()],
            CompressionCodec::SNAPPY,
            num_rows as i64,
            total_byte_size * compression_ratio,
            total_byte_size,
            None,
            -1, // Fudge the data page offset
            None,
            None,
            None,
            None, // Assume no page encoding stats for now, but this could be useful especially for estimating size of dictionary encodings
            None,
        );
        let column_chunk_metadata = ColumnChunkMetaData::new(
            ColumnChunk::new(None, 0, column_metadata, None, None, None, None, None, None),
            ColumnDescriptor::new(
                Descriptor {
                    primitive_type,
                    max_def_level: 0,
                    max_rep_level: 0,
                },
                vec![name.clone()],
                parquet_type.clone(),
            ),
        );

        (parquet_type, column_chunk_metadata)
    }

    #[test]
    fn test_from_parquet_metadata() {
        // We create all the necessary thrift-level data to represent this Parquet file:
        //
        // 1. One Rowgroup
        // 2. 10 rows
        // 3. 1 column called "foo"
        //   * String type
        //   * Pretend to be snappy compressed
        //   * 1000x compression ratio (1000_000 bytes uncompressed, 1000 bytes compressed)
        //   * Plain encoding
        let num_rows = 10;
        let colname = "foo";
        let (parquet_type, column_chunk_metadata) =
            make_simple_string_column(colname.to_string(), num_rows);
        let mut row_groups = IndexMap::new();
        row_groups.insert(
            0,
            RowGroupMetaData::new(
                vec![column_chunk_metadata.clone()],
                num_rows,
                column_chunk_metadata.compressed_size() as usize,
            ),
        );
        let parquet_meta = FileMetaData {
            version: 0,
            num_rows: num_rows,
            created_by: None,
            row_groups,
            key_value_metadata: None,
            schema_descr: SchemaDescriptor::new("schema".to_string(), vec![parquet_type]),
            column_orders: None,
        };
        let schema = Schema::new(vec![Field::new(colname.to_string(), DataType::Utf8)]).unwrap();

        // Check that estimator correctly understands the 1000x inflation
        let pushdowns = Pushdowns::new(None, None, None, None);
        let estimator =
            FileInferredEstimator::from_parquet_metadata(Arc::new(schema), &parquet_meta);
        match estimator {
            None => panic!("Cannot construct estimator from metadata"),
            Some(estimator) => {
                assert_eq!(estimator.estimate_from_size_on_disk(1, &pushdowns), 1000)
            }
        }
    }

    #[test]
    fn test_from_parquet_metadata_with_column_pushdown() {
        // We create all the necessary thrift-level data to represent this Parquet file:
        //
        // 1. One Rowgroup
        // 2. 10 rows
        // 3. 2 columns, with identical characteristics, foo1 and foo2
        //   * String type
        //   * Pretend to be snappy compressed
        //   * 1000x compression ratio (1000_000 bytes uncompressed, 1000 bytes compressed)
        //   * Plain encoding
        let num_rows = 10;
        let (parquet_type1, column_chunk_metadata1) =
            make_simple_string_column("foo1".to_string(), num_rows);
        let (parquet_type2, column_chunk_metadata2) =
            make_simple_string_column("foo2".to_string(), num_rows);
        let mut row_groups = IndexMap::new();
        row_groups.insert(
            0,
            RowGroupMetaData::new(
                vec![
                    column_chunk_metadata1.clone(),
                    column_chunk_metadata2.clone(),
                ],
                num_rows,
                (column_chunk_metadata1.compressed_size()
                    + column_chunk_metadata2.compressed_size()) as usize,
            ),
        );
        let parquet_meta = FileMetaData {
            version: 0,
            num_rows: num_rows,
            created_by: None,
            row_groups,
            key_value_metadata: None,
            schema_descr: SchemaDescriptor::new(
                "schema".to_string(),
                vec![parquet_type1, parquet_type2],
            ),
            column_orders: None,
        };
        let schema = Schema::new(vec![
            Field::new("foo1".to_string(), DataType::Utf8),
            Field::new("foo2".to_string(), DataType::Utf8),
        ])
        .unwrap();

        // Check that estimator correctly understands the 1000x inflation, but with pushdowns halving the estimations
        let pushdowns = Pushdowns::new(None, None, Some(Arc::new(vec!["foo1".to_string()])), None);
        let estimator =
            FileInferredEstimator::from_parquet_metadata(Arc::new(schema), &parquet_meta);
        match estimator {
            None => panic!("Cannot construct estimator from metadata"),
            // Estimations should be (2 * 1000) / 2, since only one column is read
            Some(estimator) => {
                assert_eq!(estimator.estimate_from_size_on_disk(2, &pushdowns), 1000)
            }
        }
    }

    #[test]
    fn test_from_parquet_metadata_with_limit_pushdown() {
        let num_rows = 100;
        let (parquet_type, column_chunk_metadata) =
            make_simple_string_column("foo".to_string(), num_rows);
        let mut row_groups = IndexMap::new();
        row_groups.insert(
            0,
            RowGroupMetaData::new(
                vec![column_chunk_metadata.clone()],
                num_rows,
                column_chunk_metadata.compressed_size() as usize,
            ),
        );
        let parquet_meta = FileMetaData {
            version: 0,
            num_rows: num_rows,
            created_by: None,
            row_groups,
            key_value_metadata: None,
            schema_descr: SchemaDescriptor::new("schema".to_string(), vec![parquet_type]),
            column_orders: None,
        };
        let schema = Schema::new(vec![Field::new("foo".to_string(), DataType::Utf8)]).unwrap();

        let estimator =
            FileInferredEstimator::from_parquet_metadata(Arc::new(schema), &parquet_meta).unwrap();

        // Test with no limit
        let pushdowns_no_limit = Pushdowns::new(None, None, None, None);
        assert_eq!(
            estimator.estimate_from_size_on_disk(1000, &pushdowns_no_limit),
            1000000
        );

        // Test with limit of 50% of rows
        let pushdowns_half_limit = Pushdowns::new(None, None, None, Some(50));
        assert_eq!(
            estimator.estimate_from_size_on_disk(1000, &pushdowns_half_limit),
            500000
        );

        // Test with limit greater than number of rows
        let pushdowns_large_limit = Pushdowns::new(None, None, None, Some(200));
        assert_eq!(
            estimator.estimate_from_size_on_disk(1000, &pushdowns_large_limit),
            1000000
        );
    }

    #[test]
    fn test_from_parquet_metadata_with_zero_rows() {
        let num_rows = 0;
        let (parquet_type, column_chunk_metadata) =
            make_simple_string_column("foo".to_string(), num_rows);
        let mut row_groups = IndexMap::new();
        row_groups.insert(
            0,
            RowGroupMetaData::new(vec![column_chunk_metadata.clone()], num_rows, 0),
        );
        let parquet_meta = FileMetaData {
            version: 0,
            num_rows: num_rows,
            created_by: None,
            row_groups,
            key_value_metadata: None,
            schema_descr: SchemaDescriptor::new("schema".to_string(), vec![parquet_type]),
            column_orders: None,
        };
        let schema = Schema::new(vec![Field::new("foo".to_string(), DataType::Utf8)]).unwrap();

        let estimator =
            FileInferredEstimator::from_parquet_metadata(Arc::new(schema), &parquet_meta);
        assert!(
            estimator.is_none(),
            "Estimator should be None for zero rows"
        );
    }
}
