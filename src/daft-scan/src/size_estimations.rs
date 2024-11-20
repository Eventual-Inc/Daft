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
    ) -> Option<usize> {
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

        Some(total_uncompressed_size as usize)
    }
}
