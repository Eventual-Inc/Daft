use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::prelude::SchemaRef;
use daft_stats::TableMetadata;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    fn concat_unchecked(mps: Vec<Self>, schema: SchemaRef) -> DaftResult<Self> {
        let mut all_tables = vec![];

        for m in &mps {
            all_tables.extend_from_slice(m.record_batches());
        }
        let mut all_stats = None;

        for stats in mps.iter().flat_map(|m| &m.statistics) {
            if all_stats.is_none() {
                all_stats = Some(stats.clone());
            }

            if let Some(curr_stats) = &all_stats {
                all_stats = Some(curr_stats.union(stats)?);
            }
        }
        let new_len = all_tables
            .iter()
            .map(daft_recordbatch::RecordBatch::len)
            .sum();

        Ok(Self {
            schema,
            chunks: Arc::new(all_tables),
            metadata: TableMetadata { length: new_len },
            statistics: all_stats,
        })
    }

    pub fn concat(mps: impl Into<Vec<Self>>) -> DaftResult<Self> {
        let mps: Vec<_> = mps.into();
        if mps.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 MicroPartition to perform concat".to_string(),
            ));
        }

        let first_table = mps.first().unwrap();

        let first_schema = &first_table.schema;

        if cfg!(debug_assertions) {
            for tab in mps.iter().skip(1) {
                if &tab.schema != first_schema {
                    return Err(DaftError::SchemaMismatch(format!(
                        "MicroPartition::concat requires all schemas to match, {} vs {}",
                        first_schema, tab.schema
                    )));
                }
            }
        }

        let first_schema = first_schema.clone();
        Self::concat_unchecked(mps, first_schema)
    }

    pub fn concat_or_empty(mps: impl Into<Vec<Self>>, schema: SchemaRef) -> DaftResult<Self> {
        let mps: Vec<_> = mps.into();
        if mps.is_empty() {
            return Ok(Self::empty(Some(schema)));
        }

        if cfg!(debug_assertions) {
            for tab in &mps {
                if tab.schema != schema {
                    return Err(DaftError::SchemaMismatch(format!(
                        "MicroPartition::concat_or_empty requires all schemas to match, {} vs {}",
                        schema, tab.schema
                    )));
                }
            }
        }

        Self::concat_unchecked(mps, schema)
    }
}
