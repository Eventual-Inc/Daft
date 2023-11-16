use std::sync::Mutex;

use common_error::{DaftError, DaftResult};
use daft_io::IOStatsContext;

use crate::micropartition::{MicroPartition, TableState};

use daft_stats::TableMetadata;

impl MicroPartition {
    pub fn concat(mps: &[&Self]) -> DaftResult<Self> {
        if mps.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 MicroPartition to perform concat".to_string(),
            ));
        }

        let first_table = mps.first().unwrap();

        let first_schema = first_table.schema.as_ref();
        for tab in mps.iter().skip(1) {
            if tab.schema.as_ref() != first_schema {
                return Err(DaftError::SchemaMismatch(format!(
                    "MicroPartition concat requires all schemas to match, {} vs {}",
                    first_schema, tab.schema
                )));
            }
        }

        let io_stats = IOStatsContext::new("MicroPartition::concat");

        let mut all_tables = vec![];

        for m in mps.iter() {
            let tables = m.tables_or_read(io_stats.clone())?;
            all_tables.extend_from_slice(tables.as_slice());
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
        let new_len = all_tables.iter().map(|t| t.len()).sum();

        Ok(MicroPartition {
            schema: mps.first().unwrap().schema.clone(),
            state: Mutex::new(TableState::Loaded(all_tables.into())),
            metadata: TableMetadata { length: new_len },
            statistics: all_stats,
        })
    }
}
