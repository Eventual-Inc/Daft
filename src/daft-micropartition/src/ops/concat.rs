use std::{borrow::Borrow, ops::Deref, sync::Mutex};

use common_error::{DaftError, DaftResult};
use daft_io::IOStatsContext;
use daft_stats::TableMetadata;

use crate::micropartition::{MicroPartition, TableState};

impl MicroPartition {
    pub fn concat<I, T>(mps: I) -> DaftResult<Self>
    where
        I: IntoIterator<Item = T>,
        T: Deref,
        T::Target: Borrow<Self>,
    {
        let mps: Vec<_> = mps.into_iter().collect();
        if mps.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 MicroPartition to perform concat".to_string(),
            ));
        }

        let first_table = mps.first().unwrap().deref().borrow();

        let first_schema = &first_table.schema;
        for tab in mps.iter().skip(1) {
            let tab = tab.deref().borrow();
            if &tab.schema != first_schema {
                return Err(DaftError::SchemaMismatch(format!(
                    "MicroPartition concat requires all schemas to match, {} vs {}",
                    first_schema, tab.schema
                )));
            }
        }

        let io_stats = IOStatsContext::new("MicroPartition::concat");

        let mut all_tables = vec![];

        for m in &mps {
            let m = m.deref().borrow();
            let tables = m.tables_or_read(io_stats.clone())?;
            all_tables.extend_from_slice(tables.as_slice());
        }
        let mut all_stats = None;

        for stats in mps.iter().flat_map(|m| &m.deref().borrow().statistics) {
            if all_stats.is_none() {
                all_stats = Some(stats.clone());
            }

            if let Some(curr_stats) = &all_stats {
                all_stats = Some(curr_stats.union(stats)?);
            }
        }
        let new_len = all_tables.iter().map(daft_table::Table::len).sum();

        Ok(Self {
            schema: first_schema.clone(),
            state: Mutex::new(TableState::Loaded(all_tables.into())),
            metadata: TableMetadata { length: new_len },
            statistics: all_stats,
        })
    }
}
