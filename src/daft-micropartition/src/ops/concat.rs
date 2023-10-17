use std::sync::Mutex;

use common_error::DaftResult;
use daft_dsl::Expr;
use snafu::ResultExt;

use crate::{
    column_stats::TruthValue,
    micropartition::{MicroPartition, TableState},
    DaftCoreComputeSnafu,
};

impl MicroPartition {
    pub fn concat(mps: &[&Self]) -> DaftResult<Self> {
        let mut all_tables = vec![];

        for m in mps.iter() {
            let tables = m.tables_or_read(None)?;
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
        // check all the schemas

        Ok(MicroPartition {
            schema: mps.first().unwrap().schema.clone(),
            state: Mutex::new(TableState::Loaded(all_tables.into())),
            statistics: all_stats,
        })
    }
}
