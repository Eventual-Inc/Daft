use std::{ops::Deref, sync::Arc};

use common_error::DaftResult;
use daft_core::schema::SchemaRef;

use crate::micropartition::{MicroPartition, TableState};

use daft_stats::TableStatistics;

impl MicroPartition {
    pub fn cast_to_schema(&self, schema: SchemaRef) -> DaftResult<Self> {
        let pruned_statistics = self.statistics.clone().map(|stats| TableStatistics {
            columns: stats
                .columns
                .into_iter()
                .filter(|(key, _)| schema.names().contains(key))
                .collect(),
        });

        let guard = self.state.lock().unwrap();
        match guard.deref() {
            // Replace schema if Unloaded, which should be applied when data is lazily loaded
            TableState::Unloaded(scan_task) => Ok(MicroPartition::new_unloaded(
                schema.clone(),
                scan_task.clone(),
                self.metadata.clone(),
                pruned_statistics.expect("Unloaded MicroPartition should have statistics"),
            )),
            // If Tables are already loaded, we map `Table::cast_to_schema` on each Table
            TableState::Loaded(tables) => Ok(MicroPartition::new_loaded(
                schema.clone(),
                Arc::new(
                    tables
                        .iter()
                        .map(|tbl| tbl.cast_to_schema(schema.as_ref()))
                        .collect::<DaftResult<Vec<_>>>()?,
                ),
                pruned_statistics,
            )),
        }
    }
}
