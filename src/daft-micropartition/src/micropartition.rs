use daft_table::Table;

use crate::table_stats::TableStatistics;

struct DeferredLoadingParams {}

enum TableState {
    Unloaded(DeferredLoadingParams),
    Loaded(Vec<Table>),
}

struct MicroPartition {
    state: std::sync::Mutex<TableState>,
    statistics: TableStatistics,
}

impl MicroPartition {
    fn tables_or_read(&self) -> &[&Table] {
        todo!("to do me")
    }
}
