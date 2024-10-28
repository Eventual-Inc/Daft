mod actor_pool_project;
mod agg;
mod broadcast_join;
mod concat;
mod csv;
#[cfg(feature = "python")]
mod deltalake_write;

mod empty_scan;
mod explode;
mod filter;
mod hash_join;
#[cfg(feature = "python")]
mod iceberg_write;
mod in_memory;
mod json;
#[cfg(feature = "python")]
mod lance_write;
mod limit;
mod monotonically_increasing_id;
mod parquet;
mod pivot;
mod project;
mod sample;
mod scan;
mod shuffle_exchange;
mod sort;
mod sort_merge_join;
mod unpivot;

pub use actor_pool_project::ActorPoolProject;
pub use agg::Aggregate;
pub use broadcast_join::BroadcastJoin;
pub use concat::Concat;
pub use csv::TabularWriteCsv;
#[cfg(feature = "python")]
pub use deltalake_write::DeltaLakeWrite;
pub use empty_scan::EmptyScan;
pub use explode::Explode;
pub use filter::Filter;
pub use hash_join::HashJoin;
#[cfg(feature = "python")]
pub use iceberg_write::IcebergWrite;
pub use in_memory::InMemoryScan;
pub use json::TabularWriteJson;
#[cfg(feature = "python")]
pub use lance_write::LanceWrite;
pub use limit::Limit;
pub use monotonically_increasing_id::MonotonicallyIncreasingId;
pub use parquet::TabularWriteParquet;
pub use pivot::Pivot;
pub use project::Project;
pub use sample::Sample;
pub use scan::TabularScan;
pub use shuffle_exchange::{ShuffleExchange, ShuffleExchangeFactory, ShuffleExchangeStrategy};
pub use sort::Sort;
pub use sort_merge_join::SortMergeJoin;
pub use unpivot::Unpivot;

#[macro_export]
/// Implement the `common_display::tree::TreeDisplay` trait for the given struct
///
/// using the `get_name` method as the compact description and the `multiline_display` method for the default and verbose descriptions.
macro_rules! impl_default_tree_display {
    ($($struct:ident),+) => {
        $(
            impl common_display::tree::TreeDisplay for $struct {
                fn display_as(&self, level: common_display::DisplayLevel) -> String {
                    match level {
                        common_display::DisplayLevel::Compact => self.get_name(),
                        _ => self.multiline_display().join("\n"),
                    }
                }
                fn get_children(&self) -> Vec<&dyn common_display::tree::TreeDisplay> {
                    vec![self.input.as_ref()]
                }
            }
        )+
    };
}
