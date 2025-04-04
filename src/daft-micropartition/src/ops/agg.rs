use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use daft_recordbatch::RecordBatch;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn agg(&self, to_agg: &[ExprRef], group_by: &[ExprRef]) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::agg");

        let tables = self.concat_or_get(io_stats)?;

        match tables.as_slice() {
            [] => {
                let empty_table = RecordBatch::empty(Some(self.schema.clone()))?;
                let agged = empty_table.agg(to_agg, group_by)?;
                Ok(Self::new_loaded(
                    agged.schema.clone(),
                    vec![agged].into(),
                    None,
                ))
            }
            [t] => {
                let agged = t.agg(to_agg, group_by)?;
                Ok(Self::new_loaded(
                    agged.schema.clone(),
                    vec![agged].into(),
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }

    /// Window aggregation that preserves all original columns
    ///
    /// Similar to `agg`, but preserves all columns from the input data
    /// in addition to the aggregation results. This is specifically
    /// designed for window functions where we need the original data
    /// alongside the aggregated results.
    pub fn window_agg(&self, to_agg: &[ExprRef], group_by: &[ExprRef]) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::window_agg");

        println!("[DEBUG MP::window_agg] Input schema: {}", self.schema());
        println!(
            "[DEBUG MP::window_agg] Input schema fields: {:#?}",
            self.schema().fields
        );
        println!(
            "[DEBUG MP::window_agg] To aggregate expressions: {:#?}",
            to_agg
        );
        println!(
            "[DEBUG MP::window_agg] Group by expressions: {:#?}",
            group_by
        );

        let tables = self.concat_or_get(io_stats)?;
        println!(
            "[DEBUG MP::window_agg] Number of tables after concat: {}",
            tables.len()
        );

        match tables.as_slice() {
            [] => {
                println!("[DEBUG MP::window_agg] Empty tables case");
                let empty_table = RecordBatch::empty(Some(self.schema.clone()))?;
                // For empty tables, just return an empty record batch
                Ok(Self::new_loaded(
                    empty_table.schema.clone(),
                    vec![empty_table].into(),
                    None,
                ))
            }
            [t] => {
                println!("[DEBUG MP::window_agg] Single table case");
                println!(
                    "[DEBUG MP::window_agg] RecordBatch schema before: {:#?}",
                    t.schema
                );

                // For non-empty tables, we need to:
                // 1. Compute the regular aggregations by group
                let agged = t.agg(to_agg, group_by)?;
                println!(
                    "[DEBUG MP::window_agg] After t.agg(), result schema: {:#?}",
                    agged.schema
                );
                println!(
                    "[DEBUG MP::window_agg] After t.agg(), result rows: {}",
                    agged.len()
                );

                // Print column names in a simpler format
                println!("[DEBUG MP::window_agg] After t.agg(), column names:");
                for key in agged.schema.fields.keys() {
                    println!("    - {}", key);
                }

                let result = Self::new_loaded(agged.schema.clone(), vec![agged].into(), None);

                println!(
                    "[DEBUG MP::window_agg] Final MicroPartition schema: {}",
                    result.schema()
                );
                println!(
                    "[DEBUG MP::window_agg] Final MicroPartition schema fields: {:#?}",
                    result.schema().fields
                );

                Ok(result)
            }
            _ => unreachable!(),
        }
    }
}
