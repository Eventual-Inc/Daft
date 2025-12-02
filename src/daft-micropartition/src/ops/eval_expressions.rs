use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::Schema;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_stats::{ColumnRangeStatistics, TableStatistics};
use snafu::ResultExt;

use crate::{DaftCoreComputeSnafu, micropartition::MicroPartition};

fn infer_schema(exprs: &[BoundExpr], schema: &Schema) -> DaftResult<Schema> {
    let fields = exprs
        .iter()
        .map(|e| e.inner().to_field(schema).context(DaftCoreComputeSnafu))
        .collect::<crate::Result<Vec<_>>>()?;

    Ok(Schema::new(fields))
}

impl MicroPartition {
    // TODO(universalmind303): make this async
    pub fn eval_expression_list(&self, exprs: &[BoundExpr]) -> DaftResult<Self> {
        let expected_schema = infer_schema(exprs, &self.schema)?;

        let evaluated_tables: Vec<_> = self
            .record_batches()
            .iter()
            .map(|table| table.eval_expression_list(exprs))
            .try_collect()?;

        let eval_stats = self
            .statistics
            .as_ref()
            .map(|table_statistics| table_statistics.eval_expression_list(exprs))
            .transpose()?;

        Ok(Self::new_loaded(
            expected_schema.into(),
            Arc::new(evaluated_tables),
            eval_stats,
        ))
    }
    pub async fn eval_expression_list_async(&self, exprs: Vec<BoundExpr>) -> DaftResult<Self> {
        let expected_schema = infer_schema(exprs.as_ref(), &self.schema)?;

        let evaluated_table_futs = self
            .record_batches()
            .iter()
            .map(|table| table.eval_expression_list_async(exprs.clone()));

        let evaluated_tables = futures::future::try_join_all(evaluated_table_futs).await?;

        let eval_stats = self
            .statistics
            .as_ref()
            .map(|table_statistics| table_statistics.eval_expression_list(exprs.as_ref()))
            .transpose()?;

        Ok(Self::new_loaded(
            expected_schema.into(),
            Arc::new(evaluated_tables),
            eval_stats,
        ))
    }

    pub async fn par_eval_expression_list(
        &self,
        exprs: &[BoundExpr],
        num_parallel_tasks: usize,
    ) -> DaftResult<Self> {
        let expected_schema = infer_schema(exprs, &self.schema)?;

        let evaluated_table_futs = self
            .record_batches()
            .iter()
            .map(|table| table.par_eval_expression_list(exprs, num_parallel_tasks));

        let evaluated_tables = futures::future::try_join_all(evaluated_table_futs).await?;

        let eval_stats = self
            .statistics
            .as_ref()
            .map(|table_statistics| table_statistics.eval_expression_list(exprs))
            .transpose()?;

        Ok(Self::new_loaded(
            expected_schema.into(),
            Arc::new(evaluated_tables),
            eval_stats,
        ))
    }

    pub fn explode(&self, exprs: &[BoundExpr]) -> DaftResult<Self> {
        let evaluated_tables = self
            .record_batches()
            .iter()
            .map(|t| t.explode(exprs))
            .collect::<DaftResult<Vec<_>>>()?;
        let expected_new_columns = infer_schema(exprs, &self.schema)?;

        let expected_schema = Arc::new(self.schema.non_distinct_union(&expected_new_columns)?);

        let eval_stats = if let Some(stats) = &self.statistics {
            let new_stats = expected_schema
                .field_names()
                .map(|name| {
                    if expected_new_columns.has_field(name) {
                        ColumnRangeStatistics::Missing
                    } else if let [(index, _)] = self.schema.get_fields_with_name(name)[..] {
                        stats[index].clone()
                    } else {
                        ColumnRangeStatistics::Missing
                    }
                })
                .collect();

            Some(TableStatistics::new(new_stats, expected_schema.clone()))
        } else {
            None
        };

        Ok(Self::new_loaded(
            expected_schema,
            Arc::new(evaluated_tables),
            eval_stats,
        ))
    }
}
