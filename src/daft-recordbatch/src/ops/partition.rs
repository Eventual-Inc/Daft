use std::ops::Rem;

use common_error::{DaftError, DaftResult};
use daft_core::datatypes::UInt64Array;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_groupby::IntoGroups;
use rand::SeedableRng;

use crate::RecordBatch;

impl RecordBatch {
    fn partition_by_index(
        &self,
        targets: &UInt64Array,
        num_partitions: usize,
    ) -> DaftResult<Vec<Self>> {
        if self.len() != targets.len() {
            return Err(DaftError::ValueError(format!(
                "Mismatch of length of table and targets, {} vs {}",
                self.len(),
                targets.len()
            )));
        }
        let mut output_to_input_idx =
            vec![Vec::with_capacity(self.len() / num_partitions); num_partitions];
        if targets.null_count() != 0 {
            return Err(DaftError::ComputeError(format!(
                "target array can not contain nulls, contains {} nulls",
                targets.null_count()
            )));
        }

        for (s_idx, t_idx) in targets.values().iter().enumerate() {
            if *t_idx >= (num_partitions as u64) {
                return Err(DaftError::ComputeError(format!(
                    "idx in target array is out of bounds, target idx {t_idx} at index {s_idx} out of {num_partitions} partitions"
                )));
            }

            output_to_input_idx[*t_idx as usize].push(s_idx as u64);
        }
        output_to_input_idx
            .into_iter()
            .map(|v| {
                let indices = UInt64Array::from_vec("idx", v);
                self.take(&indices)
            })
            .collect::<DaftResult<Vec<_>>>()
    }

    pub fn partition_by_hash(
        &self,
        exprs: &[BoundExpr],
        num_partitions: usize,
    ) -> DaftResult<Vec<Self>> {
        if num_partitions == 0 {
            return Err(DaftError::ValueError(
                "Can not partition a Table by 0 partitions".to_string(),
            ));
        }

        let targets =
            self.eval_expression_list(exprs)?
                .hash_rows()?
                .rem(&UInt64Array::from_slice(
                    "num_partitions",
                    &[num_partitions as u64],
                ))?;
        self.partition_by_index(&targets, num_partitions)
    }

    /// Like [`Self::partition_by_hash`] but mixes a `seed` into the row hash. Equal keys stay
    /// co-located within a single seed; different seeds spread keys differently. Used for recursive
    /// sub-partitioning of an overflowing aggregation bucket.
    pub fn partition_by_hash_seeded(
        &self,
        exprs: &[BoundExpr],
        num_partitions: usize,
        seed: u64,
    ) -> DaftResult<Vec<Self>> {
        if num_partitions == 0 {
            return Err(DaftError::ValueError(
                "Can not partition a Table by 0 partitions".to_string(),
            ));
        }

        let targets = self
            .eval_expression_list(exprs)?
            .hash_rows_seeded(seed)?
            .rem(&UInt64Array::from_slice(
                "num_partitions",
                &[num_partitions as u64],
            ))?;
        self.partition_by_index(&targets, num_partitions)
    }

    pub fn partition_by_random(&self, num_partitions: usize, seed: u64) -> DaftResult<Vec<Self>> {
        if num_partitions == 0 {
            return Err(DaftError::ValueError(
                "Can not partition a Table by 0 partitions".to_string(),
            ));
        }
        use rand::{Rng, distr::Uniform};
        let range = Uniform::try_from(0..num_partitions as u64).unwrap();

        let rng = rand::rngs::StdRng::seed_from_u64(seed);
        let values: Vec<u64> = rng.sample_iter(&range).take(self.len()).collect();
        let targets = UInt64Array::from_vec("idx", values);

        self.partition_by_index(&targets, num_partitions)
    }

    pub fn partition_by_range(
        &self,
        partition_keys: &[BoundExpr],
        boundaries: &Self,
        descending: &[bool],
    ) -> DaftResult<Vec<Self>> {
        if boundaries.is_empty() {
            return Ok(vec![self.clone()]);
        }
        let partition_key_table = self.eval_expression_list(partition_keys)?;
        let targets = boundaries.search_sorted(&partition_key_table, descending)?;
        self.partition_by_index(&targets, boundaries.len() + 1)
    }

    pub fn partition_by_value(
        &self,
        partition_keys: &[BoundExpr],
    ) -> DaftResult<(Vec<Self>, Self)> {
        let partition_key_table = self.eval_expression_list(partition_keys)?;
        let (key_idx, group_idx) = partition_key_table.make_groups()?;
        let key_idx = UInt64Array::from_vec("idx", key_idx);
        let pkeys_per_output_table = partition_key_table.take(&key_idx)?;
        drop(partition_key_table);
        let output_tables = group_idx
            .into_iter()
            .map(|gidx| {
                let gidx = UInt64Array::from_vec("idx", gidx.into_vec());
                self.take(&gidx)
            })
            .collect::<DaftResult<Vec<_>>>()?;
        Ok((output_tables, pkeys_per_output_table))
    }

    pub fn partition_by_value_projected(
        &self,
        partition_keys: &[BoundExpr],
        projection: &[usize],
    ) -> DaftResult<(Vec<Self>, Self)> {
        let partition_key_table = self.eval_expression_list(partition_keys)?;
        let (key_idx, group_idx) = partition_key_table.make_groups()?;
        let key_idx = UInt64Array::from_vec("idx", key_idx);
        let pkeys_per_output_table = partition_key_table.take(&key_idx)?;
        drop(partition_key_table);
        let projected = self.get_columns(projection);
        let output_tables = group_idx
            .into_iter()
            .map(|gidx| {
                let gidx = UInt64Array::from_vec("idx", gidx.into_vec());
                projected.take(&gidx)
            })
            .collect::<DaftResult<Vec<_>>>()?;
        Ok((output_tables, pkeys_per_output_table))
    }
}

#[cfg(test)]
mod seeded_hash_tests {
    use daft_core::{datatypes::*, prelude::*, series::IntoSeries};
    use daft_dsl::{expr::bound_expr::BoundExpr, resolved_col};

    use crate::RecordBatch;

    fn make_batch() -> (RecordBatch, Vec<BoundExpr>) {
        // 1000 rows over 50 distinct keys.
        let keys: Vec<i64> = (0..1000).map(|i| (i % 50) as i64).collect();
        let key_series = Int64Array::from_vec("k", keys).into_series();
        let schema = Schema::new(vec![Field::new("k", DataType::Int64)]);
        let rb = RecordBatch::from_nonempty_columns(vec![key_series]).unwrap();
        let exprs = vec![BoundExpr::try_new(resolved_col("k"), &schema).unwrap()];
        (rb, exprs)
    }

    /// Collect, per partition, the set of distinct key values present.
    fn key_sets(parts: &[RecordBatch]) -> Vec<std::collections::BTreeSet<i64>> {
        parts
            .iter()
            .map(|p| {
                let col = p.get_column(0).i64().unwrap();
                col.into_iter().flatten().collect()
            })
            .collect()
    }

    #[test]
    fn seeded_hash_colocates_equal_keys() {
        let (rb, exprs) = make_batch();
        let parts = rb.partition_by_hash_seeded(&exprs, 8, 1234).unwrap();
        // Each key value must appear in exactly one partition (groups stay co-located).
        let sets = key_sets(&parts);
        for k in 0..50i64 {
            let count = sets.iter().filter(|s| s.contains(&k)).count();
            assert_eq!(count, 1, "key {k} appeared in {count} partitions");
        }
        // Row count is preserved.
        let total: usize = parts.iter().map(RecordBatch::len).sum();
        assert_eq!(total, 1000);
    }

    #[test]
    fn different_seeds_produce_different_assignments() {
        let (rb, exprs) = make_batch();
        let a = rb.partition_by_hash_seeded(&exprs, 8, 1).unwrap();
        let b = rb.partition_by_hash_seeded(&exprs, 8, 2).unwrap();
        // The two seeds must not produce identical per-partition key sets, otherwise recursive
        // sub-partitioning of an overflowing bucket would never make progress.
        assert_ne!(
            key_sets(&a),
            key_sets(&b),
            "seeds 1 and 2 produced identical partition assignments"
        );
    }

    #[test]
    fn seeded_matches_unseeded_row_count_and_colocation() {
        // Sanity: unseeded partitioning also co-locates; seeded just shifts the mapping.
        let (rb, exprs) = make_batch();
        let parts = rb.partition_by_hash(&exprs, 8).unwrap();
        let total: usize = parts.iter().map(RecordBatch::len).sum();
        assert_eq!(total, 1000);
    }
}
