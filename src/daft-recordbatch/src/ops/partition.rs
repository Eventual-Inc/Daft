use std::ops::Rem;

use common_error::{DaftError, DaftResult};
use daft_core::datatypes::UInt64Array;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_groupby::IntoGroups;
use rand::SeedableRng;

use crate::RecordBatch;

impl RecordBatch {
    /// Partition `self` into `num_partitions` output batches based on the per-row partition
    /// index in `targets`.
    ///
    /// Algorithm: radix-permutation + zero-copy slice. We avoid N per-partition `take` calls
    /// (which scale as N × ncols allocations + N take-dispatches) by:
    ///   1) histogramming counts per partition
    ///   2) prefix-summing to get the output start offset per partition
    ///   3) building a single permutation array that groups rows by partition
    ///   4) doing ONE `take` to materialize the permuted batch
    ///   5) slicing the result into N RecordBatches (zero-copy on columns)
    ///
    /// On TPC-H-shaped workloads (small morsels, many partitions) this is 3-6× faster than the
    /// previous N-take approach. Tracked in `repartition_bench`.
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
        if targets.null_count() != 0 {
            return Err(DaftError::ComputeError(format!(
                "target array can not contain nulls, contains {} nulls",
                targets.null_count()
            )));
        }

        let nrows = self.len();
        let target_vals = targets.values();

        // Step 1: histogram counts per partition (with bounds checking).
        let mut counts = vec![0usize; num_partitions];
        for (s_idx, t_idx) in target_vals.iter().enumerate() {
            let p = *t_idx as usize;
            if p >= num_partitions {
                return Err(DaftError::ComputeError(format!(
                    "idx in target array is out of bounds, target idx {t_idx} at index {s_idx} out of {num_partitions} partitions"
                )));
            }
            counts[p] += 1;
        }

        // Step 2: prefix sum into write cursors; also remember start offsets for slicing.
        let mut cursors = vec![0usize; num_partitions];
        let mut acc = 0usize;
        for p in 0..num_partitions {
            cursors[p] = acc;
            acc += counts[p];
        }
        let starts = cursors.clone();

        // Step 3: scatter row indices into one contiguous permutation buffer.
        let mut perm: Vec<u64> = vec![0u64; nrows];
        for (r, t) in target_vals.iter().enumerate() {
            let p = *t as usize;
            // SAFETY: bounds checked in the histogram pass above. cursors[p] starts at the
            // partition's slot and advances by `counts[p]` total, never overflowing nrows.
            unsafe {
                let off = cursors.get_unchecked_mut(p);
                *perm.get_unchecked_mut(*off) = r as u64;
                *off += 1;
            }
        }

        // Step 4: ONE take to materialize the permuted batch.
        let perm_arr = UInt64Array::from_vec("idx", perm);
        let taken = self.take(&perm_arr)?;

        // Step 5: slice into N partitions (zero-copy on columns).
        let mut out = Vec::with_capacity(num_partitions);
        for p in 0..num_partitions {
            let start = starts[p];
            let end = start + counts[p];
            out.push(taken.slice(start, end)?);
        }
        Ok(out)
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
mod tests {
    use daft_core::{
        datatypes::Field,
        prelude::{DataType, Schema, UInt64Array},
        series::{IntoSeries, Series},
    };

    use super::*;

    fn make_batch(key: Vec<u64>, payload: Vec<u64>) -> RecordBatch {
        let n = key.len();
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64),
            Field::new("payload", DataType::UInt64),
        ]));
        let series: Vec<Series> = vec![
            UInt64Array::from_field_and_values(Field::new("key", DataType::UInt64), key)
                .into_series(),
            UInt64Array::from_field_and_values(Field::new("payload", DataType::UInt64), payload)
                .into_series(),
        ];
        RecordBatch::new_unchecked(schema, series, n)
    }

    #[test]
    fn radix_partition_groups_rows_correctly() {
        // 9 rows, mapped to 4 partitions. Verify each output partition contains exactly the
        // rows whose target index matches, in input order.
        let batch = make_batch(vec![0, 1, 2, 3, 4, 5, 6, 7, 8], vec![10, 11, 12, 13, 14, 15, 16, 17, 18]);
        let targets = UInt64Array::from_vec("t", vec![3, 0, 1, 0, 2, 1, 3, 0, 2]);

        let parts = batch.partition_by_index(&targets, 4).unwrap();
        assert_eq!(parts.len(), 4);
        // Partition 0: input rows 1, 3, 7 → key 1, 3, 7; payload 11, 13, 17
        let p0 = &parts[0];
        let p1 = &parts[1];
        let p2 = &parts[2];
        let p3 = &parts[3];
        assert_eq!(p0.len(), 3);
        assert_eq!(p1.len(), 2);
        assert_eq!(p2.len(), 2);
        assert_eq!(p3.len(), 2);

        let to_vec = |b: &RecordBatch, col: usize| -> Vec<u64> {
            let s = b.get_column(col);
            s.u64().unwrap().values().to_vec()
        };
        assert_eq!(to_vec(p0, 0), vec![1, 3, 7]);
        assert_eq!(to_vec(p0, 1), vec![11, 13, 17]);
        assert_eq!(to_vec(p1, 0), vec![2, 5]);
        assert_eq!(to_vec(p1, 1), vec![12, 15]);
        assert_eq!(to_vec(p2, 0), vec![4, 8]);
        assert_eq!(to_vec(p2, 1), vec![14, 18]);
        assert_eq!(to_vec(p3, 0), vec![0, 6]);
        assert_eq!(to_vec(p3, 1), vec![10, 16]);
    }

    #[test]
    fn radix_partition_handles_empty_partitions() {
        let batch = make_batch(vec![0, 1, 2], vec![10, 11, 12]);
        let targets = UInt64Array::from_vec("t", vec![0, 0, 0]);
        let parts = batch.partition_by_index(&targets, 4).unwrap();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0].len(), 3);
        assert_eq!(parts[1].len(), 0);
        assert_eq!(parts[2].len(), 0);
        assert_eq!(parts[3].len(), 0);
    }

    #[test]
    fn radix_partition_rejects_out_of_bounds_target() {
        let batch = make_batch(vec![0, 1], vec![10, 11]);
        let targets = UInt64Array::from_vec("t", vec![0, 4]);
        assert!(batch.partition_by_index(&targets, 4).is_err());
    }
}
