use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::datatypes::UInt64Array;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_recordbatch::RecordBatch;

use crate::micropartition::MicroPartition;

fn transpose2<T>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
    if v.is_empty() {
        return v;
    }
    let len = v[0].len();
    let mut iters: Vec<_> = v
        .into_iter()
        .map(std::iter::IntoIterator::into_iter)
        .collect();
    (0..len)
        .map(|_| {
            iters
                .iter_mut()
                .map(|n| n.next().unwrap())
                .collect::<Vec<T>>()
        })
        .collect()
}

impl MicroPartition {
    fn vec_part_tables_to_mps(&self, part_tables: Vec<Vec<RecordBatch>>) -> DaftResult<Vec<Self>> {
        let part_tables = transpose2(part_tables);
        Ok(part_tables
            .into_iter()
            .map(|v| Self::new_loaded(self.schema.clone(), Arc::new(v), self.statistics.clone()))
            .collect())
    }

    pub fn partition_by_hash(
        &self,
        exprs: &[BoundExpr],
        num_partitions: usize,
    ) -> DaftResult<Vec<Self>> {
        let tables = self.record_batches();

        if tables.is_empty() {
            return Ok(
                std::iter::repeat_with(|| Self::empty(Some(self.schema.clone())))
                    .take(num_partitions)
                    .collect(),
            );
        }

        let part_tables = tables
            .iter()
            .map(|t| t.partition_by_hash(exprs, num_partitions))
            .collect::<DaftResult<Vec<_>>>()?;
        self.vec_part_tables_to_mps(part_tables)
    }

    pub fn partition_by_random(&self, num_partitions: usize, seed: u64) -> DaftResult<Vec<Self>> {
        let tables = self.record_batches();

        if tables.is_empty() {
            return Ok(
                std::iter::repeat_with(|| Self::empty(Some(self.schema.clone())))
                    .take(num_partitions)
                    .collect(),
            );
        }

        let part_tables = tables
            .iter()
            .enumerate()
            .map(|(i, t)| t.partition_by_random(num_partitions, seed + i as u64))
            .collect::<DaftResult<Vec<_>>>()?;
        self.vec_part_tables_to_mps(part_tables)
    }

    pub fn partition_by_range(
        &self,
        partition_keys: &[BoundExpr],
        boundaries: &RecordBatch,
        descending: &[bool],
    ) -> DaftResult<Vec<Self>> {
        let tables = self.record_batches();

        if tables.is_empty() {
            let num_partitions = boundaries.len() + 1;
            return Ok(
                std::iter::repeat_with(|| Self::empty(Some(self.schema.clone())))
                    .take(num_partitions)
                    .collect(),
            );
        }

        let part_tables = tables
            .iter()
            .map(|t| t.partition_by_range(partition_keys, boundaries, descending))
            .collect::<DaftResult<Vec<_>>>()?;
        self.vec_part_tables_to_mps(part_tables)
    }
    pub fn partition_by_value(
        &self,
        partition_keys: &[BoundExpr],
    ) -> DaftResult<(Vec<Self>, Self)> {
        let Some(table) = self.concat_or_get()? else {
            let empty = Self::empty(Some(self.schema.clone()));
            let pkeys = empty.eval_expression_list(partition_keys)?;
            return Ok((vec![], pkeys));
        };

        let (tables, values) = table.partition_by_value(partition_keys)?;

        let mps = tables
            .into_iter()
            .map(|t| Self::new_loaded(self.schema.clone(), Arc::new(vec![t]), None))
            .collect::<Vec<_>>();

        let values = Self::new_loaded(values.schema.clone(), Arc::new(vec![values]), None);

        Ok((mps, values))
    }

    pub fn partition_by_index(
        &self,
        targets: &UInt64Array,
        num_partitions: usize,
    ) -> DaftResult<Vec<Self>> {
        let tables = self.record_batches();

        if tables.is_empty() {
            return Ok(
                std::iter::repeat_with(|| Self::empty(Some(self.schema.clone())))
                    .take(num_partitions)
                    .collect(),
            );
        }

        let mut start_idx = 0;
        let mut part_tables = Vec::with_capacity(tables.len());
        for t in tables {
            let end_idx = start_idx + t.len();
            let sub_targets = targets.slice(start_idx, end_idx)?;
            part_tables.push(t.partition_by_index(&sub_targets, num_partitions)?);
            start_idx = end_idx;
        }
        self.vec_part_tables_to_mps(part_tables)
    }

    pub fn into_partitions(&self, num_partitions: usize) -> DaftResult<Vec<Self>> {
        if num_partitions == 0 {
            return Err(DaftError::ValueError(
                "Can not partition a MicroPartition by 0 partitions".to_string(),
            ));
        }
        let targets = (0..self.len() as u64)
            .map(|i| i % num_partitions as u64)
            .collect::<Vec<_>>();
        let targets_arr = UInt64Array::from_vec("idx", targets);
        self.partition_by_index(&targets_arr, num_partitions)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::{
        datatypes::{Field, Int64Array, UInt64Array},
        prelude::{DataType, Schema},
        series::IntoSeries,
    };
    use daft_recordbatch::RecordBatch;

    use super::MicroPartition;

    fn make_mp(col_name: &str, values: Vec<i64>) -> MicroPartition {
        let schema = Arc::new(Schema::new(vec![Field::new(col_name, DataType::Int64)]));
        let array = Int64Array::from_vec(col_name, values);
        let rb = RecordBatch::from_nonempty_columns(vec![array.into_series()]).unwrap();
        MicroPartition::new_loaded(schema, Arc::new(vec![rb]), None)
    }

    fn make_empty_mp(col_name: &str) -> MicroPartition {
        let schema = Arc::new(Schema::new(vec![Field::new(col_name, DataType::Int64)]));
        MicroPartition::empty(Some(schema))
    }

    fn collect_values(mp: &MicroPartition) -> Vec<i64> {
        let rb = mp.concat_or_get().unwrap();
        let mut values = Vec::new();
        if let Some(rb) = rb.as_ref() {
            let col = rb.get_column(0);
            for i in 0..col.len() {
                if let daft_core::lit::Literal::Int64(v) = col.get_lit(i) {
                    values.push(v);
                }
            }
        }
        values
    }

    #[test]
    fn test_partition_by_index_basic() {
        let mp = make_mp("a", vec![10, 20, 30, 40, 50]);
        let targets = UInt64Array::from_vec("idx", vec![0, 1, 0, 1, 0]);
        let parts = mp.partition_by_index(&targets, 2).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(collect_values(&parts[0]), vec![10, 30, 50]);
        assert_eq!(collect_values(&parts[1]), vec![20, 40]);
    }

    #[test]
    fn test_partition_by_index_empty() {
        let mp = make_empty_mp("a");
        let targets = UInt64Array::from_vec("idx", vec![]);
        let parts = mp.partition_by_index(&targets, 3).unwrap();
        assert_eq!(parts.len(), 3);
        for p in &parts {
            assert_eq!(p.len(), 0);
        }
    }

    #[test]
    fn test_partition_by_index_all_to_one() {
        let mp = make_mp("a", vec![1, 2, 3, 4]);
        let targets = UInt64Array::from_vec("idx", vec![0, 0, 0, 0]);
        let parts = mp.partition_by_index(&targets, 2).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(collect_values(&parts[0]), vec![1, 2, 3, 4]);
        assert_eq!(parts[1].len(), 0);
    }

    #[test]
    fn test_partition_by_index_multi_record_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let rb1 = {
            let array = Int64Array::from_vec("a", vec![10, 20]);
            RecordBatch::from_nonempty_columns(vec![array.into_series()]).unwrap()
        };
        let rb2 = {
            let array = Int64Array::from_vec("a", vec![30, 40]);
            RecordBatch::from_nonempty_columns(vec![array.into_series()]).unwrap()
        };
        let mp = MicroPartition::new_loaded(schema, Arc::new(vec![rb1, rb2]), None);
        let targets = UInt64Array::from_vec("idx", vec![0, 1, 1, 0]);
        let parts = mp.partition_by_index(&targets, 2).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].len(), 2);
        assert_eq!(parts[1].len(), 2);
        assert_eq!(collect_values(&parts[0]), vec![10, 40]);
        assert_eq!(collect_values(&parts[1]), vec![20, 30]);
    }

    #[test]
    fn test_into_partitions_basic() {
        let mp = make_mp("a", vec![1, 2, 3, 4, 5, 6]);
        let parts = mp.into_partitions(3).unwrap();
        assert_eq!(parts.len(), 3);
        let total_rows: usize = parts.iter().map(|p| p.len()).sum();
        assert_eq!(total_rows, 6);
        // Round-robin: idx%3 → part[0]=[1,4], part[1]=[2,5], part[2]=[3,6]
        assert_eq!(collect_values(&parts[0]), vec![1, 4]);
        assert_eq!(collect_values(&parts[1]), vec![2, 5]);
        assert_eq!(collect_values(&parts[2]), vec![3, 6]);
    }

    #[test]
    fn test_into_partitions_zero_error() {
        let mp = make_mp("a", vec![1, 2, 3]);
        let result = mp.into_partitions(0);
        assert!(result.is_err(), "0 partitions should error");
    }

    #[test]
    fn test_into_partitions_more_than_rows() {
        let mp = make_mp("a", vec![1, 2]);
        let parts = mp.into_partitions(5).unwrap();
        assert_eq!(parts.len(), 5);
        let total_rows: usize = parts.iter().map(|p| p.len()).sum();
        assert_eq!(total_rows, 2);
        let empty_count = parts.iter().filter(|p| p.len() == 0).count();
        assert_eq!(empty_count, 3, "3 of 5 partitions should be empty");
    }
}
