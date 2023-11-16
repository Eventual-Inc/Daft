use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::Expr;
use daft_io::IOStatsContext;
use daft_table::Table;

use crate::micropartition::MicroPartition;

fn transpose2<T>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
    if v.is_empty() {
        return v;
    }
    let len = v[0].len();
    let mut iters: Vec<_> = v.into_iter().map(|n| n.into_iter()).collect();
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
    fn vec_part_tables_to_mps(
        &self,
        part_tables: Vec<Vec<Table>>,
    ) -> DaftResult<Vec<MicroPartition>> {
        let part_tables = transpose2(part_tables);
        Ok(part_tables
            .into_iter()
            .map(|v| {
                MicroPartition::new_loaded(
                    self.schema.clone(),
                    Arc::new(v),
                    self.statistics.clone(),
                )
            })
            .collect())
    }

    pub fn partition_by_hash(
        &self,
        exprs: &[Expr],
        num_partitions: usize,
    ) -> DaftResult<Vec<Self>> {
        let io_stats = IOStatsContext::new("MicroPartition::partition_by_hash");

        let tables = self.tables_or_read(io_stats)?;

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
        let io_stats = IOStatsContext::new("MicroPartition::partition_by_random");

        let tables = self.tables_or_read(io_stats)?;

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
        partition_keys: &[Expr],
        boundaries: &Table,
        descending: &[bool],
    ) -> DaftResult<Vec<Self>> {
        let io_stats = IOStatsContext::new("MicroPartition::partition_by_range");

        let tables = self.tables_or_read(io_stats)?;

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
}
