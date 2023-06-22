use std::ops::Rem;

use arrow2::array::{Array, DictionaryKey};
use daft_dsl::Expr;
use rand::SeedableRng;

use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::UInt64Array, series::IntoSeries};

use daft_core::array::ops::as_arrow::AsArrow;

use crate::Table;

impl Table {
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
        if targets.as_arrow().null_count() != 0 {
            return Err(DaftError::ComputeError(format!(
                "target array can not contain nulls, contains {} nulls",
                targets.as_arrow().null_count()
            )));
        }

        for (s_idx, t_idx) in targets.as_arrow().values_iter().enumerate() {
            if *t_idx >= (num_partitions as u64) {
                return Err(DaftError::ComputeError(format!("idx in target array is out of bounds, target idx {} at index {} out of {} partitions", t_idx, s_idx, num_partitions)));
            }

            output_to_input_idx[unsafe { t_idx.as_usize() }].push(s_idx as u64);
        }
        output_to_input_idx
            .into_iter()
            .map(|v| {
                let indices = UInt64Array::from(("idx", v));
                self.take(&indices.into_series())
            })
            .collect::<DaftResult<Vec<_>>>()
    }

    pub fn partition_by_hash(
        &self,
        exprs: &[Expr],
        num_partitions: usize,
    ) -> DaftResult<Vec<Self>> {
        if num_partitions == 0 {
            return Err(DaftError::ValueError(
                "Can not partition a Table by 0 partitions".to_string(),
            ));
        }

        let targets = self
            .eval_expression_list(exprs)?
            .hash_rows()?
            .rem(&UInt64Array::from((
                "num_partitions",
                [num_partitions as u64].as_slice(),
            )))?;
        self.partition_by_index(&targets, num_partitions)
    }

    pub fn partition_by_random(&self, num_partitions: usize, seed: u64) -> DaftResult<Vec<Self>> {
        if num_partitions == 0 {
            return Err(DaftError::ValueError(
                "Can not partition a Table by 0 partitions".to_string(),
            ));
        }
        use rand::{distributions::Uniform, Rng};
        let range = Uniform::from(0..num_partitions as u64);

        let rng = rand::rngs::StdRng::seed_from_u64(seed);
        let values: Vec<u64> = rng.sample_iter(&range).take(self.len()).collect();
        let targets = UInt64Array::from(("idx", values));

        self.partition_by_index(&targets, num_partitions)
    }

    pub fn partition_by_range(
        &self,
        partition_keys: &[Expr],
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
}
