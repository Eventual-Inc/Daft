use std::ops::Rem;

use arrow2::array::{Array, DictionaryKey};
use rand::SeedableRng;

use crate::{
    array::BaseArray,
    datatypes::UInt64Array,
    dsl::Expr,
    error::{DaftError, DaftResult},
    table::Table,
};

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
        if targets.downcast().null_count() != 0 {
            return Err(DaftError::ComputeError(format!(
                "target array can not contain nulls, contains {} nulls",
                targets.downcast().null_count()
            )));
        }

        for (s_idx, t_idx) in targets.downcast().values_iter().enumerate() {
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
}
