use std::ops::Rem;

use arrow2::array::{Array, DictionaryKey};

use crate::{
    array::{BaseArray, from},
    datatypes::UInt64Array,
    dsl::Expr,
    error::{DaftError, DaftResult},
    table::Table, series::Series,
};

impl Table {
    unsafe fn partition_by_index(
        &self,
        targets: &UInt64Array,
        num_partitions: usize,
    ) -> DaftResult<Vec<Self>> {

        if num_partitions == 0 {
            return Err(DaftError::ValueError(format!(
                "can not partition into 0 partitions",
            )));
        }

        if self.len() != targets.len() {
            return Err(DaftError::ValueError(format!(
                "Mismatch of length of table and targets, {} vs {}",
                self.len(),
                targets.len()
            )));
        }
        // let mut output_to_input_idx =
        //     vec![Vec::with_capacity(self.len() / num_partitions); num_partitions];
        if targets.downcast().null_count() != 0 {
            return Err(DaftError::ComputeError(format!(
                "target array can not contain nulls, contains {} nulls",
                targets.downcast().null_count()
            )));
        }
        let mut count_per_part = vec![0u64; num_partitions + 1];
        for t_idx in targets.downcast().values_iter() {

            unsafe {*count_per_part.get_unchecked_mut(t_idx.as_usize() + 1) += 1};
            // if *t_idx >= (num_partitions as u64) {
            //     return Err(DaftError::ComputeError(format!("idx in target array is out of bounds, target idx {} at index {} out of {} partitions", t_idx, s_idx, num_partitions)));
            // }

            // unsafe {output_to_input_idx.get_unchecked_mut(t_idx.as_usize())}.push(s_idx as u64);
        }

        for i in 1..(num_partitions + 1) {
            unsafe {*count_per_part.get_unchecked_mut(i) += *count_per_part.get_unchecked_mut(i - 1)};
        }

        let mut indices = Vec::with_capacity(self.len());
        unsafe {indices.set_len(self.len())};

        for (s_idx, t_idx) in targets.downcast().values_iter().enumerate() {
            let offset = count_per_part.get_unchecked_mut(*t_idx as usize);
            *indices.get_unchecked_mut(*offset as usize) = s_idx as u64;
            *offset += 1;
            // if *t_idx >= (num_partitions as u64) {
            //     return Err(DaftError::ComputeError(format!("idx in target array is out of bounds, target idx {} at index {} out of {} partitions", t_idx, s_idx, num_partitions)));
            // }

            // unsafe {output_to_input_idx.get_unchecked_mut(t_idx.as_usize())}.push(s_idx as u64);
        }
        
        let indices = UInt64Array::from(("idx", Box::new(arrow2::array::PrimitiveArray::<u64>::from_vec(indices))));

        let mut curr_start = 0;

        (0..num_partitions).map (|i| {
            let end = count_per_part[i] as usize;
            let result = self.take(&indices.slice(curr_start, end - curr_start)?.into_series());
            curr_start = end;
            result
        }).collect()
        

        // output_to_input_idx
        //     .iter()
        //     .map(|v| {
        //         let indices = UInt64Array::from(("idx", v.as_slice()));
        //         self.take(&indices.into_series())
        //     })
        //     .collect::<DaftResult<Vec<_>>>()
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
        unsafe {self.partition_by_index(&targets, num_partitions)}
    }
}
