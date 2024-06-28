use std::iter::repeat_with;

use arrow2::array::{MutableArray, MutablePrimitiveArray, PrimitiveArray};
use common_error::{DaftError, DaftResult};
use daft_minhash::load_simd;

use crate::{
    array::FixedSizeListArray,
    datatypes::{Field, Utf8Array},
    DataType, Series,
};

use super::{as_arrow::AsArrow, DaftMinHash};

impl DaftMinHash for Utf8Array {
    type Output = DaftResult<FixedSizeListArray>;

    fn minhash(&self, num_hashes: usize, ngram_size: usize, seed: u32) -> Self::Output {
        if num_hashes == 0 {
            return Err(DaftError::ValueError(
                "Number of hashes must be nonzero".into(),
            ));
        }
        if ngram_size == 0 {
            return Err(DaftError::ValueError("Ngram size must be nonzero".into()));
        }

        // generate permutations
        let mut rng = fastrand::Rng::with_seed(seed as u64);
        let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(num_hashes);
        let perm_a_simd = load_simd(perm_a, num_hashes);
        let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(num_hashes);
        let perm_b_simd = load_simd(perm_b, num_hashes);

        let self_arrow = self.as_arrow();
        let mut output: MutablePrimitiveArray<u32> =
            MutablePrimitiveArray::with_capacity(num_hashes * self.len());
        for maybe_s in self_arrow.iter() {
            if let Some(s) = maybe_s {
                let minhash_res = daft_minhash::minhash(
                    s,
                    (&perm_a_simd, &perm_b_simd),
                    num_hashes,
                    ngram_size,
                    seed,
                )?;
                output.extend(minhash_res.into_iter().map(Some));
            } else {
                for _ in 0..num_hashes {
                    output.push_null();
                }
            }
        }
        let output_immut: PrimitiveArray<u32> = output.into();
        let output_series = Series::from_arrow(
            Field::new(self.name(), DataType::UInt32).into(),
            Box::new(output_immut),
        )?;
        Ok(FixedSizeListArray::new(
            Field::new(
                self.name(),
                DataType::FixedSizeList(Box::new(DataType::UInt32), num_hashes),
            ),
            output_series,
            self.validity().cloned(),
        ))
    }
}
