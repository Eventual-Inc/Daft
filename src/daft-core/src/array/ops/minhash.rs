use std::{collections::VecDeque, hash::BuildHasher, iter::repeat_with};

use arrow2::array::{MutableArray, MutablePrimitiveArray, PrimitiveArray};
use common_error::{DaftError, DaftResult};
use daft_minhash::load_simd;

use super::{as_arrow::AsArrow, DaftMinHash};
use crate::{
    array::FixedSizeListArray,
    datatypes::{DataType, Field, Utf8Array},
    series::Series,
};

impl DaftMinHash for Utf8Array {
    type Output = DaftResult<FixedSizeListArray>;

    fn minhash(
        &self,
        num_hashes: usize,
        ngram_size: usize,
        seed: u32,
        hasher: &impl BuildHasher,
    ) -> Self::Output {
        if num_hashes == 0 {
            return Err(DaftError::ValueError(
                "Number of hashes must be nonzero".into(),
            ));
        }
        if ngram_size == 0 {
            return Err(DaftError::ValueError("Ngram size must be nonzero".into()));
        }

        // Generate coefficients for MinHash permutation function: (a * x + b) % p
        //
        // The MinHash algorithm uses a hash function of the form (a * x + b) % p,
        // where 'a' and 'b' are permutation coefficients, 'x' is the input hash,
        // and 'p' is typically a large prime number.
        //
        // 1. perm_a (coefficient 'a'):
        //    - Starts from 1 to ensure 'a' is never zero
        //    - A non-zero 'a' is crucial for maintaining the bijective property of the permutation
        //
        //    Example of how bijectivity fails if a = 0:
        //    Let p = 7 (prime number)
        //    If a = 0, b = 3, the function becomes: (0 * x + 3) % 7 = 3
        //    This always outputs 3, regardless of the input x, losing the bijective property
        //
        // 2. perm_b (coefficient 'b'):
        //    - Range: 0 to (i32::MAX as u64) - 1
        //    - Can start from 0 as 'b' can be any value without affecting the permutation property
        //
        // This approach ensures valid and uniformly distributed hash values, which is
        // essential for accurate set similarity estimation in MinHash.
        let mut rng = fastrand::Rng::with_seed(seed as u64);
        let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(num_hashes);
        let perm_a_simd = load_simd(perm_a, num_hashes);
        let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(num_hashes);
        let perm_b_simd = load_simd(perm_b, num_hashes);

        let internal_arrow_representation = self.as_arrow();
        let mut output: MutablePrimitiveArray<u32> =
            MutablePrimitiveArray::with_capacity(num_hashes * self.len());

        let mut alloc = VecDeque::new();

        for elem in internal_arrow_representation {
            let Some(elem) = elem else {
                for _ in 0..num_hashes {
                    output.push_null();
                }
                continue;
            };

            let minhash_res = daft_minhash::minhash_in(
                elem,
                (&perm_a_simd, &perm_b_simd),
                num_hashes,
                ngram_size,
                hasher,
                &mut alloc,
            )?;

            output.extend(minhash_res.into_iter().map(Some));
        }

        let immutable_output: PrimitiveArray<u32> = output.into();
        let output_series = Series::from_arrow(
            Field::new(self.name(), DataType::UInt32).into(),
            Box::new(immutable_output),
        )?;
        let field = Field::new(
            self.name(),
            DataType::FixedSizeList(Box::new(DataType::UInt32), num_hashes),
        );

        Ok(FixedSizeListArray::new(
            field,
            output_series,
            self.validity().cloned(),
        ))
    }
}
