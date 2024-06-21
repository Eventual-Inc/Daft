use std::{cmp, iter::repeat_with};

use arrow2::array::{MutableArray, MutablePrimitiveArray, PrimitiveArray};
use common_error::{DaftError, DaftResult};
use mur3::murmurhash3_x86_32;

use crate::{
    array::FixedSizeListArray,
    datatypes::{Field, Utf8Array},
    DataType, Series,
};

use super::{as_arrow::AsArrow, DaftMinHash};

const MERSENNE_PRIME: u64 = (1 << 61) - 1;
const MAX_HASH: u32 = 0xffffffff;
const DEFAULT_SEED: u32 = 1;

fn set_min_hashes(out: &mut [u32], s: &str, permutations: &[(u32, u32)], seed: u32) {
    let mut cur_hash = murmurhash3_x86_32(s.as_bytes(), seed);
    for (h, (a, b)) in out.iter_mut().zip(permutations) {
        // this has a very low chance to overflow but it's probably fine
        cur_hash = (((*a as u64) * (cur_hash as u64) + (*b as u64)) % MERSENNE_PRIME) as u32;
        *h = cmp::min(*h, cur_hash);
    }
}

impl DaftMinHash for Utf8Array {
    type Output = DaftResult<FixedSizeListArray>;

    fn minhash(&self, num_hashes: usize, ngram_size: usize, seed: Option<u32>) -> Self::Output {
        if num_hashes == 0 {
            return Err(DaftError::ValueError(
                "Number of hashes must be nonzero".into(),
            ));
        }
        if ngram_size == 0 {
            return Err(DaftError::ValueError("Ngram size must be nonzero".into()));
        }

        // generate permutations
        let seed = seed.unwrap_or(DEFAULT_SEED);
        let mut rng = fastrand::Rng::with_seed(seed as u64);
        let permutations: Vec<(u32, u32)> =
            repeat_with(|| (rng.u32(1..=u32::MAX), rng.u32(1..=u32::MAX)))
                .take(2 * num_hashes)
                .collect();

        let self_arrow = self.as_arrow();
        let mut output: MutablePrimitiveArray<u32> = MutablePrimitiveArray::new();
        for maybe_s in self_arrow.iter() {
            if let Some(s) = maybe_s {
                let spaces: Vec<usize> = s.match_indices(' ').map(|(i, _)| i + 1).collect();
                let mut min_hash: Vec<u32> = vec![MAX_HASH; num_hashes];
                if spaces.len() < ngram_size {
                    // hash whole string at once
                    set_min_hashes(&mut min_hash, s, &permutations, seed);
                } else {
                    let last_i = spaces.len() - ngram_size + 1;
                    for i in 0..(last_i + 1) {
                        // looking at the substring that starts BEFORE the current space
                        // surely no off by one errors
                        let start_ind = if i == 0 { 0 } else { spaces[i - 1] + 1 };
                        let end_ind = if i == last_i {
                            s.len()
                        } else {
                            spaces[i + ngram_size - 1]
                        };
                        set_min_hashes(&mut min_hash, &s[start_ind..end_ind], &permutations, seed);
                    }
                }
                output.extend_from_slice(&min_hash);
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
