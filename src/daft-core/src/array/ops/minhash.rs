use std::cmp;

use arrow2::array::{MutableArray, MutablePrimitiveArray, PrimitiveArray};
use common_error::DaftResult;
use mur3::murmurhash3_x86_32;

use crate::{
    array::FixedSizeListArray,
    datatypes::{Field, Utf8Array},
    DataType, Series,
};

use super::{as_arrow::AsArrow, DaftMinHash};

const MERSENNE_PRIME: u64 = (1 << 61) - 1;
const MAX_HASH: u32 = 0xffffffff;
fn set_min_hashes(
    out: &mut [u32],
    s: &str,
    num_hashes: usize,
    permutations: &[u32],
    hash_seed: u32,
) {
    let mut cur_hash = murmurhash3_x86_32(s.as_bytes(), hash_seed);
    for i in 0..num_hashes {
        cur_hash = (((permutations[2 * i] as u64) * (cur_hash as u64)
            + (permutations[2 * i + 1] as u64))
            % MERSENNE_PRIME) as u32;
        out[i] = cmp::min(out[i], cur_hash);
    }
}

impl DaftMinHash for Utf8Array {
    type Output = DaftResult<FixedSizeListArray>;

    fn minhash(
        &self,
        num_hashes: usize,
        ngram_size: usize,
        permutations: &[u32],
        hash_seed: Option<u32>,
    ) -> Self::Output {
        let hash_seed = hash_seed.unwrap_or(1);
        let self_arrow = self.as_arrow();
        let mut output: MutablePrimitiveArray<u32> = MutablePrimitiveArray::new();
        for maybe_s in self_arrow.iter() {
            if let Some(s) = maybe_s {
                let spaces: Vec<usize> = s.match_indices(' ').map(|(i, _)| i + 1).collect();
                let mut min_hash: Vec<u32> = vec![MAX_HASH; num_hashes];
                if spaces.len() < ngram_size {
                    // hash whole string at once
                    set_min_hashes(&mut min_hash, s, num_hashes, permutations, hash_seed);
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
                        set_min_hashes(
                            &mut min_hash,
                            &s[start_ind..end_ind],
                            num_hashes,
                            permutations,
                            hash_seed,
                        );
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
