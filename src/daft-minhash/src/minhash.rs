use std::{cmp, iter::repeat_with};

use common_error::{DaftError, DaftResult};
use mur3::murmurhash3_x86_32;

extern crate test;

const MERSENNE_PRIME: u64 = (1 << 61) - 1;
const MAX_HASH: u32 = 0xffffffff;
const DEFAULT_SEED: u32 = 1;

pub fn minhash(
    input: &[Option<&str>],
    num_hashes: usize,
    ngram_size: usize,
    seed: Option<u32>,
) -> DaftResult<Vec<Option<u32>>> {
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
    let permutations: (Vec<u32>, Vec<u32>) = (
        repeat_with(|| rng.u32(1..=(i32::MAX as u32)))
            .take(num_hashes)
            .collect(),
        repeat_with(|| rng.u32(0..=(i32::MAX as u32)))
            .take(num_hashes)
            .collect(),
    );

    let mut output: Vec<Option<u32>> = Vec::with_capacity(input.len() * num_hashes);
    for maybe_s in input.iter() {
        if let Some(s) = maybe_s {
            let spaces: Vec<usize> = s.match_indices(' ').map(|(i, _)| i).collect();
            let ngram_count = if spaces.len() < ngram_size {
                1
            } else {
                spaces.len() - ngram_size + 2
            };
            let mut hashes: Vec<u32> = Vec::with_capacity(ngram_count);
            let s_bytes = s.as_bytes();
            if spaces.len() < ngram_size {
                // hash whole string at once
                hashes.push(murmurhash3_x86_32(s_bytes, seed));
            } else {
                for i in 0..ngram_count {
                    // looking at the substring that starts BEFORE the current space
                    // surely no off by one errors
                    let start_ind = if i == 0 { 0 } else { spaces[i - 1] + 1 };
                    let end_ind = if i == ngram_count - 1 {
                        s.len()
                    } else {
                        spaces[i + ngram_size - 1]
                    };
                    hashes.push(murmurhash3_x86_32(&s_bytes[start_ind..end_ind], seed));
                }
            }
            // compute permutations
            for (a, b) in permutations.0.iter().zip(permutations.1.iter()) {
                let mut min_hash = MAX_HASH;
                for hash in hashes.iter_mut() {
                    *hash = (((*a as u64) * (*hash as u64) + (*b as u64)) % MERSENNE_PRIME) as u32;
                    min_hash = cmp::min(min_hash, *hash);
                }
                output.push(Some(min_hash));
            }
        } else {
            for _ in 0..num_hashes {
                output.push(None);
            }
        }
    }
    Ok(output)
}

// cargo bench --package daft-minhash
#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;
    use test::Bencher;

    const N_INPUTS: usize = 1000;
    const N_TOKENS: Range<usize> = 5..1000;
    const N_CHARS: Range<usize> = 1..20;

    const NUM_HASHES: usize = 128;
    const NGRAM_SIZE: usize = 13;

    #[bench]
    fn bench_minhash(b: &mut Bencher) {
        let mut rand = fastrand::Rng::new();
        let mut strs: Vec<String> = Vec::with_capacity(N_INPUTS);
        for _ in 0..N_INPUTS {
            let mut s: String = String::new();
            let s_tokens = rand.usize(N_TOKENS);
            for i in 0..s_tokens {
                if i > 0 {
                    s.push(' ');
                }
                let s_chars = rand.usize(N_CHARS);
                for _ in 0..s_chars {
                    s.push(rand.alphanumeric());
                }
            }
            strs.push(s);
        }

        let slices: Vec<Option<&str>> = strs.iter().map(|x| Some(&*x.as_str())).collect();
        b.iter(|| minhash(&slices, NUM_HASHES, NGRAM_SIZE, None));
    }
}
