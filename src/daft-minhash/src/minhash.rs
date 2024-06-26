use std::cmp;

use common_error::DaftResult;
use mur3::murmurhash3_x86_32;

extern crate test;

const MERSENNE_PRIME: u64 = (1 << 61) - 1;
const MAX_HASH: u32 = 0xffffffff;

fn calc_perms(hashes: &[u32], pa: &[u32], pb: &[u32], out: &mut [u32]) {
    for h in hashes {
        for ((a, b), o) in pa.iter().zip(pb.iter()).zip(out.iter_mut()) {
            *o = cmp::min(
                *o,
                (((*a as u64) * (*h as u64) + (*b as u64)) % MERSENNE_PRIME) as u32,
            );
        }
    }
}

pub fn minhash(
    s: &str,
    permutations: &(Vec<u32>, Vec<u32>),
    ngram_size: usize,
    seed: u32,
) -> DaftResult<Vec<u32>> {
    let num_hashes = permutations.0.len();

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

    let mut output: Vec<u32> = vec![MAX_HASH; num_hashes];
    calc_perms(&hashes, &permutations.0, &permutations.1, &mut output);
    Ok(output)
}

// cargo bench --package daft-minhash
#[cfg(test)]
mod tests {
    use super::*;
    use std::{iter::repeat_with, ops::Range};
    use test::Bencher;

    const N_TOKENS: usize = 10000;
    const N_CHARS: Range<usize> = 1..20;

    const NUM_HASHES: usize = 128;
    const NGRAM_SIZE: usize = 13;

    #[bench]
    fn bench_minhash(b: &mut Bencher) {
        let mut rng = fastrand::Rng::with_seed(42);
        let mut s: String = String::new();
        for i in 0..N_TOKENS {
            if i > 0 {
                s.push(' ');
            }
            let s_chars = rng.usize(N_CHARS);
            for _ in 0..s_chars {
                s.push(rng.alphanumeric());
            }
        }

        let permutations: (Vec<u32>, Vec<u32>) = (
            repeat_with(|| rng.u32(1..=(i32::MAX as u32)))
                .take(NUM_HASHES)
                .collect(),
            repeat_with(|| rng.u32(0..=(i32::MAX as u32)))
                .take(NUM_HASHES)
                .collect(),
        );
        b.iter(|| minhash(&s, &permutations, NGRAM_SIZE, 1));
    }
}
