use std::{
    ops::{Add, BitAnd, Mul, Rem},
    simd::{cmp::SimdOrd, Simd},
};

use common_error::DaftResult;
use mur3::murmurhash3_x86_32;

extern crate test;

const MERSENNE_PRIME: u64 = (1 << 61) - 1;
const MAX_HASH: u32 = 0xffffffff;

// which SIMD to use
const SIMD_LANES: usize = 8;
type S = Simd<u64, SIMD_LANES>;

#[inline(always)]
fn simd_min(hh: S, aa: &[S], bb: &[S], out: &mut [S]) {
    let mut h = hh;
    let mersenne = Simd::splat(MERSENNE_PRIME);
    let maxhash = Simd::splat(MAX_HASH as u64);
    for ((a, b), o) in aa.iter().zip(bb.iter()).zip(out.iter_mut()) {
        for _ in 0..SIMD_LANES {
            *o = h.mul(*a).add(*b).rem(mersenne).bitand(maxhash).simd_min(*o);
            h = h.rotate_elements_left::<1>();
        }
    }
}

#[inline(always)]
fn rem_min(hh: &[u64], aa: &[u64], bb: &[u64], out: &mut [u64]) {
    for h in hh {
        for ((a, b), o) in aa.iter().zip(bb.iter()).zip(out.iter_mut()) {
            for _ in 0..SIMD_LANES {
                *o = h
                    .mul(*a)
                    .add(*b)
                    .rem(MERSENNE_PRIME)
                    .bitand(MAX_HASH as u64)
                    .min(*o);
            }
        }
    }
}

pub fn minhash(s: &str, num_hashes: usize, ngram_size: usize, seed: u32) -> DaftResult<Vec<u32>> {
    let num_hashes_aligned = (num_hashes + SIMD_LANES - 1) / SIMD_LANES * SIMD_LANES;
    let num_simd = num_hashes_aligned / SIMD_LANES;

    // generate simd permutations
    let mut rng = fastrand::Rng::with_seed(seed as u64);
    let mut perm_a: Vec<S> = Vec::with_capacity(num_simd);
    let mut perm_b: Vec<S> = Vec::with_capacity(num_simd);
    let mut cur_simd = [0; SIMD_LANES];
    for _ in 0..num_simd {
        for v in &mut cur_simd {
            *v = rng.u64(1..=(i32::MAX as u64));
        }
        perm_a.push(S::from_array(cur_simd));
    }
    for _ in 0..num_simd {
        for v in &mut cur_simd {
            *v = rng.u64(0..=(i32::MAX as u64));
        }
        perm_b.push(S::from_array(cur_simd));
    }

    let mut out: Vec<S> = vec![S::splat(MAX_HASH as u64); num_simd];

    let spaces: Vec<usize> = s.match_indices(' ').map(|(i, _)| i).collect();
    let ngram_count = if spaces.len() < ngram_size {
        1
    } else {
        spaces.len() - ngram_size + 2
    };
    let mut hashes: Vec<u64> = Vec::with_capacity(SIMD_LANES);
    let s_bytes = s.as_bytes();
    if spaces.len() < ngram_size {
        // hash whole string at once
        hashes.push(murmurhash3_x86_32(s_bytes, seed) as u64);
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
            hashes.push(murmurhash3_x86_32(&s_bytes[start_ind..end_ind], seed) as u64);
            if hashes.len() >= SIMD_LANES {
                let hashes_simd = S::from_slice(&hashes);
                simd_min(hashes_simd, &perm_a, &perm_b, &mut out);
                hashes.clear();
            }
        }
    }

    let rem_a: Vec<u64> = perm_a
        .iter()
        .flat_map(|x| x.as_array())
        .take(num_hashes)
        .copied()
        .collect();
    let rem_b: Vec<u64> = perm_b
        .iter()
        .flat_map(|x| x.as_array())
        .take(num_hashes)
        .copied()
        .collect();
    let mut rem_out: Vec<u64> = out
        .iter()
        .flat_map(|x| x.as_array())
        .take(num_hashes)
        .copied()
        .collect();
    if !hashes.is_empty() {
        rem_min(&hashes, &rem_a, &rem_b, &mut rem_out);
    }
    Ok(rem_out.into_iter().map(|x| x as u32).collect())
}

// cargo bench --package daft-minhash
#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;
    use test::Bencher;

    const N_TOKENS: usize = 10000;
    const N_CHARS: Range<usize> = 1..20;

    const NUM_HASHES: usize = 128;
    const NGRAM_SIZE: usize = 13;

    #[test]
    fn test_rem_min() {
        // basic and not comprehensive test
        let hh = vec![11];
        let aa = vec![22];
        let bb = vec![33];
        let mut out = vec![123456];
        rem_min(&hh, &aa, &bb, &mut out);
        assert!(out[0] == 11 * 22 + 33);
    }

    #[test]
    fn test_simd_min() {
        let simd_h = S::splat(11);
        let simd_a = S::splat(22);
        let aa = vec![simd_a];
        let simd_b = S::splat(33);
        let bb = vec![simd_b];
        let simd_out = S::splat(123456);
        let mut out = vec![simd_out];
        simd_min(simd_h, &aa, &bb, &mut out);
        let out_arr = out[0].as_array();
        assert!(out_arr[0] == 11 * 22 + 33);
    }

    #[test]
    fn test_minhash() {
        // just some sanity checks
        let res1 = minhash("the quick brown fox jumped over the lazy dog", 16, 3, 1).unwrap();
        assert!(res1.len() == 16);

        let res2 = minhash("this sentence is totally different than that", 16, 3, 1).unwrap();
        assert!(res2.len() == 16);
        for i in 0..16 {
            assert!(res1[i] != res2[i]);
        }

        let res3 = minhash("this sentence is totally different than that", 16, 3, 1).unwrap();
        for i in 0..16 {
            assert!(res2[i] == res3[i]);
        }
    }

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
        b.iter(|| minhash(&s, NUM_HASHES, NGRAM_SIZE, 1));
    }
}
