use std::{
    ops::{Add, BitAnd, Mul, Rem, Shr},
    simd::{cmp::SimdOrd, Simd},
};

use common_error::DaftResult;
use mur3::murmurhash3_x86_32;

// which SIMD to use
const SIMD_LANES: usize = 8;
type S = Simd<u64, SIMD_LANES>;

const MERSENNE_PRIME: u64 = (1 << 61) - 1;
// const MERSENNE_SIMD: S = S::from_array([MERSENNE_PRIME; SIMD_LANES]);
// const MERSENNE_EXP_SIMD: S = S::from_array([61; SIMD_LANES]);
const MAX_HASH: u64 = 0xffffffff;
const MAX_HASH_SIMD: S = S::from_array([MAX_HASH; SIMD_LANES]);

#[inline(always)]
fn fast_simd_rem(x: S) -> S {
    (x + x.shr(61)).bitand(MAX_HASH_SIMD)
}

#[inline(always)]
fn simd_min(hh: S, aa: &[S], bb: &[S], out: &mut [S]) {
    let mut h = hh;
    for ((a, b), o) in aa.iter().zip(bb.iter()).zip(out.iter_mut()) {
        for _ in 0..SIMD_LANES {
            *o = fast_simd_rem(h.mul(*a).add(*b)).simd_min(*o);
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
                    .bitand(MAX_HASH)
                    .min(*o);
            }
        }
    }
}

pub fn load_simd(v: &[u64]) -> Vec<S> {
    let num_hashes = v.len();
    let num_hashes_aligned = (num_hashes + SIMD_LANES - 1) / SIMD_LANES * SIMD_LANES;
    let num_simd = num_hashes_aligned / SIMD_LANES;

    let mut v_simd: Vec<S> = Vec::with_capacity(num_simd);
    for i in 0..num_simd {
        v_simd.push(S::load_or_default(&v[(SIMD_LANES * i)..]));
    }
    v_simd
}

pub fn minhash(
    s: &str,
    perm: (&[u64], &[u64]),
    perm_simd: (&[S], &[S]),
    ngram_size: usize,
    seed: u32,
) -> DaftResult<Vec<u32>> {
    let (perm_a, perm_b) = perm;
    let (perm_a_simd, perm_b_simd) = perm_simd;
    let num_hashes = perm_a.len();
    let num_simd = (num_hashes + SIMD_LANES - 1) / SIMD_LANES;

    let mut out: Vec<S> = vec![MAX_HASH_SIMD; num_simd];

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
                simd_min(hashes_simd, perm_a_simd, perm_b_simd, &mut out);
                hashes.clear();
            }
        }
    }

    let mut rem_out: Vec<u64> = out
        .iter()
        .flat_map(|x| x.as_array())
        .take(num_hashes)
        .copied()
        .collect();
    if !hashes.is_empty() {
        rem_min(&hashes, perm_a, perm_b, &mut rem_out);
    }
    Ok(rem_out.into_iter().map(|x| x as u32).collect())
}

// cargo bench --package daft-minhash
#[cfg(test)]
mod tests {
    use std::iter::repeat_with;

    use fastrand::Rng;

    use super::*;

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
    fn test_fast_rem() {
        // test on a bunch of random numbers
        // failure probability should be small
        let mut rng = Rng::with_seed(42);
        for _ in 0..2_000_000 {
            let v = rng.u64(0..=u64::MAX);
            let out = fast_simd_rem(S::splat(v)).to_array()[0];
            let exp = (v % MERSENNE_PRIME) & MAX_HASH;
            assert!(out == exp);
        }
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
        let mut rng = Rng::with_seed(42);
        let perm_a: Vec<u64> = repeat_with(|| rng.u64(1..(i32::MAX as u64)))
            .take(16)
            .collect();
        let perm_b: Vec<u64> = repeat_with(|| rng.u64(0..(i32::MAX as u64)))
            .take(16)
            .collect();

        let perm_a_simd = load_simd(&perm_a);
        let perm_b_simd = load_simd(&perm_b);

        let res1 = minhash(
            "the quick brown fox jumped over the lazy dog",
            (&perm_a, &perm_b),
            (&perm_a_simd, &perm_b_simd),
            3,
            1,
        )
        .unwrap();
        assert!(res1.len() == 16);

        let res2 = minhash(
            "this sentence is totally different than that",
            (&perm_a, &perm_b),
            (&perm_a_simd, &perm_b_simd),
            3,
            1,
        )
        .unwrap();
        assert!(res2.len() == 16);
        for i in 0..16 {
            assert!(res1[i] != res2[i]);
        }

        let res3 = minhash(
            "this sentence is totally different than that",
            (&perm_a, &perm_b),
            (&perm_a_simd, &perm_b_simd),
            3,
            1,
        )
        .unwrap();
        for i in 0..16 {
            assert!(res2[i] == res3[i]);
        }
    }
}
