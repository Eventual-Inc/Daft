#![feature(portable_simd)]
#![feature(iter_next_chunk)]
#![feature(iter_array_chunks)]
#![feature(split_array)]
//! MinHash: Efficient Set Similarity Estimation
//!
//! MinHash is a probabilistic technique for rapidly estimating similarity between sets,
//! particularly useful for large-scale datasets.
//!
//! # Application
//!
//! This crate applies MinHash to estimate similarity between strings by breaking them
//! into word n-grams. It's effective for:
//!
//! - Identifying similar phrases or sentences in large corpora
//! - Detecting near-duplicate entries in text databases
//!
//! # Core Concept
//!
//! Similar sets of n-grams (representing similar text) are more likely to produce
//! identical minimum hash values when subjected to the same hash function.
//!
//! # Process
//!
//! 1. Generate n-grams from input strings
//! 2. Apply hash function to each n-gram
//! 3. Select minimum hash value for each set of n-grams
//! 4. Compare minimum hash values across different strings
//!
//! # Jaccard Similarity
//!
//! The probability of identical minimum hash values correlates with the Jaccard
//! similarity coefficient of the original sets:
//!
//! J(A,B) = |A ∩ B| / |A ∪ B|
//!
//! # Permutations in MinHash
//!
//! Permutations enhance accuracy and robustness:
//!
//! 1. Simulate multiple hash functions: h'(x) = (a * h(x) + b) mod p
//! 2. Ensure uniform distribution of hash values
//! 3. Generate signatures (vectors of minimum hash values)
//!
//! The prime modulus p is crucial for:
//! - Bijective property (preserving relative distances)
//! - Uniform distribution
//! - Collision resistance
//! - Preservation of randomness
//!
//! # Collision Probability
//!
//! P(collision) = 1 - d(A, B)
//!
//! Where d(A, B) is the Jaccard distance between sets A and B.
//!
//! # Applications
//!
//! - Near-duplicate detection
//! - Clustering
//! - Large-scale similarity search
//! - Data deduplication
//!
//! # Example Usage
//!
//! ```
//! use std::hash::BuildHasherDefault;
//! use mur3::murmurhash3_x86_32;
//! use daft_minhash::{load_simd, minhash};
//!
//! let perm_a = [1, 2, 3, 4];
//! let perm_b = [5, 6, 7, 8];
//! let perm_a_simd = load_simd(perm_a.into_iter(), 4);
//! let perm_b_simd = load_simd(perm_b.into_iter(), 4);
//!
//! let text1 = "the quick brown fox";
//! let text2 = "the lazy brown dog";
//!
//! let hasher = BuildHasherDefault::<ahash::AHasher>::default();
//!
//! let hash1 = minhash(text1, (&perm_a_simd, &perm_b_simd), 4, 2, &hasher).unwrap();
//! let hash2 = minhash(text2, (&perm_a_simd, &perm_b_simd), 4, 2, &hasher).unwrap();
//!
//! let similarity = hash1.iter().zip(hash2.iter()).filter(|&(a, b)| a == b).count() as f64 / 4.0;
//! println!("Estimated Jaccard similarity: {similarity}");
//! ```
//!
//! # Performance
//!
//! This implementation uses SIMD operations for enhanced performance on compatible hardware.

use std::{
    collections::VecDeque,
    hash::{BuildHasher, Hasher},
    simd::{Simd, cmp::SimdOrd},
};

use common_error::DaftResult;

use crate::windowed::WindowedWordsExt;

pub mod windowed;

// which SIMD to use
const SIMD_LANES: usize = 8;
type SimdU64 = Simd<u64, SIMD_LANES>;

const MERSENNE_EXP: u64 = 61;
const MAX_HASH: u64 = 0xffff_ffff;
const MAX_HASH_SIMD: SimdU64 = SimdU64::from_array([MAX_HASH; SIMD_LANES]);

/// Computes a fast SIMD-based remainder operation for MinHash with 2^61 - 1.
///
/// This function calculates an approximate remainder using bitwise operations,
/// which is significantly faster than a true modulo operation. It fails with a
/// probability of 2^-58 or less, which is acceptable for hashing purposes.
///
/// The remainder is computed with respect to the Mersenne prime 2^61 - 1, which
/// allows for efficient bitwise operations instead of expensive division.
///
/// # Returns
///
/// A SIMD vector of 64-bit unsigned integers containing the computed remainders.
#[inline(always)]
fn compute_fast_simd_remainder(simd_value: SimdU64) -> SimdU64 {
    (simd_value + (simd_value >> MERSENNE_EXP)) & MAX_HASH_SIMD
}

/// Computes MinHash signatures using SIMD operations.
///
/// The permutations "shuffle" the hash space, sampling different aspects of the input
/// data to create a robust signature. The permutation function used is of the form:
///
/// ```text
/// h'(x) = (a * x + b) % p
/// ```
///
/// Where:
/// - `h'(x)` is the permuted hash value
/// - `x` is the original hash value
/// - `a` and `b` are randomly chosen coefficients
/// - `p` is a large prime number (in this implementation, 2^61 - 1)
///
/// This linear congruential form ensures a uniform distribution of hash values
/// while maintaining the essential properties required for MinHash.
///
/// For more details on MinHash and its implementation, see [`crate::minhash`].
///
/// ```text
/// Initial Hash:
///   [H1]   [H2]  [H3]  [H4] [H5] [H6]  [H7] [H8]  (SIMD vector with 8 lanes)
///    |      |     |     |    |    |     |    |
///    v      v     v     v    v    v     v    v
/// +-----+-----+-----+-----+-----+-----+-----+-----+
/// | P1  | P2  | P3  | P4  | P5  | P6  | P7  | P8  |  Permutation Sets
/// +-----+-----+-----+-----+-----+-----+-----+-----+
///   |     |     |     |     |     |     |     |
///   v     v     v     v     v     v     v     v
/// +-----+-----+-----+-----+-----+-----+-----+-----+
/// | M1  | M2  | M3  | M4  | M5  | M6  | M7  | M8  |  Min Hash Values
/// +-----+-----+-----+-----+-----+-----+-----+-----+
///
///        Rotate Hash Values Left
/// [H8]   [H1]  [H2]  [H3]  [H4]  [H5]  [H6]  [H7]
///   |      |     |     |     |     |    |     |
///   v      v     v     v     v     v    v     v
/// +-----+-----+-----+-----+-----+-----+-----+-----+
/// | P1  | P2  | P3  | P4  | P5  | P6  | P7  | P8  |  Permutation Sets
/// +-----+-----+-----+-----+-----+-----+-----+-----+
///   |     |     |     |     |     |     |     |
///   v     v     v     v     v     v     v     v
/// +-----+-----+-----+-----+-----+-----+-----+-----+
/// | M1  | M2  | M3  | M4  | M5  | M6  | M7  | M8  |  Min Hash Values
/// +-----+-----+-----+-----+-----+-----+-----+-----+
///   ^     ^     ^     ^     ^     ^     ^     ^
///   |     |     |     |     |     |     |     |
///   |     |     |     |     |     |     |     |
///   +-----+-----+-----+-----+-----+-----+-----+
///    (Update with minimum of new and existing values)
///
///        Rotate Hash Values Left
///   [H7]  [H8]  [H1]  [H2]  [H3]  [H4] [H5]   [H6]
///     |    |     |     |     |     |    |      |
///     v    v     v     v     v     v    v      v
/// +-----+-----+-----+-----+-----+-----+-----+-----+
/// | P1  | P2  | P3  | P4  | P5  | P6  | P7  | P8  |  Permutation Sets
/// +-----+-----+-----+-----+-----+-----+-----+-----+
///            . . . (Process repeats)
///
/// Legend:
/// [Hx] : Hash value in SIMD lane x
/// Px   : Permutation set x, where h'(x) = (a * x + b) % p
/// Mx   : Running minimum hash value for permutation set x
/// ```
#[inline(always)]
fn simd_permute_and_min_batch(
    initial_hash: SimdU64,
    perm_a: &[SimdU64],
    perm_b: &[SimdU64],
    min_hashes: &mut [SimdU64],
) {
    let mut rotated_hash = initial_hash;

    debug_assert_eq!(
        perm_a.len(),
        perm_b.len(),
        "Permutation vectors must have the same length"
    );

    debug_assert_eq!(
        min_hashes.len(),
        perm_a.len(),
        "Minimum hash values must have the same length as the permutation vectors"
    );

    let perm_a = perm_a.iter();
    let perm_b = perm_b.iter();
    let min_hashes = min_hashes.iter_mut();

    for ((coefficient_a, coefficient_b), current_min_hash) in perm_a.zip(perm_b).zip(min_hashes) {
        // Apply permutations and update minimum hash values for each SIMD lane
        for _ in 0..SIMD_LANES {
            let permuted_hash =
                compute_fast_simd_remainder(rotated_hash * coefficient_a + coefficient_b);
            *current_min_hash = permuted_hash.simd_min(*current_min_hash);
            // Rotate the hash vector left by 1 element. This ensures that each SIMD lane
            // processes a different permutation of the initial hash in subsequent iterations,
            // effectively computing multiple hash permutations in parallel.
            rotated_hash = rotated_hash.rotate_elements_left::<1>();
        }
    }
}

#[inline(always)]
fn simd_permute_and_min_single(
    hash: u64,
    perm_a: &[SimdU64],
    perm_b: &[SimdU64],
    min_hashes: &mut [SimdU64],
) {
    let hash_vector = SimdU64::splat(hash);

    let perm_a = perm_a.iter();
    let perm_b = perm_b.iter();
    let min_hashes = min_hashes.iter_mut();

    for ((coefficient_a, coefficient_b), min_hash) in perm_a.zip(perm_b).zip(min_hashes) {
        let permuted_hash =
            compute_fast_simd_remainder(hash_vector * coefficient_a + coefficient_b);
        *min_hash = permuted_hash.simd_min(*min_hash);
    }
}

// Precalculate the SIMD vectors of the permutations, to save time.
// Output of this should be passed into the `perm_simd` argument of minhash.
pub fn load_simd(v: impl IntoIterator<Item = u64>, num_hashes: usize) -> Vec<SimdU64> {
    let mut v = v.into_iter();
    let num_simd = num_hashes.div_ceil(SIMD_LANES);

    let mut out = Vec::with_capacity(num_simd);
    loop {
        match v.next_chunk() {
            Ok(chunk) => {
                out.push(SimdU64::from_array(chunk));
            }
            Err(iter) => {
                let rem: Vec<u64> = iter.collect();
                if !rem.is_empty() {
                    out.push(SimdU64::load_or_default(&rem));
                }
                break;
            }
        }
    }
    out
}

pub fn minhash(
    s: &str,
    perm_simd: (&[SimdU64], &[SimdU64]),
    num_hashes: usize,
    word_ngram_size: usize,
    hasher: &impl BuildHasher,
) -> DaftResult<Vec<u32>> {
    let mut alloc = VecDeque::new();
    minhash_in(
        s,
        perm_simd,
        num_hashes,
        word_ngram_size,
        hasher,
        &mut alloc,
    )
}

/// Computes the MinHash signature of a string using SIMD operations.
pub fn minhash_in(
    s: &str,
    perm_simd: (&[SimdU64], &[SimdU64]),
    num_hashes: usize,
    word_ngram_size: usize,
    hasher: &impl BuildHasher,
    alloc: &mut VecDeque<isize>,
) -> DaftResult<Vec<u32>> {
    let (perm_a_simd, perm_b_simd) = perm_simd;
    let num_simd_vectors = num_hashes.div_ceil(SIMD_LANES);

    let mut min_hash_values: Vec<SimdU64> = vec![MAX_HASH_SIMD; num_simd_vectors];

    let hashes = s.windowed_words_in(word_ngram_size, alloc).map(|w| {
        let mut h = hasher.build_hasher();
        h.write(w.as_bytes());

        let (&le, _) = h.finish().to_le_bytes().split_array_ref::<4>();
        let result = u32::from_le_bytes(le);

        u64::from(result)
    });

    let mut chunks = hashes.array_chunks::<SIMD_LANES>();

    for chunk in chunks.by_ref() {
        let chunk_simd = SimdU64::from_array(chunk);
        simd_permute_and_min_batch(chunk_simd, perm_a_simd, perm_b_simd, &mut min_hash_values);
    }

    if let Some(remainder) = chunks.into_remainder() {
        for hash in remainder {
            simd_permute_and_min_single(hash, perm_a_simd, perm_b_simd, &mut min_hash_values);
        }
    }

    // Convert SIMD results to a flat vector of u32 values
    let minhash_signature: Vec<u32> = min_hash_values
        .iter()
        .flat_map(Simd::as_array)
        .take(num_hashes)
        .map(|&x| x as u32)
        .collect();

    Ok(minhash_signature)
}

// cargo bench --package daft-minhash
#[cfg(test)]
mod tests;
