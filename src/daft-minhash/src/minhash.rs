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
//! let hasher = |s: &[u8], seed: u32| -> u32 { murmurhash3_x86_32(s, seed) };
//!
//! let hash1 = minhash(text1, (&perm_a_simd, &perm_b_simd), 4, 2, 42, hasher).unwrap();
//! let hash2 = minhash(text2, (&perm_a_simd, &perm_b_simd), 4, 2, 42, hasher).unwrap();
//!
//! let similarity = hash1.iter().zip(hash2.iter()).filter(|&(a, b)| a == b).count() as f64 / 4.0;
//! println!("Estimated Jaccard similarity: {similarity}");
//! ```
//!
//! # Performance
//!
//! This implementation uses SIMD operations for enhanced performance on compatible hardware.

use std::{
    hash::{BuildHasher, Hasher},
    simd::{cmp::SimdOrd, Simd},
};

use common_error::DaftResult;

use crate::minhash::windowed::WindowedWordsExt;

mod windowed;

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

// a real     1010
// b LLM      1001
// a XOR b    0011

/// Computes the MinHash signature of a string using SIMD operations.
pub fn minhash(
    s: &str,
    perm_simd: (&[SimdU64], &[SimdU64]),
    num_hashes: usize,
    word_ngram_size: usize,
    hasher: &impl BuildHasher,
) -> DaftResult<Vec<u32>> {
    let (perm_a_simd, perm_b_simd) = perm_simd;
    let num_simd_vectors = num_hashes.div_ceil(SIMD_LANES);

    let mut min_hash_values: Vec<SimdU64> = vec![MAX_HASH_SIMD; num_simd_vectors];

    let hashes = s.windowed_words(word_ngram_size).map(|w| {
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
mod tests {
    use std::{hash::BuildHasherDefault, iter::repeat_with};

    use fastrand::Rng;

    use super::*;

    const MERSENNE_PRIME: u64 = (1 << MERSENNE_EXP) - 1;

    #[test]
    fn test_fast_rem() {
        // test on a bunch of random numbers
        // failure probability should be small
        let mut rng = Rng::with_seed(42);
        for _ in 0..2_000_000 {
            let v = rng.u64(0..=u64::MAX);
            let out = compute_fast_simd_remainder(SimdU64::splat(v)).to_array()[0];
            let exp = (v % MERSENNE_PRIME) & MAX_HASH;
            assert_eq!(out, exp);
        }
    }

    #[test]
    fn test_simd_min() {
        let simd_h = SimdU64::splat(11);
        let simd_a = SimdU64::splat(22);
        let aa = vec![simd_a];
        let simd_b = SimdU64::splat(33);
        let bb = vec![simd_b];
        let simd_out = SimdU64::splat(123_456);
        let mut out = vec![simd_out];
        simd_permute_and_min_batch(simd_h, &aa, &bb, &mut out);
        let out_arr = out[0].as_array();
        assert_eq!(out_arr[0], 11 * 22 + 33);
    }

    #[test]
    fn test_minhash() {
        // just some sanity checks
        let mut rng = Rng::with_seed(42);
        let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(16);
        let perm_a_simd = load_simd(perm_a, 16);
        let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(16);
        let perm_b_simd = load_simd(perm_b, 16);

        let res1 = minhash(
            "the quick brown fox jumped over the lazy dog",
            (&perm_a_simd, &perm_b_simd),
            16,
            3,
            &BuildHasherDefault::<ahash::AHasher>::default(),
        )
        .unwrap();
        assert_eq!(res1.len(), 16);

        let res2 = minhash(
            "this sentence is totally different than that",
            (&perm_a_simd, &perm_b_simd),
            16,
            3,
            &BuildHasherDefault::<ahash::AHasher>::default(),
        )
        .unwrap();
        assert_eq!(res2.len(), 16);
        for i in 0..16 {
            assert_ne!(res1[i], res2[i]);
        }

        let res3 = minhash(
            "this sentence is totally different than that",
            (&perm_a_simd, &perm_b_simd),
            16,
            3,
            &BuildHasherDefault::<ahash::AHasher>::default(),
        )
        .unwrap();
        for i in 0..16 {
            assert_eq!(res2[i], res3[i]);
        }
    }

    #[test]
    fn test_jaccard_similarity_estimation() {
        // Placeholder: Replace expected similarity with actual value after verification
        let mut rng = Rng::with_seed(100);
        let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(32);
        let perm_a_simd = load_simd(perm_a, 32);
        let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(32);
        let perm_b_simd = load_simd(perm_b, 32);

        let text1 = "data science is an interdisciplinary field";
        let text2 = "data analysis is an interdisciplinary science";

        let hash1 = minhash(
            text1,
            (&perm_a_simd, &perm_b_simd),
            32,
            3,
            &BuildHasherDefault::<ahash::AHasher>::default(),
        )
        .unwrap();
        let hash2 = minhash(
            text2,
            (&perm_a_simd, &perm_b_simd),
            32,
            3,
            &BuildHasherDefault::<ahash::AHasher>::default(),
        )
        .unwrap();

        // Calculate estimated Jaccard similarity
        let estimated_similarity = hash1
            .iter()
            .zip(hash2.iter())
            .filter(|&(a, b)| a == b)
            .count() as f64
            / 32.0;

        // Placeholder assertion: Replace `EXPECTED_SIMILARITY` with the actual expected value
        let expected_similarity = 0.15625; // Placeholder value
        assert!(
            (estimated_similarity - expected_similarity).abs() < 0.1,
            "Estimated similarity {} differs from expected {}",
            estimated_similarity,
            expected_similarity
        );
    }

    #[test]
    fn test_collision_probability() {
        // Placeholder: Replace expected collision probability with actual value after verification
        let mut rng = Rng::with_seed(200);
        let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(64);
        let perm_a_simd = load_simd(perm_a, 64);
        let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(64);
        let perm_b_simd = load_simd(perm_b, 64);

        let hasher = BuildHasherDefault::<ahash::AHasher>::default();

        let text_a = "minhash collision probability test case one";
        let text_b = "minhash collision probability test case two";

        let hash_a = minhash(text_a, (&perm_a_simd, &perm_b_simd), 64, 3, &hasher).unwrap();
        let hash_b = minhash(text_b, (&perm_a_simd, &perm_b_simd), 64, 3, &hasher).unwrap();

        // Calculate collision probability
        let collision_count = hash_a
            .iter()
            .zip(hash_b.iter())
            .filter(|&(a, b)| a == b)
            .count() as f64;
        let collision_probability = collision_count / 64.0;

        let expected_probability = 0.5625; // Placeholder value
        assert!(
            (collision_probability - expected_probability).abs() < 0.1,
            "Collision probability {} differs from expected {}",
            collision_probability,
            expected_probability
        );
    }

    #[test]
    fn test_permutation_consistency() {
        // Ensure that using the same permutations and inputs yields consistent results
        let mut rng = Rng::with_seed(300);
        let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(24);
        let perm_a_simd = load_simd(perm_a, 24);
        let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(24);
        let perm_b_simd = load_simd(perm_b, 24);

        let hasher = BuildHasherDefault::<ahash::AHasher>::default();

        let text = "consistency test for permutation in minhash";

        let hash_first = minhash(text, (&perm_a_simd, &perm_b_simd), 24, 3, &hasher).unwrap();
        let hash_second = minhash(text, (&perm_a_simd, &perm_b_simd), 24, 3, &hasher).unwrap();

        assert_eq!(
            hash_first, hash_second,
            "Hashes should be consistent across runs"
        );
    }

    #[test]
    fn test_edge_cases() {
        let mut rng = Rng::with_seed(400);
        let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(16);
        let perm_a_simd = load_simd(perm_a, 16);
        let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(16);
        let perm_b_simd = load_simd(perm_b, 16);

        let hasher = BuildHasherDefault::<ahash::AHasher>::default();

        // Test with empty string
        let empty_text = "";
        let empty_hash = minhash(empty_text, (&perm_a_simd, &perm_b_simd), 16, 3, &hasher).unwrap();
        assert_eq!(empty_hash.len(), 16);
        // Placeholder: Replace with expected behavior, e.g., all hash values should remain MAX_HASH_SIMD
        // Example:
        // for hash in empty_hash {
        //     assert_eq!(hash, <EXPECTED_VALUE>);
        // }

        // Test with single word
        let single_word = "singleton";
        let single_hash =
            minhash(single_word, (&perm_a_simd, &perm_b_simd), 16, 3, &hasher).unwrap();
        assert_eq!(single_hash.len(), 16);
        // Placeholder: Replace with expected hash values
        // Example:
        // for hash in single_hash {
        //     assert_eq!(hash, <EXPECTED_VALUE>);
        // }

        // Test with very long string
        let long_text = "word ".repeat(10_000); // 10,000 repetitions of "word "
        let long_hash = minhash(&long_text, (&perm_a_simd, &perm_b_simd), 16, 3, &hasher).unwrap();
        assert_eq!(long_hash.len(), 16);
        // Placeholder: Replace with expected behavior
        // Example:
        // for hash in long_hash {
        //     assert_eq!(hash, <EXPECTED_VALUE>);
        // }

        // Test with high n-gram size
        let high_ngram_text = "short";
        let high_ngram_hash = minhash(
            high_ngram_text,
            (&perm_a_simd, &perm_b_simd),
            16,
            10,
            &hasher,
        )
        .unwrap();
        assert_eq!(high_ngram_hash.len(), 16);
        // Placeholder: Replace with expected behavior (likely fewer n-grams)
        // Example:
        // for hash in high_ngram_hash {
        //     assert_eq!(hash, <EXPECTED_VALUE>);
        // }
    }

    #[test]
    fn test_large_scale_similarity() {
        // Placeholder: Implement a test that simulates a large-scale similarity search
        // This could involve generating a large number of strings and computing their MinHash signatures
        // Then, verify that similar strings have higher similarity scores

        let mut rng = Rng::with_seed(500);
        let num_hashes = 128;
        let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(num_hashes);
        let perm_a_simd = load_simd(perm_a, num_hashes);
        let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(num_hashes);
        let perm_b_simd = load_simd(perm_b, num_hashes);

        let hasher = BuildHasherDefault::<ahash::AHasher>::default();

        // Generate a large number of similar and dissimilar strings
        let base_text = "the quick brown fox jumps over the lazy dog";
        let similar_text = "the quick brown fox leaps over the lazy dog";
        let dissimilar_text = "completely different content that shares no similarity";

        let hash_base = minhash(
            base_text,
            (&perm_a_simd, &perm_b_simd),
            num_hashes,
            3,
            &hasher,
        )
        .unwrap();
        let hash_similar = minhash(
            similar_text,
            (&perm_a_simd, &perm_b_simd),
            num_hashes,
            3,
            &hasher,
        )
        .unwrap();
        let hash_dissimilar = minhash(
            dissimilar_text,
            (&perm_a_simd, &perm_b_simd),
            num_hashes,
            3,
            &hasher,
        )
        .unwrap();

        // Calculate similarities
        let similarity_similar = hash_base
            .iter()
            .zip(hash_similar.iter())
            .filter(|&(a, b)| a == b)
            .count() as f64
            / num_hashes as f64;
        let similarity_dissimilar = hash_base
            .iter()
            .zip(hash_dissimilar.iter())
            .filter(|&(a, b)| a == b)
            .count() as f64
            / num_hashes as f64;

        assert!(
            similarity_similar > 0.30,
            "Expected higher similarity for similar texts, got {}",
            similarity_similar
        );
        assert!(
            similarity_dissimilar < 0.000001,
            "Expected lower similarity for dissimilar texts, got {}",
            similarity_dissimilar
        );
    }

    #[test]
    fn test_signature_length() {
        // Ensure that the MinHash signature length matches the number of hashes specified
        let mut rng = Rng::with_seed(600);
        let num_hashes = 256;
        let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(num_hashes);
        let perm_a_simd = load_simd(perm_a, num_hashes);
        let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(num_hashes);
        let perm_b_simd = load_simd(perm_b, num_hashes);

        let hasher = BuildHasherDefault::<ahash::AHasher>::default();

        let text = "verify that the minhash signature length is correct";

        let hash = minhash(text, (&perm_a_simd, &perm_b_simd), num_hashes, 3, &hasher).unwrap();
        assert_eq!(
            hash.len(),
            num_hashes,
            "MinHash signature length should be {}",
            num_hashes
        );
    }

    #[test]
    fn test_different_seeds_produce_different_hashes() {
        // Ensure that different seeds produce different MinHash signatures
        let mut rng = Rng::with_seed(700);
        let num_hashes = 64;
        let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(num_hashes);
        let perm_a_simd = load_simd(perm_a, num_hashes);
        let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(num_hashes);
        let perm_b_simd = load_simd(perm_b, num_hashes);

        let text = "different seed test for minhash signatures";

        let hash_seed1 = minhash(
            text,
            (&perm_a_simd, &perm_b_simd),
            num_hashes,
            3,
            &ahash::RandomState::with_seed(1),
        )
        .unwrap();
        let hash_seed2 = minhash(
            text,
            (&perm_a_simd, &perm_b_simd),
            num_hashes,
            3,
            &ahash::RandomState::with_seed(2),
        )
        .unwrap();

        assert_ne!(
            hash_seed1, hash_seed2,
            "Different random states should produce different MinHash signatures"
        );
    }
}
