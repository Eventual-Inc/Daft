//! MinHash: A Probabilistic Algorithm for Efficient Set Similarity Estimation
//!
//! MinHash is a sophisticated probabilistic technique used to rapidly estimate the similarity between sets,
//! particularly advantageous for large-scale datasets where exhaustive comparisons are computationally prohibitive.
//!
//! # Application in This Crate
//!
//! In this crate, we utilize MinHash for comparing the similarity of text data at the word level.
//! Specifically, we apply MinHash to estimate the similarity between strings by breaking them down into
//! word n-grams. This approach allows us to efficiently compare large volumes of text data.
//!
//! Our implementation processes each string as a set of overlapping word n-grams, where the size of
//! the n-gram is configurable. This method captures local word patterns and order, making it
//! effective for tasks such as:
//!
//! - Identifying similar phrases or sentences in a large corpus
//! - Detecting near-duplicate entries in text databases
//!
//! # Fundamental Concept
//!
//! The core idea behind MinHash revolves around the principle that similar sets of n-grams (representing
//! similar text) are more likely to produce identical minimum hash values when subjected to the same hash
//! function. This allows for efficient similarity comparisons without the need for exhaustive set operations.
//!
//! # Operational Mechanism
//!
//! 1. N-gram Generation: Each string is broken down into overlapping word n-grams.
//! 2. Hash Function Application: Each n-gram is processed through a hash function.
//! 3. Minimum Hash Selection: The smallest hash value for each set of n-grams is retained.
//! 4. Cross-Set Comparison: These minimum hash values are compared across different strings.
//!
//! # Jaccard Similarity and MinHash
//!
//! The probability of minimum hash values being identical directly correlates with the Jaccard similarity
//! coefficient of the original sets of n-grams. For two sets A and B (representing two strings), the
//! Jaccard similarity is defined as:
//!
//! J(A,B) = |A ∩ B| / |A ∪ B|
//!
//! Where |A ∩ B| is the size of the intersection of A and B, and |A ∪ B| is the size of their union.
//! This coefficient ranges from 0 (completely dissimilar strings) to 1 (identical strings).
//!
//! # The Role of Permutations in MinHash
//!
//! Permutations are a crucial component of MinHash, enhancing its accuracy and robustness:
//!
//! 1. Multiple Hash Functions: MinHash uses permutations to simulate multiple hash functions from a single one:
//!    ```text
//!    h'(x) = (a * h(x) + b) mod p
//!    ```
//!    where h(x) is the original hash, a and b are carefully chosen constants, and p is a large prime number.
//!
//!    The primality of p is crucial for several fundamental reasons:
//!
//!    - Bijective Property: When p is prime, the function (a * x + b) mod p produces a full permutation
//!      of the input space for any non-zero 'a' and any 'b'. This bijective property ensures that:
//!      1. Every input maps to a unique output (injectivity).
//!      2. Every possible output is reached by some input (surjectivity).
//!
//!      This is essential for MinHash's accuracy and theoretical guarantees, as it preserves
//!      the relative distances between elements in the transformed space.
//!
//!    - Uniform Distribution: Prime moduli help distribute hash values more uniformly across the range.
//!      This is because prime numbers have no divisors other than 1 and themselves, reducing the chance
//!      of systematic patterns or clustering in the output.
//!
//!    - Collision Resistance: The primality of p contributes to better collision resistance.
//!      Non-prime moduli can introduce regularities that make collisions more likely for certain inputs.
//!
//!    - Preservation of Randomness: When generating multiple hash functions by varying 'a' and 'b',
//!      a prime modulus helps maintain the pseudo-random properties of the original hash function.
//!
//!    The choice of p is typically a large prime near the maximum value of common integer types
//!    (e.g., u32 or u64). This balances computational efficiency with a sufficiently large range
//!    for effective hashing. While exact powers of 2 (like 2^32 or 2^64) are convenient reference points,
//!    it's the primality of p, not its specific value, that ensures the mathematical properties
//!    required for MinHash to function correctly.
//!
//! 2. Uniform Distribution: These permutations ensure a uniform distribution of hash values across the
//!    entire hash space, reducing bias and improving the quality of similarity estimation.
//!
//! 3. Signature Generation: By applying multiple permutations, MinHash generates a "signature" for each set -
//!    a vector of minimum hash values. The length of this signature determines the accuracy of the similarity estimate.
//!
//! # Collision Probability and Distance
//!
//! The design of these permutations creates a fundamental property where the probability of collision
//! (i.e., two elements hashing to the same minimum value) is directly related to the similarity between the elements:
//!
//! - Similar elements (those with small Jaccard distance) have a higher probability of collision.
//! - Dissimilar elements (those with large Jaccard distance) have a lower probability of collision.
//!
//! Mathematically, for two sets A and B:
//!
//! P(collision) = 1 - d(A, B)
//!
//! Where d(A, B) is the Jaccard distance between A and B.
//!
//! # The MinHash Invariant
//!
//! The invariant P(collision) = 1 - D(A,B) holds in MinHash due to the fundamental properties of the algorithm:
//!
//! 1. Random Permutations: MinHash uses random permutations of the universal set of elements.
//! 2. Minimum Hash Selection: For each permutation, MinHash selects the minimum hash value for each set.
//! 3. Collision Probability: The probability of a collision is directly related to the similarity of the sets.
//!
//! This relationship holds for each permutation, and by using multiple permutations,
//! MinHash can estimate this probability (and thus the Jaccard similarity) with increasing accuracy.
//!
//! # Practical Applications
//!
//! MinHash finds extensive use in various domains:
//!
//! - Near-Duplicate Detection: Identifying similar documents or web pages in large corpora.
//! - Clustering: Grouping similar items in recommendation systems or data analysis.
//! - Large-Scale Similarity Search: Efficiently finding similar items in massive datasets.
//! - Data Deduplication: Identifying and removing duplicate or near-duplicate data entries.
//!
//! # Implementation Example
//!
//! The following example demonstrates the basic usage of MinHash:
//!
//! ```
//! use daft_minhash::{load_simd, minhash};
//!
//! // Generate permutation vectors (in practice, use random values for better distribution)
//! let perm_a = [1, 2, 3, 4];
//! let perm_b = [5, 6, 7, 8];
//! let perm_a_simd = load_simd(perm_a.into_iter(), 4);
//! let perm_b_simd = load_simd(perm_b.into_iter(), 4);
//!
//! let text1 = "the quick brown fox";
//! let text2 = "the lazy brown dog";
//!
//! // Generate MinHash signatures
//! let hash1 = minhash(text1, (&perm_a_simd, &perm_b_simd), 4, 2, 42).unwrap();
//! let hash2 = minhash(text2, (&perm_a_simd, &perm_b_simd), 4, 2, 42).unwrap();
//!
//! // Estimate similarity by comparing signatures
//! let similarity = hash1.iter().zip(hash2.iter()).filter(|&(a, b)| a == b).count() as f64 / 4.0;
//! println!("Estimated Jaccard similarity: {similarity}");
//! ```
//!
//! # Performance Optimization
//!
//! This implementation leverages SIMD (Single Instruction, Multiple Data) operations,
//! significantly enhancing performance on compatible hardware by processing multiple data points concurrently.
//!
//! # Theoretical Foundations
//!
//! The mathematical underpinning of MinHash is rooted in the probability theory of random variables
//! and the properties of hash functions. For a deeper understanding, refer to the seminal work by
//! Andrei Z. Broder (1997) on the topic of min-wise independent permutations.

use std::simd::{cmp::SimdOrd, Simd};

use common_error::DaftResult;
use mur3::murmurhash3_x86_32;

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
#[inline(always)]
fn simd_min_hash(
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

    for ((coeff_a, coeff_b), current_min_hash) in perm_a.zip(perm_b).zip(min_hashes) {
        // Apply permutations and update minimum hash values for each SIMD lane
        for _ in 0..SIMD_LANES {
            let permuted_hash = compute_fast_simd_remainder(rotated_hash * coeff_a + coeff_b);
            *current_min_hash = permuted_hash.simd_min(*current_min_hash);
            rotated_hash = rotated_hash.rotate_elements_left::<1>();
        }
    }
}

#[inline(always)]
fn simd_permute_and_min(
    hash: u64,
    perm_a: &[SimdU64],
    perm_b: &[SimdU64],
    min_hashes: &mut [SimdU64],
) {
    let hash_vector = SimdU64::splat(hash);
    for ((coeff_a, coeff_b), min_hash) in
        perm_a.iter().zip(perm_b.iter()).zip(min_hashes.iter_mut())
    {
        let permuted_hash = compute_fast_simd_remainder(hash_vector * coeff_a + coeff_b);
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

/// Computes the MinHash signature of a string using SIMD operations.
pub fn minhash(
    s: &str,
    perm_simd: (&[SimdU64], &[SimdU64]),
    num_hashes: usize,
    word_ngram_size: usize,
    seed: u32,
) -> DaftResult<Vec<u32>> {
    let (perm_a_simd, perm_b_simd) = perm_simd;
    let num_simd_vectors = num_hashes.div_ceil(SIMD_LANES);

    let mut min_hash_values: Vec<SimdU64> = vec![MAX_HASH_SIMD; num_simd_vectors];

    let windowed = windowed::WindowedWords::new(s, word_ngram_size);

    let hashes = windowed.map(|w| {
        let w_bytes = w.as_bytes();
        u64::from(murmurhash3_x86_32(w_bytes, seed))
    });

    let mut chunks = hashes.array_chunks::<SIMD_LANES>();

    for chunk in chunks.by_ref() {
        let chunk_simd = SimdU64::from_array(chunk);
        simd_min_hash(chunk_simd, perm_a_simd, perm_b_simd, &mut min_hash_values);
    }

    if let Some(remainder) = chunks.into_remainder() {
        for hash in remainder {
            simd_permute_and_min(hash, perm_a_simd, perm_b_simd, &mut min_hash_values);
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
    use std::iter::repeat_with;

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
        simd_min_hash(simd_h, &aa, &bb, &mut out);
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
            1,
        )
        .unwrap();
        assert_eq!(res1.len(), 16);

        let res2 = minhash(
            "this sentence is totally different than that",
            (&perm_a_simd, &perm_b_simd),
            16,
            3,
            1,
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
            1,
        )
        .unwrap();
        for i in 0..16 {
            assert_eq!(res2[i], res3[i]);
        }
    }
}
