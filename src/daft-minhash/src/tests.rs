use std::{collections::HashSet, iter::repeat_with};

use approx::assert_relative_eq;
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

const XX_HASH_SEED: u64 = 42;

#[test]
fn test_minhash() {
    // just some sanity checks
    let (perm_a_simd, perm_b_simd) = load_permutations(16, 16);

    let res1 = minhash(
        "the quick brown fox jumped over the lazy dog",
        (&perm_a_simd, &perm_b_simd),
        16,
        3,
        &Xxh64Builder::new(XX_HASH_SEED),
    )
    .unwrap();
    assert_eq!(res1.len(), 16);

    let res2 = minhash(
        "this sentence is totally different than that",
        (&perm_a_simd, &perm_b_simd),
        16,
        3,
        &Xxh64Builder::new(XX_HASH_SEED),
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
        &Xxh64Builder::new(XX_HASH_SEED),
    )
    .unwrap();
    for i in 0..16 {
        assert_eq!(res2[i], res3[i]);
    }
}

#[test]
fn test_jaccard_similarity_estimation() {
    let (perm_a_simd, perm_b_simd) = load_permutations(100, 32);

    let text1 = "data science is an interdisciplinary field";
    let text2 = "data analysis is an interdisciplinary science";

    let hash1 = minhash(
        text1,
        (&perm_a_simd, &perm_b_simd),
        32,
        3,
        &Xxh64Builder::new(XX_HASH_SEED),
    )
    .unwrap();
    let hash2 = minhash(
        text2,
        (&perm_a_simd, &perm_b_simd),
        32,
        3,
        &Xxh64Builder::new(XX_HASH_SEED),
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
    let mut rng = Rng::with_seed(200);
    let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(64);
    let perm_a_simd = load_simd(perm_a, 64);
    let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(64);
    let perm_b_simd = load_simd(perm_b, 64);

    let hasher = Xxh64Builder::new(42);

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

    let expected_probability = 0.578125;
    assert_relative_eq!(collision_probability, expected_probability);
}

#[test]
fn test_permutation_consistency() {
    // Ensure that using the same permutations and inputs yields consistent results
    let mut rng = Rng::with_seed(300);
    let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(24);
    let perm_a_simd = load_simd(perm_a, 24);
    let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(24);
    let perm_b_simd = load_simd(perm_b, 24);

    let text = "consistency test for permutation in minhash";

    let hasher = Xxh64Builder::new(XX_HASH_SEED);

    let hash_first = minhash(text, (&perm_a_simd, &perm_b_simd), 24, 3, &hasher).unwrap();
    let hash_second = minhash(text, (&perm_a_simd, &perm_b_simd), 24, 3, &hasher).unwrap();

    assert_eq!(
        hash_first, hash_second,
        "Hashes should be consistent across runs"
    );
}

#[test]
fn test_edge_cases() {
    const EMPTY_HASH_VALUE: u32 = 4294967295;

    let (perm_a_simd, perm_b_simd) = load_permutations(400, 16);

    let hasher = Xxh64Builder::new(XX_HASH_SEED);

    // Test with empty string
    let empty_text = "";
    let empty_hash = minhash(empty_text, (&perm_a_simd, &perm_b_simd), 16, 3, &hasher).unwrap();

    assert_eq!(empty_hash.len(), 16);
    for hash in empty_hash {
        assert_eq!(hash, EMPTY_HASH_VALUE);
    }
}

#[test]
fn test_large_scale_similarity() {
    // Placeholder: Implement a test that simulates a large-scale similarity search
    // This could involve generating a large number of strings and computing their MinHash signatures
    // Then, verify that similar strings have higher similarity scores

    let num_hashes = 128;
    let (perm_a_simd, perm_b_simd) = load_permutations(500, num_hashes);

    let hasher = Xxh64Builder::new(XX_HASH_SEED);

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

    let hasher = Xxh64Builder::new(XX_HASH_SEED);

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
        &Xxh64Builder::new(1),
    )
    .unwrap();
    let hash_seed2 = minhash(
        text,
        (&perm_a_simd, &perm_b_simd),
        num_hashes,
        3,
        &Xxh64Builder::new(2),
    )
    .unwrap();

    assert_ne!(
        hash_seed1, hash_seed2,
        "Different random states should produce different MinHash signatures"
    );
}

/// Calculate actual Jaccard similarity between two sets of n-grams
fn actual_jaccard_similarity(text1: &str, text2: &str, ngram_size: usize) -> f64 {
    let mut vec = VecDeque::new();
    let ngrams1: HashSet<_> = text1.windowed_words_in(ngram_size, &mut vec).collect();

    let mut vec = VecDeque::new();
    let ngrams2: HashSet<_> = text2.windowed_words_in(ngram_size, &mut vec).collect();

    let intersection = ngrams1.intersection(&ngrams2).count();
    let union = ngrams1.union(&ngrams2).count();

    intersection as f64 / union as f64
}

use proptest::prelude::*;
use xxhash_rust::xxh64::Xxh64Builder;
// Existing test imports remain...

#[test]
fn test_exact_vs_estimated_jaccard() {
    let mut rng = Rng::with_seed(42);
    let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(256);
    let perm_a_simd = load_simd(perm_a, 256);
    let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(256);
    let perm_b_simd = load_simd(perm_b, 256);

    let text_pairs = vec![
        // High similarity pair
        ("the quick brown fox jumps", "the quick brown fox leaps"),
        // Medium similarity pair
        ("the quick brown fox", "the slow brown dog"),
        // Low similarity pair
        ("completely different text", "another unrelated phrase"),
        // Zero similarity pair
        ("abc def ghi", "jkl mno pqr"),
    ];

    let hasher = Xxh64Builder::new(XX_HASH_SEED);

    for (text1, text2) in text_pairs {
        let hash1 = minhash(text1, (&perm_a_simd, &perm_b_simd), 256, 2, &hasher).unwrap();
        let hash2 = minhash(text2, (&perm_a_simd, &perm_b_simd), 256, 2, &hasher).unwrap();

        let estimated = hash1
            .iter()
            .zip(hash2.iter())
            .filter(|&(a, b)| a == b)
            .count() as f64
            / 256.0;

        let actual = actual_jaccard_similarity(text1, text2, 2);

        // The estimation should be within reasonable bounds
        assert!(
            (estimated - actual).abs() < 0.15,
            "Jaccard estimation too far off: estimated={}, actual={}, texts=({}, {})",
            estimated,
            actual,
            text1,
            text2
        );
    }
}

#[test]
fn test_unicode_handling() {
    let mut rng = Rng::with_seed(42);
    let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(32);
    let perm_a_simd = load_simd(perm_a, 32);
    let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(32);
    let perm_b_simd = load_simd(perm_b, 32);

    let unicode_texts = vec![
        "こんにちは世界", // Japanese
        "привет мир",     // Russian
        "مرحبا العالم",   // Arabic
        "🌟✨🌙💫⭐",     // Emojis
    ];

    let hasher = Xxh64Builder::new(XX_HASH_SEED);

    for text in unicode_texts {
        // Ensure it doesn't panic on Unicode
        let result = minhash(text, (&perm_a_simd, &perm_b_simd), 32, 2, &hasher);
        assert!(result.is_ok(), "Failed to process Unicode text: {}", text);

        // Test self-similarity
        let hash1 = result.unwrap();
        let hash2 = minhash(text, (&perm_a_simd, &perm_b_simd), 32, 2, &hasher).unwrap();
        assert_eq!(hash1, hash2, "Unicode text should have consistent hashes");
    }
}

proptest! {
    #[test]
    fn test_hash_stability(s1 in "\\PC*", s2 in "\\PC*") {
        let mut rng = Rng::with_seed(42);
        let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(32);
        let perm_a_simd = load_simd(perm_a, 32);
        let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(32);
        let perm_b_simd = load_simd(perm_b, 32);

        let hasher = Xxh64Builder::new(XX_HASH_SEED);

        // Property 1: Same input always produces same output
        let hash1 = minhash(&s1, (&perm_a_simd, &perm_b_simd), 32, 2, &hasher).unwrap();
        let hash2 = minhash(&s1, (&perm_a_simd, &perm_b_simd), 32, 2, &hasher).unwrap();
        prop_assert_eq!(hash1, hash2);

        // Property 2: Similarity is symmetric
        if !s1.is_empty() && !s2.is_empty() {
            let hash_a = minhash(&s1, (&perm_a_simd, &perm_b_simd), 32, 2, &hasher).unwrap();
            let hash_b = minhash(&s2, (&perm_a_simd, &perm_b_simd), 32, 2, &hasher).unwrap();

            let sim_ab = hash_a.iter().zip(hash_b.iter()).filter(|&(a, b)| a == b).count() as f64 / 32.0;
            let sim_ba = hash_b.iter().zip(hash_a.iter()).filter(|&(a, b)| a == b).count() as f64 / 32.0;

            prop_assert!((sim_ab - sim_ba).abs() < 1e-10);
        }
    }

    #[test]
    fn test_similarity_bounds(
        s1 in "\\PC{1,100}",
        s2 in "\\PC{1,100}"
    ) {
        let mut rng = Rng::with_seed(42);
        let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64))).take(32);
        let perm_a_simd = load_simd(perm_a, 32);
        let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64))).take(32);
        let perm_b_simd = load_simd(perm_b, 32);

        let hasher = Xxh64Builder::new(XX_HASH_SEED);

        let hash1 = minhash(&s1, (&perm_a_simd, &perm_b_simd), 32, 2, &hasher).unwrap();
        let hash2 = minhash(&s2, (&perm_a_simd, &perm_b_simd), 32, 2, &hasher).unwrap();

        let similarity = hash1.iter().zip(hash2.iter())
            .filter(|&(a, b)| a == b)
            .count() as f64 / 32.0;

        // Properties that should always hold
        prop_assert!((0.0..=1.0).contains(&similarity));

        // Self-similarity should be 1.0
        let self_sim = hash1.iter().zip(hash1.iter())
            .filter(|&(a, b)| a == b)
            .count() as f64 / 32.0;
        prop_assert!((self_sim - 1.0).abs() < 1e-10);
    }
}

fn generate_permutations(seed: u64, num_hashes: usize) -> (Vec<u64>, Vec<u64>) {
    let mut rng = Rng::with_seed(seed);
    let perm_a = repeat_with(|| rng.u64(1..(i32::MAX as u64)))
        .take(num_hashes)
        .collect::<Vec<_>>();
    let perm_b = repeat_with(|| rng.u64(0..(i32::MAX as u64)))
        .take(num_hashes)
        .collect::<Vec<_>>();
    (perm_a, perm_b)
}

fn load_permutations(seed: u64, num_hashes: usize) -> (Vec<SimdU64>, Vec<SimdU64>) {
    let (perm_a, perm_b) = generate_permutations(seed, num_hashes);
    let perm_a_simd = load_simd(perm_a, num_hashes);
    let perm_b_simd = load_simd(perm_b, num_hashes);
    (perm_a_simd, perm_b_simd)
}
