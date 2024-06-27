#![feature(test)]

extern crate test;

use daft_minhash::minhash;
use std::{iter::repeat_with, ops::Range};
use test::Bencher;

const N_TOKENS: usize = 10000;
const N_CHARS: Range<usize> = 1..20;

const NUM_HASHES: usize = 128;
const NGRAM_SIZE: usize = 13;

#[bench]
fn bench_minhash(b: &mut Bencher) {
    let mut rng = fastrand::Rng::with_seed(42);
    let perm_a: Vec<u64> = repeat_with(|| rng.u64(1..(i32::MAX as u64)))
        .take(NUM_HASHES)
        .collect();
    let perm_b: Vec<u64> = repeat_with(|| rng.u64(0..(i32::MAX as u64)))
        .take(NUM_HASHES)
        .collect();
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
    b.iter(|| minhash(&s, &perm_a, &perm_b, NGRAM_SIZE, 1));
}
