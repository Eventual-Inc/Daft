#![feature(test)]
#![feature(portable_simd)]
#![feature(iter_next_chunk)]

mod minhash;
pub use minhash::{load_simd, minhash};
