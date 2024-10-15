#![feature(test)]
#![feature(portable_simd)]
#![feature(iter_next_chunk)]
#![feature(iter_array_chunks)]

mod minhash;
pub use minhash::{load_simd, minhash};
