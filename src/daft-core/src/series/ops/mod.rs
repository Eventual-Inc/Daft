use common_error::DaftResult;

use super::Series;
use crate::utils::supertype::try_get_supertype;

pub mod abs;
pub mod agg;
pub mod arithmetic;
pub mod between;
pub mod broadcast;
pub mod cast;
pub mod cbrt;
pub mod ceil;
pub mod comparison;
pub mod concat;
pub mod downcast;
mod exp;
pub mod filter;
pub mod float;
pub mod floor;
pub mod groups;
pub mod hash;
pub mod if_else;
pub mod is_in;
pub mod len;
pub mod list;
pub mod log;
pub mod logical;
pub mod map;
pub mod minhash;
pub mod not;
pub mod null;
pub mod partitioning;
pub mod repeat;
pub mod round;
pub mod search_sorted;
pub mod shift;
pub mod sign;
pub mod sketch_percentile;
pub mod sort;
pub mod sqrt;
pub mod struct_;
pub mod take;
pub mod time;
mod trigonometry;
pub mod utf8;

pub fn cast_series_to_supertype(series: &[&Series]) -> DaftResult<Vec<Series>> {
    let supertype = series
        .iter()
        .map(|s| s.data_type().clone())
        .try_reduce(|l, r| try_get_supertype(&l, &r))?
        .unwrap();

    series.iter().map(|s| s.cast(&supertype)).collect()
}
