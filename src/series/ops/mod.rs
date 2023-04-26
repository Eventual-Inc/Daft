use crate::error::DaftResult;
use crate::utils::supertype::try_get_supertype;

use super::Series;

pub mod abs;
pub mod agg;
pub mod arithmetic;
pub mod broadcast;
pub mod cast;
pub mod comparison;
pub mod concat;
pub mod date;
pub mod downcast;
pub mod filter;
pub mod float;
pub mod full;
pub mod groups;
pub mod hash;
pub mod if_else;
pub mod len;
pub mod list;
pub mod not;
pub mod null;
pub mod pairwise;
pub mod search_sorted;
pub mod sort;
pub mod take;
pub mod utf8;

fn match_types_on_series(l: &Series, r: &Series) -> DaftResult<(Series, Series)> {
    let supertype = try_get_supertype(l.data_type(), r.data_type())?;

    let mut lhs = l.clone();

    let mut rhs = r.clone();

    if !lhs.data_type().eq(&supertype) {
        lhs = lhs.cast(&supertype)?;
    }
    if !rhs.data_type().eq(&supertype) {
        rhs = rhs.cast(&supertype)?;
    }

    Ok((lhs, rhs))
}
