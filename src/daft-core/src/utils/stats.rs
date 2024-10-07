use common_error::DaftResult;

use crate::{
    array::{
        ops::{DaftCountAggable, DaftSumAggable},
        prelude::Float64Array,
    },
    count_mode::CountMode,
};

pub struct Stats {
    pub sum: f64,
    pub count: u64,
    pub mean: Option<f64>,
}

pub fn stats(array: &Float64Array) -> DaftResult<Stats> {
    let sum = array.sum()?.get(0).unwrap();
    let count = array.count(CountMode::Valid)?.get(0).unwrap();
    let mean = match count {
        0 => None,
        _ => Some(sum / count as f64),
    };
    Ok(Stats { sum, count, mean })
}
