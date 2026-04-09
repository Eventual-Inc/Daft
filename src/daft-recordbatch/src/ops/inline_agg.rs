use std::{
    collections::hash_map::Entry::{Occupied, Vacant},
    hash::{BuildHasherDefault, Hash},
};

use common_error::DaftResult;
use daft_core::{
    array::ops::arrow::comparison::build_multi_array_is_equal,
    count_mode::CountMode,
    datatypes::*,
    series::{IntoSeries, Series},
    utils::identity_hash_set::{IdentityBuildHasher, IndexHash},
};
use daft_dsl::{
    AggExpr,
    expr::bound_expr::{BoundAggExpr, BoundExpr},
};
use fnv::FnvHashMap;
use hashbrown::{HashMap, hash_map::RawEntryMut};

use crate::RecordBatch;

/// Result from the grouping phase: group key indices, per-row group IDs, and per-group sizes.
struct GroupingResult {
    groupkey_indices: Vec<u64>,
    group_ids: Vec<u32>,
    group_sizes: Vec<u64>,
}

// ---------------------------------------------------------------------------
// Accumulator structs
// ---------------------------------------------------------------------------

struct CountAccum {
    counts: Vec<u64>,
    mode: CountMode,
    nulls: Option<arrow::buffer::NullBuffer>,
    is_null_type: bool,
}

impl CountAccum {
    fn new(source: &Series, mode: CountMode) -> Self {
        Self {
            counts: Vec::new(),
            mode,
            nulls: source.nulls().cloned(),
            is_null_type: source.data_type() == &DataType::Null,
        }
    }

    /// Use pre-computed group sizes when no per-row null checking is needed.
    /// Returns true if the optimization was applied, false if caller must use update_batch.
    fn try_use_group_sizes(&mut self, group_sizes: &[u64]) -> bool {
        if self.is_null_type {
            match self.mode {
                CountMode::All | CountMode::Null => {
                    self.counts = group_sizes.to_vec();
                    return true;
                }
                CountMode::Valid => {
                    // Null type + Valid mode = always 0
                    // counts already zeroed from init_groups
                    return true;
                }
            }
        }
        match self.mode {
            CountMode::All => {
                self.counts = group_sizes.to_vec();
                true
            }
            CountMode::Valid if self.nulls.is_none() => {
                // No nulls → every row is valid → count = group size
                self.counts = group_sizes.to_vec();
                true
            }
            CountMode::Null if self.nulls.is_none() => {
                // No nulls → null count is 0 for all groups
                // counts already zeroed from init_groups
                true
            }
            _ => false, // Has nulls — need per-row scatter loop
        }
    }

    /// Vectorized batch update: process all rows given a pre-computed group_ids array.
    fn update_batch(&mut self, group_ids: &[u32]) {
        let counts = &mut self.counts;
        match self.mode {
            CountMode::Valid => {
                if let Some(ref nulls) = self.nulls {
                    for (row_idx, &gid) in group_ids.iter().enumerate() {
                        counts[gid as usize] += nulls.is_valid(row_idx) as u64;
                    }
                }
                // else case handled by try_use_group_sizes
            }
            CountMode::Null => {
                if let Some(ref nulls) = self.nulls {
                    for (row_idx, &gid) in group_ids.iter().enumerate() {
                        counts[gid as usize] += !nulls.is_valid(row_idx) as u64;
                    }
                }
                // else case handled by try_use_group_sizes
            }
            CountMode::All => {
                // Should have been handled by try_use_group_sizes
                for &gid in group_ids {
                    counts[gid as usize] += 1;
                }
            }
        }
    }

    fn finalize(self, name: &str) -> DaftResult<Series> {
        Ok(DataArray::<UInt64Type>::from_vec(name, self.counts).into_series())
    }
}

macro_rules! define_sum_accum {
    ($name:ident, $daft_type:ty, $native:ty) => {
        struct $name {
            accumulators: Vec<Option<$native>>,
            source: DataArray<$daft_type>,
        }

        impl $name {
            fn new(source: DataArray<$daft_type>) -> Self {
                Self {
                    accumulators: Vec::new(),
                    source,
                }
            }

            /// Vectorized batch update over pre-computed group_ids.
            fn update_batch(&mut self, group_ids: &[u32]) {
                let accs = &mut self.accumulators;
                if self.source.null_count() == 0 {
                    // Tight loop: no null checks needed on source values.
                    for (&gid, &val) in group_ids.iter().zip(self.source.values().iter()) {
                        let acc = &mut accs[gid as usize];
                        *acc = Some(match *acc {
                            Some(a) => a + val,
                            None => val,
                        });
                    }
                } else {
                    // Source has nulls: check each value.
                    for (row_idx, &gid) in group_ids.iter().enumerate() {
                        if let Some(val) = self.source.get(row_idx) {
                            let acc = &mut accs[gid as usize];
                            *acc = Some(match *acc {
                                Some(a) => a + val,
                                None => val,
                            });
                        }
                    }
                }
            }

            fn finalize(self, name: &str) -> DaftResult<Series> {
                let has_nulls = self.accumulators.iter().any(|a| a.is_none());
                if has_nulls {
                    Ok(DataArray::<$daft_type>::from_iter(
                        self.source.field.clone(),
                        self.accumulators.into_iter(),
                    )
                    .rename(name)
                    .into_series())
                } else {
                    Ok(DataArray::<$daft_type>::from_field_and_values(
                        self.source.field.clone(),
                        self.accumulators.into_iter().map(|opt| opt.unwrap()),
                    )
                    .rename(name)
                    .into_series())
                }
            }
        }
    };
}

define_sum_accum!(SumAccumI64, Int64Type, i64);
define_sum_accum!(SumAccumU64, UInt64Type, u64);
define_sum_accum!(SumAccumF32, Float32Type, f32);
define_sum_accum!(SumAccumF64, Float64Type, f64);

macro_rules! define_minmax_accum {
    ($name:ident, $daft_type:ty, $native:ty, $cmp_fn:expr) => {
        struct $name {
            accumulators: Vec<Option<$native>>,
            source: DataArray<$daft_type>,
        }

        impl $name {
            fn new(source: DataArray<$daft_type>) -> Self {
                Self {
                    accumulators: Vec::new(),
                    source,
                }
            }

            fn update_batch(&mut self, group_ids: &[u32]) {
                let accs = &mut self.accumulators;
                let cmp_fn: fn($native, $native) -> $native = $cmp_fn;
                if self.source.null_count() == 0 {
                    for (&gid, &val) in group_ids.iter().zip(self.source.values().iter()) {
                        let acc = &mut accs[gid as usize];
                        *acc = Some(match *acc {
                            Some(a) => cmp_fn(a, val),
                            None => val,
                        });
                    }
                } else {
                    for (row_idx, &gid) in group_ids.iter().enumerate() {
                        if let Some(val) = self.source.get(row_idx) {
                            let acc = &mut accs[gid as usize];
                            *acc = Some(match *acc {
                                Some(a) => cmp_fn(a, val),
                                None => val,
                            });
                        }
                    }
                }
            }

            fn finalize(self, name: &str) -> DaftResult<Series> {
                let has_nulls = self.accumulators.iter().any(|a| a.is_none());
                if has_nulls {
                    Ok(DataArray::<$daft_type>::from_iter(
                        self.source.field.clone(),
                        self.accumulators.into_iter(),
                    )
                    .rename(name)
                    .into_series())
                } else {
                    Ok(DataArray::<$daft_type>::from_field_and_values(
                        self.source.field.clone(),
                        self.accumulators.into_iter().map(|opt| opt.unwrap()),
                    )
                    .rename(name)
                    .into_series())
                }
            }
        }
    };
}

define_minmax_accum!(MinAccumI8, Int8Type, i8, std::cmp::min);
define_minmax_accum!(MinAccumI16, Int16Type, i16, std::cmp::min);
define_minmax_accum!(MinAccumI32, Int32Type, i32, std::cmp::min);
define_minmax_accum!(MinAccumI64, Int64Type, i64, std::cmp::min);
define_minmax_accum!(MinAccumU8, UInt8Type, u8, std::cmp::min);
define_minmax_accum!(MinAccumU16, UInt16Type, u16, std::cmp::min);
define_minmax_accum!(MinAccumU32, UInt32Type, u32, std::cmp::min);
define_minmax_accum!(MinAccumU64, UInt64Type, u64, std::cmp::min);
define_minmax_accum!(MinAccumF32, Float32Type, f32, |a, b| if a.lt(&b) {
    a
} else {
    b
});
define_minmax_accum!(MinAccumF64, Float64Type, f64, |a, b| if a.lt(&b) {
    a
} else {
    b
});
define_minmax_accum!(MaxAccumI8, Int8Type, i8, std::cmp::max);
define_minmax_accum!(MaxAccumI16, Int16Type, i16, std::cmp::max);
define_minmax_accum!(MaxAccumI32, Int32Type, i32, std::cmp::max);
define_minmax_accum!(MaxAccumI64, Int64Type, i64, std::cmp::max);
define_minmax_accum!(MaxAccumU8, UInt8Type, u8, std::cmp::max);
define_minmax_accum!(MaxAccumU16, UInt16Type, u16, std::cmp::max);
define_minmax_accum!(MaxAccumU32, UInt32Type, u32, std::cmp::max);
define_minmax_accum!(MaxAccumU64, UInt64Type, u64, std::cmp::max);
define_minmax_accum!(MaxAccumF32, Float32Type, f32, |a, b| if a.gt(&b) {
    a
} else {
    b
});
define_minmax_accum!(MaxAccumF64, Float64Type, f64, |a, b| if a.gt(&b) {
    a
} else {
    b
});

// ---------------------------------------------------------------------------
// AggAccumulator enum — eliminates vtable dispatch in the hot loop
// ---------------------------------------------------------------------------

// Sum widens small integers (e.g. Int8 → Int64) so only 4 variants are needed.
// Min/Max preserve the original dtype, so each numeric type needs its own variant.
enum AggAccumulator {
    Count(CountAccum),
    SumI64(SumAccumI64),
    SumU64(SumAccumU64),
    SumF32(SumAccumF32),
    SumF64(SumAccumF64),
    MinI8(MinAccumI8),
    MinI16(MinAccumI16),
    MinI32(MinAccumI32),
    MinI64(MinAccumI64),
    MinU8(MinAccumU8),
    MinU16(MinAccumU16),
    MinU32(MinAccumU32),
    MinU64(MinAccumU64),
    MinF32(MinAccumF32),
    MinF64(MinAccumF64),
    MaxI8(MaxAccumI8),
    MaxI16(MaxAccumI16),
    MaxI32(MaxAccumI32),
    MaxI64(MaxAccumI64),
    MaxU8(MaxAccumU8),
    MaxU16(MaxAccumU16),
    MaxU32(MaxAccumU32),
    MaxU64(MaxAccumU64),
    MaxF32(MaxAccumF32),
    MaxF64(MaxAccumF64),
}

impl AggAccumulator {
    /// Pre-initialize storage for `n` groups.
    fn init_groups(&mut self, n: u32) {
        let n = n as usize;
        match self {
            Self::Count(s) => s.counts.resize(n, 0),
            Self::SumI64(s) => s.accumulators.resize(n, None),
            Self::SumU64(s) => s.accumulators.resize(n, None),
            Self::SumF32(s) => s.accumulators.resize(n, None),
            Self::SumF64(s) => s.accumulators.resize(n, None),
            Self::MinI8(s) => s.accumulators.resize(n, None),
            Self::MinI16(s) => s.accumulators.resize(n, None),
            Self::MinI32(s) => s.accumulators.resize(n, None),
            Self::MinI64(s) => s.accumulators.resize(n, None),
            Self::MinU8(s) => s.accumulators.resize(n, None),
            Self::MinU16(s) => s.accumulators.resize(n, None),
            Self::MinU32(s) => s.accumulators.resize(n, None),
            Self::MinU64(s) => s.accumulators.resize(n, None),
            Self::MinF32(s) => s.accumulators.resize(n, None),
            Self::MinF64(s) => s.accumulators.resize(n, None),
            Self::MaxI8(s) => s.accumulators.resize(n, None),
            Self::MaxI16(s) => s.accumulators.resize(n, None),
            Self::MaxI32(s) => s.accumulators.resize(n, None),
            Self::MaxI64(s) => s.accumulators.resize(n, None),
            Self::MaxU8(s) => s.accumulators.resize(n, None),
            Self::MaxU16(s) => s.accumulators.resize(n, None),
            Self::MaxU32(s) => s.accumulators.resize(n, None),
            Self::MaxU64(s) => s.accumulators.resize(n, None),
            Self::MaxF32(s) => s.accumulators.resize(n, None),
            Self::MaxF64(s) => s.accumulators.resize(n, None),
        }
    }

    /// Try to use pre-computed group sizes for O(groups) count instead of O(rows).
    /// Returns true if the accumulator was fully updated (no scatter loop needed).
    fn try_use_group_sizes(&mut self, group_sizes: &[u64]) -> bool {
        match self {
            Self::Count(s) => s.try_use_group_sizes(group_sizes),
            _ => false,
        }
    }

    /// Vectorized batch update: tight loop per accumulator type over group_ids.
    fn update_batch(&mut self, group_ids: &[u32]) {
        match self {
            Self::Count(s) => s.update_batch(group_ids),
            Self::SumI64(s) => s.update_batch(group_ids),
            Self::SumU64(s) => s.update_batch(group_ids),
            Self::SumF32(s) => s.update_batch(group_ids),
            Self::SumF64(s) => s.update_batch(group_ids),
            Self::MinI8(s) => s.update_batch(group_ids),
            Self::MinI16(s) => s.update_batch(group_ids),
            Self::MinI32(s) => s.update_batch(group_ids),
            Self::MinI64(s) => s.update_batch(group_ids),
            Self::MinU8(s) => s.update_batch(group_ids),
            Self::MinU16(s) => s.update_batch(group_ids),
            Self::MinU32(s) => s.update_batch(group_ids),
            Self::MinU64(s) => s.update_batch(group_ids),
            Self::MinF32(s) => s.update_batch(group_ids),
            Self::MinF64(s) => s.update_batch(group_ids),
            Self::MaxI8(s) => s.update_batch(group_ids),
            Self::MaxI16(s) => s.update_batch(group_ids),
            Self::MaxI32(s) => s.update_batch(group_ids),
            Self::MaxI64(s) => s.update_batch(group_ids),
            Self::MaxU8(s) => s.update_batch(group_ids),
            Self::MaxU16(s) => s.update_batch(group_ids),
            Self::MaxU32(s) => s.update_batch(group_ids),
            Self::MaxU64(s) => s.update_batch(group_ids),
            Self::MaxF32(s) => s.update_batch(group_ids),
            Self::MaxF64(s) => s.update_batch(group_ids),
        }
    }

    fn finalize(self, name: &str) -> DaftResult<Series> {
        match self {
            Self::Count(s) => s.finalize(name),
            Self::SumI64(s) => s.finalize(name),
            Self::SumU64(s) => s.finalize(name),
            Self::SumF32(s) => s.finalize(name),
            Self::SumF64(s) => s.finalize(name),
            Self::MinI8(s) => s.finalize(name),
            Self::MinI16(s) => s.finalize(name),
            Self::MinI32(s) => s.finalize(name),
            Self::MinI64(s) => s.finalize(name),
            Self::MinU8(s) => s.finalize(name),
            Self::MinU16(s) => s.finalize(name),
            Self::MinU32(s) => s.finalize(name),
            Self::MinU64(s) => s.finalize(name),
            Self::MinF32(s) => s.finalize(name),
            Self::MinF64(s) => s.finalize(name),
            Self::MaxI8(s) => s.finalize(name),
            Self::MaxI16(s) => s.finalize(name),
            Self::MaxI32(s) => s.finalize(name),
            Self::MaxI64(s) => s.finalize(name),
            Self::MaxU8(s) => s.finalize(name),
            Self::MaxU16(s) => s.finalize(name),
            Self::MaxU32(s) => s.finalize(name),
            Self::MaxU64(s) => s.finalize(name),
            Self::MaxF32(s) => s.finalize(name),
            Self::MaxF64(s) => s.finalize(name),
        }
    }
}

// ---------------------------------------------------------------------------
// Factory: create accumulator from a BoundAggExpr
// ---------------------------------------------------------------------------

fn try_create_accumulator(
    agg_expr: &BoundAggExpr,
    source: &RecordBatch,
) -> DaftResult<Option<(AggAccumulator, String)>> {
    match agg_expr.as_ref() {
        &AggExpr::Count(ref expr, mode) => {
            let evaluated = source.eval_agg_child(expr)?;
            let name = evaluated.name().to_string();
            Ok(Some((
                AggAccumulator::Count(CountAccum::new(&evaluated, mode)),
                name,
            )))
        }
        AggExpr::Sum(expr) => {
            let evaluated = source.eval_agg_child(expr)?;
            let name = evaluated.name().to_string();
            match evaluated.data_type() {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    let casted = evaluated.cast(&DataType::Int64)?;
                    let arr = casted.i64()?;
                    Ok(Some((
                        AggAccumulator::SumI64(SumAccumI64::new(arr.clone())),
                        name,
                    )))
                }
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    let casted = evaluated.cast(&DataType::UInt64)?;
                    let arr = casted.u64()?;
                    Ok(Some((
                        AggAccumulator::SumU64(SumAccumU64::new(arr.clone())),
                        name,
                    )))
                }
                DataType::Float32 => {
                    let arr = evaluated.downcast::<Float32Array>()?;
                    Ok(Some((
                        AggAccumulator::SumF32(SumAccumF32::new(arr.clone())),
                        name,
                    )))
                }
                DataType::Float64 => {
                    let arr = evaluated.downcast::<Float64Array>()?;
                    Ok(Some((
                        AggAccumulator::SumF64(SumAccumF64::new(arr.clone())),
                        name,
                    )))
                }
                _ => Ok(None),
            }
        }
        AggExpr::Min(expr) => {
            let evaluated = source.eval_agg_child(expr)?;
            let name = evaluated.name().to_string();
            match evaluated.data_type() {
                DataType::Int8 => {
                    let arr = evaluated.downcast::<Int8Array>()?;
                    Ok(Some((
                        AggAccumulator::MinI8(MinAccumI8::new(arr.clone())),
                        name,
                    )))
                }
                DataType::Int16 => {
                    let arr = evaluated.downcast::<Int16Array>()?;
                    Ok(Some((
                        AggAccumulator::MinI16(MinAccumI16::new(arr.clone())),
                        name,
                    )))
                }
                DataType::Int32 => {
                    let arr = evaluated.downcast::<Int32Array>()?;
                    Ok(Some((
                        AggAccumulator::MinI32(MinAccumI32::new(arr.clone())),
                        name,
                    )))
                }
                DataType::Int64 => {
                    let arr = evaluated.downcast::<Int64Array>()?;
                    Ok(Some((
                        AggAccumulator::MinI64(MinAccumI64::new(arr.clone())),
                        name,
                    )))
                }
                DataType::UInt8 => {
                    let arr = evaluated.downcast::<UInt8Array>()?;
                    Ok(Some((
                        AggAccumulator::MinU8(MinAccumU8::new(arr.clone())),
                        name,
                    )))
                }
                DataType::UInt16 => {
                    let arr = evaluated.downcast::<UInt16Array>()?;
                    Ok(Some((
                        AggAccumulator::MinU16(MinAccumU16::new(arr.clone())),
                        name,
                    )))
                }
                DataType::UInt32 => {
                    let arr = evaluated.downcast::<UInt32Array>()?;
                    Ok(Some((
                        AggAccumulator::MinU32(MinAccumU32::new(arr.clone())),
                        name,
                    )))
                }
                DataType::UInt64 => {
                    let arr = evaluated.downcast::<UInt64Array>()?;
                    Ok(Some((
                        AggAccumulator::MinU64(MinAccumU64::new(arr.clone())),
                        name,
                    )))
                }
                DataType::Float32 => {
                    let arr = evaluated.downcast::<Float32Array>()?;
                    Ok(Some((
                        AggAccumulator::MinF32(MinAccumF32::new(arr.clone())),
                        name,
                    )))
                }
                DataType::Float64 => {
                    let arr = evaluated.downcast::<Float64Array>()?;
                    Ok(Some((
                        AggAccumulator::MinF64(MinAccumF64::new(arr.clone())),
                        name,
                    )))
                }
                _ => Ok(None),
            }
        }
        AggExpr::Max(expr) => {
            let evaluated = source.eval_agg_child(expr)?;
            let name = evaluated.name().to_string();
            match evaluated.data_type() {
                DataType::Int8 => {
                    let arr = evaluated.downcast::<Int8Array>()?;
                    Ok(Some((
                        AggAccumulator::MaxI8(MaxAccumI8::new(arr.clone())),
                        name,
                    )))
                }
                DataType::Int16 => {
                    let arr = evaluated.downcast::<Int16Array>()?;
                    Ok(Some((
                        AggAccumulator::MaxI16(MaxAccumI16::new(arr.clone())),
                        name,
                    )))
                }
                DataType::Int32 => {
                    let arr = evaluated.downcast::<Int32Array>()?;
                    Ok(Some((
                        AggAccumulator::MaxI32(MaxAccumI32::new(arr.clone())),
                        name,
                    )))
                }
                DataType::Int64 => {
                    let arr = evaluated.downcast::<Int64Array>()?;
                    Ok(Some((
                        AggAccumulator::MaxI64(MaxAccumI64::new(arr.clone())),
                        name,
                    )))
                }
                DataType::UInt8 => {
                    let arr = evaluated.downcast::<UInt8Array>()?;
                    Ok(Some((
                        AggAccumulator::MaxU8(MaxAccumU8::new(arr.clone())),
                        name,
                    )))
                }
                DataType::UInt16 => {
                    let arr = evaluated.downcast::<UInt16Array>()?;
                    Ok(Some((
                        AggAccumulator::MaxU16(MaxAccumU16::new(arr.clone())),
                        name,
                    )))
                }
                DataType::UInt32 => {
                    let arr = evaluated.downcast::<UInt32Array>()?;
                    Ok(Some((
                        AggAccumulator::MaxU32(MaxAccumU32::new(arr.clone())),
                        name,
                    )))
                }
                DataType::UInt64 => {
                    let arr = evaluated.downcast::<UInt64Array>()?;
                    Ok(Some((
                        AggAccumulator::MaxU64(MaxAccumU64::new(arr.clone())),
                        name,
                    )))
                }
                DataType::Float32 => {
                    let arr = evaluated.downcast::<Float32Array>()?;
                    Ok(Some((
                        AggAccumulator::MaxF32(MaxAccumF32::new(arr.clone())),
                        name,
                    )))
                }
                DataType::Float64 => {
                    let arr = evaluated.downcast::<Float64Array>()?;
                    Ok(Some((
                        AggAccumulator::MaxF64(MaxAccumF64::new(arr.clone())),
                        name,
                    )))
                }
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Returns true if all agg expressions can be handled by the inline path.
///
/// Requirements:
/// 1. All agg expressions are Count, Sum, Min, or Max.
/// 2. For Sum/Min/Max, the value column dtype must be a supported numeric type.
///
/// Uses schema-level type inference (`to_field`) instead of expression evaluation
/// to avoid materializing computed columns just for a dtype check.
pub(super) fn can_inline_agg(to_agg: &[BoundAggExpr], source: &RecordBatch) -> bool {
    // Quick check: bail immediately if any agg type isn't supported.
    if !to_agg.iter().all(|e| {
        matches!(
            e.as_ref(),
            AggExpr::Count(..) | AggExpr::Sum(..) | AggExpr::Min(..) | AggExpr::Max(..)
        )
    }) {
        return false;
    }
    // Check value column dtypes via schema type inference (no data materialized).
    to_agg.iter().all(|e| match e.as_ref() {
        AggExpr::Count(..) => true,
        AggExpr::Sum(expr) | AggExpr::Min(expr) | AggExpr::Max(expr) => {
            if let Ok(field) = expr.to_field(&source.schema) {
                matches!(
                    field.dtype,
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                        | DataType::Float32
                        | DataType::Float64
                )
            } else {
                false
            }
        }
        _ => unreachable!("pre-check ensures only supported types reach here"),
    })
}

// ---------------------------------------------------------------------------
// Phase 2: Accumulation using group_ids and group_sizes
// ---------------------------------------------------------------------------

/// Accumulate all accumulators using the grouping result.
///
/// For Count accumulators that don't need per-row null checks, uses pre-computed
/// group_sizes in O(groups) instead of scatter-looping in O(rows). This matches
/// the fallback path's efficiency for Count(All) and Count(Valid, no nulls).
fn accumulate(accumulators: &mut [AggAccumulator], result: &GroupingResult) {
    let num_groups = result.group_sizes.len() as u32;
    for acc in accumulators.iter_mut() {
        acc.init_groups(num_groups);
        // Try O(groups) path first; fall back to O(rows) scatter loop.
        if !acc.try_use_group_sizes(&result.group_sizes) {
            acc.update_batch(&result.group_ids);
        }
    }
}

// ---------------------------------------------------------------------------
// Single-column integer fast path (FNV hash, no comparator closure)
// ---------------------------------------------------------------------------

fn agg_single_col_int<T>(
    keys: &DataArray<T>,
    accumulators: &mut [AggAccumulator],
) -> DaftResult<Vec<u64>>
where
    T: DaftIntegerType,
    T::Native: Hash + Eq + Ord,
{
    let len = keys.len();
    let initial_capacity = std::cmp::min(len, 1024).max(1);
    let mut groupkey_indices: Vec<u64> = Vec::with_capacity(initial_capacity);
    let mut num_groups: u32 = 0;
    let mut group_ids: Vec<u32> = Vec::with_capacity(len);
    let mut group_sizes: Vec<u64> = Vec::with_capacity(initial_capacity);

    if keys.null_count() == 0 {
        let mut group_map = FnvHashMap::<T::Native, u32>::with_capacity_and_hasher(
            initial_capacity,
            BuildHasherDefault::default(),
        );
        for (row_idx, val) in keys.values().iter().enumerate() {
            let gid = match group_map.entry(*val) {
                Vacant(e) => {
                    let gid = num_groups;
                    num_groups = num_groups.checked_add(1).ok_or_else(|| {
                        common_error::DaftError::ComputeError(
                            "Number of groups exceeds u32::MAX in inline aggregation".into(),
                        )
                    })?;
                    e.insert(gid);
                    groupkey_indices.push(row_idx as u64);
                    group_sizes.push(1);
                    gid
                }
                Occupied(e) => {
                    let gid = *e.get();
                    group_sizes[gid as usize] += 1;
                    gid
                }
            };
            group_ids.push(gid);
        }
    } else {
        let mut group_map = FnvHashMap::<Option<T::Native>, u32>::with_capacity_and_hasher(
            initial_capacity,
            BuildHasherDefault::default(),
        );
        for (row_idx, val) in keys.into_iter().enumerate() {
            let gid = match group_map.entry(val) {
                Vacant(e) => {
                    let gid = num_groups;
                    num_groups = num_groups.checked_add(1).ok_or_else(|| {
                        common_error::DaftError::ComputeError(
                            "Number of groups exceeds u32::MAX in inline aggregation".into(),
                        )
                    })?;
                    e.insert(gid);
                    groupkey_indices.push(row_idx as u64);
                    group_sizes.push(1);
                    gid
                }
                Occupied(e) => {
                    let gid = *e.get();
                    group_sizes[gid as usize] += 1;
                    gid
                }
            };
            group_ids.push(gid);
        }
    }

    let result = GroupingResult {
        groupkey_indices,
        group_ids,
        group_sizes,
    };
    accumulate(accumulators, &result);
    Ok(result.groupkey_indices)
}

// ---------------------------------------------------------------------------
// Generic multi-column hash path
// ---------------------------------------------------------------------------

/// Hash-based grouping using IndexHash + comparator closure.
/// Used when the groupby has multiple columns or non-integer types.
fn agg_generic_hash_path(
    groupby_physical: &RecordBatch,
    accumulators: &mut [AggAccumulator],
) -> DaftResult<Vec<u64>> {
    let num_rows = groupby_physical.len();
    let hashes = groupby_physical.hash_rows()?;
    let initial_capacity = std::cmp::min(num_rows, 1024).max(1);
    let cols: Vec<Series> = groupby_physical
        .as_materialized_series()
        .into_iter()
        .cloned()
        .collect();
    let comparator = build_multi_array_is_equal(
        cols.as_slice(),
        cols.as_slice(),
        vec![true; cols.len()].as_slice(),
        vec![true; cols.len()].as_slice(),
    )?;

    let mut group_table = HashMap::<IndexHash, u32, IdentityBuildHasher>::with_capacity_and_hasher(
        initial_capacity,
        Default::default(),
    );

    let mut groupkey_indices: Vec<u64> = Vec::with_capacity(initial_capacity);
    let mut num_groups: u32 = 0;
    let mut group_ids: Vec<u32> = Vec::with_capacity(num_rows);
    let mut group_sizes: Vec<u64> = Vec::with_capacity(initial_capacity);

    // Phase 1: Hash probe — build dense group_ids and track group_sizes.
    for (row_idx, h) in hashes.values().iter().enumerate() {
        let entry = group_table.raw_entry_mut().from_hash(*h, |other| {
            (*h == other.hash) && {
                let j = other.idx;
                comparator(row_idx, j as usize)
            }
        });

        let group_id = match entry {
            RawEntryMut::Vacant(entry) => {
                let gid = num_groups;
                num_groups = num_groups.checked_add(1).ok_or_else(|| {
                    common_error::DaftError::ComputeError(
                        "Number of groups exceeds u32::MAX in inline aggregation".into(),
                    )
                })?;
                entry.insert_hashed_nocheck(
                    *h,
                    IndexHash {
                        idx: row_idx as u64,
                        hash: *h,
                    },
                    gid,
                );
                groupkey_indices.push(row_idx as u64);
                group_sizes.push(1);
                gid
            }
            RawEntryMut::Occupied(entry) => {
                let gid = *entry.get();
                group_sizes[gid as usize] += 1;
                gid
            }
        };

        group_ids.push(group_id);
    }

    // Phase 2: Accumulation with O(groups) count optimization.
    let result = GroupingResult {
        groupkey_indices,
        group_ids,
        group_sizes,
    };
    accumulate(accumulators, &result);
    Ok(result.groupkey_indices)
}

// ---------------------------------------------------------------------------
// RecordBatch methods
// ---------------------------------------------------------------------------

/// Dispatch single-column integer groupby to the typed FNV fast path.
macro_rules! dispatch_single_col_int {
    ($col:expr, $accumulators:expr, $($dtype:ident => $downcast:ident),+ $(,)?) => {
        match $col.data_type() {
            $(DataType::$dtype => Some(agg_single_col_int($col.$downcast()?, $accumulators)?),)+
            _ => None,
        }
    };
}

impl RecordBatch {
    pub(crate) fn agg_groupby_inline(
        &self,
        to_agg: &[BoundAggExpr],
        group_by: &[BoundExpr],
    ) -> DaftResult<Self> {
        // 1. Evaluate groupby columns.
        let groupby_table = self.eval_expression_list(group_by)?;
        let groupby_physical = groupby_table.as_physical()?;

        // 2. Create accumulators for each agg expression.
        let mut accumulators: Vec<AggAccumulator> = Vec::with_capacity(to_agg.len());
        let mut output_names: Vec<String> = Vec::with_capacity(to_agg.len());

        for agg_expr in to_agg {
            let (acc, name) = try_create_accumulator(agg_expr, self)?.ok_or_else(|| {
                common_error::DaftError::ComputeError(
                    "Inline aggregation reached an unsupported type; this is a bug".into(),
                )
            })?;
            accumulators.push(acc);
            output_names.push(name);
        }

        // 3. Dispatch: single-column integer → FNV fast path, otherwise generic.
        let groupkey_indices = if groupby_physical.num_columns() == 1 {
            let col = groupby_physical.get_column(0);
            let fast_result = dispatch_single_col_int!(
                col, &mut accumulators,
                Int8 => i8, Int16 => i16, Int32 => i32, Int64 => i64,
                UInt8 => u8, UInt16 => u16, UInt32 => u32, UInt64 => u64,
            );
            match fast_result {
                Some(indices) => indices,
                None => agg_generic_hash_path(&groupby_physical, &mut accumulators)?,
            }
        } else {
            agg_generic_hash_path(&groupby_physical, &mut accumulators)?
        };

        // 4. Construct output: group keys + aggregated columns.
        let groupkeys_table = {
            let indices_as_arr = UInt64Array::from_vec("", groupkey_indices);
            groupby_table.take(&indices_as_arr)?
        };

        let grouped_cols: Vec<Series> = accumulators
            .into_iter()
            .zip(output_names.iter())
            .map(|(acc, name)| acc.finalize(name))
            .collect::<DaftResult<Vec<_>>>()?;

        let all_series: Vec<Series> = groupkeys_table
            .as_materialized_series()
            .into_iter()
            .cloned()
            .chain(grouped_cols)
            .collect();
        Self::from_nonempty_columns(all_series)
    }

    /// Fallback to the existing groupby path (used by benchmarks).
    #[cfg(test)]
    pub(crate) fn agg_groupby_fallback(
        &self,
        to_agg: &[BoundAggExpr],
        group_by: &[BoundExpr],
    ) -> DaftResult<Self> {
        use daft_groupby::IntoGroups;
        let groupby_table = self.eval_expression_list(group_by)?;

        let (groupkey_indices, groupvals_indices) = groupby_table.make_groups()?;

        let groupkeys_table = {
            let indices_as_arr = UInt64Array::from_vec("", groupkey_indices);
            groupby_table.take(&indices_as_arr)?
        };
        let group_idx_input = if groupvals_indices.len() == 1 {
            None
        } else {
            Some(&groupvals_indices)
        };
        let grouped_cols = to_agg
            .iter()
            .map(|e| self.eval_agg_expression(e, group_idx_input))
            .collect::<DaftResult<Vec<_>>>()?;

        let all_series: Vec<Series> = groupkeys_table
            .as_materialized_series()
            .into_iter()
            .cloned()
            .chain(grouped_cols)
            .collect();
        Self::from_nonempty_columns(all_series)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::{count_mode::CountMode, datatypes::*, prelude::*, series::IntoSeries};
    use daft_dsl::{
        AggExpr,
        expr::bound_expr::{BoundAggExpr, BoundExpr},
        resolved_col,
    };

    use crate::RecordBatch;

    /// Helper to build a RecordBatch and bound expressions for testing.
    fn make_test_batch() -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let keys = Series::from_arrow(
            Arc::new(Field::new("key", DataType::Utf8)),
            Arc::new(arrow::array::LargeStringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("a"),
                Some("b"),
                Some("a"),
            ])),
        )
        .unwrap();
        let vals = Int64Array::from_iter(
            Field::new("val", DataType::Int64),
            vec![Some(10), Some(20), None, Some(40), Some(50)],
        )
        .into_series();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Utf8),
            Field::new("val", DataType::Int64),
        ]);
        let rb = RecordBatch::from_nonempty_columns(vec![keys, vals]).unwrap();
        let group_by = vec![BoundExpr::try_new(resolved_col("key"), &schema).unwrap()];
        (rb, group_by, schema)
    }

    /// Helper for integer-keyed groupby tests (exercises the FNV fast path).
    fn make_int_key_test_batch() -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let keys = Int64Array::from_iter(
            Field::new("key", DataType::Int64),
            vec![Some(1), Some(2), Some(1), Some(2), Some(1)],
        )
        .into_series();
        let vals = Int64Array::from_iter(
            Field::new("val", DataType::Int64),
            vec![Some(10), Some(20), None, Some(40), Some(50)],
        )
        .into_series();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int64),
            Field::new("val", DataType::Int64),
        ]);
        let rb = RecordBatch::from_nonempty_columns(vec![keys, vals]).unwrap();
        let group_by = vec![BoundExpr::try_new(resolved_col("key"), &schema).unwrap()];
        (rb, group_by, schema)
    }

    /// Helper for integer-keyed groupby with null keys.
    fn make_int_key_with_nulls_test_batch() -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let keys = Int64Array::from_iter(
            Field::new("key", DataType::Int64),
            vec![Some(1), None, Some(1), None, Some(2)],
        )
        .into_series();
        let vals = Int64Array::from_iter(
            Field::new("val", DataType::Int64),
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        )
        .into_series();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int64),
            Field::new("val", DataType::Int64),
        ]);
        let rb = RecordBatch::from_nonempty_columns(vec![keys, vals]).unwrap();
        let group_by = vec![BoundExpr::try_new(resolved_col("key"), &schema).unwrap()];
        (rb, group_by, schema)
    }

    fn sort_by_key(rb: &RecordBatch) -> RecordBatch {
        rb.sort(
            &[BoundExpr::try_new(resolved_col("key"), rb.schema.as_ref()).unwrap()],
            &[false],
            &[false],
        )
        .unwrap()
    }

    /// Null-safe series equality: two values are equal if both null or both non-null and equal.
    fn series_equal_null_safe(a: &Series, b: &Series) -> bool {
        if a.len() != b.len() || a.data_type() != b.data_type() {
            return false;
        }
        let a_arr = a.to_arrow().unwrap();
        let b_arr = b.to_arrow().unwrap();
        a_arr == b_arr
    }

    fn assert_batches_equal(a: &RecordBatch, b: &RecordBatch) {
        let a = sort_by_key(a);
        let b = sort_by_key(b);
        assert_eq!(a.num_rows, b.num_rows, "Row count mismatch");
        assert_eq!(a.num_columns(), b.num_columns(), "Column count mismatch");
        let a_cols = a.as_materialized_series();
        let b_cols = b.as_materialized_series();
        for (ac, bc) in a_cols.iter().zip(b_cols.iter()) {
            assert_eq!(ac.name(), bc.name(), "Column name mismatch");
            assert_eq!(
                ac.data_type(),
                bc.data_type(),
                "Column dtype mismatch for {}",
                ac.name()
            );
            assert!(
                series_equal_null_safe(ac, bc),
                "Column data mismatch for '{}': {:?} vs {:?}",
                ac.name(),
                ac,
                bc
            );
        }
    }

    // --- Original string-key tests (exercise generic hash path) ---

    #[test]
    fn test_inline_count_all_matches_fallback() {
        let (rb, group_by, schema) = make_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_count_valid_matches_fallback() {
        let (rb, group_by, schema) = make_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(
                AggExpr::Count(resolved_col("val"), CountMode::Valid),
                &schema,
            )
            .unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_count_null_matches_fallback() {
        let (rb, group_by, schema) = make_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(
                AggExpr::Count(resolved_col("val"), CountMode::Null),
                &schema,
            )
            .unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_sum_matches_fallback() {
        let (rb, group_by, schema) = make_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_mixed_count_and_sum() {
        let (rb, group_by, schema) = make_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
            BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    // --- Integer-key tests (exercise FNV fast path) ---

    #[test]
    fn test_inline_int_key_count_matches_fallback() {
        let (rb, group_by, schema) = make_int_key_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_int_key_sum_matches_fallback() {
        let (rb, group_by, schema) = make_int_key_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_int_key_mixed_matches_fallback() {
        let (rb, group_by, schema) = make_int_key_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(
                AggExpr::Count(resolved_col("val"), CountMode::Valid),
                &schema,
            )
            .unwrap(),
            BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    // --- Nullable integer-key tests (exercise FNV nullable path) ---

    #[test]
    fn test_inline_int_key_with_nulls_count_matches_fallback() {
        let (rb, group_by, schema) = make_int_key_with_nulls_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_int_key_with_nulls_sum_matches_fallback() {
        let (rb, group_by, schema) = make_int_key_with_nulls_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    // --- Min/Max tests (string-key, exercises generic hash path) ---

    #[test]
    fn test_inline_min_matches_fallback() {
        let (rb, group_by, schema) = make_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Min(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_max_matches_fallback() {
        let (rb, group_by, schema) = make_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Max(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_mixed_count_sum_min_max() {
        let (rb, group_by, schema) = make_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
            BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap(),
            BoundAggExpr::try_new(AggExpr::Min(resolved_col("val")), &schema).unwrap(),
            BoundAggExpr::try_new(AggExpr::Max(resolved_col("val")), &schema).unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    // --- Min/Max tests (integer-key, exercises FNV fast path) ---

    #[test]
    fn test_inline_int_key_min_matches_fallback() {
        let (rb, group_by, schema) = make_int_key_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Min(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_int_key_max_matches_fallback() {
        let (rb, group_by, schema) = make_int_key_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Max(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    // --- Min/Max tests (nullable integer-key, exercises FNV nullable path) ---

    #[test]
    fn test_inline_int_key_with_nulls_min_matches_fallback() {
        let (rb, group_by, schema) = make_int_key_with_nulls_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Min(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_int_key_with_nulls_max_matches_fallback() {
        let (rb, group_by, schema) = make_int_key_with_nulls_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Max(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    // --- Float64 with NaN tests (validates f64::min/f64::max NaN handling) ---

    /// Helper for float-keyed groupby tests with NaN values.
    fn make_float_with_nan_test_batch() -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let keys = Int64Array::from_iter(
            Field::new("key", DataType::Int64),
            vec![Some(1), Some(1), Some(2), Some(2), Some(1)],
        )
        .into_series();
        let vals = Float64Array::from_iter(
            Field::new("val", DataType::Float64),
            vec![Some(1.0), Some(f64::NAN), Some(3.0), Some(2.0), None],
        )
        .into_series();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int64),
            Field::new("val", DataType::Float64),
        ]);
        let rb = RecordBatch::from_nonempty_columns(vec![keys, vals]).unwrap();
        let group_by = vec![BoundExpr::try_new(resolved_col("key"), &schema).unwrap()];
        (rb, group_by, schema)
    }

    #[test]
    fn test_inline_float_nan_min_matches_fallback() {
        let (rb, group_by, schema) = make_float_with_nan_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Min(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_float_nan_max_matches_fallback() {
        let (rb, group_by, schema) = make_float_with_nan_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Max(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    // --- Small integer type tests (validates per-type accumulators like MinAccumI8) ---

    /// Helper for Int8-valued groupby tests.
    fn make_i8_val_test_batch() -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let keys = Int64Array::from_iter(
            Field::new("key", DataType::Int64),
            vec![Some(1), Some(2), Some(1), Some(2), Some(1)],
        )
        .into_series();
        let vals = Int8Array::from_iter(
            Field::new("val", DataType::Int8),
            vec![Some(10), Some(20), None, Some(-5), Some(3)],
        )
        .into_series();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int64),
            Field::new("val", DataType::Int8),
        ]);
        let rb = RecordBatch::from_nonempty_columns(vec![keys, vals]).unwrap();
        let group_by = vec![BoundExpr::try_new(resolved_col("key"), &schema).unwrap()];
        (rb, group_by, schema)
    }

    #[test]
    fn test_inline_i8_min_matches_fallback() {
        let (rb, group_by, schema) = make_i8_val_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Min(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_i8_max_matches_fallback() {
        let (rb, group_by, schema) = make_i8_val_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Max(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }
}
