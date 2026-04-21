use std::{
    collections::hash_map::Entry::{Occupied, Vacant},
    hash::{BuildHasherDefault, Hash},
};

use arrow_array::Array;
use common_error::DaftResult;
use daft_core::{
    array::ops::{arrow::comparison::build_multi_array_is_equal, as_arrow::AsArrow},
    count_mode::CountMode,
    datatypes::*,
    prelude::AsArrow,
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

    fn init_groups(&mut self, n: usize) {
        self.counts.resize(n, 0);
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

            fn init_groups(&mut self, n: usize) {
                self.accumulators.resize(n, None);
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

            fn init_groups(&mut self, n: usize) {
                self.accumulators.resize(n, None);
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

/// Generates the `AggAccumulator` enum and its `init_groups` / `update_batch` /
/// `finalize` dispatch methods from a single list of variants.
///
/// Each accumulator struct must implement inherent methods:
///   - `init_groups(&mut self, n: usize)`
///   - `update_batch(&mut self, group_ids: &[u32])`
///   - `finalize(self, name: &str) -> DaftResult<Series>`
macro_rules! define_agg_accumulator_enum {
    ($($variant:ident($accum:ty)),+ $(,)?) => {
        enum AggAccumulator {
            $($variant($accum),)+
        }

        impl AggAccumulator {
            /// Pre-initialize storage for `n` groups.
            fn init_groups(&mut self, n: u32) {
                let n = n as usize;
                match self {
                    $(Self::$variant(s) => s.init_groups(n),)+
                }
            }

            /// Vectorized batch update: tight loop per accumulator type over group_ids.
            fn update_batch(&mut self, group_ids: &[u32]) {
                match self {
                    $(Self::$variant(s) => s.update_batch(group_ids),)+
                }
            }

            fn finalize(self, name: &str) -> DaftResult<Series> {
                match self {
                    $(Self::$variant(s) => s.finalize(name),)+
                }
            }
        }
    };
}

// Sum widens small integers (e.g. Int8 → Int64) so only 4 Sum variants are needed.
// Min/Max preserve the original dtype, so each numeric type gets its own variant.
define_agg_accumulator_enum!(
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
);

impl AggAccumulator {
    /// Try to use pre-computed group sizes for O(groups) count instead of O(rows).
    /// Returns true if the accumulator was fully updated (no scatter loop needed).
    ///
    /// Only `Count` benefits from this optimization today; all other accumulators
    /// must still iterate per-row to combine values.
    fn try_use_group_sizes(&mut self, group_sizes: &[u64]) -> bool {
        match self {
            Self::Count(s) => s.try_use_group_sizes(group_sizes),
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Factory: create accumulator from a BoundAggExpr
// ---------------------------------------------------------------------------

/// Matches `$evaluated`'s dtype and constructs the corresponding typed
/// `AggAccumulator` variant by downcasting to the matching array type.
macro_rules! dispatch_typed_accum {
    ($evaluated:expr, $name:expr,
        $($dtype:ident => $arr_ty:ident => $variant:ident($accum:ident)),+ $(,)?
    ) => {
        match $evaluated.data_type() {
            $(
                DataType::$dtype => {
                    let arr = $evaluated.downcast::<$arr_ty>()?;
                    Ok(Some((
                        AggAccumulator::$variant($accum::new(arr.clone())),
                        $name,
                    )))
                }
            )+
            _ => Ok(None),
        }
    };
}

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
            dispatch_typed_accum!(
                evaluated, name,
                Int8 => Int8Array => MinI8(MinAccumI8),
                Int16 => Int16Array => MinI16(MinAccumI16),
                Int32 => Int32Array => MinI32(MinAccumI32),
                Int64 => Int64Array => MinI64(MinAccumI64),
                UInt8 => UInt8Array => MinU8(MinAccumU8),
                UInt16 => UInt16Array => MinU16(MinAccumU16),
                UInt32 => UInt32Array => MinU32(MinAccumU32),
                UInt64 => UInt64Array => MinU64(MinAccumU64),
                Float32 => Float32Array => MinF32(MinAccumF32),
                Float64 => Float64Array => MinF64(MinAccumF64),
            )
        }
        AggExpr::Max(expr) => {
            let evaluated = source.eval_agg_child(expr)?;
            let name = evaluated.name().to_string();
            dispatch_typed_accum!(
                evaluated, name,
                Int8 => Int8Array => MaxI8(MaxAccumI8),
                Int16 => Int16Array => MaxI16(MaxAccumI16),
                Int32 => Int32Array => MaxI32(MaxAccumI32),
                Int64 => Int64Array => MaxI64(MaxAccumI64),
                UInt8 => UInt8Array => MaxU8(MaxAccumU8),
                UInt16 => UInt16Array => MaxU16(MaxAccumU16),
                UInt32 => UInt32Array => MaxU32(MaxAccumU32),
                UInt64 => UInt64Array => MaxU64(MaxAccumU64),
                Float32 => Float32Array => MaxF32(MaxAccumF32),
                Float64 => Float64Array => MaxF64(MaxAccumF64),
            )
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
// Single-column byte-key fast path (FNV hash on borrowed slices)
// ---------------------------------------------------------------------------
//
// Used for Utf8 and Binary single-column group-bys. Keys are borrowed slices
// into the arrow buffers, which remain alive for the duration of the call,
// so we can avoid the comparator closure used by the generic hash path.

fn agg_single_col_bytes<'a, K>(
    len: usize,
    null_count: usize,
    mut value_at: impl FnMut(usize) -> &'a K,
    mut optional_at: impl FnMut(usize) -> Option<&'a K>,
    accumulators: &mut [AggAccumulator],
) -> DaftResult<Vec<u64>>
where
    K: ?Sized + Hash + Eq + 'a,
{
    let initial_capacity = std::cmp::min(len, 1024).max(1);
    let mut groupkey_indices: Vec<u64> = Vec::with_capacity(initial_capacity);
    let mut num_groups: u32 = 0;
    let mut group_ids: Vec<u32> = Vec::with_capacity(len);
    let mut group_sizes: Vec<u64> = Vec::with_capacity(initial_capacity);

    if null_count == 0 {
        let mut group_map = FnvHashMap::<&'a K, u32>::with_capacity_and_hasher(
            initial_capacity,
            BuildHasherDefault::default(),
        );
        for row_idx in 0..len {
            let val = value_at(row_idx);
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
    } else {
        let mut group_map = FnvHashMap::<Option<&'a K>, u32>::with_capacity_and_hasher(
            initial_capacity,
            BuildHasherDefault::default(),
        );
        for row_idx in 0..len {
            let val = optional_at(row_idx);
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
// Multi-column symbolized path (string optimization)
// ---------------------------------------------------------------------------

/// Map each distinct value of type K to a dense u32 symbol ID.
/// Null values get symbol ID 0 (when nulls are present).
fn symbolize_column<'a, K>(
    len: usize,
    null_count: usize,
    value_at: impl Fn(usize) -> &'a K,
    is_null: impl Fn(usize) -> bool,
) -> DaftResult<Vec<u32>>
where
    K: ?Sized + Hash + Eq + 'a,
{
    let initial_capacity = std::cmp::min(len, 1024).max(1);
    let mut symbols = Vec::with_capacity(len);

    if null_count == 0 {
        // When there are no nulls, symbol IDs start from 0. In the nullable path below,
        // 0 is reserved for null and non-null symbols start from 1. This asymmetry is
        // intentional because symbols are only compared within this column's current
        // symbolization output; IDs are not persisted or reused across batches.
        let mut next_id: u32 = 0;
        let mut map = FnvHashMap::<&'a K, u32>::with_capacity_and_hasher(
            initial_capacity,
            BuildHasherDefault::default(),
        );
        for i in 0..len {
            let val = value_at(i);
            let id = match map.entry(val) {
                Vacant(e) => {
                    let id = next_id;
                    next_id = next_id.checked_add(1).ok_or_else(|| {
                        common_error::DaftError::ComputeError(
                            "Number of distinct symbols exceeds u32::MAX in symbolization".into(),
                        )
                    })?;
                    e.insert(id);
                    id
                }
                Occupied(e) => *e.get(),
            };
            symbols.push(id);
        }
    } else {
        let mut next_id: u32 = 1; // 0 reserved for null
        let mut map = FnvHashMap::<&'a K, u32>::with_capacity_and_hasher(
            initial_capacity,
            BuildHasherDefault::default(),
        );
        for i in 0..len {
            if is_null(i) {
                symbols.push(0);
            } else {
                let val = value_at(i);
                let id = match map.entry(val) {
                    Vacant(e) => {
                        let id = next_id;
                        next_id = next_id.checked_add(1).ok_or_else(|| {
                            common_error::DaftError::ComputeError(
                                "Number of distinct symbols exceeds u32::MAX in symbolization"
                                    .into(),
                            )
                        })?;
                        e.insert(id);
                        id
                    }
                    Occupied(e) => *e.get(),
                };
                symbols.push(id);
            }
        }
    }
    Ok(symbols)
}

/// Symbolized multi-column grouping path for string-heavy workloads.
/// Replaces Utf8/Binary columns with dense u32 symbol-ID columns, then runs
/// the generic hash path on the cheaper fixed-width representation.
/// Non-string columns (integers, etc.) are kept as-is.
/// Returns None if no Utf8/Binary columns are present.
fn agg_symbolized_path(
    groupby_physical: &RecordBatch,
    accumulators: &mut [AggAccumulator],
) -> DaftResult<Option<Vec<u64>>> {
    let cols = groupby_physical.as_materialized_series();

    // Only beneficial when at least one column is Utf8/Binary.
    if !cols
        .iter()
        .any(|c| matches!(c.data_type(), DataType::Utf8 | DataType::Binary))
    {
        return Ok(None);
    }

    // Replace Utf8/Binary columns with symbolized UInt32 columns.
    // Non-string columns are kept as-is.
    let mut replaced_cols: Vec<Series> = Vec::with_capacity(cols.len());
    for col in cols {
        match col.data_type() {
            DataType::Utf8 => {
                let utf8_arr = col.utf8()?;
                let arrow_arr = utf8_arr.as_arrow()?;
                let nulls = col.nulls();
                let null_count = nulls.map_or(0, |nb| nb.null_count());
                let is_null = |i: usize| nulls.is_some_and(|nb| !nb.is_valid(i));
                let syms =
                    symbolize_column(col.len(), null_count, |i| arrow_arr.value(i), is_null)?;
                replaced_cols.push(UInt32Array::from_vec(col.name(), syms).into_series());
            }
            DataType::Binary => {
                let bin_arr = col.binary()?;
                let arrow_arr = bin_arr.as_arrow()?;
                let nulls = col.nulls();
                let null_count = nulls.map_or(0, |nb| nb.null_count());
                let is_null = |i: usize| nulls.is_some_and(|nb| !nb.is_valid(i));
                let syms =
                    symbolize_column(col.len(), null_count, |i| arrow_arr.value(i), is_null)?;
                replaced_cols.push(UInt32Array::from_vec(col.name(), syms).into_series());
            }
            _ => replaced_cols.push(col.clone()),
        }
    }

    // Run the generic hash path on the symbolized columns.
    let symbolized_rb = RecordBatch::from_nonempty_columns(replaced_cols)?;
    let indices = agg_generic_hash_path(&symbolized_rb, accumulators)?;
    Ok(Some(indices))
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

/// Dispatch single-column Utf8/Binary groupby to the byte-key fast path.
macro_rules! dispatch_single_col_bytes {
    ($col:expr, $accumulators:expr, $($dtype:ident => $downcast:ident => $key:ty),+ $(,)?) => {
        match $col.data_type() {
            $(
                DataType::$dtype => {
                    let inner = $col.$downcast()?.as_arrow()?;
                    Some(agg_single_col_bytes::<$key>(
                        inner.len(),
                        inner.null_count(),
                        |i| inner.value(i),
                        |i| (!inner.is_null(i)).then(|| inner.value(i)),
                        $accumulators,
                    )?)
                }
            )+
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

        // 3. Dispatch: single-column typed fast paths, otherwise generic.
        let groupkey_indices = if groupby_physical.num_columns() == 1 {
            let col = groupby_physical.get_column(0);
            let fast_result = dispatch_single_col_int!(
                col, &mut accumulators,
                Int8 => i8, Int16 => i16, Int32 => i32, Int64 => i64,
                UInt8 => u8, UInt16 => u16, UInt32 => u32, UInt64 => u64,
            );
            let fast_result = match fast_result {
                Some(indices) => Some(indices),
                None => dispatch_single_col_bytes!(
                    col, &mut accumulators,
                    Utf8 => utf8 => str,
                    Binary => binary => [u8],
                ),
            };
            match fast_result {
                Some(indices) => indices,
                None => agg_generic_hash_path(&groupby_physical, &mut accumulators)?,
            }
        } else {
            // Try symbolized path when string/binary columns are present.
            match agg_symbolized_path(&groupby_physical, &mut accumulators)? {
                Some(indices) => indices,
                None => agg_generic_hash_path(&groupby_physical, &mut accumulators)?,
            }
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

    /// Helper for groupby where the aggregated column is entirely null.
    fn make_all_null_vals_test_batch() -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let keys = Int64Array::from_iter(
            Field::new("key", DataType::Int64),
            vec![Some(1), Some(2), Some(1), Some(2), Some(1)],
        )
        .into_series();
        let vals = Int64Array::from_iter(
            Field::new("val", DataType::Int64),
            vec![None, None, None, None, None],
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

    #[test]
    fn test_inline_all_null_vals_min_matches_fallback() {
        let (rb, group_by, schema) = make_all_null_vals_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Min(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_all_null_vals_max_matches_fallback() {
        let (rb, group_by, schema) = make_all_null_vals_test_batch();
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

    // --- Byte-key fast path tests (Utf8 and Binary) ---

    /// Helper for Utf8-keyed groupby with NO null keys (exercises the
    /// `null_count == 0` fast branch in `agg_single_col_bytes`).
    fn make_utf8_key_no_nulls_test_batch() -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let keys = Series::from_arrow(
            Arc::new(Field::new("key", DataType::Utf8)),
            Arc::new(arrow::array::LargeStringArray::from(vec![
                "alpha", "beta", "alpha", "gamma", "beta", "alpha",
            ])),
        )
        .unwrap();
        let vals = Int64Array::from_iter(
            Field::new("val", DataType::Int64),
            vec![Some(10), Some(20), None, Some(40), Some(50), Some(60)],
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

    /// Helper for Utf8-keyed groupby with null keys.
    fn make_utf8_key_with_nulls_test_batch() -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let keys = Series::from_arrow(
            Arc::new(Field::new("key", DataType::Utf8)),
            Arc::new(arrow::array::LargeStringArray::from(vec![
                Some("alpha"),
                None,
                Some("alpha"),
                Some(""),
                None,
                Some("beta"),
            ])),
        )
        .unwrap();
        let vals = Int64Array::from_iter(
            Field::new("val", DataType::Int64),
            vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)],
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

    /// Helper for Binary-keyed groupby (with a null key and an empty key).
    fn make_binary_key_test_batch() -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let keys = Series::from_arrow(
            Arc::new(Field::new("key", DataType::Binary)),
            Arc::new(arrow::array::LargeBinaryArray::from_opt_vec(vec![
                Some(b"hello".as_ref()),
                Some(b"world".as_ref()),
                Some(b"hello".as_ref()),
                None,
                Some(b"".as_ref()),
                Some(b"hello".as_ref()),
                None,
            ])),
        )
        .unwrap();
        let vals = Int64Array::from_iter(
            Field::new("val", DataType::Int64),
            vec![
                Some(10),
                Some(20),
                Some(30),
                Some(40),
                Some(50),
                Some(60),
                Some(70),
            ],
        )
        .into_series();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Binary),
            Field::new("val", DataType::Int64),
        ]);
        let rb = RecordBatch::from_nonempty_columns(vec![keys, vals]).unwrap();
        let group_by = vec![BoundExpr::try_new(resolved_col("key"), &schema).unwrap()];
        (rb, group_by, schema)
    }

    #[test]
    fn test_inline_utf8_key_no_nulls_count_matches_fallback() {
        let (rb, group_by, schema) = make_utf8_key_no_nulls_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_utf8_key_no_nulls_sum_matches_fallback() {
        let (rb, group_by, schema) = make_utf8_key_no_nulls_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_utf8_key_no_nulls_max_matches_fallback() {
        let (rb, group_by, schema) = make_utf8_key_no_nulls_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Max(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_utf8_key_with_nulls_count_matches_fallback() {
        let (rb, group_by, schema) = make_utf8_key_with_nulls_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_utf8_key_with_nulls_sum_matches_fallback() {
        let (rb, group_by, schema) = make_utf8_key_with_nulls_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_utf8_key_with_nulls_min_matches_fallback() {
        let (rb, group_by, schema) = make_utf8_key_with_nulls_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Min(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_binary_key_count_matches_fallback() {
        let (rb, group_by, schema) = make_binary_key_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_binary_key_sum_matches_fallback() {
        let (rb, group_by, schema) = make_binary_key_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    #[test]
    fn test_inline_binary_key_max_matches_fallback() {
        let (rb, group_by, schema) = make_binary_key_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Max(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal(&inline_result, &fallback_result);
    }

    // --- Multi-column string+int tests (exercises symbolized path) ---

    /// Helper for multi-column groupby with Utf8 + Int64 keys.
    fn make_multi_col_string_test_batch() -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let key1 = Series::from_arrow(
            Arc::new(Field::new("key1", DataType::Utf8)),
            Arc::new(arrow::array::LargeStringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("b"),
                Some("b"),
                Some("a"),
            ])),
        )
        .unwrap();
        let key2 = Int64Array::from_iter(
            Field::new("key2", DataType::Int64),
            vec![Some(1), Some(2), Some(1), Some(2), Some(1)],
        )
        .into_series();
        let vals = Int64Array::from_iter(
            Field::new("val", DataType::Int64),
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        )
        .into_series();
        let schema = Schema::new(vec![
            Field::new("key1", DataType::Utf8),
            Field::new("key2", DataType::Int64),
            Field::new("val", DataType::Int64),
        ]);
        let rb = RecordBatch::from_nonempty_columns(vec![key1, key2, vals]).unwrap();
        let group_by = vec![
            BoundExpr::try_new(resolved_col("key1"), &schema).unwrap(),
            BoundExpr::try_new(resolved_col("key2"), &schema).unwrap(),
        ];
        (rb, group_by, schema)
    }

    /// Helper for multi-column groupby with Utf8 + Int64 keys, some null keys.
    fn make_multi_col_string_with_nulls_test_batch() -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let key1 = Series::from_arrow(
            Arc::new(Field::new("key1", DataType::Utf8)),
            Arc::new(arrow::array::LargeStringArray::from(vec![
                Some("a"),
                None,
                Some("a"),
                None,
                Some("a"),
            ])),
        )
        .unwrap();
        let key2 = Int64Array::from_iter(
            Field::new("key2", DataType::Int64),
            vec![Some(1), Some(1), None, None, Some(1)],
        )
        .into_series();
        let vals = Int64Array::from_iter(
            Field::new("val", DataType::Int64),
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        )
        .into_series();
        let schema = Schema::new(vec![
            Field::new("key1", DataType::Utf8),
            Field::new("key2", DataType::Int64),
            Field::new("val", DataType::Int64),
        ]);
        let rb = RecordBatch::from_nonempty_columns(vec![key1, key2, vals]).unwrap();
        let group_by = vec![
            BoundExpr::try_new(resolved_col("key1"), &schema).unwrap(),
            BoundExpr::try_new(resolved_col("key2"), &schema).unwrap(),
        ];
        (rb, group_by, schema)
    }

    /// Helper for multi-column groupby with all Utf8 keys.
    fn make_multi_col_all_string_test_batch() -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let key1 = Series::from_arrow(
            Arc::new(Field::new("key1", DataType::Utf8)),
            Arc::new(arrow::array::LargeStringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("b"),
                Some("b"),
                Some("a"),
            ])),
        )
        .unwrap();
        let key2 = Series::from_arrow(
            Arc::new(Field::new("key2", DataType::Utf8)),
            Arc::new(arrow::array::LargeStringArray::from(vec![
                Some("x"),
                Some("y"),
                Some("x"),
                Some("y"),
                Some("x"),
            ])),
        )
        .unwrap();
        let vals = Int64Array::from_iter(
            Field::new("val", DataType::Int64),
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        )
        .into_series();
        let schema = Schema::new(vec![
            Field::new("key1", DataType::Utf8),
            Field::new("key2", DataType::Utf8),
            Field::new("val", DataType::Int64),
        ]);
        let rb = RecordBatch::from_nonempty_columns(vec![key1, key2, vals]).unwrap();
        let group_by = vec![
            BoundExpr::try_new(resolved_col("key1"), &schema).unwrap(),
            BoundExpr::try_new(resolved_col("key2"), &schema).unwrap(),
        ];
        (rb, group_by, schema)
    }

    fn sort_by_keys(rb: &RecordBatch, key_names: &[&str]) -> RecordBatch {
        let sort_exprs: Vec<_> = key_names
            .iter()
            .map(|name| BoundExpr::try_new(resolved_col(*name), rb.schema.as_ref()).unwrap())
            .collect();
        let descending = vec![false; key_names.len()];
        let nulls_first = vec![false; key_names.len()];
        rb.sort(&sort_exprs, &descending, &nulls_first).unwrap()
    }

    fn assert_batches_equal_multi_key(a: &RecordBatch, b: &RecordBatch, keys: &[&str]) {
        let a = sort_by_keys(a, keys);
        let b = sort_by_keys(b, keys);
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

    #[test]
    fn test_inline_multi_col_string_int_count_matches_fallback() {
        let (rb, group_by, schema) = make_multi_col_string_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal_multi_key(&inline_result, &fallback_result, &["key1", "key2"]);
    }

    #[test]
    fn test_inline_multi_col_string_int_sum_matches_fallback() {
        let (rb, group_by, schema) = make_multi_col_string_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal_multi_key(&inline_result, &fallback_result, &["key1", "key2"]);
    }

    #[test]
    fn test_inline_multi_col_string_int_with_nulls_matches_fallback() {
        let (rb, group_by, schema) = make_multi_col_string_with_nulls_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
            BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap(),
            BoundAggExpr::try_new(AggExpr::Min(resolved_col("val")), &schema).unwrap(),
            BoundAggExpr::try_new(AggExpr::Max(resolved_col("val")), &schema).unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal_multi_key(&inline_result, &fallback_result, &["key1", "key2"]);
    }

    #[test]
    fn test_inline_multi_col_all_string_count_matches_fallback() {
        let (rb, group_by, schema) = make_multi_col_all_string_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal_multi_key(&inline_result, &fallback_result, &["key1", "key2"]);
    }

    #[test]
    fn test_inline_multi_col_all_string_sum_matches_fallback() {
        let (rb, group_by, schema) = make_multi_col_all_string_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal_multi_key(&inline_result, &fallback_result, &["key1", "key2"]);
    }

    #[test]
    fn test_inline_multi_col_string_int_min_max_matches_fallback() {
        let (rb, group_by, schema) = make_multi_col_string_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Min(resolved_col("val")), &schema).unwrap(),
            BoundAggExpr::try_new(AggExpr::Max(resolved_col("val")), &schema).unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal_multi_key(&inline_result, &fallback_result, &["key1", "key2"]);
    }

    // --- Multi-column Binary + Int tests (exercises symbolized path for Binary) ---

    /// Helper for multi-column groupby with Binary + Int64 keys.
    fn make_multi_col_binary_test_batch() -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let key1 = Series::from_arrow(
            Arc::new(Field::new("key1", DataType::Binary)),
            Arc::new(arrow::array::LargeBinaryArray::from(vec![
                Some(b"aa".as_slice()),
                Some(b"aa"),
                Some(b"bb"),
                Some(b"bb"),
                Some(b"aa"),
            ])),
        )
        .unwrap();
        let key2 = Int64Array::from_iter(
            Field::new("key2", DataType::Int64),
            vec![Some(1), Some(2), Some(1), Some(2), Some(1)],
        )
        .into_series();
        let vals = Int64Array::from_iter(
            Field::new("val", DataType::Int64),
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        )
        .into_series();
        let schema = Schema::new(vec![
            Field::new("key1", DataType::Binary),
            Field::new("key2", DataType::Int64),
            Field::new("val", DataType::Int64),
        ]);
        let rb = RecordBatch::from_nonempty_columns(vec![key1, key2, vals]).unwrap();
        let group_by = vec![
            BoundExpr::try_new(resolved_col("key1"), &schema).unwrap(),
            BoundExpr::try_new(resolved_col("key2"), &schema).unwrap(),
        ];
        (rb, group_by, schema)
    }

    #[test]
    fn test_inline_multi_col_binary_int_count_matches_fallback() {
        let (rb, group_by, schema) = make_multi_col_binary_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
        ];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal_multi_key(&inline_result, &fallback_result, &["key1", "key2"]);
    }

    #[test]
    fn test_inline_multi_col_binary_int_sum_matches_fallback() {
        let (rb, group_by, schema) = make_multi_col_binary_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap()];
        let inline_result = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();
        let fallback_result = rb.agg_groupby_fallback(&bound_agg, &group_by).unwrap();
        assert_batches_equal_multi_key(&inline_result, &fallback_result, &["key1", "key2"]);
    }
}
