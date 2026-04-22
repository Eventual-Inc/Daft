use std::{
    collections::hash_map::Entry::{Occupied, Vacant},
    hash::{BuildHasherDefault, Hash},
};

use common_error::DaftResult;
use daft_core::{
    array::ops::{arrow::comparison::build_multi_array_is_equal, as_arrow::AsArrow},
    count_mode::CountMode,
    datatypes::*,
    prelude::SchemaRef,
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
    is_null_type: bool,
}

impl CountAccum {
    fn new_from_source(source: &Series, mode: CountMode) -> Self {
        Self {
            counts: Vec::new(),
            mode,
            is_null_type: source.data_type() == &DataType::Null,
        }
    }

    fn new_from_dtype(dtype: &DataType, mode: CountMode) -> Self {
        Self {
            counts: Vec::new(),
            mode,
            is_null_type: *dtype == DataType::Null,
        }
    }

    fn init_groups(&mut self, n: usize) {
        self.counts.resize(n, 0);
    }

    /// Use pre-computed group sizes when no per-row null checking is needed.
    /// Returns true if the optimization was applied, false if caller must use update_batch.
    fn try_use_group_sizes(&mut self, group_sizes: &[u64], source: &Series) -> bool {
        let nulls = source.nulls();
        if self.is_null_type {
            match self.mode {
                CountMode::All | CountMode::Null => {
                    // Persistent: add group_sizes to existing counts
                    for (i, &sz) in group_sizes.iter().enumerate() {
                        self.counts[i] += sz;
                    }
                    return true;
                }
                CountMode::Valid => {
                    // Null type + Valid mode = always 0
                    return true;
                }
            }
        }
        match self.mode {
            CountMode::All => {
                for (i, &sz) in group_sizes.iter().enumerate() {
                    self.counts[i] += sz;
                }
                true
            }
            CountMode::Valid if nulls.is_none() => {
                for (i, &sz) in group_sizes.iter().enumerate() {
                    self.counts[i] += sz;
                }
                true
            }
            CountMode::Null if nulls.is_none() => {
                // No nulls → null count stays 0
                true
            }
            _ => false, // Has nulls — need per-row scatter loop
        }
    }

    /// Vectorized batch update: process all rows given a pre-computed group_ids array.
    fn update_batch(&mut self, group_ids: &[u32], source: &Series) {
        let counts = &mut self.counts;
        let nulls = source.nulls();
        match self.mode {
            CountMode::Valid => {
                if let Some(nulls) = nulls {
                    for (row_idx, &gid) in group_ids.iter().enumerate() {
                        counts[gid as usize] += nulls.is_valid(row_idx) as u64;
                    }
                } else {
                    // No nulls — every row is valid.
                    for &gid in group_ids {
                        counts[gid as usize] += 1;
                    }
                }
            }
            CountMode::Null => {
                if let Some(nulls) = nulls {
                    for (row_idx, &gid) in group_ids.iter().enumerate() {
                        counts[gid as usize] += !nulls.is_valid(row_idx) as u64;
                    }
                }
                // No nulls → null count stays 0, nothing to do.
            }
            CountMode::All => {
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
            field: Field,
        }

        impl $name {
            fn new(field: Field) -> Self {
                Self {
                    accumulators: Vec::new(),
                    field,
                }
            }

            fn init_groups(&mut self, n: usize) {
                self.accumulators.resize(n, None);
            }

            /// Vectorized batch update over pre-computed group_ids.
            fn update_batch(&mut self, group_ids: &[u32], source: &DataArray<$daft_type>) {
                let accs = &mut self.accumulators;
                if source.null_count() == 0 {
                    // Tight loop: no null checks needed on source values.
                    for (&gid, &val) in group_ids.iter().zip(source.values().iter()) {
                        let acc = &mut accs[gid as usize];
                        *acc = Some(match *acc {
                            Some(a) => a + val,
                            None => val,
                        });
                    }
                } else {
                    // Source has nulls: check each value.
                    for (row_idx, &gid) in group_ids.iter().enumerate() {
                        if let Some(val) = source.get(row_idx) {
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
                        self.field,
                        self.accumulators.into_iter(),
                    )
                    .rename(name)
                    .into_series())
                } else {
                    Ok(DataArray::<$daft_type>::from_field_and_values(
                        self.field,
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
            field: Field,
        }

        impl $name {
            fn new(field: Field) -> Self {
                Self {
                    accumulators: Vec::new(),
                    field,
                }
            }

            fn init_groups(&mut self, n: usize) {
                self.accumulators.resize(n, None);
            }

            fn update_batch(&mut self, group_ids: &[u32], source: &DataArray<$daft_type>) {
                let accs = &mut self.accumulators;
                let cmp_fn: fn($native, $native) -> $native = $cmp_fn;
                if source.null_count() == 0 {
                    for (&gid, &val) in group_ids.iter().zip(source.values().iter()) {
                        let acc = &mut accs[gid as usize];
                        *acc = Some(match *acc {
                            Some(a) => cmp_fn(a, val),
                            None => val,
                        });
                    }
                } else {
                    for (row_idx, &gid) in group_ids.iter().enumerate() {
                        if let Some(val) = source.get(row_idx) {
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
                        self.field,
                        self.accumulators.into_iter(),
                    )
                    .rename(name)
                    .into_series())
                } else {
                    Ok(DataArray::<$daft_type>::from_field_and_values(
                        self.field,
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
///   - `finalize(self, name: &str) -> DaftResult<Series>`
///
/// Note: `update_batch` is NOT generated here because accumulator types
/// have non-uniform signatures (Count takes `&Series`, Sum/Min/Max take
/// `&DataArray<T>`). See `update_batch_with_source` on `AggAccumulator`.
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
    fn try_use_group_sizes(&mut self, group_sizes: &[u64], source: &Series) -> bool {
        match self {
            Self::Count(s) => s.try_use_group_sizes(group_sizes, source),
            _ => false,
        }
    }

    /// Vectorized batch update: tight loop per accumulator type over group_ids.
    fn update_batch_with_source(&mut self, group_ids: &[u32], source: &Series) -> DaftResult<()> {
        match self {
            Self::Count(s) => s.update_batch(group_ids, source),
            Self::SumI64(s) => {
                let casted = source.cast(&DataType::Int64)?;
                s.update_batch(group_ids, casted.i64()?);
            }
            Self::SumU64(s) => {
                let casted = source.cast(&DataType::UInt64)?;
                s.update_batch(group_ids, casted.u64()?);
            }
            Self::SumF32(s) => s.update_batch(group_ids, source.downcast::<Float32Array>()?),
            Self::SumF64(s) => s.update_batch(group_ids, source.downcast::<Float64Array>()?),
            Self::MinI8(s) => s.update_batch(group_ids, source.downcast::<Int8Array>()?),
            Self::MinI16(s) => s.update_batch(group_ids, source.downcast::<Int16Array>()?),
            Self::MinI32(s) => s.update_batch(group_ids, source.downcast::<Int32Array>()?),
            Self::MinI64(s) => s.update_batch(group_ids, source.downcast::<Int64Array>()?),
            Self::MinU8(s) => s.update_batch(group_ids, source.downcast::<UInt8Array>()?),
            Self::MinU16(s) => s.update_batch(group_ids, source.downcast::<UInt16Array>()?),
            Self::MinU32(s) => s.update_batch(group_ids, source.downcast::<UInt32Array>()?),
            Self::MinU64(s) => s.update_batch(group_ids, source.downcast::<UInt64Array>()?),
            Self::MinF32(s) => s.update_batch(group_ids, source.downcast::<Float32Array>()?),
            Self::MinF64(s) => s.update_batch(group_ids, source.downcast::<Float64Array>()?),
            Self::MaxI8(s) => s.update_batch(group_ids, source.downcast::<Int8Array>()?),
            Self::MaxI16(s) => s.update_batch(group_ids, source.downcast::<Int16Array>()?),
            Self::MaxI32(s) => s.update_batch(group_ids, source.downcast::<Int32Array>()?),
            Self::MaxI64(s) => s.update_batch(group_ids, source.downcast::<Int64Array>()?),
            Self::MaxU8(s) => s.update_batch(group_ids, source.downcast::<UInt8Array>()?),
            Self::MaxU16(s) => s.update_batch(group_ids, source.downcast::<UInt16Array>()?),
            Self::MaxU32(s) => s.update_batch(group_ids, source.downcast::<UInt32Array>()?),
            Self::MaxU64(s) => s.update_batch(group_ids, source.downcast::<UInt64Array>()?),
            Self::MaxF32(s) => s.update_batch(group_ids, source.downcast::<Float32Array>()?),
            Self::MaxF64(s) => s.update_batch(group_ids, source.downcast::<Float64Array>()?),
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Factory: create accumulator from a BoundAggExpr
// ---------------------------------------------------------------------------

/// Create an accumulator from a BoundAggExpr, evaluating the source expression.
/// Returns (accumulator, output_name, evaluated_source) for the one-shot path.
fn try_create_accumulator(
    agg_expr: &BoundAggExpr,
    source: &RecordBatch,
) -> DaftResult<Option<(AggAccumulator, String, Series)>> {
    match agg_expr.as_ref() {
        AggExpr::Count(expr, mode) => {
            let evaluated = source.eval_agg_child(expr)?;
            let name = evaluated.name().to_string();
            let acc = AggAccumulator::Count(CountAccum::new_from_source(&evaluated, *mode));
            Ok(Some((acc, name, evaluated)))
        }
        AggExpr::Sum(expr) | AggExpr::Min(expr) | AggExpr::Max(expr) => {
            let evaluated = source.eval_agg_child(expr)?;
            let name = evaluated.name().to_string();
            let acc = create_numeric_accumulator(agg_expr.as_ref(), &evaluated)?;
            match acc {
                Some(acc) => Ok(Some((acc, name, evaluated))),
                None => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

/// Create accumulator from expression metadata + schema (no data needed).
/// Used by InlineAggState for persistent accumulators.
fn try_create_accumulator_from_expr(
    agg_expr: &BoundAggExpr,
    schema: &SchemaRef,
) -> DaftResult<Option<(AggAccumulator, String)>> {
    match agg_expr.as_ref() {
        AggExpr::Count(expr, mode) => {
            let field = expr.to_field(schema)?;
            let name = field.name.to_string();
            let acc = AggAccumulator::Count(CountAccum::new_from_dtype(&field.dtype, *mode));
            Ok(Some((acc, name)))
        }
        AggExpr::Sum(expr) | AggExpr::Min(expr) | AggExpr::Max(expr) => {
            let field = expr.to_field(schema)?;
            let name = field.name.to_string();
            let acc = create_numeric_accumulator_from_field(agg_expr.as_ref(), &field)?;
            match acc {
                Some(acc) => Ok(Some((acc, name))),
                None => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

/// Helper: create a Sum/Min/Max accumulator from an evaluated series.
fn create_numeric_accumulator(
    agg_expr: &AggExpr,
    evaluated: &Series,
) -> DaftResult<Option<AggAccumulator>> {
    let dtype = evaluated.data_type();
    // For Sum, we need the widened field.
    let field = match agg_expr {
        AggExpr::Sum(_) => match dtype {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Field::new(evaluated.name(), DataType::Int64)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                Field::new(evaluated.name(), DataType::UInt64)
            }
            _ => Field::new(evaluated.name(), dtype.clone()),
        },
        _ => Field::new(evaluated.name(), dtype.clone()),
    };
    create_numeric_accumulator_from_field(agg_expr, &field)
}

/// Helper: create a Sum/Min/Max accumulator from dtype info only.
fn create_numeric_accumulator_from_field(
    agg_expr: &AggExpr,
    field: &Field,
) -> DaftResult<Option<AggAccumulator>> {
    match agg_expr {
        AggExpr::Sum(_) => match &field.dtype {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let f = Field::new(field.name.clone(), DataType::Int64);
                Ok(Some(AggAccumulator::SumI64(SumAccumI64::new(f))))
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                let f = Field::new(field.name.clone(), DataType::UInt64);
                Ok(Some(AggAccumulator::SumU64(SumAccumU64::new(f))))
            }
            DataType::Float32 => Ok(Some(AggAccumulator::SumF32(SumAccumF32::new(
                field.clone(),
            )))),
            DataType::Float64 => Ok(Some(AggAccumulator::SumF64(SumAccumF64::new(
                field.clone(),
            )))),
            _ => Ok(None),
        },
        AggExpr::Min(_) => match &field.dtype {
            DataType::Int8 => Ok(Some(AggAccumulator::MinI8(MinAccumI8::new(field.clone())))),
            DataType::Int16 => Ok(Some(AggAccumulator::MinI16(MinAccumI16::new(
                field.clone(),
            )))),
            DataType::Int32 => Ok(Some(AggAccumulator::MinI32(MinAccumI32::new(
                field.clone(),
            )))),
            DataType::Int64 => Ok(Some(AggAccumulator::MinI64(MinAccumI64::new(
                field.clone(),
            )))),
            DataType::UInt8 => Ok(Some(AggAccumulator::MinU8(MinAccumU8::new(field.clone())))),
            DataType::UInt16 => Ok(Some(AggAccumulator::MinU16(MinAccumU16::new(
                field.clone(),
            )))),
            DataType::UInt32 => Ok(Some(AggAccumulator::MinU32(MinAccumU32::new(
                field.clone(),
            )))),
            DataType::UInt64 => Ok(Some(AggAccumulator::MinU64(MinAccumU64::new(
                field.clone(),
            )))),
            DataType::Float32 => Ok(Some(AggAccumulator::MinF32(MinAccumF32::new(
                field.clone(),
            )))),
            DataType::Float64 => Ok(Some(AggAccumulator::MinF64(MinAccumF64::new(
                field.clone(),
            )))),
            _ => Ok(None),
        },
        AggExpr::Max(_) => match &field.dtype {
            DataType::Int8 => Ok(Some(AggAccumulator::MaxI8(MaxAccumI8::new(field.clone())))),
            DataType::Int16 => Ok(Some(AggAccumulator::MaxI16(MaxAccumI16::new(
                field.clone(),
            )))),
            DataType::Int32 => Ok(Some(AggAccumulator::MaxI32(MaxAccumI32::new(
                field.clone(),
            )))),
            DataType::Int64 => Ok(Some(AggAccumulator::MaxI64(MaxAccumI64::new(
                field.clone(),
            )))),
            DataType::UInt8 => Ok(Some(AggAccumulator::MaxU8(MaxAccumU8::new(field.clone())))),
            DataType::UInt16 => Ok(Some(AggAccumulator::MaxU16(MaxAccumU16::new(
                field.clone(),
            )))),
            DataType::UInt32 => Ok(Some(AggAccumulator::MaxU32(MaxAccumU32::new(
                field.clone(),
            )))),
            DataType::UInt64 => Ok(Some(AggAccumulator::MaxU64(MaxAccumU64::new(
                field.clone(),
            )))),
            DataType::Float32 => Ok(Some(AggAccumulator::MaxF32(MaxAccumF32::new(
                field.clone(),
            )))),
            DataType::Float64 => Ok(Some(AggAccumulator::MaxF64(MaxAccumF64::new(
                field.clone(),
            )))),
            _ => Ok(None),
        },
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
    can_inline_agg_with_schema(to_agg, &source.schema)
}

/// Schema-only version of `can_inline_agg` for use without data.
pub fn can_inline_agg_with_schema(to_agg: &[BoundAggExpr], schema: &SchemaRef) -> bool {
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
            if let Ok(field) = expr.to_field(schema) {
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
fn accumulate(accumulators: &mut [(AggAccumulator, Series)], result: &GroupingResult) {
    let num_groups = result.group_sizes.len() as u32;
    for (acc, source) in accumulators.iter_mut() {
        acc.init_groups(num_groups);
        // Try O(groups) path first; fall back to O(rows) scatter loop.
        if !acc.try_use_group_sizes(&result.group_sizes, source) {
            acc.update_batch_with_source(&result.group_ids, source)
                .expect("update_batch_with_source failed in accumulate");
        }
    }
}

// ---------------------------------------------------------------------------
// Single-column integer fast path (FNV hash, no comparator closure)
// ---------------------------------------------------------------------------

fn agg_single_col_int<T>(
    keys: &DataArray<T>,
    accumulators: &mut [(AggAccumulator, Series)],
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
    accumulators: &mut [(AggAccumulator, Series)],
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
    accumulators: &mut [(AggAccumulator, Series)],
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
// InlineAggState — persistent hash table + accumulators across batches
// ---------------------------------------------------------------------------

/// Persistent aggregation state that can receive multiple batches without
/// rebuilding the hash table each time.
pub struct InlineAggState {
    accumulators: Vec<AggAccumulator>,
    output_names: Vec<String>,
    grouping: GroupingState,
    group_sizes: Vec<u64>,
    num_groups: u32,
    group_by: Vec<BoundExpr>,
    agg_exprs: Vec<BoundAggExpr>,
}

enum GroupingState {
    /// Single non-nullable integer column — FNV hash map.
    SingleColIntNonNull(SingleColIntNonNullState),
    /// Single nullable integer column — FNV hash map with Option keys.
    SingleColIntNullable(SingleColIntNullableState),
    /// Multi-column or non-integer — generic hash table with comparators.
    Generic(GenericGroupingState),
}

/// Macro to generate the single-column integer state enum variants.
macro_rules! define_single_col_int_state {
    ($name:ident, $nullable_name:ident, $( $variant:ident($native:ty) ),+ $(,)? ) => {
        enum $name {
            $( $variant(FnvHashMap<$native, u32>), )+
        }

        enum $nullable_name {
            $( $variant(FnvHashMap<Option<$native>, u32>), )+
        }
    };
}

define_single_col_int_state!(
    SingleColIntNonNullState,
    SingleColIntNullableState,
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
);

struct GenericGroupingState {
    group_table: HashMap<IndexHash, u32, IdentityBuildHasher>,
    /// One row per group, in group_id order, storing representative key values.
    representative_keys: Option<RecordBatch>,
}

/// Helper macro to probe a single-col int map for a batch.
macro_rules! probe_single_col_int_map {
    ($map:expr, $keys:expr, $num_groups:expr, $group_ids:expr, $group_sizes:expr, $groupkey_indices:expr) => {{
        for (row_idx, val) in $keys.values().iter().enumerate() {
            let gid = match $map.entry(*val) {
                Vacant(e) => {
                    let gid = *$num_groups;
                    *$num_groups = gid.checked_add(1).ok_or_else(|| {
                        common_error::DaftError::ComputeError(
                            "Number of groups exceeds u32::MAX in inline aggregation".into(),
                        )
                    })?;
                    e.insert(gid);
                    $groupkey_indices.push(row_idx as u64);
                    $group_sizes.push(1);
                    gid
                }
                Occupied(e) => {
                    let gid = *e.get();
                    $group_sizes[gid as usize] += 1;
                    gid
                }
            };
            $group_ids.push(gid);
        }
    }};
}

/// Helper macro to probe a nullable single-col int map for a batch.
macro_rules! probe_single_col_int_nullable_map {
    ($map:expr, $keys:expr, $num_groups:expr, $group_ids:expr, $group_sizes:expr, $groupkey_indices:expr) => {{
        for (row_idx, val) in $keys.into_iter().enumerate() {
            let gid = match $map.entry(val) {
                Vacant(e) => {
                    let gid = *$num_groups;
                    *$num_groups = gid.checked_add(1).ok_or_else(|| {
                        common_error::DaftError::ComputeError(
                            "Number of groups exceeds u32::MAX in inline aggregation".into(),
                        )
                    })?;
                    e.insert(gid);
                    $groupkey_indices.push(row_idx as u64);
                    $group_sizes.push(1);
                    gid
                }
                Occupied(e) => {
                    let gid = *e.get();
                    $group_sizes[gid as usize] += 1;
                    gid
                }
            };
            $group_ids.push(gid);
        }
    }};
}

impl InlineAggState {
    /// Try to create an InlineAggState. Returns None if any agg expression
    /// is not inline-eligible.
    pub fn try_new(
        agg_exprs: &[BoundAggExpr],
        group_by: &[BoundExpr],
        schema: &SchemaRef,
    ) -> DaftResult<Option<Self>> {
        if !can_inline_agg_with_schema(agg_exprs, schema) {
            return Ok(None);
        }

        let mut accumulators = Vec::with_capacity(agg_exprs.len());
        let mut output_names = Vec::with_capacity(agg_exprs.len());

        for agg_expr in agg_exprs {
            let (acc, name) =
                try_create_accumulator_from_expr(agg_expr, schema)?.ok_or_else(|| {
                    common_error::DaftError::ComputeError(
                        "InlineAggState: unsupported agg type; this is a bug".into(),
                    )
                })?;
            accumulators.push(acc);
            output_names.push(name);
        }

        // Determine grouping strategy based on group_by expressions.
        // We'll determine single-col-int vs generic on first batch.
        // For now, start with Generic and possibly specialize on first push.
        let grouping = GroupingState::Generic(GenericGroupingState {
            group_table: HashMap::with_capacity_and_hasher(1024, Default::default()),
            representative_keys: None,
        });

        Ok(Some(Self {
            accumulators,
            output_names,
            grouping,
            group_sizes: Vec::new(),
            num_groups: 0,
            group_by: group_by.to_vec(),
            agg_exprs: agg_exprs.to_vec(),
        }))
    }

    /// Push a batch into the persistent state, updating accumulators.
    pub fn push_batch(&mut self, batch: &RecordBatch) -> DaftResult<()> {
        // 1. Evaluate group-by columns.
        let groupby_table = batch.eval_expression_list(&self.group_by)?;
        let groupby_physical = groupby_table.as_physical()?;

        // 2. Evaluate agg sources.
        let sources: Vec<Series> = self
            .agg_exprs
            .iter()
            .map(|agg_expr| match agg_expr.as_ref() {
                AggExpr::Count(expr, _)
                | AggExpr::Sum(expr)
                | AggExpr::Min(expr)
                | AggExpr::Max(expr) => batch.eval_agg_child(expr),
                _ => unreachable!("InlineAggState only handles Count/Sum/Min/Max"),
            })
            .collect::<DaftResult<Vec<_>>>()?;

        // 3. Try to specialize grouping on first batch.
        self.maybe_specialize_grouping(&groupby_physical)?;

        // 4. Probe hash table and get group_ids.
        let (group_ids, groupkey_indices) = match &mut self.grouping {
            GroupingState::SingleColIntNonNull(state) => probe_single_col_int_non_null(
                state,
                &groupby_physical,
                &mut self.num_groups,
                &mut self.group_sizes,
            )?,
            GroupingState::SingleColIntNullable(state) => probe_single_col_int_nullable(
                state,
                &groupby_physical,
                &mut self.num_groups,
                &mut self.group_sizes,
            )?,
            GroupingState::Generic(state) => probe_generic(
                state,
                &groupby_physical,
                &mut self.num_groups,
                &mut self.group_sizes,
            )?,
        };

        // 5. Update accumulators.
        let old_num_groups = self.accumulators.first().map_or(0, accum_len);

        let new_num_groups = self.num_groups as usize;
        if new_num_groups > old_num_groups {
            for acc in self.accumulators.iter_mut() {
                acc.init_groups(new_num_groups as u32);
            }
        }

        // In the persistent path, always use the scatter loop (update_batch_with_source).
        // The try_use_group_sizes optimization only works for the one-shot path where
        // group_sizes represent the final per-group counts, not cumulative state.
        for (acc, source) in self.accumulators.iter_mut().zip(sources.iter()) {
            acc.update_batch_with_source(&group_ids, source)?;
        }

        // 6. Update representative keys for generic path.
        if let GroupingState::Generic(state) = &mut self.grouping {
            if !groupkey_indices.is_empty() {
                let new_keys_indices = UInt64Array::from_vec("", groupkey_indices);
                let new_rep_keys = groupby_table.take(&new_keys_indices)?;
                state.representative_keys = Some(match state.representative_keys.take() {
                    Some(existing) => RecordBatch::concat(&[existing, new_rep_keys])?,
                    None => new_rep_keys,
                });
            }
        }

        Ok(())
    }

    /// Finalize and produce the output RecordBatch.
    pub fn finalize(self) -> DaftResult<RecordBatch> {
        let InlineAggState {
            accumulators,
            output_names,
            grouping,
            num_groups,
            group_by,
            ..
        } = self;

        if num_groups == 0 {
            // No data was pushed; return empty with correct schema.
            return Ok(RecordBatch::empty(None));
        }

        // Get group key columns from representative keys or by reconstructing.
        let group_key_series: Vec<Series> = match grouping {
            GroupingState::Generic(state) => match state.representative_keys {
                Some(rep) => rep.as_materialized_series().into_iter().cloned().collect(),
                None => Vec::new(),
            },
            GroupingState::SingleColIntNonNull(state) => {
                reconstruct_single_col_keys_non_null(num_groups, &group_by, state)
            }
            GroupingState::SingleColIntNullable(state) => {
                reconstruct_single_col_keys_nullable(num_groups, &group_by, state)
            }
        };

        let grouped_cols: Vec<Series> = accumulators
            .into_iter()
            .zip(output_names.iter())
            .map(|(acc, name)| acc.finalize(name))
            .collect::<DaftResult<Vec<_>>>()?;

        let all_series: Vec<Series> = group_key_series.into_iter().chain(grouped_cols).collect();
        RecordBatch::from_nonempty_columns(all_series)
    }

    pub fn num_groups(&self) -> u32 {
        self.num_groups
    }

    /// On first batch, decide whether to use single-col int specialization.
    fn maybe_specialize_grouping(&mut self, groupby_physical: &RecordBatch) -> DaftResult<()> {
        // Only specialize if still in Generic state with no data yet.
        if self.num_groups > 0 {
            return Ok(());
        }
        if let GroupingState::Generic(_) = &self.grouping {
            if groupby_physical.num_columns() == 1 {
                let col = groupby_physical.get_column(0);
                let cap = std::cmp::min(groupby_physical.len(), 1024).max(1);
                if col.nulls().is_none() || col.nulls().is_some_and(|n| n.null_count() == 0) {
                    // Try non-nullable specialization.
                    let specialized = match col.data_type() {
                        DataType::Int8 => Some(GroupingState::SingleColIntNonNull(
                            SingleColIntNonNullState::I8(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::Int16 => Some(GroupingState::SingleColIntNonNull(
                            SingleColIntNonNullState::I16(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::Int32 => Some(GroupingState::SingleColIntNonNull(
                            SingleColIntNonNullState::I32(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::Int64 => Some(GroupingState::SingleColIntNonNull(
                            SingleColIntNonNullState::I64(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::UInt8 => Some(GroupingState::SingleColIntNonNull(
                            SingleColIntNonNullState::U8(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::UInt16 => Some(GroupingState::SingleColIntNonNull(
                            SingleColIntNonNullState::U16(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::UInt32 => Some(GroupingState::SingleColIntNonNull(
                            SingleColIntNonNullState::U32(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::UInt64 => Some(GroupingState::SingleColIntNonNull(
                            SingleColIntNonNullState::U64(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        _ => None,
                    };
                    if let Some(s) = specialized {
                        self.grouping = s;
                    }
                } else {
                    // Nullable integer.
                    let specialized = match col.data_type() {
                        DataType::Int8 => Some(GroupingState::SingleColIntNullable(
                            SingleColIntNullableState::I8(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::Int16 => Some(GroupingState::SingleColIntNullable(
                            SingleColIntNullableState::I16(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::Int32 => Some(GroupingState::SingleColIntNullable(
                            SingleColIntNullableState::I32(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::Int64 => Some(GroupingState::SingleColIntNullable(
                            SingleColIntNullableState::I64(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::UInt8 => Some(GroupingState::SingleColIntNullable(
                            SingleColIntNullableState::U8(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::UInt16 => Some(GroupingState::SingleColIntNullable(
                            SingleColIntNullableState::U16(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::UInt32 => Some(GroupingState::SingleColIntNullable(
                            SingleColIntNullableState::U32(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        DataType::UInt64 => Some(GroupingState::SingleColIntNullable(
                            SingleColIntNullableState::U64(FnvHashMap::with_capacity_and_hasher(
                                cap,
                                BuildHasherDefault::default(),
                            )),
                        )),
                        _ => None,
                    };
                    if let Some(s) = specialized {
                        self.grouping = s;
                    }
                }
            }
        }
        Ok(())
    }
}

fn probe_single_col_int_non_null(
    state: &mut SingleColIntNonNullState,
    groupby_physical: &RecordBatch,
    num_groups: &mut u32,
    group_sizes: &mut Vec<u64>,
) -> DaftResult<(Vec<u32>, Vec<u64>)> {
    let col = groupby_physical.get_column(0);
    let len = col.len();
    let mut group_ids = Vec::with_capacity(len);
    let mut groupkey_indices = Vec::new();

    match col.data_type() {
        DataType::Int8 => {
            if let SingleColIntNonNullState::I8(map) = state {
                let keys = col.i8()?;
                probe_single_col_int_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::Int16 => {
            if let SingleColIntNonNullState::I16(map) = state {
                let keys = col.i16()?;
                probe_single_col_int_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::Int32 => {
            if let SingleColIntNonNullState::I32(map) = state {
                let keys = col.i32()?;
                probe_single_col_int_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::Int64 => {
            if let SingleColIntNonNullState::I64(map) = state {
                let keys = col.i64()?;
                probe_single_col_int_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::UInt8 => {
            if let SingleColIntNonNullState::U8(map) = state {
                let keys = col.u8()?;
                probe_single_col_int_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::UInt16 => {
            if let SingleColIntNonNullState::U16(map) = state {
                let keys = col.u16()?;
                probe_single_col_int_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::UInt32 => {
            if let SingleColIntNonNullState::U32(map) = state {
                let keys = col.u32()?;
                probe_single_col_int_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::UInt64 => {
            if let SingleColIntNonNullState::U64(map) = state {
                let keys = col.u64()?;
                probe_single_col_int_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        _ => unreachable!("SingleColIntNonNull only for integer types"),
    }

    Ok((group_ids, groupkey_indices))
}

fn probe_single_col_int_nullable(
    state: &mut SingleColIntNullableState,
    groupby_physical: &RecordBatch,
    num_groups: &mut u32,
    group_sizes: &mut Vec<u64>,
) -> DaftResult<(Vec<u32>, Vec<u64>)> {
    let col = groupby_physical.get_column(0);
    let len = col.len();
    let mut group_ids = Vec::with_capacity(len);
    let mut groupkey_indices = Vec::new();

    match col.data_type() {
        DataType::Int8 => {
            if let SingleColIntNullableState::I8(map) = state {
                let keys = col.i8()?;
                probe_single_col_int_nullable_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::Int16 => {
            if let SingleColIntNullableState::I16(map) = state {
                let keys = col.i16()?;
                probe_single_col_int_nullable_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::Int32 => {
            if let SingleColIntNullableState::I32(map) = state {
                let keys = col.i32()?;
                probe_single_col_int_nullable_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::Int64 => {
            if let SingleColIntNullableState::I64(map) = state {
                let keys = col.i64()?;
                probe_single_col_int_nullable_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::UInt8 => {
            if let SingleColIntNullableState::U8(map) = state {
                let keys = col.u8()?;
                probe_single_col_int_nullable_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::UInt16 => {
            if let SingleColIntNullableState::U16(map) = state {
                let keys = col.u16()?;
                probe_single_col_int_nullable_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::UInt32 => {
            if let SingleColIntNullableState::U32(map) = state {
                let keys = col.u32()?;
                probe_single_col_int_nullable_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        DataType::UInt64 => {
            if let SingleColIntNullableState::U64(map) = state {
                let keys = col.u64()?;
                probe_single_col_int_nullable_map!(
                    map,
                    keys,
                    num_groups,
                    group_ids,
                    group_sizes,
                    groupkey_indices
                );
            }
        }
        _ => unreachable!("SingleColIntNullable only for integer types"),
    }

    Ok((group_ids, groupkey_indices))
}

fn probe_generic(
    state: &mut GenericGroupingState,
    groupby_physical: &RecordBatch,
    num_groups: &mut u32,
    group_sizes: &mut Vec<u64>,
) -> DaftResult<(Vec<u32>, Vec<u64>)> {
    let num_rows = groupby_physical.len();
    let hashes = groupby_physical.hash_rows()?;
    let new_cols: Vec<Series> = groupby_physical
        .as_materialized_series()
        .into_iter()
        .cloned()
        .collect();

    let num_cols = new_cols.len();
    let g = *num_groups; // Number of existing groups before this batch.

    // Build two comparators:
    // cross_cmp: new_batch row i vs representative row j
    // self_cmp: new_batch row i vs new_batch row j
    let self_cmp = build_multi_array_is_equal(
        new_cols.as_slice(),
        new_cols.as_slice(),
        vec![true; num_cols].as_slice(),
        vec![true; num_cols].as_slice(),
    )?;

    let cross_cmp = if let Some(ref rep) = state.representative_keys {
        let rep_cols: Vec<Series> = rep.as_materialized_series().into_iter().cloned().collect();
        Some(build_multi_array_is_equal(
            new_cols.as_slice(),
            rep_cols.as_slice(),
            vec![true; num_cols].as_slice(),
            vec![true; num_cols].as_slice(),
        )?)
    } else {
        None
    };

    let mut group_ids = Vec::with_capacity(num_rows);
    let mut new_group_batch_rows: Vec<u64> = Vec::new();

    for (row_idx, h) in hashes.values().iter().enumerate() {
        let entry = state.group_table.raw_entry_mut().from_hash(*h, |other| {
            (*h == other.hash) && {
                let j = other.idx;
                if j < g as u64 {
                    cross_cmp.as_ref().unwrap()(row_idx, j as usize)
                } else {
                    self_cmp(row_idx, (j - g as u64) as usize)
                }
            }
        });

        let gid = match entry {
            RawEntryMut::Vacant(entry) => {
                let gid = *num_groups;
                *num_groups = gid.checked_add(1).ok_or_else(|| {
                    common_error::DaftError::ComputeError(
                        "Number of groups exceeds u32::MAX in inline aggregation".into(),
                    )
                })?;
                entry.insert_hashed_nocheck(
                    *h,
                    IndexHash {
                        idx: g as u64 + row_idx as u64,
                        hash: *h,
                    },
                    gid,
                );
                new_group_batch_rows.push(row_idx as u64);
                group_sizes.push(1);
                gid
            }
            RawEntryMut::Occupied(entry) => {
                let gid = *entry.get();
                group_sizes[gid as usize] += 1;
                gid
            }
        };
        group_ids.push(gid);
    }

    // Remap new entries' idx from (g + batch_row_idx) to (g + discovery_order).
    if !new_group_batch_rows.is_empty() {
        let mut remap: FnvHashMap<u64, u64> = FnvHashMap::with_capacity_and_hasher(
            new_group_batch_rows.len(),
            BuildHasherDefault::default(),
        );
        for (order, &batch_row) in new_group_batch_rows.iter().enumerate() {
            remap.insert(batch_row, order as u64);
        }

        let base = match &state.representative_keys {
            Some(rep) => rep.len() as u64,
            None => 0,
        };

        for bucket in state.group_table.iter_mut() {
            let idx_hash = bucket.0;
            if idx_hash.idx >= g as u64 {
                let batch_row = idx_hash.idx - g as u64;
                if let Some(&order) = remap.get(&batch_row) {
                    // SAFETY: we're modifying the key's idx field but not its hash,
                    // so the bucket position remains valid.
                    unsafe {
                        let key_ptr = bucket.0 as *const IndexHash as *mut IndexHash;
                        (*key_ptr).idx = base + order;
                    }
                }
            }
        }
    }

    Ok((group_ids, new_group_batch_rows))
}

/// Reconstruct group keys for single-col int non-nullable path.
fn reconstruct_single_col_keys_non_null(
    num_groups: u32,
    group_by: &[BoundExpr],
    state: SingleColIntNonNullState,
) -> Vec<Series> {
    macro_rules! reconstruct {
        ($map:expr, $daft_type:ty, $name:expr) => {{
            let n = num_groups as usize;
            let mut vals: Vec<Option<<$daft_type as DaftNumericType>::Native>> = vec![None; n];
            for (key, gid) in $map.into_iter() {
                vals[gid as usize] = Some(key);
            }
            let series = DataArray::<$daft_type>::from_iter(
                Field::new(*$name, <$daft_type as DaftDataType>::get_dtype()),
                vals.into_iter(),
            )
            .into_series();
            vec![series]
        }};
    }

    let name = &group_by[0].as_ref().name();
    match state {
        SingleColIntNonNullState::I8(map) => reconstruct!(map, Int8Type, name),
        SingleColIntNonNullState::I16(map) => reconstruct!(map, Int16Type, name),
        SingleColIntNonNullState::I32(map) => reconstruct!(map, Int32Type, name),
        SingleColIntNonNullState::I64(map) => reconstruct!(map, Int64Type, name),
        SingleColIntNonNullState::U8(map) => reconstruct!(map, UInt8Type, name),
        SingleColIntNonNullState::U16(map) => reconstruct!(map, UInt16Type, name),
        SingleColIntNonNullState::U32(map) => reconstruct!(map, UInt32Type, name),
        SingleColIntNonNullState::U64(map) => reconstruct!(map, UInt64Type, name),
    }
}

/// Reconstruct group keys for single-col int nullable path.
fn reconstruct_single_col_keys_nullable(
    num_groups: u32,
    group_by: &[BoundExpr],
    state: SingleColIntNullableState,
) -> Vec<Series> {
    macro_rules! reconstruct {
        ($map:expr, $daft_type:ty, $name:expr) => {{
            let n = num_groups as usize;
            let mut vals: Vec<Option<<$daft_type as DaftNumericType>::Native>> = vec![None; n];
            for (key, gid) in $map.into_iter() {
                vals[gid as usize] = key;
            }
            let series = DataArray::<$daft_type>::from_iter(
                Field::new(*$name, <$daft_type as DaftDataType>::get_dtype()),
                vals.into_iter(),
            )
            .into_series();
            vec![series]
        }};
    }

    let name = &group_by[0].as_ref().name();
    match state {
        SingleColIntNullableState::I8(map) => reconstruct!(map, Int8Type, name),
        SingleColIntNullableState::I16(map) => reconstruct!(map, Int16Type, name),
        SingleColIntNullableState::I32(map) => reconstruct!(map, Int32Type, name),
        SingleColIntNullableState::I64(map) => reconstruct!(map, Int64Type, name),
        SingleColIntNullableState::U8(map) => reconstruct!(map, UInt8Type, name),
        SingleColIntNullableState::U16(map) => reconstruct!(map, UInt16Type, name),
        SingleColIntNullableState::U32(map) => reconstruct!(map, UInt32Type, name),
        SingleColIntNullableState::U64(map) => reconstruct!(map, UInt64Type, name),
    }
}

/// Get the length of an accumulator's internal storage.
fn accum_len(acc: &AggAccumulator) -> usize {
    match acc {
        AggAccumulator::Count(s) => s.counts.len(),
        AggAccumulator::SumI64(s) => s.accumulators.len(),
        AggAccumulator::SumU64(s) => s.accumulators.len(),
        AggAccumulator::SumF32(s) => s.accumulators.len(),
        AggAccumulator::SumF64(s) => s.accumulators.len(),
        AggAccumulator::MinI8(s) => s.accumulators.len(),
        AggAccumulator::MinI16(s) => s.accumulators.len(),
        AggAccumulator::MinI32(s) => s.accumulators.len(),
        AggAccumulator::MinI64(s) => s.accumulators.len(),
        AggAccumulator::MinU8(s) => s.accumulators.len(),
        AggAccumulator::MinU16(s) => s.accumulators.len(),
        AggAccumulator::MinU32(s) => s.accumulators.len(),
        AggAccumulator::MinU64(s) => s.accumulators.len(),
        AggAccumulator::MinF32(s) => s.accumulators.len(),
        AggAccumulator::MinF64(s) => s.accumulators.len(),
        AggAccumulator::MaxI8(s) => s.accumulators.len(),
        AggAccumulator::MaxI16(s) => s.accumulators.len(),
        AggAccumulator::MaxI32(s) => s.accumulators.len(),
        AggAccumulator::MaxI64(s) => s.accumulators.len(),
        AggAccumulator::MaxU8(s) => s.accumulators.len(),
        AggAccumulator::MaxU16(s) => s.accumulators.len(),
        AggAccumulator::MaxU32(s) => s.accumulators.len(),
        AggAccumulator::MaxU64(s) => s.accumulators.len(),
        AggAccumulator::MaxF32(s) => s.accumulators.len(),
        AggAccumulator::MaxF64(s) => s.accumulators.len(),
    }
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

        // 2. Create accumulators for each agg expression (with evaluated sources).
        let mut accumulators: Vec<(AggAccumulator, Series)> = Vec::with_capacity(to_agg.len());
        let mut output_names: Vec<String> = Vec::with_capacity(to_agg.len());

        for agg_expr in to_agg {
            let (acc, name, source) = try_create_accumulator(agg_expr, self)?.ok_or_else(|| {
                common_error::DaftError::ComputeError(
                    "Inline aggregation reached an unsupported type; this is a bug".into(),
                )
            })?;
            accumulators.push((acc, source));
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
            .map(|((acc, _source), name)| acc.finalize(name))
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

    // -----------------------------------------------------------------------
    // InlineAggState tests — persistent hash table across multiple batches
    // -----------------------------------------------------------------------

    use super::InlineAggState;

    /// Helper: split a RecordBatch into multiple smaller batches for multi-push testing.
    fn split_batch(rb: &RecordBatch, sizes: &[usize]) -> Vec<RecordBatch> {
        let mut result = Vec::new();
        let mut offset = 0;
        for &size in sizes {
            let end = std::cmp::min(offset + size, rb.len());
            if offset >= end {
                break;
            }
            let sliced = rb.slice(offset, end).unwrap();
            result.push(sliced);
            offset = end;
        }
        result
    }

    #[test]
    fn test_inline_agg_state_int_key_sum_multi_batch() {
        let (rb, group_by, schema) = make_int_key_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap()];

        // One-shot result for comparison.
        let expected = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();

        // Multi-batch via InlineAggState.
        let schema_ref: Arc<Schema> = Arc::new(schema);
        let mut state = InlineAggState::try_new(&bound_agg, &group_by, &schema_ref)
            .unwrap()
            .unwrap();

        let batches = split_batch(&rb, &[2, 2, 1]);
        for batch in &batches {
            state.push_batch(batch).unwrap();
        }
        let result = state.finalize().unwrap();

        assert_batches_equal(&result, &expected);
    }

    #[test]
    fn test_inline_agg_state_int_key_count_multi_batch() {
        let (rb, group_by, schema) = make_int_key_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
        ];

        let expected = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();

        let schema_ref: Arc<Schema> = Arc::new(schema);
        let mut state = InlineAggState::try_new(&bound_agg, &group_by, &schema_ref)
            .unwrap()
            .unwrap();

        let batches = split_batch(&rb, &[1, 1, 1, 1, 1]);
        for batch in &batches {
            state.push_batch(batch).unwrap();
        }
        let result = state.finalize().unwrap();

        assert_batches_equal(&result, &expected);
    }

    #[test]
    fn test_inline_agg_state_int_key_min_max_multi_batch() {
        let (rb, group_by, schema) = make_int_key_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Min(resolved_col("val")), &schema).unwrap(),
            BoundAggExpr::try_new(AggExpr::Max(resolved_col("val")), &schema).unwrap(),
        ];

        let expected = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();

        let schema_ref: Arc<Schema> = Arc::new(schema);
        let mut state = InlineAggState::try_new(&bound_agg, &group_by, &schema_ref)
            .unwrap()
            .unwrap();

        let batches = split_batch(&rb, &[3, 2]);
        for batch in &batches {
            state.push_batch(batch).unwrap();
        }
        let result = state.finalize().unwrap();

        assert_batches_equal(&result, &expected);
    }

    #[test]
    fn test_inline_agg_state_int_key_with_nulls_multi_batch() {
        let (rb, group_by, schema) = make_int_key_with_nulls_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
            BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap(),
        ];

        let expected = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();

        let schema_ref: Arc<Schema> = Arc::new(schema);
        let mut state = InlineAggState::try_new(&bound_agg, &group_by, &schema_ref)
            .unwrap()
            .unwrap();

        let batches = split_batch(&rb, &[2, 2, 1]);
        for batch in &batches {
            state.push_batch(batch).unwrap();
        }
        let result = state.finalize().unwrap();

        assert_batches_equal(&result, &expected);
    }

    #[test]
    fn test_inline_agg_state_string_key_generic_path() {
        let (rb, group_by, schema) = make_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
            BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap(),
            BoundAggExpr::try_new(AggExpr::Min(resolved_col("val")), &schema).unwrap(),
            BoundAggExpr::try_new(AggExpr::Max(resolved_col("val")), &schema).unwrap(),
        ];

        let expected = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();

        let schema_ref: Arc<Schema> = Arc::new(schema);
        let mut state = InlineAggState::try_new(&bound_agg, &group_by, &schema_ref)
            .unwrap()
            .unwrap();

        let batches = split_batch(&rb, &[2, 2, 1]);
        for batch in &batches {
            state.push_batch(batch).unwrap();
        }
        let result = state.finalize().unwrap();

        assert_batches_equal(&result, &expected);
    }

    #[test]
    fn test_inline_agg_state_multi_col_generic_path() {
        let (rb, group_by, schema) = make_multi_col_string_test_batch();
        let bound_agg =
            vec![BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap()];

        let expected = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();

        let schema_ref: Arc<Schema> = Arc::new(schema);
        let mut state = InlineAggState::try_new(&bound_agg, &group_by, &schema_ref)
            .unwrap()
            .unwrap();

        let batches = split_batch(&rb, &[2, 2, 1]);
        for batch in &batches {
            state.push_batch(batch).unwrap();
        }
        let result = state.finalize().unwrap();

        assert_batches_equal_multi_key(&result, &expected, &["key1", "key2"]);
    }

    #[test]
    fn test_inline_agg_state_single_batch_equals_one_shot() {
        let (rb, group_by, schema) = make_int_key_test_batch();
        let bound_agg = vec![
            BoundAggExpr::try_new(
                AggExpr::Count(resolved_col("val"), CountMode::Valid),
                &schema,
            )
            .unwrap(),
            BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap(),
        ];

        let expected = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();

        let schema_ref: Arc<Schema> = Arc::new(schema);
        let mut state = InlineAggState::try_new(&bound_agg, &group_by, &schema_ref)
            .unwrap()
            .unwrap();
        state.push_batch(&rb).unwrap();
        let result = state.finalize().unwrap();

        assert_batches_equal(&result, &expected);
    }

    #[test]
    fn test_inline_agg_state_count_valid_no_nulls_multi_batch() {
        // Regression test: CountMode::Valid with a source column that has no nulls
        // must still count rows correctly in the persistent path.
        let (rb, group_by, schema) = make_int_key_test_batch();
        let bound_agg = vec![BoundAggExpr::try_new(
            AggExpr::Count(resolved_col("val"), CountMode::Valid),
            &schema,
        )
        .unwrap()];

        let expected = rb.agg_groupby_inline(&bound_agg, &group_by).unwrap();

        let schema_ref: Arc<Schema> = Arc::new(schema);
        let mut state = InlineAggState::try_new(&bound_agg, &group_by, &schema_ref)
            .unwrap()
            .unwrap();

        let batches = split_batch(&rb, &[2, 2, 1]);
        for batch in &batches {
            state.push_batch(batch).unwrap();
        }
        let result = state.finalize().unwrap();

        assert_batches_equal(&result, &expected);
    }
}
