use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    marker::PhantomData,
};

use common_error::{DaftError, DaftResult};
use daft_core::{count_mode::CountMode, datatypes::DaftPrimitiveType, prelude::*};
use daft_dsl::AggExpr;
use num_traits::{FromPrimitive, Zero};

#[derive(Debug, Clone)]
struct IndexedValue<'a, T>
where
    T: DaftPrimitiveType,
{
    value: T::Native,
    idx: usize,
    _phantom: PhantomData<&'a ()>,
}

impl<T> Eq for IndexedValue<'_, T> where T: DaftPrimitiveType {}

impl<T> PartialEq for IndexedValue<'_, T>
where
    T: DaftPrimitiveType,
{
    fn eq(&self, other: &Self) -> bool {
        if self.idx != other.idx {
            return false;
        }

        self.value == other.value
    }
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl<T> PartialOrd for IndexedValue<'_, T>
where
    T: DaftPrimitiveType,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.value < other.value {
            return Some(Ordering::Less);
        }

        if self.value > other.value {
            return Some(Ordering::Greater);
        }

        Some(Ordering::Equal)
    }
}

impl<T> Ord for IndexedValue<'_, T>
where
    T: DaftPrimitiveType,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

/// Trait for window aggregation state common operations
pub trait WindowAggStateOps {
    /// Add a value to the state with index information
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()>;

    /// Remove a value from the state with index information
    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()>;

    /// Evaluate the current state and push the result to internal buffer
    fn evaluate(&mut self) -> DaftResult<()>;

    /// Build the final result series containing all accumulated results
    fn build(&self) -> DaftResult<Series>;
}

pub struct MeanWindowState<'a, T>
where
    T: DaftPrimitiveType,
{
    source: &'a Series,
    sum: T::Native,
    count: usize,
    field: Field,
    sum_vec: Vec<T::Native>,
    count_vec: Vec<usize>,
}

impl<'a, T> MeanWindowState<'a, T>
where
    T: DaftPrimitiveType,
{
    pub fn new(
        source: &'a Series,
        out_dtype: &DataType,
        agg_expr: &AggExpr,
        total_length: usize,
    ) -> Self {
        let name = match agg_expr {
            AggExpr::Mean(expr) => expr.name().to_string(),
            _ => panic!("Expected Mean aggregation"),
        };

        let field = Field::new(name, out_dtype.clone());

        Self {
            source,
            sum: T::Native::zero(),
            count: 0,
            field,
            sum_vec: Vec::with_capacity(total_length),
            count_vec: Vec::with_capacity(total_length),
        }
    }
}

impl<T> WindowAggStateOps for MeanWindowState<'_, T>
where
    T: DaftPrimitiveType,
{
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        let value = self.source.slice(start_idx, end_idx)?;

        if value.len() > 4 {
            let sum_result = value.sum(None)?;
            let count_result = value.count(None, CountMode::Valid)?;

            if !sum_result.is_empty() && !count_result.is_empty() {
                let count_val = count_result.u64()?.get(0).unwrap_or(0) as usize;

                if count_val > 0 {
                    let sum_val = T::Native::from_f64(
                        sum_result
                            .cast(&DataType::Float64)?
                            .f64()?
                            .get(0)
                            .unwrap_or(0.0),
                    )
                    .unwrap_or_else(T::Native::zero);
                    self.sum = self.sum + sum_val;
                    self.count += count_val;
                }
            }
            return Ok(());
        }

        for i in 0..value.len() {
            if value.is_valid(i) {
                let scalar_value = value.slice(i, i + 1)?;
                let val = match T::Native::from_f64(
                    scalar_value
                        .cast(&DataType::Float64)?
                        .f64()?
                        .get(0)
                        .unwrap_or(0.0),
                ) {
                    Some(val) => val,
                    None => T::Native::zero(),
                };
                self.sum = self.sum + val;
                self.count += 1;
            }
        }

        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        if self.count == 0 {
            return Ok(());
        }

        let value = self.source.slice(start_idx, end_idx)?;

        if value.len() > 4 {
            let sum_result = value.sum(None)?;
            let count_result = value.count(None, CountMode::Valid)?;

            if !sum_result.is_empty() && !count_result.is_empty() {
                let count_val = count_result.u64()?.get(0).unwrap_or(0) as usize;

                if count_val > 0 && self.count >= count_val {
                    let sum_val = match T::Native::from_f64(
                        sum_result
                            .cast(&DataType::Float64)?
                            .f64()?
                            .get(0)
                            .unwrap_or(0.0),
                    ) {
                        Some(val) => val,
                        None => T::Native::zero(),
                    };
                    self.sum = self.sum - sum_val;
                    self.count -= count_val;
                }
            }
            return Ok(());
        }

        for i in 0..value.len() {
            if value.is_valid(i) && self.count > 0 {
                let scalar_value = value.slice(i, i + 1)?;
                let val = match T::Native::from_f64(
                    scalar_value
                        .cast(&DataType::Float64)?
                        .f64()?
                        .get(0)
                        .unwrap_or(0.0),
                ) {
                    Some(val) => val,
                    None => T::Native::zero(),
                };
                self.sum = self.sum - val;
                self.count -= 1;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        self.sum_vec.push(self.sum);
        self.count_vec.push(self.count);
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        if self.sum_vec.is_empty() {
            return Ok(Series::full_null("", &self.field.dtype, 1));
        }

        todo!()

        // let sum_series = Series::full_null("", &self.field.dtype, self.sum_vec.len());
        // let count_series = Series::full_null("", &DataType::UInt64, self.count_vec.len());

        // Ok((sum_series / count_series).unwrap())
    }
}

pub struct SumWindowState<'a, T>
where
    T: DaftPrimitiveType,
{
    source: &'a Series,
    sum: T::Native,
    field: Field,
    sum_vec: Vec<T::Native>,
}

impl<'a, T> SumWindowState<'a, T>
where
    T: DaftPrimitiveType,
{
    pub fn new(
        source: &'a Series,
        out_dtype: &DataType,
        agg_expr: &AggExpr,
        total_length: usize,
    ) -> Self {
        let name = match agg_expr {
            AggExpr::Sum(expr) => expr.name().to_string(),
            _ => panic!("Expected Sum aggregation"),
        };

        let field = Field::new(name, out_dtype.clone());

        Self {
            source,
            sum: T::Native::zero(),
            field,
            sum_vec: Vec::with_capacity(total_length),
        }
    }
}

impl<T> WindowAggStateOps for SumWindowState<'_, T>
where
    T: DaftPrimitiveType,
{
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        let value = self.source.slice(start_idx, end_idx)?;

        if value.len() > 4 {
            let sum_result = value.sum(None)?;

            if !sum_result.is_empty() {
                let sum_val = T::Native::from_f64(
                    sum_result
                        .cast(&DataType::Float64)?
                        .f64()?
                        .get(0)
                        .unwrap_or(0.0),
                )
                .unwrap_or_else(T::Native::zero);
                self.sum = self.sum + sum_val;
            }
            return Ok(());
        }

        for i in 0..value.len() {
            if value.is_valid(i) {
                let scalar_value = value.slice(i, i + 1)?;
                let sum_val = T::Native::from_f64(
                    scalar_value
                        .cast(&DataType::Float64)?
                        .f64()?
                        .get(0)
                        .unwrap_or(0.0),
                )
                .unwrap_or_else(T::Native::zero);
                self.sum = self.sum + sum_val;
            }
        }

        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        let value = self.source.slice(start_idx, end_idx)?;

        if value.len() > 4 {
            let sum_result = value.sum(None)?;

            if !sum_result.is_empty() {
                let sum_val = T::Native::from_f64(
                    sum_result
                        .cast(&DataType::Float64)?
                        .f64()?
                        .get(0)
                        .unwrap_or(0.0),
                )
                .unwrap_or_else(T::Native::zero);
                self.sum = self.sum - sum_val;
            }
            return Ok(());
        }

        for i in 0..value.len() {
            if value.is_valid(i) {
                let scalar_value = value.slice(i, i + 1)?;
                let sum_val = T::Native::from_f64(
                    scalar_value
                        .cast(&DataType::Float64)?
                        .f64()?
                        .get(0)
                        .unwrap_or(0.0),
                )
                .unwrap_or_else(T::Native::zero);
                self.sum = self.sum - sum_val;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        self.sum_vec.push(self.sum);
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        if self.sum_vec.is_empty() {
            Ok(Series::full_null("", &self.field.dtype, 1))
        } else {
            // Ok(DataArray::<T>::from(self.sum_vec).into_series())
            todo!()
        }
    }
}

pub struct MinWindowState<'a, T>
where
    T: DaftPrimitiveType,
{
    source: &'a Series,
    heap: BinaryHeap<Reverse<IndexedValue<'a, T>>>,
    cur_idx: usize,
    field: Field,
    result: Vec<usize>,
}

impl<'a, T> MinWindowState<'a, T>
where
    T: DaftPrimitiveType,
{
    pub fn new(
        source: &'a Series,
        out_dtype: &DataType,
        agg_expr: &AggExpr,
        total_length: usize,
    ) -> Self {
        let name = match agg_expr {
            AggExpr::Min(expr) => expr.name().to_string(),
            _ => panic!("Expected Min aggregation"),
        };

        let field = Field::new(name, out_dtype.clone());

        Self {
            source,
            heap: BinaryHeap::new(),
            cur_idx: 0,
            field,
            result: Vec::with_capacity(total_length),
        }
    }
}

impl<T> WindowAggStateOps for MinWindowState<'_, T>
where
    T: DaftPrimitiveType,
{
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        let value = self.source.slice(start_idx, end_idx)?;

        let mut idx = start_idx;
        for i in 0..value.len() {
            if value.is_valid(i) {
                let scalar_value = value.slice(i, i + 1)?;
                let val = T::Native::from_f64(
                    scalar_value
                        .cast(&DataType::Float64)?
                        .f64()?
                        .get(0)
                        .unwrap_or(0.0),
                )
                .unwrap_or_else(T::Native::zero);
                self.heap.push(Reverse(IndexedValue {
                    value: val,
                    idx,
                    _phantom: PhantomData,
                }));
            }
            idx += 1;
        }

        debug_assert!(
            idx == end_idx,
            "Index does not match end_idx in add operation"
        );

        Ok(())
    }

    fn remove(&mut self, _start_idx: usize, end_idx: usize) -> DaftResult<()> {
        self.cur_idx = end_idx;

        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        let mut min_idx = None;

        while let Some(Reverse(entry)) = self.heap.peek() {
            if entry.idx >= self.cur_idx {
                min_idx = Some(entry.idx);
                break;
            }
            self.heap.pop();
        }

        self.result.push(min_idx.unwrap());

        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        if self.result.is_empty() {
            Ok(Series::full_null("", &self.field.dtype, 1))
        } else {
            // Ok(self.source.take(&UInt64Array::from(("", self.result.clone))))
            todo!()
        }
    }
}

pub struct MaxWindowState<'a, T>
where
    T: DaftPrimitiveType,
{
    source: &'a Series,
    heap: BinaryHeap<IndexedValue<'a, T>>,
    cur_idx: usize,
    field: Field,
    result: Vec<usize>,
}

impl<'a, T> MaxWindowState<'a, T>
where
    T: DaftPrimitiveType,
{
    pub fn new(
        source: &'a Series,
        out_dtype: &DataType,
        agg_expr: &AggExpr,
        total_length: usize,
    ) -> Self {
        let name = match agg_expr {
            AggExpr::Max(expr) => expr.name().to_string(),
            _ => panic!("Expected Max aggregation"),
        };

        let field = Field::new(name, out_dtype.clone());

        Self {
            source,
            heap: BinaryHeap::new(),
            cur_idx: 0,
            field,
            result: Vec::with_capacity(total_length),
        }
    }
}

impl<T> WindowAggStateOps for MaxWindowState<'_, T>
where
    T: DaftPrimitiveType,
{
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        let value = self.source.slice(start_idx, end_idx)?;

        let mut idx = start_idx;
        for i in 0..value.len() {
            if value.is_valid(i) {
                let scalar_value = value.slice(i, i + 1)?;
                let val = T::Native::from_f64(
                    scalar_value
                        .cast(&DataType::Float64)?
                        .f64()?
                        .get(0)
                        .unwrap_or(0.0),
                )
                .unwrap_or_else(T::Native::zero);
                self.heap.push(IndexedValue {
                    value: val,
                    idx,
                    _phantom: PhantomData,
                });
            }
            idx += 1;
        }

        debug_assert!(
            idx == end_idx,
            "Index does not match end_idx in add operation"
        );

        Ok(())
    }

    fn remove(&mut self, _start_idx: usize, end_idx: usize) -> DaftResult<()> {
        self.cur_idx = end_idx;

        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        let mut max_idx = None;

        while let Some(entry) = self.heap.peek() {
            if entry.idx >= self.cur_idx {
                max_idx = Some(entry.idx);
                break;
            }
            self.heap.pop();
        }

        self.result.push(max_idx.unwrap());

        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        if self.result.is_empty() {
            Ok(Series::full_null("", &self.field.dtype, 1))
        } else {
            todo!()
        }
    }
}

pub struct CountDistinctWindowState<'a, T>
where
    T: DaftPrimitiveType,
    T::Native: std::cmp::Eq + std::hash::Hash,
{
    source: &'a Series,
    value_counts: HashMap<T::Native, usize>,
    field: Field,
    result: Vec<u64>,
}

impl<'a, T> CountDistinctWindowState<'a, T>
where
    T: DaftPrimitiveType,
    T::Native: std::cmp::Eq + std::hash::Hash,
{
    pub fn new(
        source: &'a Series,
        out_dtype: &DataType,
        agg_expr: &AggExpr,
        total_length: usize,
    ) -> Self {
        let name = match agg_expr {
            AggExpr::CountDistinct(expr) => expr.name().to_string(),
            _ => panic!("Expected CountDistinct aggregation"),
        };

        let field = Field::new(name, out_dtype.clone());

        Self {
            source,
            value_counts: HashMap::new(),
            field,
            result: Vec::with_capacity(total_length),
        }
    }
}

impl<T> WindowAggStateOps for CountDistinctWindowState<'_, T>
where
    T: DaftPrimitiveType,
    T::Native: std::cmp::Eq + std::hash::Hash,
{
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        let value = self.source.slice(start_idx, end_idx)?;

        for i in 0..value.len() {
            if value.is_valid(i) {
                let scalar_value = value.slice(i, i + 1)?;
                let val = T::Native::from_f64(
                    scalar_value
                        .cast(&DataType::Float64)?
                        .f64()?
                        .get(0)
                        .unwrap_or(0.0),
                )
                .unwrap_or_else(T::Native::zero);
                *self.value_counts.entry(val).or_insert(0) += 1;
            }
        }

        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        let value = self.source.slice(start_idx, end_idx)?;

        for i in 0..value.len() {
            if value.is_valid(i) {
                let scalar_value = value.slice(i, i + 1)?;
                let val = T::Native::from_f64(
                    scalar_value
                        .cast(&DataType::Float64)?
                        .f64()?
                        .get(0)
                        .unwrap_or(0.0),
                )
                .unwrap_or_else(T::Native::zero);
                if let Some(count) = self.value_counts.get_mut(&val) {
                    *count -= 1;
                    if *count == 0 {
                        self.value_counts.remove(&val);
                    }
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        let count = self.value_counts.len() as u64;
        self.result.push(count);
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        if self.result.is_empty() {
            Ok(Series::full_null("", &self.field.dtype, 1))
        } else {
            Ok(UInt64Array::from(("", self.result.clone())).into_series())
        }
    }
}

pub enum WindowAggState<'a, T>
where
    T: DaftPrimitiveType,
    T::Native: std::cmp::Eq + std::hash::Hash,
{
    Mean(MeanWindowState<'a, T>),
    Sum(SumWindowState<'a, T>),
    Min(MinWindowState<'a, T>),
    Max(MaxWindowState<'a, T>),
    CountDistinct(CountDistinctWindowState<'a, T>),
}

macro_rules! impl_window_agg_system {
    ($(($variant:ident, $state_type:ty)),*) => {
        impl<'a, T> WindowAggState<'a, T> where T: DaftPrimitiveType, T::Native: std::cmp::Eq + std::hash::Hash {
            pub fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
                match self {
                    $(
                        Self::$variant(state) => state.add(start_idx, end_idx),
                    )*
                }
            }

            pub fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
                match self {
                    $(
                        Self::$variant(state) => state.remove(start_idx, end_idx),
                    )*
                }
            }

            pub fn evaluate(&mut self) -> DaftResult<()> {
                match self {
                    $(
                        Self::$variant(state) => state.evaluate(),
                    )*
                }
            }

            pub fn build(&self) -> DaftResult<Series> {
                match self {
                    $(
                        Self::$variant(state) => state.build(),
                    )*
                }
            }
        }

        pub fn create_window_agg_state<'a, T>(source: &'a Series, agg_expr: &AggExpr, out_dtype: &DataType, total_length: usize) -> DaftResult<WindowAggState<'a, T>>
        where T: DaftPrimitiveType, T::Native: std::cmp::Eq + std::hash::Hash {
            match agg_expr {
                $(
                    AggExpr::$variant(_) => Ok(WindowAggState::$variant(<$state_type>::new(source, out_dtype, agg_expr, total_length))),
                )*
                _ => Err(DaftError::ValueError(format!("Aggregation type {:?} does not support incremental window computation", agg_expr))),
            }
        }
    };
}

impl_window_agg_system! {
    (Mean, MeanWindowState<'a, T>),
    (Sum, SumWindowState<'a, T>),
    (Min, MinWindowState<'a, T>),
    (Max, MaxWindowState<'a, T>),
    (CountDistinct, CountDistinctWindowState<'a, T>)
}
