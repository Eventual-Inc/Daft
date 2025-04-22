use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
};

use common_error::{DaftError, DaftResult};
use daft_core::{count_mode::CountMode, prelude::*};
use daft_dsl::AggExpr;

#[derive(Debug, Clone)]
struct IndexedValue {
    value: Series,
    idx: u64,
}

impl Eq for IndexedValue {}

impl PartialEq for IndexedValue {
    fn eq(&self, other: &Self) -> bool {
        if self.idx != other.idx {
            return false;
        }

        match self.value.equal(&other.value) {
            Ok(result) => result.into_iter().all(|x| x.unwrap_or(false)),
            Err(_) => false,
        }
    }
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for IndexedValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.value.lt(&other.value) {
            Ok(result) => {
                if result.into_iter().any(|x| x.unwrap_or(false)) {
                    return Some(Ordering::Less);
                }
            }
            Err(_) => return None,
        }

        match self.value.equal(&other.value) {
            Ok(result) => {
                if result.into_iter().all(|x| x.unwrap_or(false)) {
                    return Some(self.idx.cmp(&other.idx));
                }
            }
            Err(_) => return None,
        }

        Some(Ordering::Greater)
    }
}

impl Ord for IndexedValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

/// Trait for window aggregation state common operations
pub trait WindowAggStateOps {
    /// Add a value to the state with index information
    fn add(&mut self, value: &Series, start_idx: u64, end_idx: u64) -> DaftResult<()>;

    /// Remove a value from the state with index information
    fn remove(&mut self, value: &Series, start_idx: u64, end_idx: u64) -> DaftResult<()>;

    /// Evaluate the current state to produce an aggregation result
    fn evaluate(&mut self) -> DaftResult<Series>;
}

pub struct MeanWindowState {
    name: String,
    sum_series: Option<Series>,
    count: usize,
    field: Field,
}

impl MeanWindowState {
    pub fn new(out_dtype: &DataType, agg_expr: &AggExpr) -> Self {
        let name = match agg_expr {
            AggExpr::Mean(expr) => expr.name().to_string(),
            _ => panic!("Expected Mean aggregation"),
        };

        let field = Field::new(name.clone(), out_dtype.clone());

        Self {
            name,
            sum_series: None,
            count: 0,
            field,
        }
    }
}

impl WindowAggStateOps for MeanWindowState {
    fn add(&mut self, value: &Series, _start_idx: u64, _end_idx: u64) -> DaftResult<()> {
        if value.len() > 4 {
            let sum_result = value.sum(None)?;
            let count_result = value.count(None, CountMode::Valid)?;

            if !sum_result.is_empty() && !count_result.is_empty() {
                let count_val = count_result.u64()?.get(0).unwrap_or(0) as usize;

                if count_val > 0 {
                    if let Some(ref mut sum_series) = self.sum_series {
                        *sum_series = (sum_series as &Series + &sum_result)?;
                    } else {
                        self.sum_series = Some(sum_result);
                    }
                    self.count += count_val;
                }
            }
            return Ok(());
        }

        for i in 0..value.len() {
            if value.is_valid(i) {
                let scalar_value = value.slice(i, i + 1)?;

                if let Some(ref mut sum_series) = self.sum_series {
                    *sum_series = (sum_series as &Series + &scalar_value)?;
                } else {
                    self.sum_series = Some(scalar_value);
                }
                self.count += 1;
            }
        }

        Ok(())
    }

    fn remove(&mut self, value: &Series, _start_idx: u64, _end_idx: u64) -> DaftResult<()> {
        if self.sum_series.is_none() {
            return Ok(());
        }

        if value.len() > 4 {
            let sum_result = value.sum(None)?;
            let count_result = value.count(None, CountMode::Valid)?;

            if !sum_result.is_empty() && !count_result.is_empty() {
                let count_val = count_result.u64()?.get(0).unwrap_or(0) as usize;

                if count_val > 0 && self.count >= count_val {
                    if let Some(ref mut sum_series) = self.sum_series {
                        *sum_series = (sum_series as &Series - &sum_result)?;
                        self.count -= count_val;
                    }
                }
            }
            return Ok(());
        }

        for i in 0..value.len() {
            if value.is_valid(i) && self.count > 0 {
                let scalar_value = value.slice(i, i + 1)?;

                if let Some(ref mut sum_series) = self.sum_series {
                    *sum_series = (sum_series as &Series - &scalar_value)?;
                    self.count -= 1;
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<Series> {
        if self.count == 0 || self.sum_series.is_none() {
            return Ok(Series::full_null(&self.name, &self.field.dtype, 1));
        }

        let count_value = self.count as f64;
        let count_array = Float64Array::from((self.name.as_str(), vec![count_value])).into_series();

        let mean_series = (self.sum_series.as_ref().unwrap() as &Series / &count_array)?;

        let result = mean_series.rename(&self.name);

        Ok(result)
    }
}

pub struct SumWindowState {
    name: String,
    sum_series: Option<Series>,
    field: Field,
}

impl SumWindowState {
    pub fn new(out_dtype: &DataType, agg_expr: &AggExpr) -> Self {
        let name = match agg_expr {
            AggExpr::Sum(expr) => expr.name().to_string(),
            _ => panic!("Expected Sum aggregation"),
        };

        let field = Field::new(name.clone(), out_dtype.clone());

        Self {
            name,
            sum_series: None,
            field,
        }
    }
}

impl WindowAggStateOps for SumWindowState {
    fn add(&mut self, value: &Series, _start_idx: u64, _end_idx: u64) -> DaftResult<()> {
        if value.len() > 4 {
            let sum_result = value.sum(None)?;

            if !sum_result.is_empty() {
                if let Some(ref mut sum_series) = self.sum_series {
                    *sum_series = (sum_series as &Series + &sum_result)?;
                } else {
                    self.sum_series = Some(sum_result);
                }
            }
            return Ok(());
        }

        for i in 0..value.len() {
            if value.is_valid(i) {
                let scalar_value = value.slice(i, i + 1)?;

                if let Some(ref mut sum_series) = self.sum_series {
                    *sum_series = (sum_series as &Series + &scalar_value)?;
                } else {
                    self.sum_series = Some(scalar_value);
                }
            }
        }

        Ok(())
    }

    fn remove(&mut self, value: &Series, _start_idx: u64, _end_idx: u64) -> DaftResult<()> {
        if self.sum_series.is_none() {
            return Ok(());
        }

        if value.len() > 4 {
            let sum_result = value.sum(None)?;

            if !sum_result.is_empty() {
                if let Some(ref mut sum_series) = self.sum_series {
                    *sum_series = (sum_series as &Series - &sum_result)?;
                }
            }
            return Ok(());
        }

        for i in 0..value.len() {
            if value.is_valid(i) {
                let scalar_value = value.slice(i, i + 1)?;

                if let Some(ref mut sum_series) = self.sum_series {
                    *sum_series = (sum_series as &Series - &scalar_value)?;
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<Series> {
        match &self.sum_series {
            Some(sum) => {
                let result = sum.rename(&self.name);
                Ok(result)
            }
            None => Ok(Series::full_null(&self.name, &self.field.dtype, 1)),
        }
    }
}

pub struct MinWindowState {
    name: String,
    heap: BinaryHeap<Reverse<IndexedValue>>,
    cur_idx: u64,
    field: Field,
}

impl MinWindowState {
    pub fn new(out_dtype: &DataType, agg_expr: &AggExpr) -> Self {
        let name = match agg_expr {
            AggExpr::Min(expr) => expr.name().to_string(),
            _ => panic!("Expected Min aggregation"),
        };

        let field = Field::new(name.clone(), out_dtype.clone());

        Self {
            name,
            heap: BinaryHeap::new(),
            cur_idx: 0,
            field,
        }
    }
}

impl WindowAggStateOps for MinWindowState {
    fn add(&mut self, value: &Series, start_idx: u64, end_idx: u64) -> DaftResult<()> {
        let mut idx = start_idx;
        for i in 0..value.len() {
            if value.is_valid(i) {
                let scalar_value = value.slice(i, i + 1)?;
                self.heap.push(Reverse(IndexedValue {
                    value: scalar_value,
                    idx,
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

    fn remove(&mut self, _value: &Series, _start_idx: u64, end_idx: u64) -> DaftResult<()> {
        self.cur_idx = end_idx;

        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<Series> {
        let mut min_value = None;

        while let Some(Reverse(entry)) = self.heap.peek() {
            if entry.idx >= self.cur_idx {
                min_value = Some(entry.value.clone());
                break;
            }
            self.heap.pop();
        }

        match min_value {
            Some(min_val) => Ok(min_val),
            None => Ok(Series::full_null(&self.name, &self.field.dtype, 1)),
        }
    }
}

pub struct MaxWindowState {
    name: String,
    heap: BinaryHeap<IndexedValue>,
    cur_idx: u64,
    field: Field,
}

impl MaxWindowState {
    pub fn new(out_dtype: &DataType, agg_expr: &AggExpr) -> Self {
        let name = match agg_expr {
            AggExpr::Max(expr) => expr.name().to_string(),
            _ => panic!("Expected Max aggregation"),
        };

        let field = Field::new(name.clone(), out_dtype.clone());

        Self {
            name,
            heap: BinaryHeap::new(),
            cur_idx: 0,
            field,
        }
    }
}

impl WindowAggStateOps for MaxWindowState {
    fn add(&mut self, value: &Series, start_idx: u64, end_idx: u64) -> DaftResult<()> {
        let mut idx = start_idx;
        for i in 0..value.len() {
            if value.is_valid(i) {
                let scalar_value = value.slice(i, i + 1)?;
                self.heap.push(IndexedValue {
                    value: scalar_value,
                    idx,
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

    fn remove(&mut self, _value: &Series, _start_idx: u64, end_idx: u64) -> DaftResult<()> {
        self.cur_idx = end_idx;

        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<Series> {
        let mut temp_heap = self.heap.clone();
        let mut max_value = None;

        while let Some(entry) = temp_heap.peek() {
            if entry.idx >= self.cur_idx {
                max_value = Some(entry.value.clone());
                break;
            }
            temp_heap.pop();
        }

        match max_value {
            Some(max_val) => Ok(max_val),
            None => Ok(Series::full_null(&self.name, &self.field.dtype, 1)),
        }
    }
}

pub struct CountDistinctWindowState {
    name: String,
    value_counts: HashMap<u64, usize>,
    field: Field,
}

impl CountDistinctWindowState {
    pub fn new(out_dtype: &DataType, agg_expr: &AggExpr) -> Self {
        let name = match agg_expr {
            AggExpr::CountDistinct(expr) => expr.name().to_string(),
            _ => panic!("Expected CountDistinct aggregation"),
        };

        let field = Field::new(name.clone(), out_dtype.clone());

        Self {
            name,
            value_counts: HashMap::new(),
            field,
        }
    }
}

impl WindowAggStateOps for CountDistinctWindowState {
    fn add(&mut self, value: &Series, _start_idx: u64, _end_idx: u64) -> DaftResult<()> {
        let hashed = value.hash_with_validity(None)?;

        for i in 0..hashed.len() {
            if let Some(hash) = hashed.get(i) {
                *self.value_counts.entry(hash).or_insert(0) += 1;
            }
        }

        Ok(())
    }

    fn remove(&mut self, value: &Series, _start_idx: u64, _end_idx: u64) -> DaftResult<()> {
        let hashed = value.hash_with_validity(None)?;

        for i in 0..hashed.len() {
            if let Some(hash) = hashed.get(i) {
                if let Some(count) = self.value_counts.get_mut(&hash) {
                    *count -= 1;
                    if *count == 0 {
                        self.value_counts.remove(&hash);
                    }
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<Series> {
        let count = self.value_counts.len() as u64;

        match self.field.dtype {
            DataType::UInt64 => {
                let array = UInt64Array::from((self.name.as_str(), vec![count])).into_series();
                Ok(array)
            }
            _ => {
                let array = UInt64Array::from((self.name.as_str(), vec![count])).into_series();
                Ok(array)
            }
        }
    }
}

pub enum WindowAggState {
    Mean(MeanWindowState),
    Sum(SumWindowState),
    Min(MinWindowState),
    Max(MaxWindowState),
    CountDistinct(CountDistinctWindowState),
}

macro_rules! impl_window_agg_system {
    ($(($variant:ident, $state_type:ty)),*) => {
        impl WindowAggState {
            pub fn add(&mut self, value: &Series, start_idx: u64, end_idx: u64) -> DaftResult<()> {
                match self {
                    $(
                        Self::$variant(state) => state.add(value, start_idx, end_idx),
                    )*
                }
            }

            pub fn remove(&mut self, value: &Series, start_idx: u64, end_idx: u64) -> DaftResult<()> {
                match self {
                    $(
                        Self::$variant(state) => state.remove(value, start_idx, end_idx),
                    )*
                }
            }

            pub fn evaluate(&mut self) -> DaftResult<Series> {
                match self {
                    $(
                        Self::$variant(state) => state.evaluate(),
                    )*
                }
            }
        }

        pub fn create_window_agg_state(agg_expr: &AggExpr, out_dtype: &DataType) -> DaftResult<WindowAggState> {
            match agg_expr {
                $(
                    AggExpr::$variant(_) => Ok(WindowAggState::$variant(<$state_type>::new(out_dtype, agg_expr))),
                )*
                _ => Err(DaftError::ValueError(format!("Aggregation type {:?} does not support incremental window computation", agg_expr))),
            }
        }
    };
}

impl_window_agg_system! {
    (Mean, MeanWindowState),
    (Sum, SumWindowState),
    (Min, MinWindowState),
    (Max, MaxWindowState),
    (CountDistinct, CountDistinctWindowState)
}
