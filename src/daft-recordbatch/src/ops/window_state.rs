use std::{
    cmp::{Eq, Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
};

use arrow2::bitmap::{Bitmap, MutableBitmap};
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::arrow2::comparison::build_is_equal,
    prelude::*,
    series::IntoSeries,
    utils::identity_hash_set::{IdentityBuildHasher, IndexHash},
};
use daft_dsl::AggExpr;
use num_traits::Zero;

/// Trait for window aggregation state inner implementations
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

// Macro to define window state structs with their WindowAggStateOps implementations
macro_rules! define_window_state {
    ($name:ident) => {
        pub struct $name {
            inner: Box<dyn WindowAggStateOps>,
        }

        impl WindowAggStateOps for $name {
            fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
                self.inner.add(start_idx, end_idx)
            }

            fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
                self.inner.remove(start_idx, end_idx)
            }

            fn evaluate(&mut self) -> DaftResult<()> {
                self.inner.evaluate()
            }

            fn build(&self) -> DaftResult<Series> {
                self.inner.build()
            }
        }
    };
}

struct SumWindowStateInner<T>
where
    T: DaftNumericType,
{
    source: DataArray<T>,
    sum: T::Native,
    sum_vec: Vec<T::Native>,
}

impl<T> SumWindowStateInner<T>
where
    T: DaftNumericType,
{
    fn new(source: &Series, total_length: usize) -> Self {
        let source_array = source.downcast::<DataArray<T>>().unwrap().clone();
        Self {
            source: source_array,
            sum: T::Native::zero(),
            sum_vec: Vec::with_capacity(total_length),
        }
    }
}

impl<T> WindowAggStateOps for SumWindowStateInner<T>
where
    T: DaftNumericType,
    DataArray<T>: IntoSeries,
{
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if self.source.is_valid(i) {
                self.sum = self.sum + self.source.get(i).unwrap();
            }
        }
        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if self.source.is_valid(i) {
                self.sum = self.sum - self.source.get(i).unwrap();
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        self.sum_vec.push(self.sum);
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        Ok(DataArray::<T>::from(("", self.sum_vec.clone())).into_series())
    }
}

struct CountWindowStateInner {
    source: Bitmap,
    count: usize,
    count_vec: Vec<u64>,
}

impl CountWindowStateInner {
    fn new(source: &Series, total_length: usize) -> Self {
        let source_bitmap = source
            .validity()
            .cloned()
            .unwrap_or_else(|| Bitmap::new_constant(true, source.len()));
        Self {
            source: source_bitmap,
            count: 0,
            count_vec: Vec::with_capacity(total_length),
        }
    }
}

impl WindowAggStateOps for CountWindowStateInner {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if self.source.get_bit(i) {
                self.count += 1;
            }
        }
        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if self.source.get_bit(i) {
                self.count -= 1;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        self.count_vec.push(self.count as u64);
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        Ok(DataArray::<UInt64Type>::from(("", self.count_vec.clone())).into_series())
    }
}

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

struct MinWindowStateInner {
    source: Series,
    min_heap: BinaryHeap<Reverse<IndexedValue>>,
    cur_idx: usize,
    validity: MutableBitmap,
    min_idxs: Vec<u64>,
}

impl MinWindowStateInner {
    fn new(source: &Series, total_length: usize) -> Self {
        Self {
            source: source.clone(),
            min_heap: BinaryHeap::new(),
            cur_idx: 0,
            validity: MutableBitmap::with_capacity(total_length),
            min_idxs: Vec::with_capacity(total_length),
        }
    }
}

impl WindowAggStateOps for MinWindowStateInner {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if self.source.is_valid(i) {
                self.min_heap.push(Reverse(IndexedValue {
                    value: self.source.slice(i, i + 1).unwrap(),
                    idx: i as u64,
                }));
            }
        }
        Ok(())
    }

    fn remove(&mut self, _start_idx: usize, end_idx: usize) -> DaftResult<()> {
        self.cur_idx = end_idx;
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        while !self.min_heap.is_empty() && self.min_heap.peek().unwrap().0.idx < self.cur_idx as u64
        {
            self.min_heap.pop();
        }
        if self.min_heap.is_empty() {
            self.validity.push(false);
            self.min_idxs.push(0);
        } else {
            self.validity.push(true);
            self.min_idxs.push(self.min_heap.peek().unwrap().0.idx);
        }
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        let result = self
            .source
            .take(&DataArray::<UInt64Type>::from(("", self.min_idxs.clone())).into_series())
            .unwrap();
        result.with_validity(Some(self.validity.clone().into()))
    }
}

struct MaxWindowStateInner {
    source: Series,
    max_heap: BinaryHeap<IndexedValue>,
    cur_idx: usize,
    max_idxs: Vec<u64>,
    validity: MutableBitmap,
}

impl MaxWindowStateInner {
    fn new(source: &Series, total_length: usize) -> Self {
        Self {
            source: source.clone(),
            max_heap: BinaryHeap::new(),
            cur_idx: 0,
            max_idxs: Vec::with_capacity(total_length),
            validity: MutableBitmap::with_capacity(total_length),
        }
    }
}

impl WindowAggStateOps for MaxWindowStateInner {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if self.source.is_valid(i) {
                self.max_heap.push(IndexedValue {
                    value: self.source.slice(i, i + 1).unwrap(),
                    idx: i as u64,
                });
            }
        }
        Ok(())
    }

    fn remove(&mut self, _start_idx: usize, end_idx: usize) -> DaftResult<()> {
        self.cur_idx = end_idx;
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        while !self.max_heap.is_empty() && self.max_heap.peek().unwrap().idx < self.cur_idx as u64 {
            self.max_heap.pop();
        }
        if self.max_heap.is_empty() {
            self.validity.push(false);
            self.max_idxs.push(0);
        } else {
            self.validity.push(true);
            self.max_idxs.push(self.max_heap.peek().unwrap().idx);
        }
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        let result = self
            .source
            .take(&DataArray::<UInt64Type>::from(("", self.max_idxs.clone())).into_series())
            .unwrap();
        result.with_validity(Some(self.validity.clone().into()))
    }
}

struct CountDistinctWindowStateInner {
    hashed: DataArray<UInt64Type>,
    counts: HashMap<IndexHash, usize, IdentityBuildHasher>,
    count_vec: Vec<u64>,
    comparator: Box<dyn Fn(usize, usize) -> bool>,
}

impl CountDistinctWindowStateInner {
    fn new(source: &Series, total_length: usize) -> Self {
        let hashed = source.hash_with_validity(None).unwrap();

        let array = source.to_arrow();
        let comparator = build_is_equal(&*array, &*array, true, false).unwrap();

        Self {
            hashed,
            counts: HashMap::with_capacity_and_hasher(total_length, Default::default()),
            count_vec: Vec::with_capacity(total_length),
            comparator: Box::new(comparator),
        }
    }
}

impl WindowAggStateOps for CountDistinctWindowStateInner {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if let Some(hash) = self.hashed.get(i) {
                let index_hash = IndexHash {
                    idx: i as u64,
                    hash,
                };

                let mut found_match = false;
                for (existing_hash, count) in &mut self.counts {
                    if existing_hash.hash == hash
                        && (self.comparator)(i, existing_hash.idx as usize)
                    {
                        *count += 1;
                        found_match = true;
                        break;
                    }
                }

                if !found_match {
                    self.counts.insert(index_hash, 1);
                }
            }
        }
        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        for i in start_idx..end_idx {
            if let Some(hash) = self.hashed.get(i) {
                let mut keys_to_remove = Vec::new();

                for (k, v) in &mut self.counts {
                    if k.hash == hash && (self.comparator)(i, k.idx as usize) {
                        *v -= 1;
                        if *v == 0 {
                            keys_to_remove.push(IndexHash {
                                idx: k.idx,
                                hash: k.hash,
                            });
                        }
                        break;
                    }
                }

                for key in keys_to_remove {
                    self.counts.remove(&key);
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        self.count_vec.push(self.counts.len() as u64);
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        Ok(DataArray::<UInt64Type>::from(("", self.count_vec.clone())).into_series())
    }
}

// Define window state structs using the macro
define_window_state!(SumWindowState);
define_window_state!(CountWindowState);
define_window_state!(MinWindowState);
define_window_state!(MaxWindowState);
define_window_state!(CountDistinctWindowState);

pub struct MeanWindowState {
    inner_sum: SumWindowState,
    inner_count: CountWindowState,
}

// Macro to define the WindowAggState enum and its implementations
macro_rules! define_window_agg_states {
    ($(($variant:ident, $state:ident, $pattern:pat)),+) => {
        pub enum WindowAggState {
            $($variant($state)),+
        }

        impl WindowAggStateOps for WindowAggState {
            fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
                match self {
                    $(Self::$variant(state) => state.add(start_idx, end_idx),)+
                }
            }

            fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
                match self {
                    $(Self::$variant(state) => state.remove(start_idx, end_idx),)+
                }
            }

            fn evaluate(&mut self) -> DaftResult<()> {
                match self {
                    $(Self::$variant(state) => state.evaluate(),)+
                }
            }

            fn build(&self) -> DaftResult<Series> {
                match self {
                    $(Self::$variant(state) => state.build(),)+
                }
            }
        }

        pub fn create_window_agg_state(
            source: &Series,
            agg_expr: &AggExpr,
            total_length: usize,
        ) -> DaftResult<WindowAggState> {
            match agg_expr {
                $(
                $pattern => Ok(WindowAggState::$variant($state::new(source, total_length))),
                )+
                _ => Err(DaftError::ValueError(format!(
                    "Aggregation type {:?} does not support incremental window computation",
                    agg_expr
                ))),
            }
        }
    };
}

// Use the macro to define WindowAggState and implementations
define_window_agg_states! {
    (Sum, SumWindowState, AggExpr::Sum(_)),
    (Count, CountWindowState, AggExpr::Count(..)),
    (Mean, MeanWindowState, AggExpr::Mean(_)),
    (Min, MinWindowState, AggExpr::Min(_)),
    (Max, MaxWindowState, AggExpr::Max(_)),
    (CountDistinct, CountDistinctWindowState, AggExpr::CountDistinct(_))
}

impl WindowAggStateOps for MeanWindowState {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        self.inner_sum.add(start_idx, end_idx)?;
        self.inner_count.add(start_idx, end_idx)
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        self.inner_sum.remove(start_idx, end_idx)?;
        self.inner_count.remove(start_idx, end_idx)
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        self.inner_sum.evaluate()?;
        self.inner_count.evaluate()
    }

    fn build(&self) -> DaftResult<Series> {
        let sum = self.inner_sum.build()?;
        let count = self.inner_count.build()?;
        Ok((sum / count).unwrap())
    }
}

impl SumWindowState {
    fn new(source: &Series, total_length: usize) -> Self {
        match source.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let casted = source.cast(&DataType::Int64).unwrap();
                let inner = Box::new(SumWindowStateInner::<Int64Type>::new(&casted, total_length));
                Self { inner }
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                let casted = source.cast(&DataType::UInt64).unwrap();
                let inner = Box::new(SumWindowStateInner::<UInt64Type>::new(
                    &casted,
                    total_length,
                ));
                Self { inner }
            }
            DataType::Float32 => {
                let inner = Box::new(SumWindowStateInner::<Float32Type>::new(
                    source,
                    total_length,
                ));
                Self { inner }
            }
            DataType::Float64 => {
                let inner = Box::new(SumWindowStateInner::<Float64Type>::new(
                    source,
                    total_length,
                ));
                Self { inner }
            }
            _ => panic!("Unsupported data type for SumWindowState"),
        }
    }
}

impl CountWindowState {
    fn new(source: &Series, total_length: usize) -> Self {
        let inner = CountWindowStateInner::new(source, total_length);
        Self {
            inner: Box::new(inner),
        }
    }
}

impl MeanWindowState {
    fn new(source: &Series, total_length: usize) -> Self {
        let inner_sum = SumWindowState::new(source, total_length);
        let inner_count = CountWindowState::new(source, total_length);
        Self {
            inner_sum,
            inner_count,
        }
    }
}

impl MinWindowState {
    fn new(source: &Series, total_length: usize) -> Self {
        let inner = MinWindowStateInner::new(source, total_length);
        Self {
            inner: Box::new(inner),
        }
    }
}

impl MaxWindowState {
    fn new(source: &Series, total_length: usize) -> Self {
        let inner = MaxWindowStateInner::new(source, total_length);
        Self {
            inner: Box::new(inner),
        }
    }
}

impl CountDistinctWindowState {
    fn new(source: &Series, total_length: usize) -> Self {
        let inner = CountDistinctWindowStateInner::new(source, total_length);
        Self {
            inner: Box::new(inner),
        }
    }
}
