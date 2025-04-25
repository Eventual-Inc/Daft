use arrow2::bitmap::Bitmap;
use common_error::{DaftError, DaftResult};
use daft_core::{prelude::*, series::IntoSeries};
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

pub struct SumWindowState {
    inner: Box<dyn WindowAggStateOps>,
}

pub struct CountWindowState {
    inner: Box<dyn WindowAggStateOps>,
}

pub struct MeanWindowState {
    inner_sum: SumWindowState,
    inner_count: CountWindowState,
}

pub enum WindowAggState {
    Sum(SumWindowState),
    Count(CountWindowState),
    Mean(MeanWindowState),
}

impl WindowAggStateOps for SumWindowState {
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

impl WindowAggStateOps for CountWindowState {
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

impl WindowAggStateOps for WindowAggState {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        match self {
            Self::Sum(state) => state.add(start_idx, end_idx),
            Self::Count(state) => state.add(start_idx, end_idx),
            Self::Mean(state) => state.add(start_idx, end_idx),
        }
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        match self {
            Self::Sum(state) => state.remove(start_idx, end_idx),
            Self::Count(state) => state.remove(start_idx, end_idx),
            Self::Mean(state) => state.remove(start_idx, end_idx),
        }
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        match self {
            Self::Sum(state) => state.evaluate(),
            Self::Count(state) => state.evaluate(),
            Self::Mean(state) => state.evaluate(),
        }
    }

    fn build(&self) -> DaftResult<Series> {
        match self {
            Self::Sum(state) => state.build(),
            Self::Count(state) => state.build(),
            Self::Mean(state) => state.build(),
        }
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

pub fn create_window_agg_state(
    source: &Series,
    agg_expr: &AggExpr,
    total_length: usize,
) -> DaftResult<WindowAggState> {
    match agg_expr {
        AggExpr::Sum(_) => Ok(WindowAggState::Sum(SumWindowState::new(
            source,
            total_length,
        ))),
        AggExpr::Count(..) => Ok(WindowAggState::Count(CountWindowState::new(
            source,
            total_length,
        ))),
        AggExpr::Mean(_) => Ok(WindowAggState::Mean(MeanWindowState::new(
            source,
            total_length,
        ))),
        _ => Err(DaftError::ValueError(format!(
            "Aggregation type {:?} does not support incremental window computation",
            agg_expr
        ))),
    }
}
