use std::collections::{BTreeMap, HashMap};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::AggExpr;
use ordered_float::OrderedFloat;

/// Trait for window aggregation state common operations
pub trait WindowAggStateOps {
    /// Add a value to the state
    fn add(&mut self, value: &Series) -> DaftResult<()>;

    /// Remove a value from the state
    fn remove(&mut self, value: &Series) -> DaftResult<()>;

    /// Evaluate the current state to produce an aggregation result
    fn evaluate(&self) -> DaftResult<Series>;
}

/// State for computing mean in a sliding window
pub struct MeanWindowState {
    /// Name of the output column
    name: String,
    /// Running sum
    sum: f64,
    /// Count of non-null values
    count: usize,
    /// Field information for the output
    field: Field,
}

impl MeanWindowState {
    pub fn new(out_dtype: &DataType, agg_expr: &AggExpr) -> Self {
        // Extract expression name for output column
        let name = match agg_expr {
            AggExpr::Mean(expr) => expr.name().to_string(),
            _ => panic!("Expected Mean aggregation"),
        };

        let field = Field::new(name.clone(), out_dtype.clone());

        Self {
            name,
            sum: 0.0,
            count: 0,
            field,
        }
    }
}

impl WindowAggStateOps for MeanWindowState {
    fn add(&mut self, value: &Series) -> DaftResult<()> {
        // Convert to f64 to ensure we can compute the mean correctly
        let value = match value.data_type() {
            dt if dt.is_numeric() => value.cast(&DataType::Float64)?,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Mean can only be computed on numeric types, got {}",
                    value.data_type()
                )))
            }
        };

        // Get the f64 array and extract the value
        let value_array = value.f64()?;

        // Process each value in the array
        for i in 0..value_array.len() {
            if let Some(val) = value_array.get(i) {
                self.sum += val;
                self.count += 1;
            }
        }

        Ok(())
    }

    fn remove(&mut self, value: &Series) -> DaftResult<()> {
        // Convert to f64 to ensure we can compute the mean correctly
        let value = match value.data_type() {
            dt if dt.is_numeric() => value.cast(&DataType::Float64)?,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Mean can only be computed on numeric types, got {}",
                    value.data_type()
                )))
            }
        };

        // Get the f64 array and extract the value
        let value_array = value.f64()?;

        // Process each value in the array
        for i in 0..value_array.len() {
            if let Some(val) = value_array.get(i) {
                self.sum -= val;
                self.count -= 1;
            }
        }

        Ok(())
    }

    fn evaluate(&self) -> DaftResult<Series> {
        if self.count == 0 {
            // Return null if no data
            return Ok(Series::full_null(&self.name, &self.field.dtype, 1));
        }

        // Compute mean
        let mean = self.sum / self.count as f64;

        // Create array with single value
        let array = Float64Array::from((self.name.as_str(), vec![mean])).into_series();

        Ok(array)
    }
}

/// State for computing sum in a sliding window
pub struct SumWindowState {
    /// Name of the output column
    name: String,
    /// Running sum (stored as f64 for compatibility across numeric types)
    sum: f64,
    /// Field information for the output
    field: Field,
}

impl SumWindowState {
    pub fn new(out_dtype: &DataType, agg_expr: &AggExpr) -> Self {
        // Extract expression name for output column
        let name = match agg_expr {
            AggExpr::Sum(expr) => expr.name().to_string(),
            _ => panic!("Expected Sum aggregation"),
        };

        let field = Field::new(name.clone(), out_dtype.clone());

        Self {
            name,
            sum: 0.0,
            field,
        }
    }
}

impl WindowAggStateOps for SumWindowState {
    fn add(&mut self, value: &Series) -> DaftResult<()> {
        // Convert to f64 to ensure we can compute the sum correctly
        let value = match value.data_type() {
            dt if dt.is_numeric() => value.cast(&DataType::Float64)?,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Sum can only be computed on numeric types, got {}",
                    value.data_type()
                )))
            }
        };

        // Get the f64 array and extract the value
        let value_array = value.f64()?;

        // Process each value in the array
        for i in 0..value_array.len() {
            if let Some(val) = value_array.get(i) {
                self.sum += val;
            }
        }

        Ok(())
    }

    fn remove(&mut self, value: &Series) -> DaftResult<()> {
        // Convert to f64 to ensure we can compute the sum correctly
        let value = match value.data_type() {
            dt if dt.is_numeric() => value.cast(&DataType::Float64)?,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Sum can only be computed on numeric types, got {}",
                    value.data_type()
                )))
            }
        };

        // Get the f64 array and extract the value
        let value_array = value.f64()?;

        // Process each value in the array
        for i in 0..value_array.len() {
            if let Some(val) = value_array.get(i) {
                self.sum -= val;
            }
        }

        Ok(())
    }

    fn evaluate(&self) -> DaftResult<Series> {
        // Create appropriate numeric array based on output type
        match self.field.dtype {
            DataType::Float64 => {
                let array = Float64Array::from((self.name.as_str(), vec![self.sum])).into_series();
                Ok(array)
            }
            DataType::Int64 => {
                let array =
                    Int64Array::from((self.name.as_str(), vec![self.sum as i64])).into_series();
                Ok(array)
            }
            // Add other numeric types as needed
            _ => {
                // Default to Float64 for other types
                let array = Float64Array::from((self.name.as_str(), vec![self.sum])).into_series();
                Ok(array)
            }
        }
    }
}

/// State for computing min in a sliding window using an efficient sorted data structure
pub struct MinWindowState {
    /// Name of the output column
    name: String,
    /// Map of values and their counts, sorted naturally (smallest to largest)
    values: BTreeMap<OrderedFloat<f64>, usize>,
    /// Field information for the output
    field: Field,
}

impl MinWindowState {
    pub fn new(out_dtype: &DataType, agg_expr: &AggExpr) -> Self {
        // Extract expression name for output column
        let name = match agg_expr {
            AggExpr::Min(expr) => expr.name().to_string(),
            _ => panic!("Expected Min aggregation"),
        };

        let field = Field::new(name.clone(), out_dtype.clone());

        Self {
            name,
            values: BTreeMap::new(),
            field,
        }
    }
}

impl WindowAggStateOps for MinWindowState {
    fn add(&mut self, value: &Series) -> DaftResult<()> {
        // Convert to f64 to ensure we can compute the min correctly
        let value = match value.data_type() {
            dt if dt.is_numeric() => value.cast(&DataType::Float64)?,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Min can only be computed on numeric types, got {}",
                    value.data_type()
                )))
            }
        };

        // Get the f64 array and extract the value
        let value_array = value.f64()?;

        // Process each value in the array
        for i in 0..value_array.len() {
            if let Some(val) = value_array.get(i) {
                // Increment count or insert with count 1
                *self.values.entry(OrderedFloat(val)).or_insert(0) += 1;
            }
        }

        Ok(())
    }

    fn remove(&mut self, value: &Series) -> DaftResult<()> {
        // Convert to f64 to ensure we can compute the min correctly
        let value = match value.data_type() {
            dt if dt.is_numeric() => value.cast(&DataType::Float64)?,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Min can only be computed on numeric types, got {}",
                    value.data_type()
                )))
            }
        };

        // Get the f64 array and extract the value
        let value_array = value.f64()?;

        // Process each value in the array
        for i in 0..value_array.len() {
            if let Some(val) = value_array.get(i) {
                let ordered_val = OrderedFloat(val);
                if let Some(count) = self.values.get_mut(&ordered_val) {
                    *count -= 1;
                    // Remove entry if count reaches 0
                    if *count == 0 {
                        self.values.remove(&ordered_val);
                    }
                }
            }
        }

        Ok(())
    }

    fn evaluate(&self) -> DaftResult<Series> {
        // BTreeMap is sorted, so the first key is the minimum value
        match self.values.keys().next() {
            Some(min) => {
                let min_val = min.0;
                // Create array with appropriate type
                match self.field.dtype {
                    DataType::Float64 => {
                        let array =
                            Float64Array::from((self.name.as_str(), vec![min_val])).into_series();
                        Ok(array)
                    }
                    DataType::Int64 => {
                        let array = Int64Array::from((self.name.as_str(), vec![min_val as i64]))
                            .into_series();
                        Ok(array)
                    }
                    // Add other numeric types as needed
                    _ => {
                        // Default to Float64 for other types
                        let array =
                            Float64Array::from((self.name.as_str(), vec![min_val])).into_series();
                        Ok(array)
                    }
                }
            }
            None => Ok(Series::full_null(&self.name, &self.field.dtype, 1)),
        }
    }
}

/// State for computing max in a sliding window using an efficient sorted data structure
pub struct MaxWindowState {
    /// Name of the output column
    name: String,
    /// Map of values and their counts, sorted naturally (smallest to largest)
    values: BTreeMap<OrderedFloat<f64>, usize>,
    /// Field information for the output
    field: Field,
}

impl MaxWindowState {
    pub fn new(out_dtype: &DataType, agg_expr: &AggExpr) -> Self {
        // Extract expression name for output column
        let name = match agg_expr {
            AggExpr::Max(expr) => expr.name().to_string(),
            _ => panic!("Expected Max aggregation"),
        };

        let field = Field::new(name.clone(), out_dtype.clone());

        Self {
            name,
            values: BTreeMap::new(),
            field,
        }
    }
}

impl WindowAggStateOps for MaxWindowState {
    fn add(&mut self, value: &Series) -> DaftResult<()> {
        // Convert to f64 to ensure we can compute the max correctly
        let value = match value.data_type() {
            dt if dt.is_numeric() => value.cast(&DataType::Float64)?,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Max can only be computed on numeric types, got {}",
                    value.data_type()
                )))
            }
        };

        // Get the f64 array and extract the value
        let value_array = value.f64()?;

        // Process each value in the array
        for i in 0..value_array.len() {
            if let Some(val) = value_array.get(i) {
                // Increment count or insert with count 1
                *self.values.entry(OrderedFloat(val)).or_insert(0) += 1;
            }
        }

        Ok(())
    }

    fn remove(&mut self, value: &Series) -> DaftResult<()> {
        // Convert to f64 to ensure we can compute the max correctly
        let value = match value.data_type() {
            dt if dt.is_numeric() => value.cast(&DataType::Float64)?,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Max can only be computed on numeric types, got {}",
                    value.data_type()
                )))
            }
        };

        // Get the f64 array and extract the value
        let value_array = value.f64()?;

        // Process each value in the array
        for i in 0..value_array.len() {
            if let Some(val) = value_array.get(i) {
                let ordered_val = OrderedFloat(val);
                if let Some(count) = self.values.get_mut(&ordered_val) {
                    *count -= 1;
                    // Remove entry if count reaches 0
                    if *count == 0 {
                        self.values.remove(&ordered_val);
                    }
                }
            }
        }

        Ok(())
    }

    fn evaluate(&self) -> DaftResult<Series> {
        // For max, we need the last key in the BTreeMap
        match self.values.keys().next_back() {
            Some(max) => {
                let max_val = max.0;
                // Create array with appropriate type
                match self.field.dtype {
                    DataType::Float64 => {
                        let array =
                            Float64Array::from((self.name.as_str(), vec![max_val])).into_series();
                        Ok(array)
                    }
                    DataType::Int64 => {
                        let array = Int64Array::from((self.name.as_str(), vec![max_val as i64]))
                            .into_series();
                        Ok(array)
                    }
                    // Add other numeric types as needed
                    _ => {
                        // Default to Float64 for other types
                        let array =
                            Float64Array::from((self.name.as_str(), vec![max_val])).into_series();
                        Ok(array)
                    }
                }
            }
            None => Ok(Series::full_null(&self.name, &self.field.dtype, 1)),
        }
    }
}

/// State for computing count distinct in a sliding window
pub struct CountDistinctWindowState {
    /// Name of the output column
    name: String,
    /// Map of values to their counts
    value_counts: HashMap<u64, usize>,
    /// Field information for the output
    field: Field,
}

impl CountDistinctWindowState {
    pub fn new(out_dtype: &DataType, agg_expr: &AggExpr) -> Self {
        // Extract expression name for output column
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
    fn add(&mut self, value: &Series) -> DaftResult<()> {
        // Hash the values including validity information
        let hashed = value.hash_with_validity(None)?;

        // Iterate through the hash values and increment counts
        for i in 0..hashed.len() {
            if let Some(hash) = hashed.get(i) {
                *self.value_counts.entry(hash).or_insert(0) += 1;
            }
        }

        Ok(())
    }

    fn remove(&mut self, value: &Series) -> DaftResult<()> {
        // Hash the values including validity information
        let hashed = value.hash_with_validity(None)?;

        // Iterate through the hash values and decrement counts
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

    fn evaluate(&self) -> DaftResult<Series> {
        // Count of distinct values is the number of keys in the hashmap
        let count = self.value_counts.len() as u64;

        // Create array with proper type matching field dtype
        match self.field.dtype {
            DataType::UInt64 => {
                let array = UInt64Array::from((self.name.as_str(), vec![count])).into_series();
                Ok(array)
            }
            _ => {
                // Default to UInt64 for other types
                let array = UInt64Array::from((self.name.as_str(), vec![count])).into_series();
                Ok(array)
            }
        }
    }
}

/// Enum that holds different types of window aggregation states
pub enum WindowAggState {
    Mean(MeanWindowState),
    Sum(SumWindowState),
    Min(MinWindowState),
    Max(MaxWindowState),
    CountDistinct(CountDistinctWindowState),
}

// Define a unified macro to implement both WindowAggState methods and the factory function
macro_rules! impl_window_agg_system {
    ($(($variant:ident, $state_type:ty)),*) => {
        // Implement WindowAggState methods
        impl WindowAggState {
            /// Forward the add operation to the appropriate state implementation
            pub fn add(&mut self, value: &Series) -> DaftResult<()> {
                match self {
                    $(
                        Self::$variant(state) => state.add(value),
                    )*
                }
            }

            /// Forward the remove operation to the appropriate state implementation
            pub fn remove(&mut self, value: &Series) -> DaftResult<()> {
                match self {
                    $(
                        Self::$variant(state) => state.remove(value),
                    )*
                }
            }

            /// Forward the evaluate operation to the appropriate state implementation
            pub fn evaluate(&self) -> DaftResult<Series> {
                match self {
                    $(
                        Self::$variant(state) => state.evaluate(),
                    )*
                }
            }
        }

        // Implement factory function for creating window state instances
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

// Implement the entire window aggregation system with a single macro call
impl_window_agg_system! {
    (Mean, MeanWindowState),
    (Sum, SumWindowState),
    (Min, MinWindowState),
    (Max, MaxWindowState),
    (CountDistinct, CountDistinctWindowState)
}
