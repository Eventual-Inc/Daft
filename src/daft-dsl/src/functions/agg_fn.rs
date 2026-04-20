use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_error::DaftResult;
use daft_core::prelude::{Field, Schema, Series};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Trait for native extension aggregate UDFs.
///
/// Registered with `typetag::serde` so that plan serialization/deserialization works.
///
/// Three mandatory stages:
///
/// ```text
/// Map:     call_agg_block(inputs, groups) → Vec<Series>
///              One typed Series per state field, one row per group.
///          ↓  [typed state columns travel through the Arrow pipeline]
/// Combine: call_agg_combine(states, groups) → Vec<Series>   (batched, associative)
///              One call folds all partial states for all groups at once.
///          ↓
/// Reduce:  call_agg_finalize(states, return_field) → typed Series
///              Called once with one-row-per-group state to produce final output.
/// ```
///
/// The intermediate state is declared via `state_fields` and flows as a Struct column
/// (one child field per state component). This avoids serialization overhead and lets
/// the planner and Arrow pipeline work with properly typed data throughout.
#[typetag::serde(tag = "type")]
pub trait AggFn: Send + Sync {
    fn name(&self) -> &'static str;
    fn get_return_field(&self, inputs: &[Field], schema: &Schema) -> DaftResult<Field>;

    /// Declare the typed Arrow fields for the intermediate accumulator state.
    ///
    /// Returns one `Field` per state component. The names and types declared here
    /// are used to build the Struct column that carries intermediate state between
    /// the Map and Reduce stages.
    fn state_fields(&self, inputs: &[Field]) -> DaftResult<Vec<Field>>;

    /// Process `inputs` for one block and return one typed [`Series`] per state field,
    /// each containing one row per group.
    fn call_agg_block(
        &self,
        inputs: Vec<Series>,
        groups: Option<&daft_core::array::ops::GroupIndices>,
    ) -> DaftResult<Vec<Series>>;

    /// Merge all partial states in a single batched call.
    ///
    /// `states` contains one [`Series`] per state field (matching `state_fields`),
    /// each with one row per partial result across all groups.
    /// `groups` maps rows to output groups (same contract as `call_agg_block`).
    ///
    /// Must be **associative** so that the framework can fold partial states in any
    /// order. Returns one [`Series`] per state field, with one row per output group.
    fn call_agg_combine(
        &self,
        states: Vec<Series>,
        groups: Option<&daft_core::array::ops::GroupIndices>,
    ) -> DaftResult<Vec<Series>>;

    /// Receives one [`Series`] per state field (one row per group) and returns the
    /// final typed output column.
    ///
    /// For simple accumulators (e.g. sum) this is just a rename/cast.
    /// For derived quantities (e.g. mean = sum/count) apply post-processing here.
    fn call_agg_finalize(&self, states: Vec<Series>, return_field: &Field) -> DaftResult<Series>;
}

/// A cloneable, hashable (by name) wrapper around `Arc<dyn AggFn>`.
///
/// `Hash` and `PartialEq` compare by function name only, matching the
/// expression-identity semantics used elsewhere in the planner.
#[derive(Clone)]
pub struct AggFnHandle(pub Arc<dyn AggFn>);

impl AggFnHandle {
    pub fn new(udf: Arc<dyn AggFn>) -> Self {
        Self(udf)
    }

    pub fn name(&self) -> &'static str {
        self.0.name()
    }

    pub fn get_return_field(&self, inputs: &[Field], schema: &Schema) -> DaftResult<Field> {
        self.0.get_return_field(inputs, schema)
    }

    pub fn state_fields(&self, inputs: &[Field]) -> DaftResult<Vec<Field>> {
        self.0.state_fields(inputs)
    }

    pub fn call_agg_block(
        &self,
        inputs: Vec<Series>,
        groups: Option<&daft_core::array::ops::GroupIndices>,
    ) -> DaftResult<Vec<Series>> {
        self.0.call_agg_block(inputs, groups)
    }

    pub fn call_agg_combine(
        &self,
        states: Vec<Series>,
        groups: Option<&daft_core::array::ops::GroupIndices>,
    ) -> DaftResult<Vec<Series>> {
        self.0.call_agg_combine(states, groups)
    }

    pub fn call_agg_finalize(&self, states: Vec<Series>, return_field: &Field) -> DaftResult<Series> {
        self.0.call_agg_finalize(states, return_field)
    }
}

impl Hash for AggFnHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.name().hash(state);
    }
}

impl PartialEq for AggFnHandle {
    fn eq(&self, other: &Self) -> bool {
        self.0.name() == other.0.name()
    }
}

impl Eq for AggFnHandle {}

impl std::fmt::Debug for AggFnHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AggFnHandle({})", self.0.name())
    }
}

impl std::fmt::Display for AggFnHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.name())
    }
}

impl Serialize for AggFnHandle {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AggFnHandle {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let udf = <Box<dyn AggFn>>::deserialize(deserializer)?;
        Ok(Self(Arc::from(udf)))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        sync::Arc,
    };

    use common_error::DaftResult;
    use daft_core::{array::ops::GroupIndices, prelude::*};
    use serde::{Deserialize, Serialize};

    use super::*;

    // Two distinct unit structs so we can test inequality without parameterized names.
    #[derive(Serialize, Deserialize)]
    struct NoopA;

    #[typetag::serde(name = "NoopA")]
    impl AggFn for NoopA {
        fn name(&self) -> &'static str {
            "noop_a"
        }

        fn get_return_field(&self, inputs: &[Field], _schema: &Schema) -> DaftResult<Field> {
            Ok(inputs[0].clone())
        }

        fn state_fields(&self, inputs: &[Field]) -> DaftResult<Vec<Field>> {
            Ok(vec![inputs[0].clone()])
        }

        fn call_agg_block(
            &self,
            _inputs: Vec<Series>,
            _groups: Option<&GroupIndices>,
        ) -> DaftResult<Vec<Series>> {
            unimplemented!("only used for handle identity tests")
        }

        fn call_agg_combine(
            &self,
            _states: Vec<Series>,
            _groups: Option<&GroupIndices>,
        ) -> DaftResult<Vec<Series>> {
            unimplemented!("only used for handle identity tests")
        }

        fn call_agg_finalize(&self, _states: Vec<Series>, _return_field: &Field) -> DaftResult<Series> {
            unimplemented!("only used for handle identity tests")
        }
    }

    #[derive(Serialize, Deserialize)]
    struct NoopB;

    #[typetag::serde(name = "NoopB")]
    impl AggFn for NoopB {
        fn name(&self) -> &'static str {
            "noop_b"
        }

        fn get_return_field(&self, inputs: &[Field], _schema: &Schema) -> DaftResult<Field> {
            Ok(inputs[0].clone())
        }

        fn state_fields(&self, inputs: &[Field]) -> DaftResult<Vec<Field>> {
            Ok(vec![inputs[0].clone()])
        }

        fn call_agg_block(
            &self,
            _inputs: Vec<Series>,
            _groups: Option<&GroupIndices>,
        ) -> DaftResult<Vec<Series>> {
            unimplemented!("NoopB is only used for handle identity tests")
        }

        fn call_agg_combine(
            &self,
            _states: Vec<Series>,
            _groups: Option<&GroupIndices>,
        ) -> DaftResult<Vec<Series>> {
            unimplemented!("NoopB is only used for handle identity tests")
        }

        fn call_agg_finalize(&self, _states: Vec<Series>, _return_field: &Field) -> DaftResult<Series> {
            unimplemented!("NoopB is only used for handle identity tests")
        }
    }

    #[test]
    fn test_name() {
        let h = AggFnHandle::new(Arc::new(NoopA));
        assert_eq!(h.name(), "noop_a");
    }

    #[test]
    fn test_eq_by_name() {
        let a1 = AggFnHandle::new(Arc::new(NoopA));
        let a2 = AggFnHandle::new(Arc::new(NoopA));
        let b = AggFnHandle::new(Arc::new(NoopB));
        assert_eq!(a1, a2);
        assert_ne!(a1, b);
    }

    #[test]
    fn test_hash_consistent_with_eq() {
        let hash_of = |x: &AggFnHandle| -> u64 {
            let mut s = DefaultHasher::new();
            x.hash(&mut s);
            s.finish()
        };
        let a1 = AggFnHandle::new(Arc::new(NoopA));
        let a2 = AggFnHandle::new(Arc::new(NoopA));
        let b = AggFnHandle::new(Arc::new(NoopB));
        assert_eq!(hash_of(&a1), hash_of(&a2));
        assert_ne!(hash_of(&a1), hash_of(&b));
    }

    #[test]
    fn test_display() {
        let h = AggFnHandle::new(Arc::new(NoopA));
        assert_eq!(format!("{h}"), "noop_a");
    }

    #[test]
    fn test_debug() {
        let h = AggFnHandle::new(Arc::new(NoopA));
        assert_eq!(format!("{h:?}"), "AggFnHandle(noop_a)");
    }
}
