use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_error::DaftResult;
use daft_core::prelude::{DataType, Field, Literal, Series};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Trait for native extension aggregate UDFs.
///
/// Registered with `typetag::serde` so that plan serialization/deserialization works.
///
/// Three mandatory stages:
///
/// ```text
/// Map:     call_agg_block(inputs) → Vec<Literal>
///              One Literal per state field, for this group.
///          ↓  [framework packs Literals into typed Struct columns]
/// Combine: call_agg_combine(states) → Vec<Literal>   (associative)
///              Folds all partial states for this group into one.
///          ↓
/// Reduce:  call_agg_finalize(state) → Literal
///              Called once per group to produce the final value.
/// ```
///
/// The intermediate state is declared via `state_fields` and flows as a Struct column
/// (one child field per state component). This avoids serialization overhead and lets
/// the planner and Arrow pipeline work with properly typed data throughout.
#[typetag::serde(tag = "type")]
pub trait AggFn: Send + Sync {
    fn name(&self) -> &'static str;
    fn return_dtype(&self, input_types: &[DataType]) -> DaftResult<DataType>;

    /// Declare the typed Arrow fields for the intermediate accumulator state.
    ///
    /// Returns one `Field` per state component. The names and types declared here
    /// are used to build the Struct column that carries intermediate state between
    /// the Map and Reduce stages.
    fn state_fields(&self, inputs: &[Field]) -> DaftResult<Vec<Field>>;

    /// Process `inputs` for one group and return one [`Literal`] per state field.
    fn call_agg_block(&self, inputs: Vec<Series>) -> DaftResult<Vec<Literal>>;

    /// Merge all partial states for one group into a single state.
    ///
    /// `states` contains one [`Series`] per state field (matching `state_fields`),
    /// where each row is one partial result. Must be **associative**.
    /// Returns one [`Literal`] per state field.
    fn call_agg_combine(&self, states: Vec<Series>) -> DaftResult<Vec<Literal>>;

    /// Receives one [`Literal`] per state field for this group and returns the
    /// final value.
    ///
    /// For derived quantities (e.g. mean = sum/count) apply post-processing here.
    fn call_agg_finalize(&self, state: Vec<Literal>) -> DaftResult<Literal>;
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

    pub fn return_dtype(&self, input_types: &[DataType]) -> DaftResult<DataType> {
        self.0.return_dtype(input_types)
    }

    pub fn state_fields(&self, inputs: &[Field]) -> DaftResult<Vec<Field>> {
        self.0.state_fields(inputs)
    }

    pub fn call_agg_block(&self, inputs: Vec<Series>) -> DaftResult<Vec<Literal>> {
        self.0.call_agg_block(inputs)
    }

    pub fn call_agg_combine(&self, states: Vec<Series>) -> DaftResult<Vec<Literal>> {
        self.0.call_agg_combine(states)
    }

    pub fn call_agg_finalize(&self, state: Vec<Literal>) -> DaftResult<Literal> {
        self.0.call_agg_finalize(state)
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
    use daft_core::prelude::*;
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

        fn return_dtype(&self, input_types: &[DataType]) -> DaftResult<DataType> {
            Ok(input_types[0].clone())
        }

        fn state_fields(&self, inputs: &[Field]) -> DaftResult<Vec<Field>> {
            Ok(vec![inputs[0].clone()])
        }

        fn call_agg_block(&self, _inputs: Vec<Series>) -> DaftResult<Vec<Literal>> {
            unimplemented!("only used for handle identity tests")
        }

        fn call_agg_combine(&self, _states: Vec<Series>) -> DaftResult<Vec<Literal>> {
            unimplemented!("only used for handle identity tests")
        }

        fn call_agg_finalize(&self, _state: Vec<Literal>) -> DaftResult<Literal> {
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

        fn return_dtype(&self, input_types: &[DataType]) -> DaftResult<DataType> {
            Ok(input_types[0].clone())
        }

        fn state_fields(&self, inputs: &[Field]) -> DaftResult<Vec<Field>> {
            Ok(vec![inputs[0].clone()])
        }

        fn call_agg_block(&self, _inputs: Vec<Series>) -> DaftResult<Vec<Literal>> {
            unimplemented!("NoopB is only used for handle identity tests")
        }

        fn call_agg_combine(&self, _states: Vec<Series>) -> DaftResult<Vec<Literal>> {
            unimplemented!("NoopB is only used for handle identity tests")
        }

        fn call_agg_finalize(&self, _state: Vec<Literal>) -> DaftResult<Literal> {
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
