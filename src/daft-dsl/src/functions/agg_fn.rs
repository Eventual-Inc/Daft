use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_error::DaftResult;
use daft_core::prelude::{DataType, Field, Literal, Series};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub type State = Literal;

/// Trait for user-defined aggregate functions (UDAFs) that plug into the native execution engine.
///
/// Registered with `typetag::serde` so implementations survive plan serialization across
/// worker boundaries without extra registration steps.
///
/// Execution follows a three-stage Map → Combine → Reduce pipeline:
///
/// ```text
/// Map:     call_agg_block(inputs) → Vec<State>          (one call per group per input block)
///          ↓  [framework packs States into a typed Struct column]
/// Combine: call_agg_combine(states) → Vec<State>        (one call per group, may be repeated)
///          ↓
/// Reduce:  call_agg_finalize(state) → State             (one call per group)
/// ```
///
/// Intermediate state is typed: `state_fields` declares one Arrow [`Field`] per state component,
/// and the framework carries them as a Struct column between stages. This avoids binary
/// serialization and lets Arrow and the query planner reason about state types.
#[typetag::serde(tag = "type")]
pub trait AggFn: Send + Sync {
    /// Globally unique name for this function.
    ///
    /// Used as the serde discriminant (`typetag` tag value) and as the sole basis for
    /// [`AggFnHandle`]'s `Hash` and `PartialEq`. Two handles with the same name are
    /// considered the same function — choose a name that cannot collide across crates.
    fn name(&self) -> &'static str;

    /// Infer the output [`DataType`] from the types of the input columns.
    ///
    /// Called once during planning. `input_types` is parallel to the `inputs` slice
    /// passed at execution time.
    fn return_dtype(&self, input_types: &[DataType]) -> DaftResult<DataType>;

    /// Declare the schema of the intermediate accumulator state.
    ///
    /// Returns one [`Field`] per state component. The framework uses these fields to
    /// build the Struct column that carries partial results between Map and Combine stages.
    /// `inputs` are the input column fields, available here to derive state types that
    /// depend on the input schema (e.g., keeping the same dtype as the input).
    fn state_fields(&self, inputs: &[Field]) -> DaftResult<Vec<Field>>;

    /// **Map stage.** Consume all rows of `inputs` for one group and emit the initial
    /// accumulator state as one [`State`] per field declared by [`state_fields`].
    ///
    /// `inputs` contains one [`Series`] per input column; every Series has the same length
    /// (all rows belonging to this group). Nulls within a Series must be handled explicitly.
    fn call_agg_block(&self, inputs: Vec<Series>) -> DaftResult<Vec<State>>;

    /// **Combine stage.** Merge all partial states for one group into a single state.
    ///
    /// `states` contains one [`Series`] per state field declared by [`state_fields`];
    /// each row in a Series is one partial result from a prior Map or Combine call.
    /// Must be **associative and commutative** — the framework does not guarantee the
    /// order in which partial states arrive. Returns one [`State`] per state field.
    fn call_agg_combine(&self, states: Vec<Series>) -> DaftResult<Vec<State>>;

    /// **Reduce stage.** Produce the final output value from the fully-merged state.
    ///
    /// Receives one [`State`] per state field. Called exactly once per group after all
    /// Combine passes are complete. Apply any post-processing here (e.g., `mean = sum / count`).
    fn call_agg_finalize(&self, state: Vec<State>) -> DaftResult<State>;
}

/// A cloneable, cheaply-shareable handle to an [`AggFn`] implementation.
///
/// Wraps `Arc<dyn AggFn>` so handles can be embedded in expression trees that require
/// `Clone`. Identity comparisons (`Hash`, `PartialEq`) use the function name only,
/// consistent with how the planner treats other expression kinds.
#[derive(Clone)]
pub struct AggFnHandle(Arc<dyn AggFn>);

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

    pub fn call_agg_block(&self, inputs: Vec<Series>) -> DaftResult<Vec<State>> {
        self.0.call_agg_block(inputs)
    }

    pub fn call_agg_combine(&self, states: Vec<Series>) -> DaftResult<Vec<State>> {
        self.0.call_agg_combine(states)
    }

    pub fn call_agg_finalize(&self, state: Vec<State>) -> DaftResult<State> {
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

        fn call_agg_block(&self, _inputs: Vec<Series>) -> DaftResult<Vec<State>> {
            unimplemented!("only used for handle identity tests")
        }

        fn call_agg_combine(&self, _states: Vec<Series>) -> DaftResult<Vec<State>> {
            unimplemented!("only used for handle identity tests")
        }

        fn call_agg_finalize(&self, _state: Vec<State>) -> DaftResult<State> {
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

        fn call_agg_block(&self, _inputs: Vec<Series>) -> DaftResult<Vec<State>> {
            unimplemented!("NoopB is only used for handle identity tests")
        }

        fn call_agg_combine(&self, _states: Vec<Series>) -> DaftResult<Vec<State>> {
            unimplemented!("NoopB is only used for handle identity tests")
        }

        fn call_agg_finalize(&self, _state: Vec<State>) -> DaftResult<State> {
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
