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
/// Map:     call_agg_block(inputs, groups) → Binary Series
///              One serialized accumulator state per group,
///              produced independently per block.
///          ↓  [binary columns travel through Arrow pipeline]
/// Combine: call_agg_combine(&[u8], &[u8]) → Vec<u8>    (binary, associative)
///              Called by the framework N-1 times per group to fold
///              partial states into one.
///          ↓
/// Reduce:  call_agg_finalize(states, return_field) → typed Series
///              Called once with the one-state-per-group Binary Series.
/// ```
///
/// All three methods are required — there is no single-pass fallback.
#[typetag::serde(tag = "type")]
pub trait AggFn: Send + Sync {
    fn name(&self) -> &'static str;
    fn get_return_field(&self, inputs: &[Field], schema: &Schema) -> DaftResult<Field>;

    /// Process `inputs` for one block and return a `Binary`-typed [`Series`]
    /// containing one serialized accumulator state per group.
    fn call_agg_block(
        &self,
        inputs: Vec<Series>,
        groups: Option<&daft_core::array::ops::GroupIndices>,
    ) -> DaftResult<Series>;

    /// Merge two serialized accumulator states into one.  Must be **associative**
    /// (and ideally commutative) so that the framework can fold partial states
    /// in any order or tree-reduce them in parallel.
    ///
    /// The framework calls this N-1 times per group, where N is the number of
    /// partial states produced by `call_agg_block` across all blocks.
    fn call_agg_combine(&self, a: &[u8], b: &[u8]) -> DaftResult<Vec<u8>>;

    /// Receives a `Binary`-typed [`Series`] with one combined accumulator per
    /// group and returns the final typed output column.
    ///
    /// For simple accumulators (e.g. sum, count) this is just deserialization.
    /// For derived quantities (e.g. mean = sum/count) apply post-processing here.
    fn call_agg_finalize(&self, states: Series, return_field: &Field) -> DaftResult<Series>;
}

/// A cloneable, hashable (by name) wrapper around `Arc<dyn AggFn>`.
///
/// `Hash` and `PartialEq` compare by function name only, matching the
/// expression-identity semantics used elsewhere in the planner.
///
/// **Name uniqueness requirement**: Because equality and hashing are based
/// solely on `AggFn::name()`, two handles with the same name are considered
/// identical by the planner's expression deduplication (`IndexSet`).
/// Registering two distinct `AggFn` implementations under the same name
/// will cause one to silently shadow the other in intermediate aggregation
/// stages.  Ensure that every `AggFn` implementation returns a globally
/// unique name (e.g. include the crate/module prefix).
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

    pub fn call_agg_block(
        &self,
        inputs: Vec<Series>,
        groups: Option<&daft_core::array::ops::GroupIndices>,
    ) -> DaftResult<Series> {
        self.0.call_agg_block(inputs, groups)
    }

    pub fn call_agg_combine(&self, a: &[u8], b: &[u8]) -> DaftResult<Vec<u8>> {
        self.0.call_agg_combine(a, b)
    }

    pub fn call_agg_finalize(&self, states: Series, return_field: &Field) -> DaftResult<Series> {
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

        fn call_agg_block(
            &self,
            _inputs: Vec<Series>,
            _groups: Option<&GroupIndices>,
        ) -> DaftResult<Series> {
            unimplemented!("only used for handle identity tests")
        }

        fn call_agg_combine(&self, _a: &[u8], _b: &[u8]) -> DaftResult<Vec<u8>> {
            unimplemented!("only used for handle identity tests")
        }

        fn call_agg_finalize(&self, _states: Series, _return_field: &Field) -> DaftResult<Series> {
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

        fn call_agg_block(
            &self,
            _inputs: Vec<Series>,
            _groups: Option<&GroupIndices>,
        ) -> DaftResult<Series> {
            unimplemented!("NoopB is only used for handle identity tests")
        }

        fn call_agg_combine(&self, _a: &[u8], _b: &[u8]) -> DaftResult<Vec<u8>> {
            unimplemented!("NoopB is only used for handle identity tests")
        }

        fn call_agg_finalize(&self, _states: Series, _return_field: &Field) -> DaftResult<Series> {
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
