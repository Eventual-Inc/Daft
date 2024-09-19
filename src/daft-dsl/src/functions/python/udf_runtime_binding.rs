use std::hash::{Hash, Hasher};

use serde::{de::Visitor, Deserialize, Serialize};

use super::RuntimePyObject;

/// A binding between the StatefulPythonUDF and an initialized Python callable
///
/// This is `Unbound` during planning, and bound to an initialized Python callable
/// by an Actor right before execution.
///
/// Note that attempting to Hash, Eq, Serde this when it is bound will panic!
#[derive(Debug, Clone)]
pub enum UDFRuntimeBinding {
    Unbound,
    Bound(RuntimePyObject),
}

impl PartialEq for UDFRuntimeBinding {
    fn eq(&self, other: &Self) -> bool {
        matches!((self, other), (Self::Unbound, Self::Unbound))
    }
}

impl Eq for UDFRuntimeBinding {}

impl Hash for UDFRuntimeBinding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Unbound => state.write_u8(0),
            Self::Bound(_) => panic!("Cannot hash a bound UDFRuntimeBinding."),
        }
    }
}

impl Serialize for UDFRuntimeBinding {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Unbound => serializer.serialize_unit(),
            Self::Bound(_) => panic!("Cannot serialize a bound UDFRuntimeBinding."),
        }
    }
}

struct UDFRuntimeBindingVisitor;

impl<'de> Visitor<'de> for UDFRuntimeBindingVisitor {
    type Value = UDFRuntimeBinding;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("unit")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(UDFRuntimeBinding::Unbound)
    }
}

impl<'de> Deserialize<'de> for UDFRuntimeBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_unit(UDFRuntimeBindingVisitor)
    }
}
