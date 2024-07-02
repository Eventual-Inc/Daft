use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use common_error::DaftResult;
use daft_core::datatypes::FieldID;
use daft_core::{datatypes::Field, schema::Schema, series::Series};

use crate::ExprRef;

use super::registry::REGISTRY;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone)]
pub struct ScalarFunction {
    pub udf: Arc<dyn ScalarUDF>,
    pub inputs: Vec<ExprRef>,
}

impl ScalarFunction {
    pub fn new<UDF: ScalarUDF + 'static>(udf: UDF, inputs: Vec<ExprRef>) -> Self {
        Self {
            udf: Arc::new(udf),
            inputs,
        }
    }

    pub fn name(&self) -> &str {
        self.udf.name()
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        self.udf.to_field(&self.inputs, schema)
    }
}

pub trait ScalarUDF: Send + Sync + std::fmt::Debug + erased_serde::Serialize {
    fn as_any(&self) -> &dyn Any;
    fn name(&self) -> &'static str;
    // TODO: evaluate should allow for &[Series | LiteralValue] inputs.
    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series>;
    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field>;
}

erased_serde::serialize_trait_object!(ScalarUDF);

pub fn scalar_function_semantic_id(func: &ScalarFunction, schema: &Schema) -> FieldID {
    let inputs = func
        .inputs
        .iter()
        .map(|expr| expr.semantic_id(schema).id.to_string())
        .collect::<Vec<String>>()
        .join(", ");
    // TODO: check for function idempotency here.
    FieldID::new(format!("Function_{func:?}({inputs})"))
}

impl PartialEq for ScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name() && self.inputs == other.inputs
    }
}

impl Eq for ScalarFunction {}
impl std::hash::Hash for ScalarFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.inputs.hash(state);
    }
}

impl Serialize for ScalarFunction {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut struct_ = serializer.serialize_struct("ScalarFunction", 2)?;
        struct_.serialize_field("name", &self.name())?;
        struct_.serialize_field("inputs", &self.inputs)?;

        struct_.end()
    }
}

impl<'de> Deserialize<'de> for ScalarFunction {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ScalarFunctionVisitor;

        impl<'de> serde::de::Visitor<'de> for ScalarFunctionVisitor {
            type Value = ScalarFunction;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct ScalarFunction")
            }
            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let name: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &"2"))?;

                match REGISTRY.get(&name) {
                    None => Err(serde::de::Error::unknown_field(&name, &[])),
                    Some(udf) => {
                        let inputs = seq
                            .next_element::<Vec<ExprRef>>()?
                            .ok_or_else(|| serde::de::Error::invalid_length(1, &"2"))?;

                        Ok(ScalarFunction {
                            udf: udf.clone(),
                            inputs,
                        })
                    }
                }
            }
            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let name = map
                    .next_key::<String>()?
                    .ok_or_else(|| serde::de::Error::missing_field("name"))?;

                match REGISTRY.get(&name) {
                    None => Err(serde::de::Error::unknown_field(&name, &[])),
                    Some(udf) => {
                        let inputs = map.next_value::<Vec<ExprRef>>()?;
                        Ok(ScalarFunction {
                            udf: udf.clone(),
                            inputs,
                        })
                    }
                }
            }
        }
        deserializer.deserialize_struct(
            "ScalarFunction",
            &["name", "inputs"],
            ScalarFunctionVisitor,
        )
    }
}

impl Display for ScalarFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}(", self.name())?;
        for (i, input) in self.inputs.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{input}")?;
        }
        write!(f, ")")?;
        Ok(())
    }
}
