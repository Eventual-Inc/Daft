pub mod float;
pub mod hash;
pub mod image;
pub mod json;
pub mod list;
pub mod map;
pub mod minhash;
pub mod numeric;
pub mod partitioning;
pub mod registry;
pub mod sketch;
pub mod struct_;
pub mod temporal;
pub mod uri;
pub mod utf8;

use std::any::Any;
use std::fmt::{Display, Formatter, Result};
use std::hash::Hash;
use std::sync::Arc;

use crate::ExprRef;

use self::image::ImageExpr;
use self::json::JsonExpr;
use self::list::ListExpr;
use self::map::MapExpr;
use self::numeric::NumericExpr;
use self::partitioning::PartitioningExpr;
use self::sketch::SketchExpr;
use self::struct_::StructExpr;
use self::temporal::TemporalExpr;
use self::utf8::Utf8Expr;
use self::{float::FloatExpr, uri::UriExpr};
use common_error::DaftResult;
use daft_core::datatypes::FieldID;
use daft_core::{datatypes::Field, schema::Schema, series::Series};
use hash::HashEvaluator;

use registry::REGISTRY;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
pub mod python;
#[cfg(feature = "python")]
use python::PythonUDF;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FunctionExpr {
    Numeric(NumericExpr),
    Float(FloatExpr),
    Utf8(Utf8Expr),
    Temporal(TemporalExpr),
    List(ListExpr),
    Map(MapExpr),
    Sketch(SketchExpr),
    Struct(StructExpr),
    Json(JsonExpr),
    Image(ImageExpr),
    #[cfg(feature = "python")]
    Python(PythonUDF),
    Partitioning(PartitioningExpr),
    Uri(UriExpr),
    Hash,
}

#[derive(Debug, Clone)]
pub struct ScalarFunction {
    pub udf: Arc<dyn ScalarUDF>,
    pub args: Vec<ExprRef>,
}

impl ScalarFunction {
    pub fn new<UDF: ScalarUDF + 'static>(udf: UDF, inputs: Vec<ExprRef>) -> Self {
        Self {
            udf: Arc::new(udf),
            args: inputs,
        }
    }

    pub fn name(&self) -> &str {
        &self.udf.name()
    }
    pub fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        self.udf.to_field(&inputs, schema)
    }
}

impl PartialEq for ScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name() && self.udf.semantic_id() == other.udf.semantic_id()
    }
}

impl Eq for ScalarFunction {}
impl Hash for ScalarFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.udf.semantic_id().hash(state);
    }
}

impl Serialize for ScalarFunction {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut struct_ = serializer.serialize_struct("ScalarFunction", 2)?;
        struct_.serialize_field("name", &self.name())?;
        struct_.serialize_field("inputs", &self.args)?;

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
                    None => return Err(serde::de::Error::unknown_field(&name, &[])),
                    Some(udf) => {
                        let inputs = seq
                            .next_element::<Vec<ExprRef>>()?
                            .ok_or_else(|| serde::de::Error::invalid_length(1, &"2"))?;

                        Ok(ScalarFunction {
                            udf: udf.clone(),
                            args: inputs,
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
                    None => return Err(serde::de::Error::unknown_field(&name, &[])),
                    Some(udf) => {
                        let inputs = map.next_value::<Vec<ExprRef>>()?;
                        Ok(ScalarFunction {
                            udf: udf.clone(),
                            args: inputs,
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

pub trait ScalarUDF: Send + Sync + std::fmt::Debug + erased_serde::Serialize {
    fn semantic_id(&self) -> FieldID;
    /// Returns this object as an [`Any`] trait object
    fn as_any(&self) -> &dyn Any;
    fn name(&self) -> &'static str;
    fn evaluate(&self, inputs: &[Series], args: &[ExprRef]) -> DaftResult<Series>;
    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field>;
}

erased_serde::serialize_trait_object!(ScalarUDF);

pub trait FunctionEvaluator {
    fn fn_name(&self) -> &'static str;
    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        expr: &FunctionExpr,
    ) -> DaftResult<Field>;
    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series>;
}

impl FunctionExpr {
    #[inline]
    fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use FunctionExpr::*;
        match self {
            Numeric(expr) => expr.get_evaluator(),
            Float(expr) => expr.get_evaluator(),
            Utf8(expr) => expr.get_evaluator(),
            Temporal(expr) => expr.get_evaluator(),
            List(expr) => expr.get_evaluator(),
            Map(expr) => expr.get_evaluator(),
            Sketch(expr) => expr.get_evaluator(),
            Struct(expr) => expr.get_evaluator(),
            Json(expr) => expr.get_evaluator(),
            Image(expr) => expr.get_evaluator(),
            Uri(expr) => expr.get_evaluator(),
            #[cfg(feature = "python")]
            Python(expr) => expr,
            Partitioning(expr) => expr.get_evaluator(),
            Hash => &HashEvaluator {},
        }
    }
}

impl Display for FunctionExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.fn_name())
    }
}

impl Display for ScalarFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.udf.name())
    }
}

impl FunctionEvaluator for FunctionExpr {
    fn fn_name(&self) -> &'static str {
        self.get_evaluator().fn_name()
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        expr: &FunctionExpr,
    ) -> DaftResult<Field> {
        self.get_evaluator().to_field(inputs, schema, expr)
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        self.get_evaluator().evaluate(inputs, expr)
    }
}

pub fn function_display(f: &mut Formatter, func: &FunctionExpr, inputs: &[ExprRef]) -> Result {
    write!(f, "{}(", func)?;
    for (i, input) in inputs.iter().enumerate() {
        if i != 0 {
            write!(f, ", ")?;
        }
        write!(f, "{input}")?;
    }
    write!(f, ")")?;
    Ok(())
}

pub fn scalar_function_display(
    f: &mut Formatter,
    func: &ScalarFunction,
    inputs: &[ExprRef],
) -> Result {
    write!(f, "{}(", func)?;
    for (i, input) in inputs.iter().enumerate() {
        if i != 0 {
            write!(f, ", ")?;
        }
        write!(f, "{input}")?;
    }
    write!(f, ")")?;
    Ok(())
}

pub fn function_semantic_id(func: &FunctionExpr, inputs: &[ExprRef], schema: &Schema) -> FieldID {
    let inputs = inputs
        .iter()
        .map(|expr| expr.semantic_id(schema).id.to_string())
        .collect::<Vec<String>>()
        .join(", ");
    // TODO: check for function idempotency here.
    FieldID::new(format!("Function_{func:?}({inputs})"))
}

pub fn scalar_function_semantic_id(
    func: &ScalarFunction,
    inputs: &[ExprRef],
    schema: &Schema,
) -> FieldID {
    let inputs = inputs
        .iter()
        .map(|expr| expr.semantic_id(schema).id.to_string())
        .collect::<Vec<String>>()
        .join(", ");
    // TODO: check for function idempotency here.
    FieldID::new(format!("Function_{func:?}({inputs})"))
}
