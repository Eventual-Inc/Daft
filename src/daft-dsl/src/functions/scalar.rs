use std::{
    any::Any,
    borrow::Cow,
    fmt::{Display, Formatter},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl From<ScalarFunction> for ExprRef {
    fn from(func: ScalarFunction) -> Self {
        Expr::ScalarFunction(func).into()
    }
}

#[derive(Debug)]
pub enum FunctionArg<T> {
    Named {
        name: Arc<str>, // todo: use Identifier instead of String

        arg: T,
    },
    Unnamed(T),
}

impl<T> From<T> for FunctionArg<T> {
    fn from(arg: T) -> Self {
        FunctionArg::Unnamed(arg)
    }
}

#[derive(Debug)]
pub struct FunctionArgs<T>(Vec<FunctionArg<T>>);

impl<T> FunctionArgs<T> {
    pub fn into_inner(self) -> Vec<T> {
        self.0
            .into_iter()
            .map(|arg| match arg {
                FunctionArg::Named { name: _, arg } => arg,
                FunctionArg::Unnamed(arg) => arg,
            })
            .collect()
    }
}

/// trait to look up either positional or named values
pub trait FunctionArgKey {
    fn required<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<&'a T>;
    fn optional<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<Option<&'a T>>;
}

impl FunctionArgKey for &str {
    fn required<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<&'a T> {
        let arg = args.0.iter().find(|arg| match *arg {
            FunctionArg::Named { name: n, .. } => &n.as_ref() == self,
            _ => false,
        });
        if let Some(arg) = arg {
            match arg {
                FunctionArg::Named { name: _, arg } => Ok(arg),
                FunctionArg::Unnamed(_) => {
                    Err(DaftError::ComputeError("Argument not found".to_string()))
                }
            }
        } else {
            Err(DaftError::ComputeError("Argument not found".to_string()))
        }
    }

    fn optional<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<Option<&'a T>> {
        let arg = args.0.iter().find(|arg| match *arg {
            FunctionArg::Named { name: n, .. } => &n.as_ref() == self,
            _ => false,
        });
        if let Some(arg) = arg {
            match arg {
                FunctionArg::Named { name: _, arg } => Ok(Some(arg)),
                FunctionArg::Unnamed(_) => {
                    Err(DaftError::ComputeError("Argument not found".to_string()))
                }
            }
        } else {
            Ok(None)
        }
    }
}

impl FunctionArgKey for usize {
    fn required<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<&'a T> {
        match &args.0[*self] {
            FunctionArg::Unnamed(value) => Ok(value),
            _ => Err(DaftError::ComputeError(format!(
                "Expected positional argument at position {}",
                self
            ))),
        }
    }

    fn optional<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<Option<&'a T>> {
        match &args.0[*self] {
            FunctionArg::Unnamed(value) => Ok(Some(value)),
            _ => Err(DaftError::ComputeError(format!(
                "Expected positional argument at position {}",
                self
            ))),
        }
    }
}

impl<F1, F2> FunctionArgKey for (F1, F2)
where
    F1: FunctionArgKey,
    F2: FunctionArgKey,
{
    fn required<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<&'a T> {
        self.0.required(args).or_else(|_| self.1.required(args))
    }

    fn optional<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<Option<&'a T>> {
        self.0.optional(args).or_else(|_| self.1.optional(args))
    }
}

impl<F1, F2, F3> FunctionArgKey for (F1, F2, F3)
where
    F1: FunctionArgKey,
    F2: FunctionArgKey,
    F3: FunctionArgKey,
{
    fn required<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<&'a T> {
        self.0
            .required(args)
            .or_else(|_| self.1.required(args))
            .or_else(|_| self.2.required(args))
    }

    fn optional<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<Option<&'a T>> {
        self.0
            .optional(args)
            .or_else(|_| self.1.optional(args))
            .or_else(|_| self.2.optional(args))
    }
}

impl<F1, F2, F3, F4> FunctionArgKey for (F1, F2, F3, F4)
where
    F1: FunctionArgKey,
    F2: FunctionArgKey,
    F3: FunctionArgKey,
    F4: FunctionArgKey,
{
    fn required<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<&'a T> {
        self.0
            .required(args)
            .or_else(|_| self.1.required(args))
            .or_else(|_| self.2.required(args))
            .or_else(|_| self.3.required(args))
    }

    fn optional<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<Option<&'a T>> {
        self.0
            .optional(args)
            .or_else(|_| self.1.optional(args))
            .or_else(|_| self.2.optional(args))
            .or_else(|_| self.3.optional(args))
    }
}

impl<T> FunctionArgs<T> {
    pub fn new(inner: Vec<FunctionArg<T>>) -> Self {
        FunctionArgs(inner)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    // Get required positional argument
    pub fn required<Key: FunctionArgKey>(&self, position: Key) -> DaftResult<&T> {
        position.required(self)
    }

    pub fn optional(&self, position: usize, name: &str) -> DaftResult<Cow<T>>
    where
        T: Default + Clone,
    {
        if position < self.0.len() {
            if let FunctionArg::Unnamed(value) = &self.0[position] {
                return Ok(Cow::Borrowed(value));
            }
        }

        for arg in &self.0 {
            if let FunctionArg::Named { name: n, arg } = arg {
                if n.as_ref() == name {
                    return Ok(Cow::Borrowed(arg));
                }
            }
        }

        Ok(Cow::Owned(T::default()))
    }

    pub fn optional_with_default(
        &self,
        position: usize,
        name: &str,
        default: T,
    ) -> DaftResult<Cow<T>>
    where
        T: Default + Clone,
    {
        if position < self.0.len() {
            if let FunctionArg::Unnamed(value) = &self.0[position] {
                return Ok(Cow::Borrowed(value));
            }
        }

        for arg in &self.0 {
            if let FunctionArg::Named { name: n, arg } = arg {
                if n.as_ref() == name {
                    return Ok(Cow::Borrowed(arg));
                }
            }
        }

        Ok(Cow::Owned(default))
    }
}

impl<T> From<Vec<T>> for FunctionArgs<T> {
    fn from(args: Vec<T>) -> Self {
        FunctionArgs(args.into_iter().map(FunctionArg::from).collect())
    }
}

#[typetag::serde(tag = "type")]
pub trait ScalarUDF: Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;
    fn name(&self) -> &'static str;
    fn evaluate_from_series(&self, _inputs: &[Series]) -> DaftResult<Series> {
        panic!("evaluate_from_series is deprecated")
    }

    fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series>;

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field>;
}

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
