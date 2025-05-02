use std::{
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
impl<T> FunctionArg<T> {
    pub fn unnamed(t: T) -> Self {
        Self::Unnamed(t)
    }
    pub fn named<S: Into<Arc<str>>>(name: S, arg: T) -> Self {
        Self::Named {
            name: name.into(),
            arg,
        }
    }
}

impl<T> From<T> for FunctionArg<T> {
    fn from(arg: T) -> Self {
        Self::Unnamed(arg)
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
/// We use a trait here so the user can
pub trait FunctionArgKey: std::fmt::Debug {
    fn required<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<&'a T>;
    fn optional<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<Option<&'a T>>;
}

/// access a function arg by name
impl FunctionArgKey for &str {
    fn required<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<&'a T> {
        let arg = args.0.iter().find(|arg| match *arg {
            FunctionArg::Named { name: n, .. } => &n.as_ref() == self,
            _ => false,
        });
        if let Some(arg) = arg {
            match arg {
                FunctionArg::Named { name: _, arg } => Ok(arg),
                FunctionArg::Unnamed(_) => Err(DaftError::ComputeError(format!(
                    "Argument not found at position `{self:?}`"
                ))),
            }
        } else {
            Err(DaftError::ComputeError(format!(
                "Argument not found at position `{self:?}`"
            )))
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
                FunctionArg::Unnamed(_) => Err(DaftError::ComputeError(format!(
                    "Argument not found at position `{self:?}"
                ))),
            }
        } else {
            Ok(None)
        }
    }
}

/// access a function arg by position
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
// implemented as a utility. It allows users to get the function arg using multiple patterns
// such as args.required((0, "my_arg"))
// This tries the position `0` first, then if that doesn't exist, it looks for the named arg "my_arg"
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

// implemented as a utility. It allows users to get the function arg using multiple patterns
// such as args.required((0, "my_arg", "some_other_alias"))
// This tries the position `0` first, then if that doesn't exist, it looks for the named arg "my_arg",
// finally it looks for "some_other_alias"
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

impl<T> FunctionArgs<T> {
    pub fn try_new(inner: Vec<FunctionArg<T>>) -> DaftResult<Self> {
        let slf = Self(inner);
        slf.assert_ordering()?;
        Ok(slf)
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Asserts that all unnamed args are before named args
    fn assert_ordering(&self) -> DaftResult<()> {
        let mut has_named = false;
        for arg in &self.0 {
            if has_named && matches!(arg, FunctionArg::Unnamed(_)) {
                return Err(DaftError::ValueError(
                    "Unnamed arguments must come before named arguments".to_string(),
                ));
            }
            if matches!(arg, FunctionArg::Named { .. }) {
                has_named = true;
            }
        }
        Ok(())
    }

    // Get required positional argument
    pub fn required<Key: FunctionArgKey>(&self, position: Key) -> DaftResult<&T> {
        position.required(self).map_err(|_| {
            DaftError::ValueError(format!(
                "Expected a value for the required argument at position `{position:?}`"
            ))
        })
    }

    pub fn optional<Key: FunctionArgKey>(&self, position: Key) -> DaftResult<Option<&T>> {
        position.optional(self).map_err(|_| {
            DaftError::ValueError(format!(
                "Expected a value for the optional argument at position `{position:?}`"
            ))
        })
    }
}

impl<T> From<Vec<T>> for FunctionArgs<T> {
    fn from(args: Vec<T>) -> Self {
        Self(args.into_iter().map(FunctionArg::from).collect())
    }
}

#[typetag::serde(tag = "type")]
pub trait ScalarUDF: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &'static str;
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }
    fn evaluate_from_series(&self, _inputs: &[Series]) -> DaftResult<Series> {
        panic!("evaluate_from_series is deprecated")
    }

    fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series>;
    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field>;
    fn docstring(&self) -> &'static str {
        "No documentation available"
    }
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

#[cfg(test)]
mod tests {
    use super::FunctionArg;
    use crate::functions::FunctionArgs;
    #[test]
    fn test_function_args_ordering() {
        let res = FunctionArgs::try_new(vec![
            FunctionArg::unnamed(1),
            FunctionArg::unnamed(2),
            FunctionArg::named("arg1", 3),
        ]);

        assert!(res.is_err());
    }
    #[test]
    fn test_function_args_ordering_invalid() {
        let res = FunctionArgs::try_new(vec![
            FunctionArg::unnamed(1),
            FunctionArg::named("arg1", 2),
            FunctionArg::unnamed(3),
        ]);

        assert!(res.is_err());
    }
}
