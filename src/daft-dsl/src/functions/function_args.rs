use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
pub use common_macros::FunctionArgs;
use serde::{Deserialize, Serialize};

/// Wrapper around T to hold either a named or an unnamed argument.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
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

    /// apply a function on the inner T value
    pub fn map<F, R>(&self, f: F) -> FunctionArg<R>
    where
        F: Fn(&T) -> R,
    {
        match self {
            Self::Named { name, arg } => FunctionArg::Named {
                name: name.clone(),
                arg: f(arg),
            },
            Self::Unnamed(arg) => FunctionArg::Unnamed(f(arg)),
        }
    }

    #[inline]
    pub fn into_inner(self) -> T {
        match self {
            Self::Named { name: _, arg } => arg,
            Self::Unnamed(arg) => arg,
        }
    }

    #[inline]
    pub fn inner(&self) -> &T {
        match self {
            Self::Named { name: _, arg } => arg,
            Self::Unnamed(arg) => arg,
        }
    }
}

impl<T, E> FunctionArg<Result<T, E>> {
    /// transposes a FunctionArg<Result<T, E>> into a Result<FunctionArg<T>, E>
    pub fn transpose(self) -> Result<FunctionArg<T>, E> {
        match self {
            Self::Named { name, arg } => match arg {
                Ok(arg) => Ok(FunctionArg::Named { name, arg }),
                Err(err) => Err(err),
            },
            Self::Unnamed(arg) => match arg {
                Ok(arg) => Ok(FunctionArg::Unnamed(arg)),
                Err(err) => Err(err),
            },
        }
    }
}

/// FunctionArgs is a wrapper around a Vec<T> where T can either be a named or an unnamed argument.
/// FunctionArgs handles the following
/// 1. ensure that all unnamed arguments are before named arguments
/// 2. provide a structured way for accessing either named or unnamed arguments.
///
/// The reason FunctionArgs is needed is that different frontends have flexible ways of calling the functions.
/// Instead of delegating that logic to the frontend, and thus duplicating it, FunctionArgs provides a structured way to handle arguments for all frontends.
///
/// Let's take a look at SQL to get a better understanding
/// All of these are valid sql:
/// - `select round(2)`                           -> [unnamed(lit(2))]
/// - `select round(input:=3.14159, decimal:= 2)` -> [named("input", lit(3.14159)), named("decimal", lit(2))]
/// - `select round(decimal:=2, input:=3.14159)`  -> [named("decimal", lit(2)), named("input", lit(3.14159))]
/// - `select round(3.14159, decimal:=2)`         -> [unnamed(lit(3.14159)), named("decimal", lit(2))]
/// - `select round(3.14159, 2)`                  -> [unnamed(lit(3.14159)), unnamed(lit(2))]
///
/// But this is not valid:
/// - `select round(2, 3.14159)`
/// - `select round(2, input:=3.14159)`
/// - `select round(decimal:=2, 3.14159)`
///
/// Similarly, in python:
/// - `lit(3.14159).round()`          -> [unnamed(lit(3.14159))]
/// - `lit(3.14159).round(2)`         -> [unnamed(lit(3.14159)), unnamed(lit(2))]
/// - `lit(3.14159).round(decimal=2)` -> [unnamed(lit(3.14159)), named("decimal", lit(2))]
///
/// The 2 main types this will act on are:
/// - `Vec<Series>` for execution in `ScalarUDF::evaluate`
/// - `Vec<ExprRef>` for planning in `ScalarUDF::to_field`
///
/// Example usage:
///
/// let's look at round's function signature:
/// `round(input: Column, decimal: i32)`
///
/// for planning this would be:      `round(input: Expr, decimal: Expr)`
/// and for execution this would be: `round(input: Series, decimal: Series)`
///
/// we can extract `input` either as position 0
/// ```rs, no_run
/// let args: FunctionArgs<ExprRef> = FunctionArgs::try_new(vec![unnamed(col("foo"))])?;
/// let input: &ExprRef = args.required(0)?;
/// let decimal: ExprRef = args.optional(1)?.cloned().unwrap_or(lit(0));
/// ```
///
/// or by name of "input"
/// ```rs, no_run
/// let args = vec![
///   unnamed(col("foo")),
///   named("decimal", lit(1)),
/// ];
/// let args: FunctionArgs<ExprRef> = FunctionArgs::try_new(args)?;
/// let input: &ExprRef = args.required(0)?;
/// let decimal: ExprRef = args.optional("decimal")?.cloned().unwrap_or(lit(0));
///
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
pub struct FunctionArgs<T>(Vec<FunctionArg<T>>);

impl<T> FunctionArgs<T> {
    /// Extract the inner `Vec<T>` values
    #[inline]
    pub fn into_inner(self) -> Vec<T> {
        self.0
            .into_iter()
            .map(|arg| match arg {
                FunctionArg::Named { name: _, arg } => arg,
                FunctionArg::Unnamed(arg) => arg,
            })
            .collect()
    }

    pub fn iter(&self) -> std::slice::Iter<FunctionArg<T>> {
        self.0.iter()
    }

    pub fn first(&self) -> Option<&T> {
        self.0.first().map(|f| f.inner())
    }

    /// Split a `FunctionArgs` into an ordered list of unnamed arguments and a map of named arguments
    #[allow(clippy::type_complexity)]
    pub fn into_unnamed_and_named(self) -> DaftResult<(Vec<T>, HashMap<Arc<str>, T>)> {
        let mut seen_named = false;

        let mut unnamed = Vec::new();
        let mut named: HashMap<Arc<str>, T> = HashMap::new();

        for function_arg in self.0 {
            match function_arg {
                FunctionArg::Named { name, arg } => {
                    seen_named = true;

                    if named.contains_key(&name) {
                        return Err(DaftError::ValueError(format!(
                            "Received multiple arguments with the same name: {name}"
                        )));
                    }
                    named.insert(name, arg);
                }
                FunctionArg::Unnamed(arg) => {
                    if seen_named {
                        return Err(DaftError::ValueError(
                            "Cannot have unnamed arguments after named arguments".to_string(),
                        ));
                    }

                    unnamed.push(arg);
                }
            }
        }

        Ok((unnamed, named))
    }
}

impl<T> IntoIterator for FunctionArgs<T> {
    type Item = FunctionArg<T>;
    type IntoIter = std::vec::IntoIter<FunctionArg<T>>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T> FromIterator<FunctionArg<T>> for DaftResult<FunctionArgs<T>> {
    fn from_iter<I: IntoIterator<Item = FunctionArg<T>>>(iter: I) -> Self {
        let vec: Vec<FunctionArg<T>> = iter.into_iter().collect();
        FunctionArgs::try_new(vec)
    }
}

impl<T, E> FromIterator<FunctionArg<Result<T, E>>> for Result<FunctionArgs<T>, DaftError>
where
    E: Into<DaftError>,
{
    fn from_iter<I: IntoIterator<Item = FunctionArg<Result<T, E>>>>(iter: I) -> Self {
        let vec = iter
            .into_iter()
            .map(|v| v.transpose())
            .collect::<Result<Vec<_>, E>>()
            .map_err(|e| e.into())?;
        FunctionArgs::try_new(vec)
    }
}
/// trait to look up either positional or named values
///
/// We use a trait here so the user can access function args by different values such as by name (str), or by position (usize),
/// or by a combination, (position, name), (name, fallback_name), (position, name, fallback_name)
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
        match &args.0.get(*self) {
            Some(FunctionArg::Unnamed(value)) => Ok(value),
            None => Err(DaftError::ComputeError(format!(
                "Argument not found at position `{self:?}"
            ))),
            _ => Err(DaftError::ComputeError(format!(
                "Expected positional argument at position {}",
                self
            ))),
        }
    }

    fn optional<'a, T>(&self, args: &'a FunctionArgs<T>) -> DaftResult<Option<&'a T>> {
        match &args.0.get(*self) {
            Some(FunctionArg::Unnamed(value)) => Ok(Some(value)),
            _ => Ok(None),
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
        if let Ok(Some(first)) = self.0.optional(args) {
            Ok(Some(first))
        } else if let Ok(Some(second)) = self.1.optional(args) {
            Ok(Some(second))
        } else {
            Ok(None)
        }
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
        if let Ok(Some(first)) = self.0.optional(args) {
            Ok(Some(first))
        } else if let Ok(Some(second)) = self.1.optional(args) {
            Ok(Some(second))
        } else if let Ok(Some(third)) = self.2.optional(args) {
            Ok(Some(third))
        } else {
            Ok(None)
        }
    }
}

impl<T> FunctionArgs<T> {
    /// Tries to create a new instance of FunctionArgs.
    /// This method will error if named arguments come before any unnamed arguments
    /// ex: `[unnamed, unnamed, named]` -> Ok
    /// ex: `[named, named, named]` -> Ok
    /// ex: `[unnamed, unnamed, unnamed]` -> Ok
    /// ex: `[named, unnamed, unnamed]` -> Err
    /// ex: `[unnamed, named, unnamed]` -> Err
    pub fn try_new(inner: Vec<FunctionArg<T>>) -> DaftResult<Self> {
        let slf = Self(inner);
        slf.assert_ordering()?;
        Ok(slf)
    }

    /// Creates an empty FunctionArgs<T> instance.
    pub fn empty() -> Self {
        Self(vec![])
    }

    pub fn new_unchecked(inner: Vec<FunctionArg<T>>) -> Self {
        Self(inner)
    }

    pub fn new_unnamed(inner: Vec<T>) -> Self {
        Self(inner.into_iter().map(FunctionArg::Unnamed).collect())
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

    /// Get required argument
    /// ex:
    /// ```rs, no_run
    /// let args = FunctionArgs::try_new(vec![unnamed("foo"), unnamed("bar"), named("arg1", "baz")]).unwrap();
    /// let foo = args.required(0).unwrap();
    /// let bar = args.required(1).unwrap();
    /// let baz = args.required("arg1").unwrap();
    /// assert!(foo == "foo");
    /// assert!(bar == "bar");
    /// assert!(baz == "baz");
    /// let other = args.optional("arg2").unwrap();
    /// assert!(other.is_none())
    /// ```
    ///
    pub fn required<Key: FunctionArgKey>(&self, position: Key) -> DaftResult<&T> {
        position.required(self).map_err(|_| {
            DaftError::ValueError(format!(
                "Expected a value for the required argument at position `{position:?}`"
            ))
        })
    }

    /// Get optional argument
    /// ex:
    /// ```rs, no_run
    /// let args = FunctionArgs::try_new(vec![unnamed("foo"), unnamed("bar"), named("arg1", "baz")]).unwrap();
    /// let foo = args.required(0).unwrap();
    /// let bar = args.required(1).unwrap();
    /// let baz = args.required("arg1").unwrap();
    /// assert!(foo == "foo");
    /// assert!(bar == "bar");
    /// assert!(baz == "baz");
    /// let other = args.optional("arg2").unwrap();
    /// assert!(other.is_none())
    /// ```
    pub fn optional<Key: FunctionArgKey>(&self, position: Key) -> DaftResult<Option<&T>> {
        position.optional(self).map_err(|_| {
            DaftError::ValueError(format!(
                "Expected a value for the optional argument at position `{position:?}`"
            ))
        })
    }
}

#[derive(FunctionArgs)]
/// A single required argument named `input`
pub struct UnaryArg<T> {
    pub input: T,
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use crate::functions::function_args::{FunctionArg, FunctionArgs};
    #[test]
    fn test_function_args_ordering() {
        let res = FunctionArgs::try_new(vec![
            FunctionArg::unnamed(1),
            FunctionArg::unnamed(2),
            FunctionArg::named("arg1", 3),
        ]);

        assert!(res.is_ok());
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

    #[test]
    fn test_lookup_simple() {
        let args = FunctionArgs::try_new(vec![
            FunctionArg::unnamed(1),
            FunctionArg::unnamed(2),
            FunctionArg::named("arg1", 2),
        ])
        .unwrap();

        let first = args.required(0).unwrap();
        assert_eq!(*first, 1);

        let second = args.required(1).unwrap();
        assert_eq!(*second, 2);
        let third = args.required("arg1").unwrap();
        assert_eq!(*third, 2);
        // can't access it by position since it's a named argument.
        let third = args.required(2);
        assert!(third.is_err())
    }

    #[test]
    fn test_lookup_multi_required() -> DaftResult<()> {
        let args = FunctionArgs::try_new(vec![
            FunctionArg::unnamed(100),
            FunctionArg::unnamed(222),
            FunctionArg::named("arg0", 123),
            FunctionArg::named("arg1", 333),
        ])
        .unwrap();

        // first try position 2, then try named argument "arg1"
        let arg1 = args.required((2, "arg1"))?;

        assert_eq!(*arg1, 333);

        // try named "arg0", then named "arg1", then position 0
        let arg0 = args.required(("arg0", "arg1", 0))?;
        assert_eq!(*arg0, 123);

        let arg2 = args.required(("arg2", 1))?;
        assert_eq!(*arg2, 222);

        let invalid = args.required(2);
        assert!(invalid.is_err());

        let invalid = args.required("arg2");
        assert!(invalid.is_err());

        let invalid = args.required((3, "arg2", 2));
        assert!(invalid.is_err());

        Ok(())
    }
    #[test]
    fn test_lookup_multi_optional() -> DaftResult<()> {
        let args = FunctionArgs::try_new(vec![
            FunctionArg::unnamed(100),
            FunctionArg::unnamed(222),
            FunctionArg::named("arg0", 123),
            FunctionArg::named("arg1", 333),
        ])
        .unwrap();

        // first try position 2, then try named argument "arg1"
        let arg1 = args.optional((2, "arg1")).unwrap();
        assert!(arg1.is_some());

        // try named "arg0", then named "arg1", then position 0
        let arg0 = args.optional(("arg0", "arg1", 0)).unwrap();
        assert!(arg0.is_some());

        let arg2 = args.optional(("arg2", 1)).unwrap();
        assert!(arg2.is_some());

        let invalid = args.optional(2).unwrap();
        assert!(invalid.is_none());

        let invalid = args.optional("arg2").unwrap();
        assert!(invalid.is_none());

        let invalid = args.optional((3, "arg2", 2)).unwrap();
        assert!(invalid.is_none());

        Ok(())
    }
    #[test]
    fn test_lookup_out_of_range() -> DaftResult<()> {
        let args = FunctionArgs::try_new(vec![
            FunctionArg::unnamed(100),
            FunctionArg::unnamed(222),
            FunctionArg::named("arg0", 123),
            FunctionArg::named("arg1", 333),
        ])
        .unwrap();

        let res = args.required(99);
        assert!(res.is_err());
        let res = args.required((99, 5));
        assert!(res.is_err());

        Ok(())
    }
    #[test]
    fn test_len_and_is_empty() {
        let args = FunctionArgs::try_new(vec![
            FunctionArg::unnamed(100),
            FunctionArg::unnamed(222),
            FunctionArg::named("arg0", 123),
            FunctionArg::named("arg1", 333),
        ])
        .unwrap();

        assert_eq!(args.len(), 4);
        assert!(!args.is_empty());
        let args: FunctionArgs<usize> = FunctionArgs::try_new(Vec::new()).unwrap();
        assert!(args.is_empty());
        assert_eq!(args.len(), 0);
    }
}
