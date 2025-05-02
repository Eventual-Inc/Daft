use std::sync::Arc;

use common_error::{DaftError, DaftResult};

#[derive(Debug, Clone)]
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

    /// Get required argument
    pub fn required<Key: FunctionArgKey>(&self, position: Key) -> DaftResult<&T> {
        position.required(self).map_err(|_| {
            DaftError::ValueError(format!(
                "Expected a value for the required argument at position `{position:?}`"
            ))
        })
    }

    /// Get optional argument
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
