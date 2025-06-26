use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TreeNode};
use daft_core::prelude::Schema;
use derive_more::derive::Display;
use serde::{Deserialize, Serialize};

use super::{
    bound_col, AggExpr, Column, Expr, ExprRef, ResolvedColumn, UnresolvedColumn, WindowExpr,
};

#[derive(Clone, Display, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
/// A simple newtype around ExprRef that ensures that all of the columns in the held expression are bound.
///
/// We have several column variants: unresolved, resolved, and bound.
/// Resolved columns are being phased out in favor of bound columns, and `BoundExpr` serves two purposes in this process:
/// 1. To hold the logic for conversion from other column variants to the bound variant (a.k.a column binding)
/// 2. Th use Rust's type checker to enforce the boundary between unbound and bound columns in the code.
pub struct BoundExpr(ExprRef);

impl BoundExpr {
    /// Create a BoundExpr by attempting to bind all unbound columns.
    pub fn try_new(expr: impl Into<ExprRef>, schema: &Schema) -> DaftResult<Self> {
        expr.into()
            .transform(|e| {
                if let Expr::Column(column) = e.as_ref() {
                    match column {
                        Column::Bound(_) => Ok(Transformed::no(e)),

                        // TODO: remove ability to bind unresolved columns once we fix all tests
                        Column::Unresolved(UnresolvedColumn { name, .. })
                        | Column::Resolved(ResolvedColumn::Basic(name)) => {
                            let index = schema.get_index(name)?;
                            let field = schema.get_field(name)?.clone();

                            Ok(Transformed::yes(bound_col(index, field)))
                        }

                        Column::Resolved(ResolvedColumn::JoinSide(..)) => {
                            Err(DaftError::InternalError(format!(
                                "Join side columns cannot be bound: {e}"
                            )))
                        }
                        Column::Resolved(ResolvedColumn::OuterRef(..)) => {
                            Err(DaftError::InternalError(format!(
                                "Outer reference columns cannot be bound: {e}"
                            )))
                        }
                    }
                } else {
                    Ok(Transformed::no(e))
                }
            })
            .map(|t| Self(t.data))
    }

    /// Create a BoundExpr without binding columns.
    ///
    /// Only use this when you are sure that the columns are already bound.
    pub fn new_unchecked(expr: ExprRef) -> Self {
        debug_assert!(
            !expr.exists(|e| {
                matches!(e.as_ref(), Expr::Column(col) if !matches!(col, Column::Bound(_)))
            }),
            "BoundExpr::new_unchecked should not receive unbound columns: {expr}"
        );

        Self(expr)
    }

    pub fn inner(&self) -> &ExprRef {
        &self.0
    }

    pub fn bind_all(
        exprs: &[impl Into<ExprRef> + Clone],
        schema: &Schema,
    ) -> DaftResult<Vec<Self>> {
        exprs
            .iter()
            .map(|expr| Self::try_new(expr.clone(), schema))
            .collect()
    }
}

impl From<BoundExpr> for ExprRef {
    fn from(value: BoundExpr) -> Self {
        value.0
    }
}

impl<'a> From<&'a BoundExpr> for &'a ExprRef {
    fn from(value: &'a BoundExpr) -> &'a ExprRef {
        &value.0
    }
}

impl AsRef<Expr> for BoundExpr {
    fn as_ref(&self) -> &Expr {
        &self.0
    }
}

macro_rules! impl_bound_wrapper {
    ($name:ident, $inner:ty, $expr_variant:ident) => {
        #[derive(Clone, Display, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
        /// See [`BoundExpr`] for details about how this newtype pattern.
        pub struct $name($inner);

        impl $name {
            /// Create a bound expression by attempting to bind all unbound columns.
            pub fn try_new(inner: $inner, schema: &Schema) -> DaftResult<Self> {
                let bound_expr = BoundExpr::try_new(Arc::new(Expr::$expr_variant(inner)), schema)?;

                let Expr::$expr_variant(bound_inner) = bound_expr.as_ref() else {
                    unreachable!()
                };

                Ok(Self(bound_inner.clone()))
            }

            /// Create a bound expression without binding columns.
            ///
            /// Only use this when you are sure that the columns are already bound.
            pub fn new_unchecked(inner: $inner) -> Self {
                debug_assert!(
                    !Arc::new(Expr::$expr_variant(inner.clone())).exists(|e| {
                        matches!(e.as_ref(), Expr::Column(col) if !matches!(col, Column::Bound(_)))
                    }),
                    "{}::new_unchecked should not receive unbound columns: {}", stringify!($name), inner
                );

                Self(inner)
            }

            pub fn bind_all(exprs: &[$inner], schema: &Schema) -> DaftResult<Vec<Self>> {
                exprs.iter()
                    .map(|expr| Self::try_new(expr.clone(), schema))
                    .collect()
            }
        }

        impl AsRef<$inner> for $name {
            fn as_ref(&self) -> &$inner {
                &self.0
            }
        }

        impl From<$name> for $inner {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl<'a> From<&'a $name> for &'a $inner {
            fn from(value: &'a $name) -> Self {
                &value.0
            }
        }
    };
}

impl_bound_wrapper!(BoundAggExpr, AggExpr, Agg);
impl_bound_wrapper!(BoundWindowExpr, WindowExpr, WindowFunction);
