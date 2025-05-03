use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TreeNode};
use daft_core::prelude::Schema;
use derive_more::derive::Display;

use super::{
    bound_col, AggExpr, Column, Expr, ExprRef, ResolvedColumn, UnresolvedColumn, WindowExpr,
};

#[derive(Clone, Display)]
pub struct BoundExpr(ExprRef);

impl BoundExpr {
    pub fn try_new(expr: ExprRef, schema: &Schema) -> DaftResult<Self> {
        expr.transform(|e| {
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

                    Column::Resolved(ResolvedColumn::JoinSide(..)) => Err(
                        DaftError::InternalError(format!("Join side columns cannot be bound: {e}")),
                    ),
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

    pub fn expr(&self) -> &ExprRef {
        &self.0
    }
}

macro_rules! impl_bound_wrapper {
    ($name:ident, $inner:ty, $expr_variant:ident) => {
        #[derive(Clone, Display)]
        pub struct $name($inner);

        impl $name {
            pub fn try_new(inner: $inner, schema: &Schema) -> DaftResult<Self> {
                let bound_expr = BoundExpr::try_new(Arc::new(Expr::$expr_variant(inner)), schema)?;

                let Expr::$expr_variant(bound_inner) = bound_expr.expr().as_ref() else {
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

            pub fn expr(&self) -> &$inner {
                &self.0
            }
        }
    };
}

impl_bound_wrapper!(BoundAggExpr, AggExpr, Agg);
impl_bound_wrapper!(BoundWindowExpr, WindowExpr, WindowFunction);
