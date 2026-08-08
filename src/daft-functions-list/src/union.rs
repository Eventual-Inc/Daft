use common_error::{DaftResult, ensure};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
    utils::supertype::try_get_supertype,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListUnion;

#[typetag::serde]
impl ScalarUDF for ListUnion {
    fn name(&self) -> &'static str {
        "list_union"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["array_union"]
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let lhs = inputs.required((0, "lhs"))?;
        let rhs = inputs.required((1, "rhs"))?;

        let (lhs_b, rhs_b);
        let (lhs_ref, rhs_ref) = match (lhs.len(), rhs.len()) {
            (a, b) if a == b => (lhs, rhs),
            (1, b) => {
                lhs_b = lhs.broadcast(b)?;
                (&lhs_b, rhs)
            }
            (a, 1) => {
                rhs_b = rhs.broadcast(a)?;
                (lhs, &rhs_b)
            }
            _ => {
                return Err(common_error::DaftError::ValueError(format!(
                    "list_union: input lengths must match or broadcast, got {} vs {}",
                    lhs.len(),
                    rhs.len()
                )));
            }
        };

        lhs_ref.list_union(rhs_ref)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 2,
            SchemaMismatch: "Expected 2 input args, got {}",
            inputs.len()
        );

        let lhs = inputs.required((0, "lhs"))?.to_field(schema)?;
        let rhs = inputs.required((1, "rhs"))?.to_field(schema)?;

        ensure!(
            lhs.dtype.is_list() || lhs.dtype.is_fixed_size_list(),
            TypeError: "First argument must be a list, got {}",
            lhs.dtype
        );
        ensure!(
            rhs.dtype.is_list() || rhs.dtype.is_fixed_size_list(),
            TypeError: "Second argument must be a list, got {}",
            rhs.dtype
        );

        let lhs_inner = lhs.to_exploded_field()?.dtype;
        let rhs_inner = rhs.to_exploded_field()?.dtype;
        // Promote element types to a common supertype (Spark-compatible).
        let inner_type = if lhs_inner == rhs_inner {
            lhs_inner
        } else {
            try_get_supertype(&lhs_inner, &rhs_inner).map_err(|_| {
                common_error::DaftError::TypeError(format!(
                    "Cannot compute list_union between list of {} and list of {}",
                    lhs_inner, rhs_inner
                ))
            })?
        };
        Ok(Field::new(lhs.name, DataType::List(Box::new(inner_type))))
    }
}

/// Returns an array of the union of elements in both input lists, with duplicates removed.
///
/// Uses null-safe-equal semantics: if either input contains a null, the result
/// contains a single null. Spark-compatible alias: `array_union`.
#[must_use]
pub fn list_union(lhs: ExprRef, rhs: ExprRef) -> ExprRef {
    ScalarFn::builtin(ListUnion {}, vec![lhs, rhs]).into()
}
