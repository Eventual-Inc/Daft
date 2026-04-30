use daft_common::error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Int64Array, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    expr::lit,
    functions::{
        prelude::*,
        scalar::{EvalContext, ScalarFn},
    },
};
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RandomIntFunction;

#[derive(FunctionArgs)]
struct RandomIntArgs {
    low: i64,
    high: i64,
    #[arg(optional)]
    seed: Option<u64>,
}

#[typetag::serde]
impl ScalarUDF for RandomIntFunction {
    fn name(&self) -> &'static str {
        "random_int"
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<Field> {
        let RandomIntArgs { low, high, .. } = inputs.try_into()?;
        ensure!(low < high, ValueError: "lower bound (`low`) must be strictly less than the upper bound (`high`)");
        Ok(Field::new("random_int", DataType::Int64))
    }

    fn docstring(&self) -> &'static str {
        "Generates a column of random integer values."
    }

    fn call(&self, inputs: FunctionArgs<Series>, ctx: &EvalContext) -> DaftResult<Series> {
        let RandomIntArgs { low, high, seed } = inputs.try_into()?;

        let values = match seed {
            Some(seed) => {
                let mut rng = StdRng::seed_from_u64(seed);
                (0..ctx.row_count)
                    .map(|_| rng.random_range(low..=high))
                    .collect::<Vec<_>>()
            }
            None => {
                let mut rng = rand::rng();
                (0..ctx.row_count)
                    .map(|_| rng.random_range(low..=high))
                    .collect::<Vec<_>>()
            }
        };

        Ok(Int64Array::from_values("random_int", values).into_series())
    }
}

/// Builds a `random_int(low, high, seed?)` expression for use in logical plan construction.
#[must_use]
pub fn random_int_expr(low: i64, high: i64, seed: Option<u64>) -> ExprRef {
    let mut inputs = vec![lit(low), lit(high)];
    if let Some(s) = seed {
        inputs.push(lit(s));
    }
    ScalarFn::builtin(RandomIntFunction, inputs).into()
}
