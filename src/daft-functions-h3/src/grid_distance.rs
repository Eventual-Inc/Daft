use daft_core::{prelude::Int32Array, series::IntoSeries};
use daft_dsl::functions::prelude::*;

use crate::utils::{ensure_cell_dtype, series_to_cell_indices};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct H3GridDistance;

#[derive(FunctionArgs)]
struct Args<T> {
    a: T,
    b: T,
}

#[typetag::serde]
impl ScalarUDF for H3GridDistance {
    fn name(&self) -> &'static str {
        "h3_grid_distance"
    }

    fn docstring(&self) -> &'static str {
        "Returns the grid distance (number of H3 cells) between two cells. Returns null if cells are not comparable (different resolutions or pentagonal distortion)."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let Args { a, b } = args.try_into()?;
        let a = a.to_field(schema)?;
        let b = b.to_field(schema)?;
        ensure_cell_dtype("h3_grid_distance", &a)?;
        ensure_cell_dtype("h3_grid_distance", &b)?;
        Ok(Field::new(a.name, DataType::Int32))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let Args { a, b } = args.try_into()?;
        let name = a.name().to_string();
        let a_cells = series_to_cell_indices(&a)?;
        let b_cells = series_to_cell_indices(&b)?;

        let result: Int32Array = a_cells
            .into_iter()
            .zip(b_cells)
            .map(|(a_opt, b_opt)| match (a_opt, b_opt) {
                (Some(a_cell), Some(b_cell)) => a_cell.grid_distance(b_cell).ok(),
                _ => None,
            })
            .collect();
        Ok(result.rename(&name).into_series())
    }
}
