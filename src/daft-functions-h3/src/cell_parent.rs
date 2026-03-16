use daft_core::{prelude::UInt64Array, series::IntoSeries};
use daft_dsl::functions::prelude::*;
use h3o::Resolution;

use crate::utils::{ensure_cell_dtype, series_to_cell_indices};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct H3CellParent;

#[derive(FunctionArgs)]
struct Args<T> {
    cell: T,
    resolution: u8,
}

#[typetag::serde]
impl ScalarUDF for H3CellParent {
    fn name(&self) -> &'static str {
        "h3_cell_parent"
    }

    fn docstring(&self) -> &'static str {
        "Returns the parent cell index at the given resolution. Resolution must be coarser (lower) than the cell's resolution."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let Args { cell, resolution } = args.try_into()?;
        let cell = cell.to_field(schema)?;
        ensure_cell_dtype("h3_cell_parent", &cell)?;
        ensure!(
            resolution <= 15,
            ValueError: "h3_cell_parent: resolution must be 0-15, got {resolution}"
        );
        Ok(Field::new(cell.name, DataType::UInt64))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let Args { cell, resolution } = args.try_into()?;
        let name = cell.name().to_string();
        let res = Resolution::try_from(resolution).map_err(|e| {
            common_error::DaftError::ValueError(format!(
                "h3_cell_parent: invalid resolution {resolution}: {e}"
            ))
        })?;
        let cells = series_to_cell_indices(&cell)?;
        let result: UInt64Array = cells
            .into_iter()
            .map(|opt| opt.and_then(|c| c.parent(res)).map(u64::from))
            .collect();
        Ok(result.rename(&name).into_series())
    }
}
