use daft_core::{prelude::Float64Array, series::IntoSeries};
use daft_dsl::functions::prelude::*;

use crate::utils::{ensure_cell_dtype, series_to_cell_indices};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct H3CellToLat;

#[derive(FunctionArgs)]
struct CellArg<T> {
    cell: T,
}

#[typetag::serde]
impl ScalarUDF for H3CellToLat {
    fn name(&self) -> &'static str {
        "h3_cell_to_lat"
    }

    fn docstring(&self) -> &'static str {
        "Returns the latitude of the center of an H3 cell."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let CellArg { cell } = args.try_into()?;
        let cell = cell.to_field(schema)?;
        ensure_cell_dtype("h3_cell_to_lat", &cell)?;
        Ok(Field::new(cell.name, DataType::Float64))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let CellArg { cell } = args.try_into()?;
        let name = cell.name().to_string();
        let cells = series_to_cell_indices(&cell)?;
        let result: Float64Array = cells
            .into_iter()
            .map(|opt| opt.map(|c| h3o::LatLng::from(c).lat()))
            .collect();
        Ok(result.rename(&name).into_series())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct H3CellToLng;

#[typetag::serde]
impl ScalarUDF for H3CellToLng {
    fn name(&self) -> &'static str {
        "h3_cell_to_lng"
    }

    fn docstring(&self) -> &'static str {
        "Returns the longitude of the center of an H3 cell."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let CellArg { cell } = args.try_into()?;
        let cell = cell.to_field(schema)?;
        ensure_cell_dtype("h3_cell_to_lng", &cell)?;
        Ok(Field::new(cell.name, DataType::Float64))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let CellArg { cell } = args.try_into()?;
        let name = cell.name().to_string();
        let cells = series_to_cell_indices(&cell)?;
        let result: Float64Array = cells
            .into_iter()
            .map(|opt| opt.map(|c| h3o::LatLng::from(c).lng()))
            .collect();
        Ok(result.rename(&name).into_series())
    }
}
