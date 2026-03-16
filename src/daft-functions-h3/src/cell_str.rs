use daft_core::{
    prelude::{UInt64Array, Utf8Array},
    series::IntoSeries,
};
use daft_dsl::functions::prelude::*;
use h3o::CellIndex;

use crate::utils::{ensure_cell_dtype, series_to_cell_indices};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct H3CellToStr;

#[derive(FunctionArgs)]
struct CellArg<T> {
    cell: T,
}

#[typetag::serde]
impl ScalarUDF for H3CellToStr {
    fn name(&self) -> &'static str {
        "h3_cell_to_str"
    }

    fn docstring(&self) -> &'static str {
        "Converts an H3 cell index (UInt64 or Utf8) to its hex string representation."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let CellArg { cell } = args.try_into()?;
        let cell = cell.to_field(schema)?;
        ensure_cell_dtype("h3_cell_to_str", &cell)?;
        Ok(Field::new(cell.name, DataType::Utf8))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let CellArg { cell } = args.try_into()?;
        let name = cell.name().to_string();
        let cells = series_to_cell_indices(&cell)?;
        let result: Utf8Array = cells
            .into_iter()
            .map(|opt| opt.map(|c| c.to_string()))
            .collect();
        Ok(result.rename(&name).into_series())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct H3StrToCell;

#[derive(FunctionArgs)]
struct StrArg<T> {
    hex: T,
}

#[typetag::serde]
impl ScalarUDF for H3StrToCell {
    fn name(&self) -> &'static str {
        "h3_str_to_cell"
    }

    fn docstring(&self) -> &'static str {
        "Converts an H3 hex string to a cell index (UInt64)."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let StrArg { hex } = args.try_into()?;
        let hex = hex.to_field(schema)?;
        ensure!(
            hex.dtype == DataType::Utf8,
            TypeError: "h3_str_to_cell: hex must be Utf8, got {}", hex.dtype
        );
        Ok(Field::new(hex.name, DataType::UInt64))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let StrArg { hex } = args.try_into()?;
        let arr = hex.utf8()?;
        let result: UInt64Array = arr
            .into_iter()
            .map(|opt| opt.and_then(|s| s.parse::<CellIndex>().ok().map(u64::from)))
            .collect();
        Ok(result.rename(arr.name()).into_series())
    }
}
