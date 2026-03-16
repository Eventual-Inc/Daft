use daft_core::{
    prelude::{BooleanArray, DataType as DT, UInt8Array},
    series::IntoSeries,
};
use daft_dsl::functions::prelude::*;
use h3o::CellIndex;

use crate::utils::{ensure_cell_dtype, series_to_cell_indices};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct H3CellResolution;

#[derive(FunctionArgs)]
struct CellArg<T> {
    cell: T,
}

#[typetag::serde]
impl ScalarUDF for H3CellResolution {
    fn name(&self) -> &'static str {
        "h3_cell_resolution"
    }

    fn docstring(&self) -> &'static str {
        "Returns the resolution (0-15) of an H3 cell index."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let CellArg { cell } = args.try_into()?;
        let cell = cell.to_field(schema)?;
        ensure_cell_dtype("h3_cell_resolution", &cell)?;
        Ok(Field::new(cell.name, DataType::UInt8))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let CellArg { cell } = args.try_into()?;
        let name = cell.name().to_string();
        let cells = series_to_cell_indices(&cell)?;
        let result: UInt8Array = cells
            .into_iter()
            .map(|opt| opt.map(|c| u8::from(c.resolution())))
            .collect();
        Ok(result.rename(&name).into_series())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct H3CellIsValid;

#[typetag::serde]
impl ScalarUDF for H3CellIsValid {
    fn name(&self) -> &'static str {
        "h3_cell_is_valid"
    }

    fn docstring(&self) -> &'static str {
        "Returns true if the value is a valid H3 cell index."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let CellArg { cell } = args.try_into()?;
        let cell = cell.to_field(schema)?;
        ensure_cell_dtype("h3_cell_is_valid", &cell)?;
        Ok(Field::new(cell.name, DataType::Boolean))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let CellArg { cell } = args.try_into()?;
        let name = cell.name().to_string();
        // is_valid needs to distinguish null (-> null) from invalid (-> false),
        // so we can't use series_to_cell_indices which collapses both to None.
        let result: BooleanArray = match cell.data_type() {
            DT::UInt64 => {
                let arr = cell.u64()?;
                arr.into_iter()
                    .map(|opt| opt.map(|v| CellIndex::try_from(v).is_ok()))
                    .collect()
            }
            DT::Utf8 => {
                let arr = cell.utf8()?;
                arr.into_iter()
                    .map(|opt| opt.map(|s| s.parse::<CellIndex>().is_ok()))
                    .collect()
            }
            dt => {
                return Err(common_error::DaftError::TypeError(format!(
                    "h3_cell_is_valid: cell must be UInt64 or Utf8, got {dt}"
                )));
            }
        };
        Ok(result.rename(&name).into_series())
    }
}
