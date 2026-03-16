use daft_core::{prelude::UInt64Array, series::IntoSeries};
use daft_dsl::functions::prelude::*;
use h3o::{LatLng, Resolution};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct H3LatLngToCell;

#[derive(FunctionArgs)]
struct Args<T> {
    lat: T,
    lng: T,
    resolution: u8,
}

#[typetag::serde]
impl ScalarUDF for H3LatLngToCell {
    fn name(&self) -> &'static str {
        "h3_latlng_to_cell"
    }

    fn docstring(&self) -> &'static str {
        "Converts latitude/longitude coordinates to an H3 cell index at the given resolution (0-15)."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let Args {
            lat,
            lng,
            resolution,
        } = args.try_into()?;
        let lat = lat.to_field(schema)?;
        let lng = lng.to_field(schema)?;
        ensure!(
            lat.dtype == DataType::Float64,
            TypeError: "h3_latlng_to_cell: lat must be Float64, got {}", lat.dtype
        );
        ensure!(
            lng.dtype == DataType::Float64,
            TypeError: "h3_latlng_to_cell: lng must be Float64, got {}", lng.dtype
        );
        ensure!(
            resolution <= 15,
            ValueError: "h3_latlng_to_cell: resolution must be 0-15, got {resolution}"
        );
        Ok(Field::new(lat.name, DataType::UInt64))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let Args {
            lat,
            lng,
            resolution,
        } = args.try_into()?;
        let lat_arr = lat.f64()?;
        let lng_arr = lng.f64()?;
        let res = Resolution::try_from(resolution).map_err(|e| {
            common_error::DaftError::ValueError(format!(
                "h3_latlng_to_cell: invalid resolution {resolution}: {e}"
            ))
        })?;

        let result: UInt64Array = lat_arr
            .into_iter()
            .zip(lng_arr)
            .map(|(lat_opt, lng_opt)| match (lat_opt, lng_opt) {
                (Some(lat), Some(lng)) => LatLng::new(lat, lng)
                    .ok()
                    .map(|ll| u64::from(ll.to_cell(res))),
                _ => None,
            })
            .collect();
        Ok(result.rename(lat_arr.name()).into_series())
    }
}
