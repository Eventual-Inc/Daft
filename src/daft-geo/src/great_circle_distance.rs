use std::sync::Arc;

use arrow_buffer::{NullBuffer, NullBufferBuilder};
use daft_core::prelude::{Float64Array, IntoSeries};
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};

const EARTH_RADIUS_M: f64 = 6_371_000.0;

fn haversine_meters(lat1_deg: f64, lon1_deg: f64, lat2_deg: f64, lon2_deg: f64) -> f64 {
    let lat1_rad = lat1_deg.to_radians();
    let lon1_rad = lon1_deg.to_radians();
    let lat2_rad = lat2_deg.to_radians();
    let lon2_rad = lon2_deg.to_radians();

    let delta_lat = lat2_rad - lat1_rad;
    let delta_lon = lon2_rad - lon1_rad;

    let sin_half_delta_lat = (delta_lat * 0.5).sin();
    let sin_half_delta_lon = (delta_lon * 0.5).sin();

    let a = (sin_half_delta_lat * sin_half_delta_lat
        + lat1_rad.cos() * lat2_rad.cos() * sin_half_delta_lon * sin_half_delta_lon)
        .clamp(0.0, 1.0);

    let central_angle = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_M * central_angle
}

fn is_valid_lat_lon(lat: f64, lon: f64) -> bool {
    lat.is_finite()
        && lon.is_finite()
        && (-90.0..=90.0).contains(&lat)
        && (-180.0..=180.0).contains(&lon)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GreatCircleDistance;

#[typetag::serde]
impl ScalarUDF for GreatCircleDistance {
    fn name(&self) -> &'static str {
        "great_circle_distance"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let lat1 = inputs.required(0)?.cast(&DataType::Float64)?;
        let lon1 = inputs.required(1)?.cast(&DataType::Float64)?;
        let lat2 = inputs.required(2)?.cast(&DataType::Float64)?;
        let lon2 = inputs.required(3)?.cast(&DataType::Float64)?;

        let lat1 = lat1.f64().expect("type should have been validated already");
        let lon1 = lon1.f64().expect("type should have been validated already");
        let lat2 = lat2.f64().expect("type should have been validated already");
        let lon2 = lon2.f64().expect("type should have been validated already");

        let len = lat1.len();

        let input_nulls = [lat1.nulls(), lon1.nulls(), lat2.nulls(), lon2.nulls()]
            .into_iter()
            .fold(None, |acc, next| NullBuffer::union(acc.as_ref(), next));

        let lat1_v = lat1.values();
        let lon1_v = lon1.values();
        let lat2_v = lat2.values();
        let lon2_v = lon2.values();

        let mut values = Vec::with_capacity(len);
        let mut validity = NullBufferBuilder::new(len);

        for i in 0..len {
            let row_is_present = input_nulls.as_ref().map(|n| !n.is_null(i)).unwrap_or(true);

            if !row_is_present {
                values.push(0.0);
                validity.append_null();
                continue;
            }

            let a = lat1_v[i];
            let b = lon1_v[i];
            let c = lat2_v[i];
            let d = lon2_v[i];

            let row_is_valid = is_valid_lat_lon(a, b) && is_valid_lat_lon(c, d);

            if row_is_valid {
                values.push(haversine_meters(a, b, c, d));
                validity.append_non_null();
            } else {
                values.push(0.0);
                validity.append_null();
            }
        }

        let field = Arc::new(Field::new(self.name(), DataType::Float64));
        let result =
            Float64Array::from_field_and_values(field, values).with_nulls(validity.finish())?;

        Ok(result.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 4,
            SchemaMismatch: "Expected 4 inputs, but received {}",
            inputs.len()
        );

        let lat1 = inputs.required(0)?.to_field(schema)?;
        let lon1 = inputs.required(1)?.to_field(schema)?;
        let lat2 = inputs.required(2)?.to_field(schema)?;
        let lon2 = inputs.required(3)?.to_field(schema)?;

        for (arg_name, field) in [
            ("lat1", &lat1),
            ("lon1", &lon1),
            ("lat2", &lat2),
            ("lon2", &lon2),
        ] {
            ensure!(
                field.dtype.is_numeric(),
                TypeError: "{} must be numeric, got {}",
                arg_name,
                field.dtype
            );
        }

        Ok(Field::new(self.name(), DataType::Float64))
    }

    fn docstring(&self) -> &'static str {
        "Computes the great-circle distance in meters between two points from \
         (lat1, lon1) and (lat2, lon2), where latitudes and longitudes are \
         specified in degrees, using the haversine formula. Null inputs or \
         non-finite/out-of-range coordinates produce null."
    }
}

#[must_use]
pub fn great_circle_distance(
    lat1: ExprRef,
    lon1: ExprRef,
    lat2: ExprRef,
    lon2: ExprRef,
) -> ExprRef {
    ScalarFn::builtin(GreatCircleDistance, vec![lat1, lon1, lat2, lon2]).into()
}
