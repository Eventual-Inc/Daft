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

    let a = sin_half_delta_lat * sin_half_delta_lat
        + lat1_rad.cos() * lat2_rad.cos() * sin_half_delta_lon * sin_half_delta_lon;

    let central_angle = 2.0 * a.sqrt().min(1.0).asin();
    EARTH_RADIUS_M * central_angle
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

        let result = lat1
            .into_iter()
            .zip(lon1)
            .zip(lat2)
            .zip(lon2)
            .map(
                |(((lat1, lon1), lat2), lon2)| match (lat1, lon1, lat2, lon2) {
                    (Some(lat1), Some(lon1), Some(lat2), Some(lon2)) => {
                        Some(haversine_meters(lat1, lon1, lat2, lon2))
                    }
                    _ => None,
                },
            )
            .collect::<Float64Array>()
            .rename(self.name());

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
        "Computes the great circle distance in meters between two latitude/longitude points using the haversine formula."
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
