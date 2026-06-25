use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};
use wkb::geom_to_wkb;
use wkt::TryFromWkt;

/// Parse a WKT string into WKB bytes.
fn wkt_to_wkb(s: &str) -> Option<Vec<u8>> {
    let geom: geo::Geometry<f64> = geo::Geometry::try_from_wkt_str(s).ok()?;
    geom_to_wkb(&geom).ok()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StGeomFromText;

#[typetag::serde]
impl ScalarUDF for StGeomFromText {
    fn name(&self) -> &'static str {
        "st_geomfromtext"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let series = inputs.required(0)?;
        let utf8 = series.utf8().map_err(|_| {
            DaftError::TypeError(format!(
                "st_geomfromtext: expected Utf8 input, got {}",
                series.data_type()
            ))
        })?;

        let values: Vec<Option<Vec<u8>>> =
            utf8.into_iter().map(|opt| opt.and_then(wkt_to_wkb)).collect();

        crate::utils::wkb_opts_to_geometry_series(self.name(), values)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let f = inputs.required(0)?.to_field(schema)?;
        if !matches!(f.dtype, DataType::Utf8) {
            return Err(DaftError::TypeError(format!(
                "st_geomfromtext: expected Utf8, got {}",
                f.dtype
            )));
        }
        Ok(Field::new(self.name(), DataType::Geometry))
    }

    fn docstring(&self) -> &'static str {
        "Parses a Well-Known Text (WKT) string and returns a Geometry."
    }
}

#[must_use]
pub fn st_geomfromtext(wkt: ExprRef) -> ExprRef {
    ScalarFn::builtin(StGeomFromText, vec![wkt]).into()
}
