use arrow_buffer::NullBufferBuilder;
use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{BinaryArray, DataType, Field, IntoSeries, Schema},
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

        let len = utf8.len();
        let mut values: Vec<Option<Vec<u8>>> = Vec::with_capacity(len);
        let mut validity = NullBufferBuilder::new(len);

        for opt in utf8.into_iter() {
            match opt.and_then(wkt_to_wkb) {
                Some(wkb) => {
                    values.push(Some(wkb));
                    validity.append_non_null();
                }
                None => {
                    values.push(None);
                    validity.append_null();
                }
            }
        }

        let arr = BinaryArray::from_iter(self.name(), values.iter().map(|v| v.as_deref()));
        Ok(arr.into_series())
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
        Ok(Field::new(self.name(), DataType::Binary))
    }

    fn docstring(&self) -> &'static str {
        "Parses a Well-Known Text (WKT) string and returns a WKB geometry."
    }
}

#[must_use]
pub fn st_geomfromtext(wkt: ExprRef) -> ExprRef {
    ScalarFn::builtin(StGeomFromText, vec![wkt]).into()
}
