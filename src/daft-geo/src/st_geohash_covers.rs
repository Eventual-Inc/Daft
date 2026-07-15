use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{BooleanArray, DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::{Deserialize, Serialize};

/// `st_geohash_covers(hash_col, query_wkb, precision)` → Boolean
///
/// Returns true if the geohash cell `hash_col` overlaps the bounding box of `query_wkb`.
/// Used for automatic partition pruning: add a `_geohash` column to your table and Daft
/// will rewrite `st_intersects(geom, query)` to also filter on `_geohash`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StGeohashCovers {
    pub precision: u8,
    /// Pre-computed covering cells (serialized as newline-separated hashes)
    pub covering_cells: String,
}

#[typetag::serde]
impl ScalarUDF for StGeohashCovers {
    fn name(&self) -> &'static str {
        "st_geohash_covers"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let hash_series = inputs.required(0)?;
        let covering: Vec<String> = self
            .covering_cells
            .split('\n')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();

        let hash_arr = hash_series.utf8()?;
        let values = hash_arr.into_iter().map(|opt| {
            opt.map(|h| {
                covering
                    .iter()
                    .any(|c| h.starts_with(c.as_str()) || c.starts_with(h))
            })
        });

        Ok(BooleanArray::from_iter(self.name(), values).into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let f = inputs.required(0)?.to_field(schema)?;
        if !matches!(f.dtype, DataType::Utf8) {
            return Err(DaftError::TypeError(format!(
                "st_geohash_covers: hash column must be Utf8, got {}",
                f.dtype
            )));
        }
        Ok(Field::new(self.name(), DataType::Boolean))
    }

    fn docstring(&self) -> &'static str {
        "Returns true if the geohash cell overlaps with the pre-computed covering cells of a query geometry. \
         Used internally for geohash-based partition pruning."
    }
}
