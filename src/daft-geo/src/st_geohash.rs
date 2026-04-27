use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{BoundingRect, Centroid, Geometry};
use geohash::{encode, Coord as GeohashCoord};
use serde::{Deserialize, Serialize};

use crate::utils::{get_geometry_binary, parse_wkb, validate_geometry_field};

/// Compute geohash of the centroid of a geometry.
fn geom_geohash(g: &Geometry, precision: usize) -> Option<String> {
    let centroid = g.centroid()?;
    let coord = GeohashCoord {
        x: centroid.x(),
        y: centroid.y(),
    };
    encode(coord, precision).ok()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StGeohash {
    pub precision: u8,
}

#[typetag::serde]
impl ScalarUDF for StGeohash {
    fn name(&self) -> &'static str {
        "st_geohash"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let precision = self.precision as usize;
        let binary = get_geometry_binary(inputs.required(0)?)?;

        let values: Vec<Option<String>> = binary
            .into_iter()
            .map(|opt| {
                opt.and_then(|b| parse_wkb(b).ok())
                    .and_then(|g| geom_geohash(&g, precision))
            })
            .collect();

        Ok(Utf8Array::from_iter(self.name(), values.iter().map(|v| v.as_deref())).into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Returns the geohash string of a geometry's centroid at the given precision (1-12)."
    }
}

#[must_use]
pub fn st_geohash(geom: ExprRef, precision: u8) -> ExprRef {
    ScalarFn::builtin(StGeohash { precision }, vec![geom]).into()
}

// ──────────────────────────────────────────────────────────────────────────────
// Geohash bounding-box coverage: returns geohash prefixes that cover a bbox
// ──────────────────────────────────────────────────────────────────────────────

/// Compute all geohash cells at `precision` that overlap with the given geometry's bounding box.
/// Used for automatic geohash-based partition pruning.
pub fn geohash_covers_geometry(g: &Geometry, precision: usize) -> Vec<String> {
    let bbox = match g.bounding_rect() {
        Some(b) => b,
        None => {
            if let Some(c) = g.centroid() {
                let coord = GeohashCoord { x: c.x(), y: c.y() };
                return encode(coord, precision).ok().into_iter().collect();
            }
            return vec![];
        }
    };

    // Enumerate all geohash cells covering the bounding box
    let mut cells = std::collections::HashSet::new();
    let (min_hash, _) = geohash::encode(
        GeohashCoord { x: bbox.min().x, y: bbox.min().y },
        precision,
    )
    .map(|h| (h, ()))
    .unwrap_or_default();

    let (max_hash, _) = geohash::encode(
        GeohashCoord { x: bbox.max().x, y: bbox.max().y },
        precision,
    )
    .map(|h| (h, ()))
    .unwrap_or_default();

    // Walk neighbors to cover the entire bounding box
    collect_covering_cells(&mut cells, &min_hash, &max_hash, precision);
    cells.into_iter().collect()
}

fn collect_covering_cells(
    cells: &mut std::collections::HashSet<String>,
    start: &str,
    end: &str,
    precision: usize,
) {
    if start.is_empty() || end.is_empty() {
        return;
    }
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(start.to_string());
    let mut visited = std::collections::HashSet::new();

    while let Some(current) = queue.pop_front() {
        if visited.contains(&current) || current.len() != precision {
            continue;
        }
        visited.insert(current.clone());
        cells.insert(current.clone());

        if &current == end {
            break;
        }

        // Add all neighbors
        if let Ok(neighbors) = geohash::neighbors(&current) {
            for neighbor in [
                neighbors.n, neighbors.ne, neighbors.e, neighbors.se,
                neighbors.s, neighbors.sw, neighbors.w, neighbors.nw,
            ] {
                if !visited.contains(&neighbor) {
                    queue.push_back(neighbor);
                }
            }
        }
    }
}

/// Check if a geohash string is covered by any of the given covering cells.
/// Used as a fast pre-filter before exact spatial evaluation.
pub fn geohash_in_covering_cells(hash: &str, covering: &[String]) -> bool {
    covering.iter().any(|c| hash.starts_with(c.as_str()) || c.starts_with(hash))
}
