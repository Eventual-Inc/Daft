use std::sync::Arc;

use daft_dsl::{
    ExprRef,
    functions::{BuiltinScalarFn, BuiltinScalarFnVariant, FunctionArgs},
    lit,
};
use daft_geo::{StArea, StAsText, StBuffer, StCentroid, StContains, StCrosses, StDisjoint, StDistance, StEquals, StGeohash, StGeometryType, StGeomFromGeoJson, StGeomFromText, StGeoJsonFromGeom, StIntersects, StIsValid, StLength, StOverlaps, StTouches, StWithin, StX, StY};
use sqlparser::ast;

use super::SQLModule;
use crate::{
    error::SQLPlannerResult,
    functions::{SQLFunction, SQLFunctions},
    invalid_operation_err,
};

pub struct SQLModuleSpatial;

impl SQLModule for SQLModuleSpatial {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("st_area", SQLStMeasureUnary("st_area"));
        parent.add_fn("st_length", SQLStMeasureUnary("st_length"));
        parent.add_fn("st_isvalid", SQLSpatialUnary(Arc::new(StIsValid)));
        parent.add_fn("st_geometrytype", SQLSpatialUnary(Arc::new(StGeometryType)));
        parent.add_fn("st_x", SQLSpatialUnary(Arc::new(StX)));
        parent.add_fn("st_y", SQLSpatialUnary(Arc::new(StY)));
        parent.add_fn("st_centroid", SQLSpatialUnary(Arc::new(StCentroid)));
        parent.add_fn("st_contains", SQLSpatialBinary(Arc::new(StContains)));
        parent.add_fn("st_intersects", SQLSpatialBinary(Arc::new(StIntersects)));
        parent.add_fn("st_within", SQLSpatialBinary(Arc::new(StWithin)));
        parent.add_fn("st_distance", SQLStMeasureBinary("st_distance"));
        parent.add_fn("st_touches", SQLSpatialBinary(Arc::new(StTouches)));
        parent.add_fn("st_crosses", SQLSpatialBinary(Arc::new(StCrosses)));
        parent.add_fn("st_overlaps", SQLSpatialBinary(Arc::new(StOverlaps)));
        parent.add_fn("st_disjoint", SQLSpatialBinary(Arc::new(StDisjoint)));
        parent.add_fn("st_equals", SQLSpatialBinary(Arc::new(StEquals)));
        parent.add_fn("st_buffer", SQLStBuffer);
        parent.add_fn("st_geohash", SQLStGeohash);
        parent.add_fn("st_astext", SQLSpatialUnary(Arc::new(StAsText)));
        parent.add_fn("st_geomfromtext", SQLSpatialUnary(Arc::new(StGeomFromText)));
        parent.add_fn("st_geomfromgeojson", SQLSpatialUnary(Arc::new(StGeomFromGeoJson)));
        parent.add_fn("st_geojsonfromgeom", SQLSpatialUnary(Arc::new(StGeoJsonFromGeom)));
    }
}

// ── Generic wrappers ────────────────────────────────────────────────────────

use daft_dsl::functions::ScalarUDF;

/// SQL wrapper for unary spatial functions (single geometry argument).
pub struct SQLSpatialUnary(Arc<dyn ScalarUDF>);

impl SQLFunction for SQLSpatialUnary {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 1 {
            invalid_operation_err!("{} expects 1 argument, got {}", self.0.name(), inputs.len());
        }
        let geom = planner.plan_function_arg(&inputs[0])?.into_inner();
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(self.0.clone()),
            inputs: FunctionArgs::new_unchecked(vec![daft_dsl::functions::FunctionArg::unnamed(geom)]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        self.0.docstring().to_string()
    }
}

/// SQL wrapper for binary spatial predicates (two geometry arguments).
pub struct SQLSpatialBinary(Arc<dyn ScalarUDF>);

impl SQLFunction for SQLSpatialBinary {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 2 {
            invalid_operation_err!("{} expects 2 arguments, got {}", self.0.name(), inputs.len());
        }
        let a = planner.plan_function_arg(&inputs[0])?.into_inner();
        let b = planner.plan_function_arg(&inputs[1])?.into_inner();
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(self.0.clone()),
            inputs: FunctionArgs::new_unchecked(vec![
                daft_dsl::functions::FunctionArg::unnamed(a),
                daft_dsl::functions::FunctionArg::unnamed(b),
            ]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        self.0.docstring().to_string()
    }
}

// ── st_buffer(geom, distance) ────────────────────────────────────────────────

pub struct SQLStBuffer;

impl SQLFunction for SQLStBuffer {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 2 {
            invalid_operation_err!("st_buffer expects 2 arguments (geom, distance), got {}", inputs.len());
        }
        let geom = planner.plan_function_arg(&inputs[0])?.into_inner();
        let distance_expr = planner.plan_function_arg(&inputs[1])?.into_inner();
        // Validate that distance is a numeric literal at plan time
        let _ = distance_expr
            .as_literal()
            .and_then(|l| l.as_f64().or_else(|| l.as_i64().map(|v| v as f64)))
            .ok_or_else(|| crate::error::PlannerError::invalid_operation(
                "st_buffer: distance must be a numeric literal",
            ))?;
        // Pass distance as a trailing positional arg so StBuffer (unit struct) can read it
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(StBuffer)),
            inputs: FunctionArgs::new_unchecked(vec![
                daft_dsl::functions::FunctionArg::unnamed(geom),
                daft_dsl::functions::FunctionArg::unnamed(distance_expr),
            ]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns a geometry expanded by the given distance (planar Cartesian).".to_string()
    }
}

// ── st_geohash(geom [, precision]) ──────────────────────────────────────────

pub struct SQLStGeohash;

impl SQLFunction for SQLStGeohash {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        let precision = match inputs.len() {
            1 => 5u8,
            2 => {
                let p = planner.plan_function_arg(&inputs[1])?.into_inner();
                p.as_literal()
                    .and_then(|l| l.as_i64())
                    .and_then(|v| u8::try_from(v).ok())
                    .ok_or_else(|| crate::error::PlannerError::invalid_operation(
                        "st_geohash: precision must be an integer literal between 1 and 12",
                    ))?
            }
            n => invalid_operation_err!("st_geohash expects 1 or 2 arguments, got {n}"),
        };
        let geom = planner.plan_function_arg(&inputs[0])?.into_inner();
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(StGeohash { precision })),
            inputs: FunctionArgs::new_unchecked(vec![daft_dsl::functions::FunctionArg::unnamed(geom)]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns the geohash string of a geometry's centroid at the given precision (default 5).".to_string()
    }
}

// ── st_area/st_length(geom [, use_spheroid]) ────────────────────────────────

/// SQL wrapper for unary measure functions that accept an optional boolean use_spheroid.
/// Supports: st_area(geom [, use_spheroid]), st_length(geom [, use_spheroid])
pub struct SQLStMeasureUnary(&'static str);

impl SQLFunction for SQLStMeasureUnary {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        let use_spheroid: bool = match inputs.len() {
            1 => false,
            2 => {
                let arg = planner.plan_function_arg(&inputs[1])?.into_inner();
                arg.as_literal()
                    .and_then(|l| l.as_bool())
                    .ok_or_else(|| crate::error::PlannerError::invalid_operation(
                        format!("{}: use_spheroid must be a boolean literal", self.0),
                    ))?
            }
            n => invalid_operation_err!("{} expects 1 or 2 arguments, got {n}", self.0),
        };
        let geom = planner.plan_function_arg(&inputs[0])?.into_inner();
        let func: Arc<dyn daft_dsl::functions::ScalarUDF> = match self.0 {
            "st_area" => Arc::new(StArea),
            "st_length" => Arc::new(StLength),
            name => invalid_operation_err!("unknown measure function: {}", name),
        };
        let mut args = vec![daft_dsl::functions::FunctionArg::unnamed(geom)];
        if use_spheroid {
            args.push(daft_dsl::functions::FunctionArg::named(
                "use_spheroid",
                lit(use_spheroid),
            ));
        }
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(func),
            inputs: FunctionArgs::new_unchecked(args),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        match self.0 {
            "st_area" => "2D area. Coordinate units² by default; WGS84 geodesic m² when use_spheroid=true.".to_string(),
            "st_length" => "Length/perimeter. Coordinate units by default; WGS84 geodesic meters when use_spheroid=true.".to_string(),
            _ => format!("{} measure function", self.0),
        }
    }
}

// ── st_distance(a, b [, use_spheroid]) ──────────────────────────────────────

/// SQL wrapper for binary measure functions that accept an optional boolean use_spheroid.
/// Supports: st_distance(a, b [, use_spheroid])
pub struct SQLStMeasureBinary(&'static str);

impl SQLFunction for SQLStMeasureBinary {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        let use_spheroid: bool = match inputs.len() {
            2 => false,
            3 => {
                let arg = planner.plan_function_arg(&inputs[2])?.into_inner();
                arg.as_literal()
                    .and_then(|l| l.as_bool())
                    .ok_or_else(|| crate::error::PlannerError::invalid_operation(
                        format!("{}: use_spheroid must be a boolean literal", self.0),
                    ))?
            }
            n => invalid_operation_err!("{} expects 2 or 3 arguments, got {n}", self.0),
        };
        let a = planner.plan_function_arg(&inputs[0])?.into_inner();
        let b = planner.plan_function_arg(&inputs[1])?.into_inner();
        let func: Arc<dyn daft_dsl::functions::ScalarUDF> = match self.0 {
            "st_distance" => Arc::new(StDistance),
            name => invalid_operation_err!("unknown measure function: {}", name),
        };
        let mut args = vec![
            daft_dsl::functions::FunctionArg::unnamed(a),
            daft_dsl::functions::FunctionArg::unnamed(b),
        ];
        if use_spheroid {
            args.push(daft_dsl::functions::FunctionArg::named(
                "use_spheroid",
                lit(use_spheroid),
            ));
        }
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(func),
            inputs: FunctionArgs::new_unchecked(args),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Minimum distance between A and B. Planar by default; WGS84 geodesic meters when use_spheroid=true.".to_string()
    }
}
