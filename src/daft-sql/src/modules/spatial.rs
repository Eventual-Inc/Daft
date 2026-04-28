use std::sync::Arc;

use daft_dsl::{
    ExprRef,
    functions::{BuiltinScalarFn, BuiltinScalarFnVariant, FunctionArgs},
};
use daft_geo::{StArea, StAsText, StBuffer, StCentroid, StContains, StDistance, StGeohash, StGeometryType, StGeomFromGeoJson, StGeomFromText, StGeoJsonFromGeom, StIntersects, StIsValid, StLength, StWithin, StX, StY};
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
        parent.add_fn("st_area", SQLSpatialUnary(Arc::new(StArea)));
        parent.add_fn("st_length", SQLSpatialUnary(Arc::new(StLength)));
        parent.add_fn("st_isvalid", SQLSpatialUnary(Arc::new(StIsValid)));
        parent.add_fn("st_geometrytype", SQLSpatialUnary(Arc::new(StGeometryType)));
        parent.add_fn("st_x", SQLSpatialUnary(Arc::new(StX)));
        parent.add_fn("st_y", SQLSpatialUnary(Arc::new(StY)));
        parent.add_fn("st_centroid", SQLSpatialUnary(Arc::new(StCentroid)));
        parent.add_fn("st_contains", SQLSpatialBinary(Arc::new(StContains)));
        parent.add_fn("st_intersects", SQLSpatialBinary(Arc::new(StIntersects)));
        parent.add_fn("st_within", SQLSpatialBinary(Arc::new(StWithin)));
        parent.add_fn("st_distance", SQLSpatialBinary(Arc::new(StDistance)));
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
        let distance = distance_expr
            .as_literal()
            .and_then(|l| l.as_f64())
            .ok_or_else(|| crate::error::PlannerError::invalid_operation(
                "st_buffer: distance must be a numeric literal",
            ))?;
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(StBuffer {
                distance: ordered_float::OrderedFloat(distance),
            })),
            inputs: FunctionArgs::new_unchecked(vec![daft_dsl::functions::FunctionArg::unnamed(geom)]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns a geometry expanded by the given distance.".to_string()
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
