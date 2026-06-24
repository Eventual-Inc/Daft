use daft_dsl::functions::FunctionModule;

pub mod great_circle_distance;
pub mod h3_index;
pub mod mbr;
pub(crate) mod relate;
pub mod st_area;
pub mod st_astext;
pub mod st_buffer;
pub mod st_centroid;
pub mod st_convexhull;
pub mod st_envelope;
pub mod st_contains;
pub mod st_crosses;
pub mod st_difference;
pub mod st_disjoint;
pub mod st_distance;
pub mod st_equals;
pub mod st_geojson;
pub mod st_geometrytype;
pub mod st_geohash;
pub mod st_geohash_covers;
pub mod st_geomfromtext;
pub mod st_intersection;
pub mod st_intersects;
pub mod st_isvalid;
pub mod st_length;
pub mod st_overlaps;
pub mod st_simplify;
pub mod st_symdifference;
pub mod st_touches;
pub mod st_union;
pub mod st_within;
pub mod st_xy;
pub mod utils;

pub use great_circle_distance::GreatCircleDistance;
pub use mbr::{Mbr, mbrs_intersect, wkb_to_mbr};
pub use utils::get_geometry_binary;
pub use st_area::StArea;
pub use st_astext::StAsText;
pub use st_buffer::StBuffer;
pub use st_centroid::StCentroid;
pub use st_convexhull::StConvexHull;
pub use st_envelope::StEnvelope;
pub use st_contains::StContains;
pub use st_crosses::StCrosses;
pub use st_difference::StDifference;
pub use st_disjoint::StDisjoint;
pub use st_distance::StDistance;
pub use st_equals::StEquals;
pub use st_geojson::{StGeomFromGeoJson, StGeoJsonFromGeom};
pub use st_geometrytype::StGeometryType;
pub use st_geohash::{StGeohash, geohash_covers_geometry};
pub use st_geohash_covers::StGeohashCovers;
pub use st_geomfromtext::StGeomFromText;
pub use st_intersection::StIntersection;
pub use st_intersects::StIntersects;
pub use st_isvalid::StIsValid;
pub use st_length::StLength;
pub use st_overlaps::StOverlaps;
pub use st_simplify::StSimplify;
pub use st_symdifference::StSymDifference;
pub use st_touches::StTouches;
pub use st_union::StUnion;
pub use st_within::StWithin;
pub use st_xy::{StX, StY};

pub struct SpatialFunctions;

impl FunctionModule for SpatialFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(GreatCircleDistance);
        parent.add_fn(StArea);
        parent.add_fn(StLength);
        parent.add_fn(StIsValid);
        parent.add_fn(StGeometryType);
        parent.add_fn(StX);
        parent.add_fn(StY);
        parent.add_fn(StContains);
        parent.add_fn(StIntersects);
        parent.add_fn(StWithin);
        parent.add_fn(StTouches);
        parent.add_fn(StCrosses);
        parent.add_fn(StOverlaps);
        parent.add_fn(StDisjoint);
        parent.add_fn(StEquals);
        parent.add_fn(StDistance);
        parent.add_fn(StCentroid);
        parent.add_fn(StUnion);
        parent.add_fn(StIntersection);
        parent.add_fn(StDifference);
        parent.add_fn(StSymDifference);
        parent.add_fn(StGeohash { precision: 5 });
        parent.add_fn(StGeohashCovers {
            precision: 5,
            covering_cells: String::new(),
        });
        parent.add_fn(StAsText);
        parent.add_fn(StGeomFromText);
        parent.add_fn(StGeomFromGeoJson);
        parent.add_fn(StGeoJsonFromGeom);
        parent.add_fn(StBuffer);
        parent.add_fn(StEnvelope);
        parent.add_fn(StConvexHull);
        parent.add_fn(StSimplify);
    }
}
