#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![doc(
    html_logo_url = "https://github.com/geoarrow.png",
    html_favicon_url = "https://github.com/geoarrow.png?size=32"
)]

mod affine_ops;
mod area;
mod bounding_rect;
mod center;
mod centroid;
mod chaikin_smoothing;
mod chamberlain_duquette_area;
mod concave_hull;
mod contains;
mod convex_hull;
mod densify;
mod dimensions;
mod distance;
mod frechet_distance;
mod geodesic_area;
mod geodesic_length;
mod haversine_length;
mod interior_point;
mod intersects;
mod length;
mod line_interpolate_point;
mod line_locate_point;
mod minimum_rotated_rect;
mod relate;
mod remove_repeated_points;
mod rotate;
mod scale;
mod simplify;
mod simplify_vw;
mod simplify_vw_preserve;
mod skew;
mod translate;
pub mod util;
pub mod validation;
mod vincenty_length;
mod within;

pub use affine_ops::affine_transform;
pub use area::{signed_area, unsigned_area};
pub use bounding_rect::bounding_rect;
pub use center::center;
pub use centroid::centroid;
pub use chaikin_smoothing::chaikin_smoothing;
pub use chamberlain_duquette_area::{
    chamberlain_duquette_signed_area, chamberlain_duquette_unsigned_area,
};
pub use concave_hull::concave_hull;
pub use contains::contains;
pub use convex_hull::convex_hull;
pub use densify::densify;
pub use dimensions::is_empty;
pub use distance::euclidean_distance;
pub use frechet_distance::frechet_distance;
pub use geodesic_area::{geodesic_area_signed, geodesic_area_unsigned, geodesic_perimeter};
pub use geodesic_length::geodesic_length;
pub use haversine_length::haversine_length;
pub use interior_point::interior_point;
pub use intersects::intersects;
pub use length::euclidean_length;
pub use line_interpolate_point::line_interpolate_point;
pub use line_locate_point::line_locate_point;
pub use minimum_rotated_rect::minimum_rotated_rect;
pub use relate::relate_boolean;
pub use remove_repeated_points::remove_repeated_points;
pub use rotate::{rotate_around_center, rotate_around_centroid, rotate_around_point};
pub use scale::{scale, scale_around_point};
pub use simplify::simplify;
pub use simplify_vw::simplify_vw;
pub use simplify_vw_preserve::simplify_vw_preserve;
pub use skew::{skew, skew_around_point};
pub use translate::translate;
pub use vincenty_length::vincenty_length;
pub use within::within;
