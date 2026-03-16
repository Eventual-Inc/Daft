use daft_dsl::functions::{FunctionModule, FunctionRegistry};

mod cell_info;
mod cell_parent;
mod cell_str;
mod cell_to_latlng;
mod grid_distance;
mod latlng_to_cell;
mod utils;

pub struct H3Functions;

impl FunctionModule for H3Functions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(latlng_to_cell::H3LatLngToCell);
        parent.add_fn(cell_to_latlng::H3CellToLat);
        parent.add_fn(cell_to_latlng::H3CellToLng);
        parent.add_fn(cell_str::H3CellToStr);
        parent.add_fn(cell_str::H3StrToCell);
        parent.add_fn(cell_info::H3CellResolution);
        parent.add_fn(cell_info::H3CellIsValid);
        parent.add_fn(cell_parent::H3CellParent);
        parent.add_fn(grid_distance::H3GridDistance);
    }
}
