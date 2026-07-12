//! Rotation expressions for pose data.
//!
//! Daft already reads rotations (`daft.datasets.droid`, `daft.datasets.lerobot`,
//! `daft.datasets.egodex`, `daft.read_mcap`) but stores them as untyped float
//! columns, so composing or comparing them means a per-row Python UDF. These
//! expressions do the algebra natively.

mod functions;
mod math;

use daft_dsl::functions::{FunctionModule, FunctionRegistry};
pub use functions::{
    MatrixToQuat, QuatInverse, QuatMultiply, QuatRotate, QuatToMatrix, Rot6dToMatrix,
    RotationGeodesicAngle, matrix_to_quat, quat_inverse, quat_multiply, quat_rotate,
    quat_to_matrix, rot6d_to_matrix, rotation_geodesic_angle,
};

pub struct RotationFunctions;

impl FunctionModule for RotationFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(MatrixToQuat);
        parent.add_fn(QuatInverse);
        parent.add_fn(QuatMultiply);
        parent.add_fn(QuatRotate);
        parent.add_fn(QuatToMatrix);
        parent.add_fn(Rot6dToMatrix);
        parent.add_fn(RotationGeodesicAngle);
    }
}
