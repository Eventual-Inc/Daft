use arrow_buffer::NullBuffer;
use common_error::{DaftError, DaftResult, value_err};
use daft_core::prelude::*;
use daft_dsl::functions::{
    prelude::*,
    scalar::{EvalContext, ScalarFn},
};
use serde::{Deserialize, Serialize};

use super::math::{self, QuatOrder};

const QUAT: usize = 4;
const VEC3: usize = 3;
const ROT6D: usize = 6;
const MAT3: usize = 9;

fn matrix_dtype() -> DataType {
    DataType::FixedShapeTensor(Box::new(DataType::Float64), vec![3, 3])
}

fn list_dtype(size: usize) -> DataType {
    DataType::FixedSizeList(Box::new(DataType::Float64), size)
}

fn parse_order(fname: &str, order: Option<String>) -> DaftResult<QuatOrder> {
    match order {
        None => Ok(QuatOrder::Xyzw),
        Some(s) => QuatOrder::parse(&s).ok_or_else(|| {
            DaftError::ValueError(format!(
                "Expected 'order' for '{fname}' to be 'xyzw' or 'wxyz', instead got '{s}'"
            ))
        }),
    }
}

/// Check that `field` holds `size` floats per row, and name the output field.
///
/// Both `FixedSizeList` and the tensor types are accepted, since a fixed-shape
/// tensor lowers to a fixed size list of the product of its shape.
fn validate(fname: &str, field: &Field, size: usize, out: DataType) -> DaftResult<Field> {
    match field.dtype.to_physical() {
        DataType::FixedSizeList(inner, n)
            if n == size && matches!(*inner, DataType::Float32 | DataType::Float64) =>
        {
            Ok(Field::new(field.name.clone(), out))
        }
        _ => value_err!(
            "Expected input '{}' to '{}' to hold {} floats per row, instead got {}",
            field.name,
            fname,
            size,
            field.dtype
        ),
    }
}

/// Reduce any accepted input to a `FixedSizeList(Float64, size)` series.
fn to_f64_list(input: &Series, size: usize) -> DaftResult<Series> {
    let physical = input.as_physical()?;
    let want = list_dtype(size);
    if physical.data_type() == &want {
        Ok(physical)
    } else {
        physical.cast(&want)
    }
}

fn build_list(name: &str, size: usize, values: Vec<f64>, valid: Vec<bool>) -> FixedSizeListArray {
    let nulls = (!valid.iter().all(|v| *v)).then(|| NullBuffer::from(valid));
    let child = Float64Array::from_values(name, values.into_iter()).into_series();
    FixedSizeListArray::new(Field::new(name, list_dtype(size)), child, nulls)
}

fn as_matrix(name: &str, list: FixedSizeListArray) -> Series {
    FixedShapeTensorArray::new(Field::new(name, matrix_dtype()), list).into_series()
}

/// Align two inputs to a common row count, broadcasting a single row when needed.
fn align(fname: &str, a: Series, b: Series) -> DaftResult<(Series, Series)> {
    match (a.len(), b.len()) {
        (x, y) if x == y => Ok((a, b)),
        (1, y) => Ok((a.broadcast(y)?, b)),
        (x, 1) => Ok((a, b.broadcast(x)?)),
        (x, y) => Err(DaftError::ValueError(format!(
            "Expected inputs to '{fname}' to have equal lengths or length 1, instead got {x} and {y}"
        ))),
    }
}

/// A fixed size list of `Float64`, viewed as one contiguous slice plus a validity mask.
///
/// Iterating a `FixedSizeListArray` yields `Option<Series>`, which allocates a
/// `Series` per row. These expressions run over the flat child instead, so a
/// column of a million rotations costs no allocation at all.
struct Rows<'a> {
    values: &'a [f64],
    nulls: Option<&'a NullBuffer>,
    width: usize,
    len: usize,
}

impl<'a> Rows<'a> {
    fn new(list: &'a FixedSizeListArray, width: usize) -> DaftResult<Self> {
        Ok(Self {
            values: list.flat_child.try_as_slice::<f64>()?,
            nulls: list.nulls(),
            width,
            len: list.len(),
        })
    }

    /// The `i`th row, or `None` when it is null.
    fn get(&self, i: usize) -> Option<&'a [f64]> {
        if self.nulls.is_some_and(|n| n.is_null(i)) {
            return None;
        }
        self.values.get(i * self.width..(i + 1) * self.width)
    }
}

fn unary<const OUT: usize>(
    name: &str,
    input: &Series,
    size: usize,
    f: impl Fn(&[f64]) -> Option<[f64; OUT]>,
) -> DaftResult<FixedSizeListArray> {
    let list = to_f64_list(input, size)?;
    let list = list.fixed_size_list()?;
    let rows = Rows::new(list, size)?;

    let mut values = vec![0.0; rows.len * OUT];
    let mut valid = Vec::with_capacity(rows.len);
    for i in 0..rows.len {
        match rows.get(i).and_then(&f) {
            Some(out) => {
                values[i * OUT..(i + 1) * OUT].copy_from_slice(&out);
                valid.push(true);
            }
            None => valid.push(false),
        }
    }
    Ok(build_list(name, OUT, values, valid))
}

fn binary<const OUT: usize>(
    fname: &str,
    name: &str,
    a: &Series,
    b: &Series,
    sizes: (usize, usize),
    f: impl Fn(&[f64], &[f64]) -> Option<[f64; OUT]>,
) -> DaftResult<FixedSizeListArray> {
    let (a, b) = align(fname, to_f64_list(a, sizes.0)?, to_f64_list(b, sizes.1)?)?;
    let (a, b) = (a.fixed_size_list()?, b.fixed_size_list()?);
    let (a, b) = (Rows::new(a, sizes.0)?, Rows::new(b, sizes.1)?);

    let mut values = vec![0.0; a.len * OUT];
    let mut valid = Vec::with_capacity(a.len);
    for i in 0..a.len {
        match a.get(i).zip(b.get(i)).and_then(|(x, y)| f(x, y)) {
            Some(out) => {
                values[i * OUT..(i + 1) * OUT].copy_from_slice(&out);
                valid.push(true);
            }
            None => valid.push(false),
        }
    }
    Ok(build_list(name, OUT, values, valid))
}

#[derive(FunctionArgs)]
struct OneQuat<T> {
    input: T,
    #[arg(optional)]
    order: Option<String>,
}

#[derive(FunctionArgs)]
struct TwoQuats<T> {
    left: T,
    right: T,
    #[arg(optional)]
    order: Option<String>,
}

#[derive(FunctionArgs)]
struct QuatAndVector<T> {
    input: T,
    vector: T,
    #[arg(optional)]
    order: Option<String>,
}

#[derive(FunctionArgs)]
struct OneInput<T> {
    input: T,
}

#[derive(FunctionArgs)]
struct TwoMatrices<T> {
    left: T,
    right: T,
}

// ---------------------------------------------------------------------------
// rot6d_to_matrix
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Rot6dToMatrix;

#[typetag::serde]
impl ScalarUDF for Rot6dToMatrix {
    fn name(&self) -> &'static str {
        "rot6d_to_matrix"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let OneInput { input } = inputs.try_into()?;
        let list = unary(input.name(), &input, ROT6D, math::rot6d_to_mat)?;
        Ok(as_matrix(input.name(), list))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let OneInput { input } = inputs.try_into()?;
        validate(self.name(), &input.to_field(schema)?, ROT6D, matrix_dtype())
    }

    fn docstring(&self) -> &'static str {
        "Converts the 6D continuous rotation representation of Zhou et al. (2019) into a 3x3 rotation matrix.\n\nThe six values are the first two columns of the matrix. They are orthonormalized by Gram-Schmidt, and the third column is their cross product, so the result always lies in SO(3). Rows whose two vectors are zero or parallel produce null, since they determine no rotation."
    }
}

#[must_use]
pub fn rot6d_to_matrix(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Rot6dToMatrix {}, vec![input]).into()
}

// ---------------------------------------------------------------------------
// quat_to_matrix
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct QuatToMatrix;

#[typetag::serde]
impl ScalarUDF for QuatToMatrix {
    fn name(&self) -> &'static str {
        "quat_to_matrix"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let OneQuat { input, order } = inputs.try_into()?;
        let order = parse_order(self.name(), order)?;
        let list = unary(input.name(), &input, QUAT, |q| {
            math::quat_to_mat(order.read(q))
        })?;
        Ok(as_matrix(input.name(), list))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let OneQuat { input, order } = inputs.try_into()?;
        parse_order(self.name(), order)?;
        validate(self.name(), &input.to_field(schema)?, QUAT, matrix_dtype())
    }

    fn docstring(&self) -> &'static str {
        "Converts a quaternion into a 3x3 rotation matrix.\n\nThe quaternion is normalized first. Pass order='wxyz' for the scalar-first convention used by Eigen, MuJoCo, and Isaac Sim; the default 'xyzw' matches ROS, tf2, and scipy. Zero quaternions produce null."
    }
}

#[must_use]
pub fn quat_to_matrix(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(QuatToMatrix {}, vec![input]).into()
}

// ---------------------------------------------------------------------------
// matrix_to_quat
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MatrixToQuat;

#[typetag::serde]
impl ScalarUDF for MatrixToQuat {
    fn name(&self) -> &'static str {
        "matrix_to_quat"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let OneQuat { input, order } = inputs.try_into()?;
        let order = parse_order(self.name(), order)?;
        let list = unary(input.name(), &input, MAT3, |m| {
            let m: [f64; MAT3] = m.try_into().ok()?;
            Some(order.write(math::mat_to_quat(m)))
        })?;
        Ok(list.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let OneQuat { input, order } = inputs.try_into()?;
        parse_order(self.name(), order)?;
        validate(
            self.name(),
            &input.to_field(schema)?,
            MAT3,
            list_dtype(QUAT),
        )
    }

    fn docstring(&self) -> &'static str {
        "Converts a 3x3 rotation matrix into a quaternion, using Shepperd's method for numerical stability near a half turn.\n\nThe sign is not canonicalized: a quaternion and its negation denote the same rotation. Pass order='wxyz' for the scalar-first convention; the default is 'xyzw'."
    }
}

#[must_use]
pub fn matrix_to_quat(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(MatrixToQuat {}, vec![input]).into()
}

// ---------------------------------------------------------------------------
// quat_multiply
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct QuatMultiply;

#[typetag::serde]
impl ScalarUDF for QuatMultiply {
    fn name(&self) -> &'static str {
        "quat_multiply"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let TwoQuats { left, right, order } = inputs.try_into()?;
        let order = parse_order(self.name(), order)?;
        let list = binary(
            self.name(),
            left.name(),
            &left,
            &right,
            (QUAT, QUAT),
            |a, b| Some(order.write(math::multiply(order.read(a), order.read(b)))),
        )?;
        Ok(list.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let TwoQuats { left, right, order } = inputs.try_into()?;
        parse_order(self.name(), order)?;
        validate(
            self.name(),
            &right.to_field(schema)?,
            QUAT,
            list_dtype(QUAT),
        )?;
        validate(self.name(), &left.to_field(schema)?, QUAT, list_dtype(QUAT))
    }

    fn docstring(&self) -> &'static str {
        "Hamilton product of two quaternions.\n\nThe result applies the right rotation first, then the left, matching composition of the corresponding rotation matrices. Neither input is normalized."
    }
}

#[must_use]
pub fn quat_multiply(left: ExprRef, right: ExprRef) -> ExprRef {
    ScalarFn::builtin(QuatMultiply {}, vec![left, right]).into()
}

// ---------------------------------------------------------------------------
// quat_inverse
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct QuatInverse;

#[typetag::serde]
impl ScalarUDF for QuatInverse {
    fn name(&self) -> &'static str {
        "quat_inverse"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let OneQuat { input, order } = inputs.try_into()?;
        let order = parse_order(self.name(), order)?;
        let list = unary(input.name(), &input, QUAT, |q| {
            math::inverse(order.read(q)).map(|r| order.write(r))
        })?;
        Ok(list.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let OneQuat { input, order } = inputs.try_into()?;
        parse_order(self.name(), order)?;
        validate(
            self.name(),
            &input.to_field(schema)?,
            QUAT,
            list_dtype(QUAT),
        )
    }

    fn docstring(&self) -> &'static str {
        "Inverse of a quaternion, the conjugate divided by the squared norm.\n\nFor unit quaternions this is the conjugate, and it undoes the rotation. Zero quaternions produce null."
    }
}

#[must_use]
pub fn quat_inverse(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(QuatInverse {}, vec![input]).into()
}

// ---------------------------------------------------------------------------
// quat_rotate
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct QuatRotate;

#[typetag::serde]
impl ScalarUDF for QuatRotate {
    fn name(&self) -> &'static str {
        "quat_rotate"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let QuatAndVector {
            input,
            vector,
            order,
        } = inputs.try_into()?;
        let order = parse_order(self.name(), order)?;
        let list = binary(
            self.name(),
            input.name(),
            &input,
            &vector,
            (QUAT, VEC3),
            |q, v| {
                let v: [f64; VEC3] = v.try_into().ok()?;
                math::rotate(order.read(q), v)
            },
        )?;
        Ok(list.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let QuatAndVector {
            input,
            vector,
            order,
        } = inputs.try_into()?;
        parse_order(self.name(), order)?;
        validate(
            self.name(),
            &vector.to_field(schema)?,
            VEC3,
            list_dtype(VEC3),
        )?;
        validate(
            self.name(),
            &input.to_field(schema)?,
            QUAT,
            list_dtype(VEC3),
        )
    }

    fn docstring(&self) -> &'static str {
        "Rotates a 3-vector by a quaternion.\n\nThe quaternion is normalized first, so the length of the vector is preserved. Zero quaternions produce null."
    }
}

#[must_use]
pub fn quat_rotate(input: ExprRef, vector: ExprRef) -> ExprRef {
    ScalarFn::builtin(QuatRotate {}, vec![input, vector]).into()
}

// ---------------------------------------------------------------------------
// rotation_geodesic_angle
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RotationGeodesicAngle;

#[typetag::serde]
impl ScalarUDF for RotationGeodesicAngle {
    fn name(&self) -> &'static str {
        "rotation_geodesic_angle"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let TwoMatrices { left, right } = inputs.try_into()?;
        let name = left.name().to_string();
        let (a, b) = align(
            self.name(),
            to_f64_list(&left, MAT3)?,
            to_f64_list(&right, MAT3)?,
        )?;
        let (a, b) = (a.fixed_size_list()?, b.fixed_size_list()?);
        let (a, b) = (Rows::new(a, MAT3)?, Rows::new(b, MAT3)?);

        let out = (0..a.len)
            .map(|i| {
                a.get(i)
                    .zip(b.get(i))
                    .map(|(x, y)| math::geodesic_angle(x, y))
            })
            .collect::<Float64Array>()
            .rename(&name);
        Ok(out.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let TwoMatrices { left, right } = inputs.try_into()?;
        validate(
            self.name(),
            &right.to_field(schema)?,
            MAT3,
            DataType::Float64,
        )?;
        validate(
            self.name(),
            &left.to_field(schema)?,
            MAT3,
            DataType::Float64,
        )
    }

    fn docstring(&self) -> &'static str {
        "Angle in radians of the relative rotation between two 3x3 rotation matrices.\n\nComputes arccos((trace(A B^T) - 1) / 2), the geodesic distance on SO(3), which lies in [0, pi]. This is the bi-invariant Riemannian metric, so it is symmetric and unchanged by rotating both arguments."
    }
}

#[must_use]
pub fn rotation_geodesic_angle(left: ExprRef, right: ExprRef) -> ExprRef {
    ScalarFn::builtin(RotationGeodesicAngle {}, vec![left, right]).into()
}
