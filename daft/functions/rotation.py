"""Rotation functions.

Robot trajectories, camera extrinsics, and hand-tracking datasets carry rotations.
These functions operate on them natively, so composing, inverting, and comparing
rotations does not require a Python UDF.

Quaternions are fixed-size lists of four floats. The component order defaults to
``"xyzw"``, matching ROS, tf2, and ``scipy.spatial.transform.Rotation``. Pass
``order="wxyz"`` for the scalar-first convention used by Eigen, MuJoCo, and Isaac Sim.
The argument is case-insensitive.

Rotation matrices are ``Tensor[Float64; [3, 3]]``, in row-major order.

Every function here returns null for a row that does not denote a rotation: a null
input, a value that is ``NaN`` or infinite, a zero quaternion, a 6D pair whose two
vectors are zero or parallel, or a matrix that is not in ``SO(3)``. Nothing is
silently coerced into a plausible rotation.
"""

from __future__ import annotations

from typing import Literal

from daft.expressions import Expression

__all__ = [
    "matrix_to_quat",
    "quat_inverse",
    "quat_multiply",
    "quat_rotate",
    "quat_to_matrix",
    "rot6d_to_matrix",
    "rotation_geodesic_angle",
]

QuatOrder = Literal["xyzw", "wxyz"]


def rot6d_to_matrix(rot6d: Expression) -> Expression:
    """Converts the 6D continuous rotation representation into a 3x3 rotation matrix.

    The six values are the first two columns of the matrix. They are orthonormalized by
    Gram-Schmidt, and the third column is their cross product, so the result always lies
    in SO(3). This is the representation of Zhou et al. (2019), widely used as a network
    output because it is continuous, unlike quaternions and Euler angles.

    Rows whose two vectors are zero or parallel, or which hold a non-finite value,
    produce null, since they determine no rotation.

    Args:
        rot6d (FixedSizeList[Float64, 6] Expression): The 6D rotation representation.

    Returns:
        Expression (Tensor[Float64, [3, 3]] Expression): The rotation matrix.

    Examples:
        >>> import daft
        >>> from daft.functions import rot6d_to_matrix
        >>> df = daft.from_pydict({"r": [[1.0, 0.0, 0.0, 0.0, 1.0, 0.0]]})
        >>> df = df.select(rot6d_to_matrix(df["r"].cast(daft.DataType.fixed_size_list(daft.DataType.float64(), 6))))
        >>> df.to_pydict()["r"][0].tolist()
        [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
    """
    return Expression._call_builtin_scalar_fn("rot6d_to_matrix", rot6d)


def quat_to_matrix(quat: Expression, *, order: QuatOrder = "xyzw") -> Expression:
    """Converts a quaternion into a 3x3 rotation matrix.

    The quaternion is normalized first. Zero quaternions, and rows holding a
    non-finite value, produce null.

    Args:
        quat (FixedSizeList[Float64, 4] Expression): The quaternion.
        order (str, default="xyzw"): Component order, either "xyzw" or "wxyz". Case-insensitive.

    Returns:
        Expression (Tensor[Float64, [3, 3]] Expression): The rotation matrix.
    """
    return Expression._call_builtin_scalar_fn("quat_to_matrix", quat, order=order)


def matrix_to_quat(matrix: Expression, *, order: QuatOrder = "xyzw") -> Expression:
    """Converts a 3x3 rotation matrix into a quaternion.

    Uses Shepperd's method, which stays numerically stable near a half turn where the
    naive trace formula loses precision. The sign is not canonicalized, since a
    quaternion and its negation denote the same rotation.

    Shepperd's method yields a unit quaternion exactly when its input lies in ``SO(3)``,
    so a matrix that is scaled, is a reflection, or holds a non-finite value produces
    null rather than a plausible quaternion.

    Args:
        matrix (Tensor[Float64, [3, 3]] Expression): The rotation matrix.
        order (str, default="xyzw"): Component order, either "xyzw" or "wxyz". Case-insensitive.

    Returns:
        Expression (FixedSizeList[Float64, 4] Expression): The quaternion.
    """
    return Expression._call_builtin_scalar_fn("matrix_to_quat", matrix, order=order)


def quat_multiply(left: Expression, right: Expression, *, order: QuatOrder = "xyzw") -> Expression:
    """Hamilton product of two quaternions.

    The result applies `right` first and then `left`, matching composition of the
    corresponding rotation matrices. Neither input is normalized. Rows holding a
    non-finite value produce null.

    Args:
        left (FixedSizeList[Float64, 4] Expression): The left quaternion.
        right (FixedSizeList[Float64, 4] Expression): The right quaternion.
        order (str, default="xyzw"): Component order, either "xyzw" or "wxyz". Case-insensitive.

    Returns:
        Expression (FixedSizeList[Float64, 4] Expression): The product.
    """
    return Expression._call_builtin_scalar_fn("quat_multiply", left, right, order=order)


def quat_inverse(quat: Expression, *, order: QuatOrder = "xyzw") -> Expression:
    """Inverse of a quaternion, the conjugate divided by the squared norm.

    For unit quaternions this is the conjugate, and it undoes the rotation.
    Zero quaternions, and rows holding a non-finite value, produce null.

    Args:
        quat (FixedSizeList[Float64, 4] Expression): The quaternion.
        order (str, default="xyzw"): Component order, either "xyzw" or "wxyz". Case-insensitive.

    Returns:
        Expression (FixedSizeList[Float64, 4] Expression): The inverse.
    """
    return Expression._call_builtin_scalar_fn("quat_inverse", quat, order=order)


def quat_rotate(quat: Expression, vector: Expression, *, order: QuatOrder = "xyzw") -> Expression:
    """Rotates a 3-vector by a quaternion.

    The quaternion is normalized first, so the length of the vector is preserved.
    Zero quaternions, and rows where the quaternion or the vector holds a non-finite
    value, produce null.

    Args:
        quat (FixedSizeList[Float64, 4] Expression): The quaternion.
        vector (FixedSizeList[Float64, 3] Expression): The vector to rotate.
        order (str, default="xyzw"): Component order, either "xyzw" or "wxyz". Case-insensitive.

    Returns:
        Expression (FixedSizeList[Float64, 3] Expression): The rotated vector.
    """
    return Expression._call_builtin_scalar_fn("quat_rotate", quat, vector, order=order)


def rotation_geodesic_angle(left: Expression, right: Expression) -> Expression:
    """Angle in radians of the relative rotation between two 3x3 rotation matrices.

    Computes ``arccos((trace(A @ B.T) - 1) / 2)``, the geodesic distance on SO(3),
    which lies in ``[0, pi]``. This is the bi-invariant Riemannian metric, so it is
    symmetric and unchanged by rotating both arguments.

    Rounding is absorbed by clamping the cosine into ``[-1, 1]``. A row where either
    argument is not a rotation, and so the cosine lies well outside that range, produces
    null rather than a plausible angle.

    Applied to consecutive frames of a trajectory, it measures how much a body turned
    between them.

    Args:
        left (Tensor[Float64, [3, 3]] Expression): The first rotation matrix.
        right (Tensor[Float64, [3, 3]] Expression): The second rotation matrix.

    Returns:
        Expression (Float64 Expression): The angle in radians.
    """
    return Expression._call_builtin_scalar_fn("rotation_geodesic_angle", left, right)
