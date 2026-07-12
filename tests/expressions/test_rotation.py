from __future__ import annotations

import math

import numpy as np
import pytest

import daft
from daft.datatype import DataType
from daft.expressions import col, lit
from daft.functions import (
    matrix_to_quat,
    quat_inverse,
    quat_multiply,
    quat_rotate,
    quat_to_matrix,
    rot6d_to_matrix,
    rotation_geodesic_angle,
)

scipy_rotation = pytest.importorskip("scipy.spatial.transform").Rotation

QUAT = DataType.fixed_size_list(DataType.float64(), 4)
VEC3 = DataType.fixed_size_list(DataType.float64(), 3)
ROT6D = DataType.fixed_size_list(DataType.float64(), 6)
MAT3 = DataType.tensor(DataType.float64(), (3, 3))

# Identity, three half turns about the axes (trace = -1, the numerically hard case),
# an equal-weight quaternion, a near-identity rotation, and two generic ones.
# Stored xyzw.
QUATS_XYZW = [
    [0.0, 0.0, 0.0, 1.0],
    [1.0, 0.0, 0.0, 0.0],
    [0.0, 1.0, 0.0, 0.0],
    [0.0, 0.0, 1.0, 0.0],
    [0.5, 0.5, 0.5, 0.5],
    [1e-7, 0.0, 0.0, 0.9999999],
    [-0.2, 0.8, 0.1, 0.3],
    [0.1, -0.3, 0.7, -0.6],
]

VECTORS = [[1.0, 0.0, 0.0], [0.0, 2.0, -1.0], [0.3, -1.7, 2.1], [1.0, 1.0, 1.0]]


def _normalize(q):
    return (np.asarray(q) / np.linalg.norm(q)).tolist()


UNIT_QUATS_XYZW = [_normalize(q) for q in QUATS_XYZW]


def _frame(**cols):
    return daft.from_pydict(cols)


def _quat_col(name, values, dtype=QUAT):
    return _frame(**{name: values}).select(col(name).cast(dtype))


def _rotations():
    """The same rotations as scipy objects, for use as an independent oracle."""
    return scipy_rotation.from_quat(np.asarray(UNIT_QUATS_XYZW))


def test_repr():
    assert repr(quat_inverse(col("q"))) == 'quat_inverse(col(q), lit("xyzw"))'
    assert repr(rot6d_to_matrix(col("r"))) == "rot6d_to_matrix(col(r))"


# ---------------------------------------------------------------------------
# Differential tests against scipy
# ---------------------------------------------------------------------------


def test_quat_to_matrix_matches_scipy():
    df = _quat_col("q", UNIT_QUATS_XYZW).select(quat_to_matrix(col("q")))
    got = np.asarray([np.asarray(m) for m in df.to_pydict()["q"]])
    want = _rotations().as_matrix()
    np.testing.assert_allclose(got, want, atol=1e-12)


def test_matrix_to_quat_matches_scipy_up_to_sign():
    matrices = [m.flatten().tolist() for m in _rotations().as_matrix()]
    df = _frame(m=matrices).select(col("m").cast(MAT3)).select(matrix_to_quat(col("m")))
    got = np.asarray([np.asarray(q) for q in df.to_pydict()["m"]])
    got = got / np.linalg.norm(got, axis=1, keepdims=True)
    want = np.asarray(UNIT_QUATS_XYZW)
    # A quaternion and its negation are the same rotation, so align signs first.
    signs = np.sign(np.sum(got * want, axis=1))[:, None]
    np.testing.assert_allclose(got * signs, want, atol=1e-9)


def test_quat_multiply_matches_scipy():
    left, right = UNIT_QUATS_XYZW, list(reversed(UNIT_QUATS_XYZW))
    df = (
        _frame(a=left, b=right)
        .select(col("a").cast(QUAT), col("b").cast(QUAT))
        .select(quat_multiply(col("a"), col("b")))
    )
    got = np.asarray([np.asarray(q) for q in df.to_pydict()["a"]])
    # scipy composes left * right the same way, applying right first.
    want = (scipy_rotation.from_quat(np.asarray(left)) * scipy_rotation.from_quat(np.asarray(right))).as_quat()
    signs = np.sign(np.sum(got * want, axis=1))[:, None]
    np.testing.assert_allclose(got * signs, want, atol=1e-12)


def test_quat_inverse_matches_scipy():
    df = _quat_col("q", UNIT_QUATS_XYZW).select(quat_inverse(col("q")))
    got = np.asarray([np.asarray(q) for q in df.to_pydict()["q"]])
    want = _rotations().inv().as_quat()
    signs = np.sign(np.sum(got * want, axis=1))[:, None]
    np.testing.assert_allclose(got * signs, want, atol=1e-12)


@pytest.mark.parametrize("vector", VECTORS)
def test_quat_rotate_matches_scipy(vector):
    n = len(UNIT_QUATS_XYZW)
    df = (
        _frame(q=UNIT_QUATS_XYZW, v=[vector] * n)
        .select(col("q").cast(QUAT), col("v").cast(VEC3))
        .select(quat_rotate(col("q"), col("v")))
    )
    got = np.asarray([np.asarray(v) for v in df.to_pydict()["q"]])
    want = _rotations().apply(np.asarray([vector] * n))
    np.testing.assert_allclose(got, want, atol=1e-12)


def test_rot6d_to_matrix_matches_gram_schmidt_reference():
    # The reference implementation from the literature, and from Eventual's own
    # EgoDex blog post: normalize the first column, orthogonalize the second
    # against it, take the cross product for the third.
    rng = np.random.default_rng(0)
    raw = rng.normal(size=(16, 6))

    def reference(r):
        a1, a2 = r[:3], r[3:]
        c1 = a1 / np.linalg.norm(a1)
        u = a2 - np.dot(c1, a2) * c1
        c2 = u / np.linalg.norm(u)
        return np.stack([c1, c2, np.cross(c1, c2)], axis=1)

    want = np.asarray([reference(r) for r in raw])
    df = _frame(r=raw.tolist()).select(col("r").cast(ROT6D)).select(rot6d_to_matrix(col("r")))
    got = np.asarray([np.asarray(m) for m in df.to_pydict()["r"]])
    np.testing.assert_allclose(got, want, atol=1e-12)


def test_rotation_geodesic_angle_matches_scipy_magnitude():
    a = _rotations()
    b = scipy_rotation.from_quat(np.asarray(list(reversed(UNIT_QUATS_XYZW))))
    left = [m.flatten().tolist() for m in a.as_matrix()]
    right = [m.flatten().tolist() for m in b.as_matrix()]

    df = (
        _frame(a=left, b=right)
        .select(col("a").cast(MAT3), col("b").cast(MAT3))
        .select(rotation_geodesic_angle(col("a"), col("b")))
    )
    got = np.asarray(df.to_pydict()["a"])
    want = (a * b.inv()).magnitude()
    np.testing.assert_allclose(got, want, atol=1e-7)


# ---------------------------------------------------------------------------
# Algebraic properties
# ---------------------------------------------------------------------------


def test_multiply_by_inverse_is_identity():
    df = (
        _quat_col("q", UNIT_QUATS_XYZW)
        .with_column("inv", quat_inverse(col("q")))
        .select(quat_multiply(col("q"), col("inv")).alias("r"))
    )
    got = np.asarray([np.asarray(q) for q in df.to_pydict()["r"]])
    identity = np.tile([0.0, 0.0, 0.0, 1.0], (len(UNIT_QUATS_XYZW), 1))
    np.testing.assert_allclose(np.abs(got), np.abs(identity), atol=1e-9)


def test_rotation_preserves_vector_norm():
    vector = [0.3, -1.7, 2.1]
    n = len(UNIT_QUATS_XYZW)
    df = (
        _frame(q=UNIT_QUATS_XYZW, v=[vector] * n)
        .select(col("q").cast(QUAT), col("v").cast(VEC3))
        .select(quat_rotate(col("q"), col("v")))
    )
    got = np.asarray([np.asarray(v) for v in df.to_pydict()["q"]])
    np.testing.assert_allclose(np.linalg.norm(got, axis=1), np.linalg.norm(vector), atol=1e-12)


def test_double_cover_negated_quaternion_is_the_same_rotation():
    negated = [[-c for c in q] for q in UNIT_QUATS_XYZW]
    a = _quat_col("q", UNIT_QUATS_XYZW).select(quat_to_matrix(col("q"))).to_pydict()["q"]
    b = _quat_col("q", negated).select(quat_to_matrix(col("q"))).to_pydict()["q"]
    np.testing.assert_allclose(
        np.asarray([np.asarray(m) for m in a]), np.asarray([np.asarray(m) for m in b]), atol=1e-12
    )


def test_matrices_are_special_orthogonal():
    df = _quat_col("q", UNIT_QUATS_XYZW).select(quat_to_matrix(col("q")))
    for m in df.to_pydict()["q"]:
        m = np.asarray(m)
        np.testing.assert_allclose(m @ m.T, np.eye(3), atol=1e-12)
        assert math.isclose(float(np.linalg.det(m)), 1.0, abs_tol=1e-12)


def test_geodesic_angle_is_zero_on_the_diagonal():
    matrices = [m.flatten().tolist() for m in _rotations().as_matrix()]
    df = (
        _frame(a=matrices, b=matrices)
        .select(col("a").cast(MAT3), col("b").cast(MAT3))
        .select(rotation_geodesic_angle(col("a"), col("b")))
    )
    np.testing.assert_allclose(np.asarray(df.to_pydict()["a"]), 0.0, atol=1e-7)


def test_geodesic_angle_recovers_a_known_rotation_angle():
    thetas = [0.0, 0.1, 1.0, math.pi / 2, 3.0]
    rotations = scipy_rotation.from_rotvec([[0.0, 0.0, t] for t in thetas])
    left = [m.flatten().tolist() for m in rotations.as_matrix()]
    right = [np.eye(3).flatten().tolist()] * len(thetas)
    df = (
        _frame(a=left, b=right)
        .select(col("a").cast(MAT3), col("b").cast(MAT3))
        .select(rotation_geodesic_angle(col("a"), col("b")))
    )
    np.testing.assert_allclose(np.asarray(df.to_pydict()["a"]), thetas, atol=1e-7)


# ---------------------------------------------------------------------------
# Component order
# ---------------------------------------------------------------------------


def test_wxyz_order_agrees_with_xyzw_on_the_same_rotation():
    wxyz = [[q[3], q[0], q[1], q[2]] for q in UNIT_QUATS_XYZW]
    a = _quat_col("q", UNIT_QUATS_XYZW).select(quat_to_matrix(col("q"))).to_pydict()["q"]
    b = _quat_col("q", wxyz).select(quat_to_matrix(col("q"), order="wxyz")).to_pydict()["q"]
    np.testing.assert_allclose(
        np.asarray([np.asarray(m) for m in a]), np.asarray([np.asarray(m) for m in b]), atol=1e-12
    )


def test_matrix_to_quat_respects_order():
    matrices = [m.flatten().tolist() for m in _rotations().as_matrix()]
    df = _frame(m=matrices).select(col("m").cast(MAT3))
    xyzw = np.asarray([np.asarray(q) for q in df.select(matrix_to_quat(col("m"))).to_pydict()["m"]])
    wxyz = np.asarray([np.asarray(q) for q in df.select(matrix_to_quat(col("m"), order="wxyz")).to_pydict()["m"]])
    np.testing.assert_allclose(xyzw, wxyz[:, [1, 2, 3, 0]], atol=1e-12)


def test_invalid_order_is_rejected():
    with pytest.raises(Exception, match="xyzw"):
        _quat_col("q", UNIT_QUATS_XYZW).select(quat_inverse(col("q"), order="wxzy")).collect()


# ---------------------------------------------------------------------------
# Nulls, degenerate input, broadcasting, and schema errors
# ---------------------------------------------------------------------------


def test_zero_quaternion_produces_null():
    df = _quat_col("q", [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 1.0]]).select(quat_inverse(col("q")))
    got = df.to_pydict()["q"]
    assert got[0] is None
    assert got[1] is not None


def test_null_row_propagates():
    df = _quat_col("q", [None, [0.0, 0.0, 0.0, 1.0]]).select(quat_to_matrix(col("q")))
    got = df.to_pydict()["q"]
    assert got[0] is None
    assert got[1] is not None


def test_degenerate_rot6d_produces_null():
    rows = [
        [0.0, 0.0, 0.0, 1.0, 0.0, 0.0],  # first vector is zero
        [1.0, 0.0, 0.0, 2.0, 0.0, 0.0],  # parallel vectors
        [1.0, 0.0, 0.0, 0.0, 1.0, 0.0],  # valid
    ]
    got = _frame(r=rows).select(col("r").cast(ROT6D)).select(rot6d_to_matrix(col("r"))).to_pydict()["r"]
    assert got[0] is None
    assert got[1] is None
    assert got[2] is not None


def test_broadcast_against_a_single_row():
    n = 3
    quats = UNIT_QUATS_XYZW[:n]
    df = (
        _frame(q=quats, v=[[1.0, 0.0, 0.0]] * n)
        .select(col("q").cast(QUAT), col("v").cast(VEC3))
        .select(quat_rotate(col("q"), col("v")))
    )
    want = np.asarray([np.asarray(v) for v in df.to_pydict()["q"]])

    broadcast = (
        _frame(q=quats)
        .select(col("q").cast(QUAT))
        .with_column("v", daft.lit([1.0, 0.0, 0.0]).cast(VEC3))
        .select(quat_rotate(col("q"), col("v")))
    )
    got = np.asarray([np.asarray(v) for v in broadcast.to_pydict()["q"]])
    np.testing.assert_allclose(got, want, atol=1e-12)


def test_literal_vector_is_broadcast():
    """A literal arrives as a length-1 series and must broadcast, not truncate."""
    vector = [0.3, -1.7, 2.1]
    df = _quat_col("q", UNIT_QUATS_XYZW).select(quat_rotate(col("q"), lit(vector).cast(VEC3)))
    got = np.asarray([np.asarray(v) for v in df.to_pydict()["q"]])
    assert len(got) == len(UNIT_QUATS_XYZW)
    want = _rotations().apply(np.asarray([vector] * len(UNIT_QUATS_XYZW)))
    np.testing.assert_allclose(got, want, atol=1e-12)


def test_literal_quaternion_is_broadcast():
    quat = [0.5, 0.5, 0.5, 0.5]
    vectors = VECTORS
    df = _frame(v=vectors).select(col("v").cast(VEC3)).select(quat_rotate(lit(quat).cast(QUAT), col("v")).alias("r"))
    got = np.asarray([np.asarray(v) for v in df.to_pydict()["r"]])
    assert len(got) == len(vectors)
    want = scipy_rotation.from_quat(np.asarray([quat] * len(vectors))).apply(np.asarray(vectors))
    np.testing.assert_allclose(got, want, atol=1e-12)


def test_float32_input_is_accepted():
    dtype = DataType.fixed_size_list(DataType.float32(), 4)
    df = _quat_col("q", UNIT_QUATS_XYZW, dtype=dtype).select(quat_to_matrix(col("q")))
    got = np.asarray([np.asarray(m) for m in df.to_pydict()["q"]])
    np.testing.assert_allclose(got, _rotations().as_matrix(), atol=1e-6)


@pytest.mark.parametrize("k", [1, 3, 7, 8])
def test_sliced_input_reads_the_right_rows(k):
    """These expressions index the flat child directly, so a sliced array must stay aligned."""
    df = _quat_col("q", UNIT_QUATS_XYZW).limit(k).select(quat_to_matrix(col("q")))
    got = np.asarray([np.asarray(m) for m in df.to_pydict()["q"]])
    want = scipy_rotation.from_quat(np.asarray(UNIT_QUATS_XYZW[:k])).as_matrix()
    np.testing.assert_allclose(got, want, atol=1e-12)


def test_filtered_input_reads_the_right_rows():
    n = len(UNIT_QUATS_XYZW)
    df = (
        _frame(q=UNIT_QUATS_XYZW, i=list(range(n)))
        .select(col("q").cast(QUAT), col("i"))
        .where(col("i") >= 5)
        .select(quat_to_matrix(col("q")))
    )
    got = np.asarray([np.asarray(m) for m in df.to_pydict()["q"]])
    want = scipy_rotation.from_quat(np.asarray(UNIT_QUATS_XYZW[5:])).as_matrix()
    np.testing.assert_allclose(got, want, atol=1e-12)


@pytest.mark.parametrize(
    "matrix,reason",
    [
        ([1.0, 2, 3, 4, 5, 6, 7, 8, 9], "arbitrary numbers"),
        ([2.0, 0, 0, 0, 2, 0, 0, 0, 2], "a scaled rotation"),
        ([1.0, 0, 0, 0, 1, 0, 0, 0, -1], "a reflection"),
        ([-1.0, 0, 0, 0, -1, 0, 0, 0, -1], "negated identity, determinant -1"),
        ([1.0, 5, 0, 5, 1, 0, 0, 0, 1], "identity plus a trace-free symmetric part, determinant -24"),
        ([1.0, 1, 0, 0, 1, 0, 0, 0, 1], "a volume-preserving shear, determinant +1"),
    ],
)
def test_matrix_to_quat_rejects_matrices_outside_so3(matrix, reason):
    got = _frame(m=[matrix]).select(col("m").cast(MAT3)).select(matrix_to_quat(col("m"))).to_pydict()["m"]
    assert got[0] is None, f"{reason} should not yield a quaternion, got {got[0]}"


@pytest.mark.parametrize(
    "matrix,reason",
    [
        ([1.0, 2, 3, 4, 5, 6, 7, 8, 9], "cosine is 7, far outside [-1, 1]"),
        ([2.0, 0, 0, 0, 2, 0, 0, 0, 2], "a scaled rotation"),
        ([1.0, 5, 0, 5, 1, 0, 0, 0, 1], "determinant -24, yet the cosine lands on exactly 1"),
        ([1.0, 1, 0, 0, 1, 0, 0, 0, 1], "a shear, whose cosine also lands inside [-1, 1]"),
    ],
)
def test_geodesic_angle_rejects_arguments_outside_so3(matrix, reason):
    """The clamp that absorbs rounding must not turn nonsense into a plausible angle.

    The last two cases are the ones a cosine-range check cannot catch: they sit far
    outside SO(3) and still put the cosine inside [-1, 1], where the formula would
    happily report an angle of 0.
    """
    identity = [1.0, 0, 0, 0, 1, 0, 0, 0, 1]
    df = _frame(a=[matrix], b=[identity]).select(col("a").cast(MAT3), col("b").cast(MAT3))
    got = df.select(rotation_geodesic_angle(col("a"), col("b"))).to_pydict()["a"]
    assert got[0] is None, f"{reason} should yield null, got {got[0]}"


def test_geodesic_angle_still_absorbs_rounding_for_real_rotations():
    rotations = scipy_rotation.random(64, random_state=3)
    left = [m.flatten().tolist() for m in rotations.as_matrix()]
    df = _frame(a=left, b=left).select(col("a").cast(MAT3), col("b").cast(MAT3))
    got = df.select(rotation_geodesic_angle(col("a"), col("b"))).to_pydict()["a"]
    assert all(v is not None for v in got), "rounding must not reject genuine rotations"
    np.testing.assert_allclose(np.asarray(got), 0.0, atol=1e-6)


def test_float32_rotations_are_not_rejected_by_the_tolerance():
    """f32 rounding costs about 1e-7, well inside the 1e-6 tolerance."""
    dtype = DataType.tensor(DataType.float32(), (3, 3))
    rotations = scipy_rotation.random(64, random_state=5)
    rows = [m.flatten().tolist() for m in rotations.as_matrix()]
    got = _frame(m=rows).select(col("m").cast(dtype)).select(matrix_to_quat(col("m"))).to_pydict()["m"]
    assert all(q is not None for q in got), "f32 rotation matrices must survive"


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_inputs_produce_null_everywhere(bad):
    """One policy across the module: a row that is not a rotation is null, never NaN."""
    identity_q = [0.0, 0.0, 0.0, 1.0]
    identity_m = [1.0, 0, 0, 0, 1, 0, 0, 0, 1]
    q_bad = [bad, 0.0, 0.0, 1.0]
    m_bad = [bad] + [0.0] * 8

    cases = {
        "quat_inverse": _quat_col("q", [q_bad]).select(quat_inverse(col("q"))),
        "quat_to_matrix": _quat_col("q", [q_bad]).select(quat_to_matrix(col("q"))),
        "matrix_to_quat": _frame(m=[m_bad]).select(col("m").cast(MAT3)).select(matrix_to_quat(col("m"))),
        "quat_multiply": (
            _frame(a=[q_bad], b=[identity_q])
            .select(col("a").cast(QUAT), col("b").cast(QUAT))
            .select(quat_multiply(col("a"), col("b")))
        ),
        "quat_rotate_bad_quat": (
            _frame(q=[q_bad], v=[[1.0, 0.0, 0.0]])
            .select(col("q").cast(QUAT), col("v").cast(VEC3))
            .select(quat_rotate(col("q"), col("v")))
        ),
        "quat_rotate_bad_vector": (
            _frame(q=[identity_q], v=[[bad, 0.0, 0.0]])
            .select(col("q").cast(QUAT), col("v").cast(VEC3))
            .select(quat_rotate(col("q"), col("v")))
        ),
        "rot6d_to_matrix": (
            _frame(r=[[bad, 0.0, 0.0, 0.0, 1.0, 0.0]]).select(col("r").cast(ROT6D)).select(rot6d_to_matrix(col("r")))
        ),
        "rotation_geodesic_angle": (
            _frame(a=[m_bad], b=[identity_m])
            .select(col("a").cast(MAT3), col("b").cast(MAT3))
            .select(rotation_geodesic_angle(col("a"), col("b")))
        ),
    }
    for name, df in cases.items():
        got = next(iter(df.to_pydict().values()))[0]
        assert got is None, f"{name} returned {got!r} for a {bad} input, expected null"


def test_null_component_within_a_row_produces_null_everywhere():
    """A row can be valid while one of its numbers is null.

    The value buffer still holds a number under a null slot, usually a zero, so
    reading the row without its validity mask turns [1, null, 0, 0] into a perfectly
    plausible half turn about x. It is not a rotation, and the answer is null.
    """
    identity_q = [0.0, 0.0, 0.0, 1.0]
    identity_m = [1.0, 0, 0, 0, 1, 0, 0, 0, 1]
    q_bad = [1.0, None, 0.0, 0.0]
    m_bad = [1.0, None] + [0.0] * 7

    cases = {
        "quat_inverse": _quat_col("q", [q_bad]).select(quat_inverse(col("q"))),
        "quat_to_matrix": _quat_col("q", [q_bad]).select(quat_to_matrix(col("q"))),
        "matrix_to_quat": _frame(m=[m_bad]).select(col("m").cast(MAT3)).select(matrix_to_quat(col("m"))),
        "quat_multiply": (
            _frame(a=[q_bad], b=[identity_q])
            .select(col("a").cast(QUAT), col("b").cast(QUAT))
            .select(quat_multiply(col("a"), col("b")))
        ),
        "quat_rotate_bad_quat": (
            _frame(q=[q_bad], v=[[1.0, 0.0, 0.0]])
            .select(col("q").cast(QUAT), col("v").cast(VEC3))
            .select(quat_rotate(col("q"), col("v")))
        ),
        "quat_rotate_bad_vector": (
            _frame(q=[identity_q], v=[[None, 0.0, 0.0]])
            .select(col("q").cast(QUAT), col("v").cast(VEC3))
            .select(quat_rotate(col("q"), col("v")))
        ),
        "rot6d_to_matrix": (
            _frame(r=[[None, 0.0, 0.0, 0.0, 1.0, 0.0]]).select(col("r").cast(ROT6D)).select(rot6d_to_matrix(col("r")))
        ),
        "rotation_geodesic_angle": (
            _frame(a=[m_bad], b=[identity_m])
            .select(col("a").cast(MAT3), col("b").cast(MAT3))
            .select(rotation_geodesic_angle(col("a"), col("b")))
        ),
    }
    for name, df in cases.items():
        got = next(iter(df.to_pydict().values()))[0]
        assert got is None, f"{name} returned {got!r} for a row with a null component, expected null"


def test_null_component_does_not_disturb_its_neighbours():
    """The offending row goes null; the rows either side of it are untouched."""
    rows = [UNIT_QUATS_XYZW[6], [1.0, None, 0.0, 0.0], UNIT_QUATS_XYZW[7]]
    got = _quat_col("q", rows).select(quat_to_matrix(col("q"))).to_pydict()["q"]
    assert got[1] is None
    want = scipy_rotation.from_quat(np.asarray([UNIT_QUATS_XYZW[6], UNIT_QUATS_XYZW[7]])).as_matrix()
    np.testing.assert_allclose(np.asarray([np.asarray(got[0]), np.asarray(got[2])]), want, atol=1e-12)


def test_order_is_case_insensitive():
    upper = _quat_col("q", UNIT_QUATS_XYZW).select(quat_inverse(col("q"), order="XYZW")).to_pydict()["q"]
    lower = _quat_col("q", UNIT_QUATS_XYZW).select(quat_inverse(col("q"), order="xyzw")).to_pydict()["q"]
    np.testing.assert_allclose(np.asarray(upper), np.asarray(lower), atol=0.0)


def test_wrong_length_is_rejected():
    wrong = DataType.fixed_size_list(DataType.float64(), 3)
    with pytest.raises(Exception, match="4 floats"):
        _frame(q=[[1.0, 0.0, 0.0]]).select(col("q").cast(wrong)).select(quat_inverse(col("q"))).collect()


def test_non_list_input_is_rejected():
    with pytest.raises(Exception, match="4 floats"):
        _frame(q=[1.0]).select(quat_inverse(col("q"))).collect()
