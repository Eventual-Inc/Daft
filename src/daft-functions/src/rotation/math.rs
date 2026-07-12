//! Rotation mathematics on plain slices, free of any Daft types.
//!
//! Quaternions are canonically `(w, x, y, z)` here; the expressions convert on the
//! boundary. Matrices are row-major: `m[3 * i + j]` is row `i`, column `j`.

/// Component order of a quaternion as it is laid out in a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum QuatOrder {
    /// `(x, y, z, w)`, used by ROS, tf2, and `scipy.spatial.transform.Rotation`.
    Xyzw,
    /// `(w, x, y, z)`, used by Eigen constructors, MuJoCo, and Isaac Sim.
    Wxyz,
}

impl QuatOrder {
    pub(super) fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "xyzw" => Some(Self::Xyzw),
            "wxyz" => Some(Self::Wxyz),
            _ => None,
        }
    }

    /// Read a quaternion from a column slice into canonical `(w, x, y, z)`.
    pub(super) fn read(self, s: &[f64]) -> [f64; 4] {
        match self {
            Self::Xyzw => [s[3], s[0], s[1], s[2]],
            Self::Wxyz => [s[0], s[1], s[2], s[3]],
        }
    }

    /// Write a canonical `(w, x, y, z)` quaternion out in this order.
    pub(super) fn write(self, q: [f64; 4]) -> [f64; 4] {
        let [w, x, y, z] = q;
        match self {
            Self::Xyzw => [x, y, z, w],
            Self::Wxyz => [w, x, y, z],
        }
    }
}

fn norm3(v: [f64; 3]) -> f64 {
    dot3(v, v).sqrt()
}

fn dot3(a: [f64; 3], b: [f64; 3]) -> f64 {
    a.iter().zip(&b).map(|(x, y)| x * y).sum()
}

fn cross(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [
        [a[1] * b[2], -(a[2] * b[1])],
        [a[2] * b[0], -(a[0] * b[2])],
        [a[0] * b[1], -(a[1] * b[0])],
    ]
    .map(|terms| terms.iter().sum())
}

fn scale(k: f64, v: [f64; 3]) -> [f64; 3] {
    v.map(|c| k * c)
}

fn add3(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
}

/// Normalize a quaternion. `None` when the norm is zero or non-finite.
pub(super) fn normalize(q: [f64; 4]) -> Option<[f64; 4]> {
    let n = q.iter().map(|c| c * c).sum::<f64>().sqrt();
    (n.is_finite() && n > 0.0).then(|| q.map(|c| c / n))
}

/// Quaternion inverse, `conjugate / |q|^2`. `None` when the norm is zero.
pub(super) fn inverse(q: [f64; 4]) -> Option<[f64; 4]> {
    let n2 = q.iter().map(|c| c * c).sum::<f64>();
    (n2.is_finite() && n2 > 0.0).then(|| [q[0] / n2, -q[1] / n2, -q[2] / n2, -q[3] / n2])
}

/// Hamilton product `a * b`, applying `b` first when acting on vectors.
pub(super) fn multiply(a: [f64; 4], b: [f64; 4]) -> [f64; 4] {
    let [aw, ax, ay, az] = a;
    let [bw, bx, by, bz] = b;
    [
        [aw * bw, -(ax * bx), -(ay * by), -(az * bz)],
        [aw * bx, ax * bw, ay * bz, -(az * by)],
        [aw * by, -(ax * bz), ay * bw, az * bx],
        [aw * bz, ax * by, -(ay * bx), az * bw],
    ]
    .map(|terms| terms.iter().sum())
}

/// Rotate `v` by `q`. The quaternion is normalized first. `None` when `q` is zero.
pub(super) fn rotate(q: [f64; 4], v: [f64; 3]) -> Option<[f64; 3]> {
    let [w, x, y, z] = normalize(q)?;
    let u = [x, y, z];
    // v + 2 * (u x (u x v + w * v)), the rotation of a vector by a unit quaternion.
    let inner = add3(cross(u, v), scale(w, v));
    Some(add3(v, scale(2.0, cross(u, inner))))
}

/// Rotation matrix of a quaternion, row-major. The quaternion is normalized first.
pub(super) fn quat_to_mat(q: [f64; 4]) -> Option<[f64; 9]> {
    let [w, x, y, z] = normalize(q)?;

    let (xx, yy, zz) = (x * x, y * y, z * z);
    let (xy, xz, yz) = (x * y, x * z, y * z);
    let (wx, wy, wz) = (w * x, w * y, w * z);

    // Bind each doubled term before combining, so none of these is an FMA in disguise.
    let diag = [yy + zz, xx + zz, xx + yy].map(|s| {
        let doubled = 2.0 * s;
        1.0 - doubled
    });
    let off = [xy - wz, xz + wy, xy + wz, yz - wx, xz - wy, yz + wx].map(|s| 2.0 * s);

    Some([
        diag[0], off[0], off[1], //
        off[2], diag[1], off[3], //
        off[4], off[5], diag[2],
    ])
}

/// How far a quantity may drift before its input is judged not to be a rotation.
/// `Float64` rounding costs a few ulps; `Float32` inputs, widened on the way in,
/// cost about `1e-7`. More than this means a scaling, a reflection, or noise.
const TOL: f64 = 1e-6;

/// Whether `m` is a rotation: `m^T m = I` with determinant `+1`. Both halves are
/// needed, since orthonormality alone admits reflections and the determinant alone
/// admits any volume-preserving shear.
///
/// Testing instead that Shepperd's method returns a unit quaternion looks
/// equivalent and is not: on its principal branch it reads only the trace and the
/// antisymmetric part, so a trace-free symmetric perturbation is invisible to it.
/// `[[1, 5, 0], [5, 1, 0], [0, 0, 1]]` has determinant `-24` and still yields the
/// identity quaternion, of unit norm.
pub(super) fn is_rotation(m: &[f64]) -> bool {
    let col = |j: usize| [m[j], m[3 + j], m[6 + j]];
    let (c0, c1, c2) = (col(0), col(1), col(2));

    let gram = [
        (dot3(c0, c0), 1.0),
        (dot3(c1, c1), 1.0),
        (dot3(c2, c2), 1.0),
        (dot3(c0, c1), 0.0),
        (dot3(c0, c2), 0.0),
        (dot3(c1, c2), 0.0),
    ];
    if !gram.iter().all(|(got, want)| (got - want).abs() <= TOL) {
        return false;
    }

    // The frame is orthonormal, so its determinant is +-1 and only the handedness
    // is left to check.
    (dot3(cross(c0, c1), c2) - 1.0).abs() <= TOL
}

/// Quaternion of a rotation matrix, via Shepperd's method. `None` when `m` is not
/// a rotation.
///
/// Branching on the largest diagonal term keeps the divisor away from zero, which
/// the naive trace formula does not do for rotations near a half turn.
pub(super) fn mat_to_quat(m: [f64; 9]) -> Option<[f64; 4]> {
    is_rotation(&m).then(|| shepperd(m))
}

fn shepperd(m: [f64; 9]) -> [f64; 4] {
    let (m00, m01, m02) = (m[0], m[1], m[2]);
    let (m10, m11, m12) = (m[3], m[4], m[5]);
    let (m20, m21, m22) = (m[6], m[7], m[8]);
    let trace = m00 + m11 + m22;

    if trace > 0.0 {
        let s = (trace + 1.0).sqrt() * 2.0;
        [0.25 * s, (m21 - m12) / s, (m02 - m20) / s, (m10 - m01) / s]
    } else if m00 > m11 && m00 > m22 {
        let s = (1.0 + m00 - m11 - m22).sqrt() * 2.0;
        [(m21 - m12) / s, 0.25 * s, (m01 + m10) / s, (m02 + m20) / s]
    } else if m11 > m22 {
        let s = (1.0 + m11 - m00 - m22).sqrt() * 2.0;
        [(m02 - m20) / s, (m01 + m10) / s, 0.25 * s, (m12 + m21) / s]
    } else {
        let s = (1.0 + m22 - m00 - m11).sqrt() * 2.0;
        [(m10 - m01) / s, (m02 + m20) / s, (m12 + m21) / s, 0.25 * s]
    }
}

/// Rotation matrix from the 6D continuous representation of Zhou et al. (2019).
///
/// The two input vectors are the first two columns, up to Gram-Schmidt; the third
/// is their cross product, so the result always has determinant `+1`. `None` when
/// they are parallel or either is zero, which determines no rotation.
pub(super) fn rot6d_to_mat(r: &[f64]) -> Option<[f64; 9]> {
    let a1 = [r[0], r[1], r[2]];
    let a2 = [r[3], r[4], r[5]];

    let n1 = norm3(a1);
    if !(n1.is_finite() && n1 > 0.0) {
        return None;
    }
    let c1 = a1.map(|c| c / n1);

    let proj = dot3(c1, a2);
    let u = [
        proj.mul_add(-c1[0], a2[0]),
        proj.mul_add(-c1[1], a2[1]),
        proj.mul_add(-c1[2], a2[2]),
    ];
    let n2 = norm3(u);
    if !(n2.is_finite() && n2 > 0.0) {
        return None;
    }
    let c2 = u.map(|c| c / n2);
    let c3 = cross(c1, c2);

    // Columns c1, c2, c3 laid out row-major.
    Some([
        c1[0], c2[0], c3[0], //
        c1[1], c2[1], c3[1], //
        c1[2], c2[2], c3[2],
    ])
}

/// Geodesic distance between two rotations, in radians on `[0, pi]`:
/// `arccos((tr(A B^T) - 1) / 2)`, the angle of the relative rotation `A B^T`.
///
/// The angle is only defined between rotations, so an argument outside `SO(3)`
/// gives `None`. Range-checking the cosine would not be enough: a non-rotation can
/// land it inside `[-1, 1]`, where the formula reports a plausible angle. Rounding
/// can push it a hair outside for genuine rotations, which the clamp absorbs.
pub(super) fn geodesic_angle(a: &[f64], b: &[f64]) -> Option<f64> {
    if !(is_rotation(a) && is_rotation(b)) {
        return None;
    }
    // tr(A B^T) is the elementwise inner product of A and B.
    let trace: f64 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let cos = 0.5 * (trace - 1.0);
    Some(cos.clamp(-1.0, 1.0).acos())
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-12;

    fn approx(a: f64, b: f64, eps: f64) -> bool {
        (a - b).abs() < eps
    }

    // Determinant by cofactor expansion along the first row.
    fn det3(m: [f64; 9]) -> f64 {
        let cofactors = [
            [m[4] * m[8], -(m[5] * m[7])],
            [m[3] * m[8], -(m[5] * m[6])],
            [m[3] * m[7], -(m[4] * m[6])],
        ]
        .map(|terms| terms.iter().sum::<f64>());
        [
            m[0] * cofactors[0],
            -(m[1] * cofactors[1]),
            m[2] * cofactors[2],
        ]
        .iter()
        .sum()
    }

    // Identity, half turns (trace = -1, the hard case), small angles, generic ones.
    fn sample_quats() -> Vec<[f64; 4]> {
        vec![
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0],
            [0.0, 0.0, 0.0, 1.0],
            [0.5, 0.5, 0.5, 0.5],
            [0.9999999, 1e-7, 0.0, 0.0],
            [0.3, -0.2, 0.8, 0.1],
            [-0.6, 0.1, -0.3, 0.7],
        ]
        .into_iter()
        .map(|q| normalize(q).unwrap())
        .collect()
    }

    #[test]
    fn normalize_yields_unit_norm() {
        for q in sample_quats() {
            let n = q.iter().map(|c| c * c).sum::<f64>().sqrt();
            assert!(approx(n, 1.0, EPS), "norm {n} for {q:?}");
        }
    }

    #[test]
    fn normalize_rejects_zero() {
        assert!(normalize([0.0; 4]).is_none());
        assert!(inverse([0.0; 4]).is_none());
    }

    #[test]
    fn multiply_by_inverse_is_identity() {
        for q in sample_quats() {
            let r = multiply(q, inverse(q).unwrap());
            assert!(approx(r[0].abs(), 1.0, 1e-9), "{r:?}");
            for c in &r[1..] {
                assert!(approx(*c, 0.0, 1e-9), "{r:?}");
            }
        }
    }

    #[test]
    fn multiplication_is_associative() {
        let qs = sample_quats();
        for a in &qs {
            for b in &qs {
                for c in &qs {
                    let lhs = multiply(multiply(*a, *b), *c);
                    let rhs = multiply(*a, multiply(*b, *c));
                    for (l, r) in lhs.iter().zip(&rhs) {
                        assert!(approx(*l, *r, 1e-12), "{lhs:?} vs {rhs:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn rotation_preserves_norm() {
        let v = [0.3, -1.7, 2.1];
        let n = norm3(v);
        for q in sample_quats() {
            let r = rotate(q, v).unwrap();
            assert!(approx(norm3(r), n, 1e-12), "{r:?}");
        }
    }

    #[test]
    fn double_cover_q_and_negative_q_agree() {
        let v = [0.3, -1.7, 2.1];
        for q in sample_quats() {
            let a = rotate(q, v).unwrap();
            let b = rotate(q.map(|c| -c), v).unwrap();
            for (x, y) in a.iter().zip(&b) {
                assert!(approx(*x, *y, 1e-12), "{a:?} vs {b:?}");
            }
        }
    }

    #[test]
    fn quat_to_mat_round_trips_up_to_sign() {
        for q in sample_quats() {
            let back = normalize(mat_to_quat(quat_to_mat(q).unwrap()).unwrap()).unwrap();
            // q and -q are the same rotation, so compare up to a global sign.
            let same = q.iter().zip(&back).all(|(a, b)| approx(*a, *b, 1e-9));
            let flipped = q.iter().zip(&back).all(|(a, b)| approx(*a, -*b, 1e-9));
            assert!(same || flipped, "{q:?} -> {back:?}");
        }
    }

    #[test]
    fn mat_to_quat_is_stable_near_half_turn() {
        // Trace is -1 here, which is exactly where the naive formula divides by zero.
        for q in [
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0],
            [0.0, 0.0, 0.0, 1.0],
        ] {
            let m = quat_to_mat(q).unwrap();
            let back = normalize(mat_to_quat(m).unwrap()).unwrap();
            assert!(back.iter().all(|c| c.is_finite()), "{back:?}");
            let same = q.iter().zip(&back).all(|(a, b)| approx(*a, *b, 1e-9));
            let flipped = q.iter().zip(&back).all(|(a, b)| approx(*a, -*b, 1e-9));
            assert!(same || flipped, "{q:?} -> {back:?}");
        }
    }

    #[test]
    fn rotation_matrices_are_special_orthogonal() {
        for q in sample_quats() {
            let m = quat_to_mat(q).unwrap();
            // Columns are orthonormal.
            for j in 0..3 {
                let col = [m[j], m[3 + j], m[6 + j]];
                assert!(approx(norm3(col), 1.0, 1e-12));
            }
            // Determinant is +1.
            let det = det3(m);
            assert!(approx(det, 1.0, 1e-12), "det {det}");
        }
    }

    #[test]
    fn rot6d_orthonormalizes_and_matches_columns() {
        // Deliberately non-orthogonal, non-unit inputs.
        let r = [2.0, 0.0, 0.0, 1.0, 3.0, 0.0];
        let m = rot6d_to_mat(&r).unwrap();
        // First column is the normalized first input vector.
        assert!(approx(m[0], 1.0, EPS) && approx(m[3], 0.0, EPS) && approx(m[6], 0.0, EPS));
        // Second column is orthogonal to the first and unit length.
        let c1 = [m[0], m[3], m[6]];
        let c2 = [m[1], m[4], m[7]];
        assert!(approx(dot3(c1, c2), 0.0, 1e-12));
        assert!(approx(norm3(c2), 1.0, 1e-12));
        // Third column is the cross product, so the determinant is +1.
        let det = det3(m);
        assert!(approx(det, 1.0, 1e-12), "det {det}");
    }

    #[test]
    fn rot6d_rejects_degenerate_inputs() {
        assert!(rot6d_to_mat(&[0.0, 0.0, 0.0, 1.0, 0.0, 0.0]).is_none());
        // Parallel inputs leave the second column undetermined.
        assert!(rot6d_to_mat(&[1.0, 0.0, 0.0, 2.0, 0.0, 0.0]).is_none());
    }

    #[test]
    fn geodesic_angle_is_zero_on_the_diagonal_and_symmetric() {
        for q in sample_quats() {
            let m = quat_to_mat(q).unwrap();
            assert!(approx(geodesic_angle(&m, &m).unwrap(), 0.0, 1e-7));
        }
        let a = quat_to_mat(sample_quats()[4]).unwrap();
        let b = quat_to_mat(sample_quats()[6]).unwrap();
        assert!(approx(
            geodesic_angle(&a, &b).unwrap(),
            geodesic_angle(&b, &a).unwrap(),
            1e-12
        ));
    }

    #[test]
    fn geodesic_angle_recovers_a_known_rotation_angle() {
        // A rotation of theta about z, compared against the identity.
        for theta in [0.0, 0.1, 1.0, std::f64::consts::FRAC_PI_2, 3.0] {
            let (s, c) = (theta / 2.0).sin_cos();
            let m = quat_to_mat([c, 0.0, 0.0, s]).unwrap();
            let identity = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
            assert!(
                approx(geodesic_angle(&m, &identity).unwrap(), theta, 1e-7),
                "theta {theta}"
            );
        }
    }

    #[test]
    fn geodesic_angle_is_bounded_by_pi_under_numerical_noise() {
        // trace slightly above 3 must not produce NaN from acos.
        let m = [1.0 + 1e-15, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        let identity = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        assert!(geodesic_angle(&m, &identity).unwrap().is_finite());
    }

    #[test]
    fn quat_order_round_trips() {
        let canonical = [0.1, 0.2, 0.3, 0.4];
        for order in [QuatOrder::Xyzw, QuatOrder::Wxyz] {
            assert_eq!(order.read(&order.write(canonical)), canonical);
        }
        // xyzw places the scalar part last.
        assert_eq!(QuatOrder::Xyzw.write(canonical), [0.2, 0.3, 0.4, 0.1]);
        assert_eq!(QuatOrder::Wxyz.write(canonical), canonical);
    }

    #[test]
    fn quat_order_parse() {
        assert_eq!(QuatOrder::parse("XYZW"), Some(QuatOrder::Xyzw));
        assert_eq!(QuatOrder::parse("wxyz"), Some(QuatOrder::Wxyz));
        assert_eq!(QuatOrder::parse("wxzy"), None);
    }

    #[test]
    fn mat_to_quat_rejects_matrices_outside_so3() {
        let identity = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        // Arbitrary numbers. Shepperd's method used to return finite nonsense here.
        assert!(mat_to_quat([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]).is_none());
        // A uniform scaling of a rotation is not a rotation.
        assert!(mat_to_quat(identity.map(|c| 2.0 * c)).is_none());
        // A reflection has determinant -1.
        assert!(mat_to_quat([1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, -1.0]).is_none());
        assert!(mat_to_quat(identity.map(|c| -c)).is_none());
        // Genuine rotations still pass.
        for q in sample_quats() {
            assert!(mat_to_quat(quat_to_mat(q).unwrap()).is_some());
        }
    }

    #[test]
    fn so3_test_sees_trace_free_symmetric_perturbations() {
        // Determinant -24, and shepperd maps it to the identity quaternion.
        let shear = [1.0, 5.0, 0.0, 5.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        assert!(approx(det3(shear), -24.0, 1e-9));
        let q = shepperd(shear);
        assert!(
            approx(q.iter().map(|c| c * c).sum::<f64>(), 1.0, 1e-12),
            "the norm test passes on this non-rotation: {q:?}"
        );

        assert!(!is_rotation(&shear));
        assert!(mat_to_quat(shear).is_none());

        let identity = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        assert!(geodesic_angle(&shear, &identity).is_none());
        assert!(geodesic_angle(&identity, &shear).is_none());
    }

    #[test]
    fn so3_test_accepts_rotations_and_rejects_the_usual_impostors() {
        let identity = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        for q in sample_quats() {
            let m = quat_to_mat(q).unwrap();
            assert!(is_rotation(&m), "{m:?}");
            // Single precision inputs are widened on the way in, and must survive.
            assert!(is_rotation(&m.map(|c| f64::from(c as f32))), "{m:?}");
        }
        // Orthonormal but left handed: the determinant is the only thing that says so.
        assert!(!is_rotation(&[
            1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, -1.0
        ]));
        // Determinant +1 but not orthonormal: a volume-preserving shear.
        let shear = [1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        assert!(approx(det3(shear), 1.0, 1e-12));
        assert!(!is_rotation(&shear));
        // Scalings, in both directions.
        assert!(!is_rotation(&identity.map(|c| 2.0 * c)));
        assert!(!is_rotation(&identity.map(|c| 0.5 * c)));
        assert!(!is_rotation(&[0.0; 9]));
    }

    #[test]
    fn mat_to_quat_tolerates_single_precision_rounding() {
        // Widen a rotation matrix that has been through f32, as the expressions do.
        for q in sample_quats() {
            let m = quat_to_mat(q).unwrap();
            let rounded = m.map(|c| f64::from(c as f32));
            assert!(mat_to_quat(rounded).is_some(), "{rounded:?}");
        }
    }

    #[test]
    fn geodesic_angle_rejects_arguments_outside_so3() {
        let identity = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        // trace(A B^T) = 15, so the cosine is 7: the clamp used to turn this into 0.
        assert!(
            geodesic_angle(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0], &identity).is_none()
        );
        // A scaled rotation.
        let scaled: Vec<f64> = identity.iter().map(|c| 2.0 * c).collect();
        assert!(geodesic_angle(&scaled, &identity).is_none());
    }

    #[test]
    fn geodesic_angle_rejects_non_finite_arguments() {
        let identity = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        let mut nan = identity;
        nan[0] = f64::NAN;
        assert!(geodesic_angle(&nan, &identity).is_none());
        let mut inf = identity;
        inf[4] = f64::INFINITY;
        assert!(geodesic_angle(&inf, &identity).is_none());
    }

    #[test]
    fn geodesic_angle_absorbs_rounding_at_the_endpoints() {
        // Nearly identical rotations: the cosine drifts just past 1 and must clamp.
        let identity = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        let nudged = identity.map(|c| c * (1.0 + 1e-13));
        let angle = geodesic_angle(&nudged, &identity).expect("rounding must not reject");
        assert!(angle.is_finite() && angle < 1e-5, "{angle}");
        // A half turn: the cosine drifts just past -1.
        let flip = quat_to_mat([0.0, 1.0, 0.0, 0.0]).unwrap();
        let angle = geodesic_angle(&flip, &identity).unwrap();
        assert!(approx(angle, std::f64::consts::PI, 1e-7), "{angle}");
    }
}
