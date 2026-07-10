//! Rotation mathematics on plain slices, free of any Daft types.
//!
//! Quaternions are canonically `(w, x, y, z)` inside this module. The public
//! expressions accept either component order and convert on the boundary.
//!
//! Rotation matrices are row-major: `m[3 * i + j]` is row `i`, column `j`.

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
///
/// Each row below is one component of the product, written as the four terms
/// that sum to it.
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

    // Bind each doubled term before combining, so no expression is a fused
    // multiply-add in disguise.
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

/// Quaternion of a rotation matrix, via Shepperd's method.
///
/// Branching on the largest diagonal term keeps the divisor away from zero, which
/// the naive trace formula does not do for rotations near a half turn.
pub(super) fn mat_to_quat(m: [f64; 9]) -> [f64; 4] {
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
/// The two input vectors are the first two columns of the matrix, up to
/// Gram-Schmidt orthonormalization. The third column is their cross product, so
/// the result always has determinant `+1`. `None` when the inputs are parallel
/// or either is zero, in which case no rotation is determined.
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

/// Geodesic (angular) distance between two rotations, in radians on `[0, pi]`.
///
/// `arccos((tr(A B^T) - 1) / 2)`, the angle of the relative rotation `A B^T`.
pub(super) fn geodesic_angle(a: &[f64], b: &[f64]) -> f64 {
    // tr(A B^T) is the elementwise inner product of A and B.
    let trace: f64 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    (0.5 * (trace - 1.0)).clamp(-1.0, 1.0).acos()
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-12;

    fn approx(a: f64, b: f64, eps: f64) -> bool {
        (a - b).abs() < eps
    }

    /// Determinant by cofactor expansion along the first row.
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

    /// A few rotations spanning identity, small angles, half turns, and generic ones.
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
            let back = normalize(mat_to_quat(quat_to_mat(q).unwrap())).unwrap();
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
            let back = normalize(mat_to_quat(m)).unwrap();
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
            assert!(approx(geodesic_angle(&m, &m), 0.0, 1e-7));
        }
        let a = quat_to_mat(sample_quats()[4]).unwrap();
        let b = quat_to_mat(sample_quats()[6]).unwrap();
        assert!(approx(
            geodesic_angle(&a, &b),
            geodesic_angle(&b, &a),
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
                approx(geodesic_angle(&m, &identity), theta, 1e-7),
                "theta {theta}"
            );
        }
    }

    #[test]
    fn geodesic_angle_is_bounded_by_pi_under_numerical_noise() {
        // trace slightly above 3 must not produce NaN from acos.
        let m = [1.0 + 1e-15, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        let identity = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0];
        assert!(geodesic_angle(&m, &identity).is_finite());
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
}
