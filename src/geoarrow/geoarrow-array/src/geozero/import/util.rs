use geo_traits::CoordTrait;

/// Construct a coordinate from x and y values.
pub(super) fn from_xy(x: f64, y: f64) -> Option<impl CoordTrait<T = f64>> {
    if x.is_finite() && y.is_finite() {
        let coord = wkt::types::Coord {
            x,
            y,
            z: None,
            m: None,
        };
        Some(coord)
    } else {
        None
    }
}

/// Construct a coordinate from x, y, z, and m values.
pub(super) fn from_xyzm(
    x: f64,
    y: f64,
    z: Option<f64>,
    m: Option<f64>,
) -> Option<impl CoordTrait<T = f64>> {
    if let (Some(z), Some(m)) = (z, m) {
        if [x, y, z, m].iter().all(|v| v.is_finite()) {
            Some(wkt::types::Coord {
                x,
                y,
                z: Some(z),
                m: Some(m),
            })
        } else {
            None
        }
    } else if let Some(z) = z {
        if [x, y, z].iter().all(|v| v.is_finite()) {
            Some(wkt::types::Coord {
                x,
                y,
                z: Some(z),
                m: None,
            })
        } else {
            None
        }
    } else if let Some(m) = m {
        if [x, y, m].iter().all(|v| v.is_finite()) {
            Some(wkt::types::Coord {
                x,
                y,
                z: None,
                m: Some(m),
            })
        } else {
            None
        }
    } else if [x, y].iter().all(|v| v.is_finite()) {
        Some(wkt::types::Coord {
            x,
            y,
            z: None,
            m: None,
        })
    } else {
        None
    }
}
