/// Minimum bounding rectangle: `[min_x, min_y, max_x, max_y]`.
pub type Mbr = [f64; 4];

// ── Low-level WKB byte helpers ────────────────────────────────────────────

#[inline]
fn read_u32(data: &[u8], offset: usize, le: bool) -> Option<u32> {
    let b: [u8; 4] = data.get(offset..offset + 4)?.try_into().ok()?;
    Some(if le {
        u32::from_le_bytes(b)
    } else {
        u32::from_be_bytes(b)
    })
}

#[inline]
fn read_f64(data: &[u8], offset: usize, le: bool) -> Option<f64> {
    let b: [u8; 8] = data.get(offset..offset + 8)?.try_into().ok()?;
    Some(if le {
        f64::from_le_bytes(b)
    } else {
        f64::from_be_bytes(b)
    })
}

/// Scan all (x, y) coordinate pairs in a WKB geometry starting at `pos`,
/// updating the running min/max.  Returns the byte offset just past the
/// geometry.  Handles:
///   - Standard ISO WKB 2D (types 1-7)
///   - ISO WKB Z/M/ZM (types 1001-1007 / 2001-2007 / 3001-3007)
///   - EWKB (PostGIS) with SRID/Z/M flags
///   - Multi-geometries and GeometryCollections (recursive)
fn scan_wkb_mbr(
    data: &[u8],
    pos: usize,
    min_x: &mut f64,
    min_y: &mut f64,
    max_x: &mut f64,
    max_y: &mut f64,
) -> Option<usize> {
    let byte_order = *data.get(pos)?;
    let le = byte_order == 1;

    let raw_type = read_u32(data, pos + 1, le)?;
    let mut offset = pos + 5;

    // EWKB flags (PostGIS extension)
    let ewkb_has_srid = (raw_type & 0x20000000) != 0;
    let ewkb_has_z = (raw_type & 0x80000000) != 0;
    let ewkb_has_m = (raw_type & 0x40000000) != 0;

    // Strip all flag bits to get the base ISO type (1–7)
    let base_type_raw = raw_type & 0x0FFF_FFFF;

    let (base_type, has_z, has_m) = if base_type_raw >= 3000 {
        (base_type_raw - 3000, true, true)
    } else if base_type_raw >= 2000 {
        (base_type_raw - 2000, false, true)
    } else if base_type_raw >= 1000 {
        (base_type_raw - 1000, true, false)
    } else {
        (base_type_raw, ewkb_has_z, ewkb_has_m)
    };

    // Skip SRID if present (4-byte integer)
    if ewkb_has_srid {
        offset += 4;
    }

    // Coordinate stride in bytes (x + y, plus optional z/m)
    let coord_stride = (2 + usize::from(has_z) + usize::from(has_m)) * 8;

    /// Inline helper: read a coordinate sequence and update the MBR.
    #[inline]
    fn scan_coords(
        data: &[u8],
        mut off: usize,
        n: usize,
        le: bool,
        stride: usize,
        min_x: &mut f64,
        min_y: &mut f64,
        max_x: &mut f64,
        max_y: &mut f64,
    ) -> Option<usize> {
        for _ in 0..n {
            let x = read_f64(data, off, le)?;
            let y = read_f64(data, off + 8, le)?;
            if x.is_finite() && y.is_finite() {
                if x < *min_x {
                    *min_x = x;
                }
                if y < *min_y {
                    *min_y = y;
                }
                if x > *max_x {
                    *max_x = x;
                }
                if y > *max_y {
                    *max_y = y;
                }
            }
            off += stride;
        }
        Some(off)
    }

    match base_type {
        1 => {
            // Point
            offset = scan_coords(
                data,
                offset,
                1,
                le,
                coord_stride,
                min_x,
                min_y,
                max_x,
                max_y,
            )?;
        }
        2 => {
            // LineString
            let n = read_u32(data, offset, le)? as usize;
            offset += 4;
            offset = scan_coords(
                data,
                offset,
                n,
                le,
                coord_stride,
                min_x,
                min_y,
                max_x,
                max_y,
            )?;
        }
        3 => {
            // Polygon
            let n_rings = read_u32(data, offset, le)? as usize;
            offset += 4;
            for _ in 0..n_rings {
                let n_pts = read_u32(data, offset, le)? as usize;
                offset += 4;
                offset = scan_coords(
                    data,
                    offset,
                    n_pts,
                    le,
                    coord_stride,
                    min_x,
                    min_y,
                    max_x,
                    max_y,
                )?;
            }
        }
        4 | 5 | 6 | 7 => {
            // Multi* / GeometryCollection — each sub-geometry is a full WKB
            let n_geoms = read_u32(data, offset, le)? as usize;
            offset += 4;
            for _ in 0..n_geoms {
                offset = scan_wkb_mbr(data, offset, min_x, min_y, max_x, max_y)?;
            }
        }
        _ => return None,
    }

    Some(offset)
}

/// Extract the MBR from a WKB geometry.
///
/// Returns `None` if the WKB is invalid or represents an empty geometry.
///
/// This scans the raw coordinate bytes directly — no `geo::Geometry` allocation.
pub fn wkb_to_mbr(wkb: &[u8]) -> Option<Mbr> {
    let mut min_x = f64::INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut max_y = f64::NEG_INFINITY;

    scan_wkb_mbr(wkb, 0, &mut min_x, &mut min_y, &mut max_x, &mut max_y)?;

    if min_x.is_infinite() {
        return None; // empty geometry
    }
    Some([min_x, min_y, max_x, max_y])
}

/// Return `true` when the two MBRs intersect (edges touching counts).
#[inline]
pub fn mbrs_intersect(a: &Mbr, b: &Mbr) -> bool {
    a[0] <= b[2] && b[0] <= a[2] && a[1] <= b[3] && b[1] <= a[3]
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn point_wkb(x: f64, y: f64) -> Vec<u8> {
        let mut buf = vec![];
        buf.write_all(&[1u8]).unwrap(); // little-endian
        buf.write_all(&1u32.to_le_bytes()).unwrap(); // Point
        buf.write_all(&x.to_le_bytes()).unwrap();
        buf.write_all(&y.to_le_bytes()).unwrap();
        buf
    }

    /// Build a WKB Polygon for a closed box (x0,y0)–(x1,y1).
    fn polygon_box_wkb(x0: f64, y0: f64, x1: f64, y1: f64) -> Vec<u8> {
        let mut buf = vec![];
        buf.write_all(&[1u8]).unwrap(); // little-endian
        buf.write_all(&3u32.to_le_bytes()).unwrap(); // Polygon
        buf.write_all(&1u32.to_le_bytes()).unwrap(); // 1 ring
        buf.write_all(&5u32.to_le_bytes()).unwrap(); // 5 points (closed)
        for (x, y) in [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)] {
            buf.write_all(&x.to_le_bytes()).unwrap();
            buf.write_all(&y.to_le_bytes()).unwrap();
        }
        buf
    }

    #[test]
    fn test_point_mbr() {
        let mbr = wkb_to_mbr(&point_wkb(3.0, 7.0)).unwrap();
        assert_eq!(mbr, [3.0, 7.0, 3.0, 7.0]);
    }

    #[test]
    fn test_polygon_mbr() {
        let mbr = wkb_to_mbr(&polygon_box_wkb(-1.0, -2.0, 5.0, 3.0)).unwrap();
        assert_eq!(mbr, [-1.0, -2.0, 5.0, 3.0]);
    }

    #[test]
    fn test_empty_returns_none() {
        assert!(wkb_to_mbr(&[]).is_none());
        assert!(wkb_to_mbr(&[0xFF; 3]).is_none());
    }

    #[test]
    fn test_intersect() {
        assert!(mbrs_intersect(&[0.0, 0.0, 2.0, 2.0], &[1.0, 1.0, 3.0, 3.0]));
        assert!(!mbrs_intersect(
            &[0.0, 0.0, 1.0, 1.0],
            &[2.0, 2.0, 3.0, 3.0]
        ));
        // touching edge
        assert!(mbrs_intersect(&[0.0, 0.0, 1.0, 1.0], &[1.0, 1.0, 2.0, 2.0]));
    }
}
