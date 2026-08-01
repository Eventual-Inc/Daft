//! Flatbush-style packed static R-tree.
//!
//! Replaces `rstar` for the GLOBAL spatial-join build side. `rstar`'s
//! `bulk_load` is strictly single-threaded (recursive STR partitioning —
//! measured at ~20s for 11.3M entries, >2/3 of a full join's wall time). This
//! tree builds from one rayon `par_sort` over hilbert-curve keys plus a
//! bottom-up level packing pass, and answers the same edge-INCLUSIVE bbox
//! intersection queries.
//!
//! Layout (classic flatbush): `boxes` stores every node level by level —
//! level 0 = the items in hilbert order, then their parents, ..., up to a top
//! level of at most `NODE_SIZE` nodes. A node at position `pos` of level `L`
//! has children at positions
//! `level_starts[L-1] + (pos - level_starts[L]) * NODE_SIZE ..` in level L-1.
//! `row_idx[i]` maps leaf position `i` back to the caller's u32 item id.

use rayon::prelude::*;

const NODE_SIZE: usize = 16;
/// Hilbert grid resolution per axis (flatbush uses the same 16-bit grid).
const HILBERT_ORDER: u32 = 16;

/// Map bbox-center coordinates (scaled to the 16-bit hilbert grid) to their
/// hilbert-curve distance. Classic iterative rotate-and-accumulate; `d` spans
/// the full u32 range at order 16 (65536² cells).
fn hilbert_d(x: u32, y: u32) -> u32 {
    let n: u32 = 1 << HILBERT_ORDER;
    let (mut x, mut y) = (x, y);
    let mut d: u32 = 0;
    let mut s: u32 = n >> 1;
    while s > 0 {
        let rx = u32::from(x & s > 0);
        let ry = u32::from(y & s > 0);
        d = d.wrapping_add(s.wrapping_mul(s).wrapping_mul((3 * rx) ^ ry));
        // Rotate the quadrant so the curve stays continuous.
        if ry == 0 {
            if rx == 1 {
                x = s.wrapping_sub(1).wrapping_sub(x) & (n - 1);
                y = s.wrapping_sub(1).wrapping_sub(y) & (n - 1);
            }
            std::mem::swap(&mut x, &mut y);
        }
        s >>= 1;
    }
    d
}

#[inline]
fn intersects(a: &[f64; 4], b: &[f64; 4]) -> bool {
    // Edge-inclusive, same as rstar's AABB intersection this tree replaces.
    a[0] <= b[2] && a[2] >= b[0] && a[1] <= b[3] && a[3] >= b[1]
}

pub struct PackedRTree {
    /// All node bboxes, level by level (level 0 first). Empty tree = empty.
    boxes: Vec<[f64; 4]>,
    /// Item ids in leaf (hilbert) order; parallel to `boxes[0..n_items]`.
    row_idx: Vec<u32>,
    /// Start offset of each level in `boxes`; `level_starts[0] == 0`.
    level_starts: Vec<usize>,
    n_items: usize,
}

impl PackedRTree {
    /// Build from `(bbox [min_x, min_y, max_x, max_y], item_id)` pairs.
    ///
    /// Non-finite boxes are kept (they simply sort to a fixed key and still
    /// answer intersection queries correctly) — filtering is the caller's
    /// policy, not the tree's.
    pub fn build(items: Vec<([f64; 4], u32)>) -> Self {
        let n_items = items.len();
        if n_items == 0 {
            return Self {
                boxes: vec![],
                row_idx: vec![],
                level_starts: vec![0],
                n_items: 0,
            };
        }

        // World bounds over finite boxes, for scaling centers to the hilbert grid.
        let world = items
            .par_iter()
            .map(|(b, _)| *b)
            .reduce(
                || [f64::INFINITY, f64::INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY],
                |a, b| {
                    [
                        a[0].min(b[0]),
                        a[1].min(b[1]),
                        a[2].max(b[2]),
                        a[3].max(b[3]),
                    ]
                },
            );
        let hilbert_max = ((1u32 << HILBERT_ORDER) - 1) as f64;
        let w = (world[2] - world[0]).max(0.0);
        let h = (world[3] - world[1]).max(0.0);
        let sx = if w > 0.0 && w.is_finite() { hilbert_max / w } else { 0.0 };
        let sy = if h > 0.0 && h.is_finite() { hilbert_max / h } else { 0.0 };

        let key = |b: &[f64; 4]| -> u32 {
            let cx = (b[0] + b[2]) * 0.5 - world[0];
            let cy = (b[1] + b[3]) * 0.5 - world[1];
            let gx = if cx.is_finite() { (cx * sx).clamp(0.0, hilbert_max) as u32 } else { 0 };
            let gy = if cy.is_finite() { (cy * sy).clamp(0.0, hilbert_max) as u32 } else { 0 };
            hilbert_d(gx, gy)
        };
        // Compute each hilbert key EXACTLY ONCE, packed with the item's
        // position as `key << 32 | pos`, and sort those u64s. Sorting with
        // `par_sort_unstable_by_key(|it| key(it))` instead re-evaluates the
        // 16-round hilbert function on every comparison — measured at ~110s
        // of CPU for 11.3M items vs ~1s for this precomputed form.
        // (n <= u32::MAX is guaranteed by the caller's row-index width.)
        let mut keyed: Vec<u64> = items
            .par_iter()
            .enumerate()
            .map(|(pos, (b, _))| (u64::from(key(b)) << 32) | pos as u64)
            .collect();
        keyed.par_sort_unstable();

        // Level sizes: n, ceil(n/16), ... down to <= NODE_SIZE top nodes.
        let mut level_starts = vec![0usize];
        let mut level_len = n_items;
        let mut total = n_items;
        while level_len > NODE_SIZE {
            level_len = level_len.div_ceil(NODE_SIZE);
            level_starts.push(total);
            total += level_len;
        }

        // Gather items into hilbert order (single parallel pass).
        let mut boxes: Vec<[f64; 4]> = Vec::with_capacity(total);
        let mut row_idx: Vec<u32> = Vec::with_capacity(n_items);
        keyed
            .par_iter()
            .map(|k| items[(k & 0xFFFF_FFFF) as usize].0)
            .collect_into_vec(&mut boxes);
        keyed
            .par_iter()
            .map(|k| items[(k & 0xFFFF_FFFF) as usize].1)
            .collect_into_vec(&mut row_idx);
        drop(keyed);
        drop(items);
        boxes.reserve_exact(total - n_items);

        // Pack upper levels bottom-up: each parent = union of its children.
        for level in 1..level_starts.len() {
            let (child_start, child_end) = (level_starts[level - 1], level_starts[level]);
            let mut pos = child_start;
            while pos < child_end {
                let end = (pos + NODE_SIZE).min(child_end);
                let mut node = boxes[pos];
                for child in &boxes[pos + 1..end] {
                    node[0] = node[0].min(child[0]);
                    node[1] = node[1].min(child[1]);
                    node[2] = node[2].max(child[2]);
                    node[3] = node[3].max(child[3]);
                }
                boxes.push(node);
                pos = end;
            }
        }
        debug_assert_eq!(boxes.len(), total);

        Self {
            boxes,
            row_idx,
            level_starts,
            n_items,
        }
    }

    /// Invoke `f` with the item id of every box intersecting `q`
    /// (edge-inclusive). This is the probe hot path — no allocation beyond
    /// the traversal stack.
    pub fn for_each_intersecting(&self, q: &[f64; 4], mut f: impl FnMut(u32)) {
        if self.n_items == 0 {
            return;
        }
        let top = self.level_starts.len() - 1;
        // (level, start_pos, end_pos) node ranges still to scan.
        let mut stack: Vec<(usize, usize, usize)> = Vec::with_capacity(self.level_starts.len() * 2);
        stack.push((top, self.level_starts[top], self.boxes.len()));
        while let Some((level, start, end)) = stack.pop() {
            for pos in start..end {
                if !intersects(&self.boxes[pos], q) {
                    continue;
                }
                if level == 0 {
                    f(self.row_idx[pos]);
                } else {
                    let child_level_start = self.level_starts[level - 1];
                    let child_level_end = self.level_starts[level];
                    let ordinal = pos - self.level_starts[level];
                    let child_start = child_level_start + ordinal * NODE_SIZE;
                    let child_end = (child_start + NODE_SIZE).min(child_level_end);
                    stack.push((level - 1, child_start, child_end));
                }
            }
        }
    }

    /// Collecting convenience for tests and small callers.
    pub fn search(&self, q: &[f64; 4]) -> Vec<u32> {
        let mut out = Vec::new();
        self.for_each_intersecting(q, |i| out.push(i));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic LCG so tests need no rand dependency (and no clock).
    struct Lcg(u64);
    impl Lcg {
        fn next_f64(&mut self, lo: f64, hi: f64) -> f64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let unit = ((self.0 >> 11) as f64) / ((1u64 << 53) as f64);
            lo + unit * (hi - lo)
        }
    }

    fn random_items(n: usize, seed: u64) -> Vec<([f64; 4], u32)> {
        let mut rng = Lcg(seed);
        (0..n as u32)
            .map(|i| {
                let x = rng.next_f64(-180.0, 180.0);
                let y = rng.next_f64(-90.0, 90.0);
                let w = rng.next_f64(0.0, 2.0);
                let h = rng.next_f64(0.0, 2.0);
                ([x, y, x + w, y + h], i)
            })
            .collect()
    }

    fn intersects(a: &[f64; 4], b: &[f64; 4]) -> bool {
        a[0] <= b[2] && a[2] >= b[0] && a[1] <= b[3] && a[3] >= b[1]
    }

    fn brute_force(items: &[([f64; 4], u32)], q: &[f64; 4]) -> Vec<u32> {
        let mut out: Vec<u32> = items
            .iter()
            .filter(|(b, _)| intersects(b, q))
            .map(|&(_, i)| i)
            .collect();
        out.sort_unstable();
        out
    }

    fn sorted_search(tree: &PackedRTree, q: &[f64; 4]) -> Vec<u32> {
        let mut out = tree.search(q);
        out.sort_unstable();
        out
    }

    /// The tree must return EXACTLY the brute-force bbox-intersection set, at
    /// every size class: empty, single, sub-node, node-boundary, multi-level.
    #[test]
    fn packed_rtree_matches_brute_force_at_all_sizes() {
        for n in [0usize, 1, 15, 16, 17, 255, 256, 1000, 10_000] {
            let items = random_items(n, 42 + n as u64);
            let tree = PackedRTree::build(items.clone());
            let queries = [
                [-180.0, -90.0, 180.0, 90.0], // everything
                [0.0, 0.0, 10.0, 10.0],       // region
                [-179.9, -89.9, -179.8, -89.8], // corner sliver
                [500.0, 500.0, 501.0, 501.0], // fully outside
                [3.0, 3.0, 3.0, 3.0],         // degenerate point query
            ];
            for q in &queries {
                assert_eq!(
                    sorted_search(&tree, q),
                    brute_force(&items, q),
                    "mismatch at n={n} q={q:?}"
                );
            }
        }
    }

    /// Bbox intersection must be edge-inclusive (touching boxes match), the
    /// same semantics as rstar's AABB intersection this tree replaces —
    /// st_dwithin's padded query boxes and st_touches rely on it.
    #[test]
    fn packed_rtree_edge_touch_is_inclusive() {
        let items = vec![([0.0, 0.0, 1.0, 1.0], 7u32)];
        let tree = PackedRTree::build(items);
        assert_eq!(tree.search(&[1.0, 1.0, 2.0, 2.0]), vec![7]); // corner touch
        assert_eq!(tree.search(&[1.0, 0.0, 2.0, 1.0]), vec![7]); // edge touch
        assert_eq!(tree.search(&[1.0 + 1e-12, 0.0, 2.0, 1.0]), Vec::<u32>::new());
    }

    /// Degenerate (point) boxes and exact duplicates must all be found —
    /// the build side of a point-in-polygon join can contain both.
    #[test]
    fn packed_rtree_degenerate_and_duplicate_boxes() {
        let mut items = vec![([5.0, 5.0, 5.0, 5.0], 0u32); 40]; // 40 identical points
        for (i, item) in items.iter_mut().enumerate() {
            item.1 = i as u32;
        }
        items.push(([5.0, 5.0, 6.0, 6.0], 40));
        let tree = PackedRTree::build(items.clone());
        assert_eq!(sorted_search(&tree, &[5.0, 5.0, 5.0, 5.0]), (0..=40).collect::<Vec<u32>>());
        assert_eq!(sorted_search(&tree, &[4.0, 4.0, 4.5, 4.5]), Vec::<u32>::new());
    }

    /// All-identical coordinates make the hilbert scaling range zero-width;
    /// the tree must still build and answer correctly.
    #[test]
    fn packed_rtree_zero_extent_world() {
        let items: Vec<([f64; 4], u32)> =
            (0..100).map(|i| ([1.0, 2.0, 1.0, 2.0], i)).collect();
        let tree = PackedRTree::build(items);
        assert_eq!(sorted_search(&tree, &[1.0, 2.0, 1.0, 2.0]), (0..100).collect::<Vec<u32>>());
        assert_eq!(tree.search(&[0.0, 0.0, 0.5, 0.5]), Vec::<u32>::new());
    }

    /// for_each_intersecting is the hot-path API; it must visit the same set
    /// search() returns.
    #[test]
    fn packed_rtree_callback_matches_search() {
        let items = random_items(5000, 7);
        let tree = PackedRTree::build(items);
        let q = [10.0, -20.0, 40.0, 15.0];
        let mut via_cb: Vec<u32> = Vec::new();
        tree.for_each_intersecting(&q, |i| via_cb.push(i));
        via_cb.sort_unstable();
        assert_eq!(via_cb, sorted_search(&tree, &q));
        assert!(!via_cb.is_empty(), "query returned nothing — test would be vacuous");
    }
}
