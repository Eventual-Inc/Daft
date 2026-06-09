use crate::{
    BYTES_IN_KEY, BYTES_OUT_KEY, BYTES_READ_KEY, DURATION_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, Stat,
    ops::NodeInfo,
    snapshot::{StatSnapshot, StatSnapshotImpl},
};

/// External I/O totals for a single distributed task, summed across the local
/// plan nodes that ran inside it.
///
/// Filled by repeatedly calling [`Self::accumulate`] with each snapshot and
/// its `NodeInfo`. The `is_task_root` / `is_task_leaf` markers on `NodeInfo`
/// (set by `LocalPhysicalPlan::mark_task_topology` on the driver) are what
/// keep the totals correct in fused chains: only leaf snapshots contribute
/// external `rows.in` / `bytes.in`, only the root snapshot contributes
/// external `rows.out` / `bytes.out`. `cpu_us` sums across every node.
///
/// Source-leaf snapshots (`SourceSnapshot`) report `rows.out` / `bytes.read`
/// instead of `rows.in` / `bytes.in` because a source has no upstream
/// operator; both equal the task's external input, so we fall back to them
/// when the leaf doesn't expose an `*.in` key.
#[derive(Clone, Copy, Debug, Default)]
pub struct TaskExternalIo {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

impl TaskExternalIo {
    /// Fold a single `(NodeInfo, StatSnapshot)` pair into the running totals.
    /// Call once per snapshot; the result on the accumulator is the sum-so-far.
    pub fn accumulate(&mut self, node_info: &NodeInfo, snapshot: &StatSnapshot) {
        let is_task_root = node_info.is_task_root;
        let is_task_leaf = node_info.is_task_leaf;

        let mut leaf_rows_in: Option<u64> = None;
        let mut leaf_rows_out_for_fallback: Option<u64> = None;
        let mut leaf_bytes_in: Option<u64> = None;
        let mut leaf_bytes_read: Option<u64> = None;

        for (name, stat) in snapshot.to_stats().iter() {
            match (name, stat) {
                (DURATION_KEY, Stat::Duration(d)) => {
                    self.cpu_us += d.as_micros() as u64;
                }
                (ROWS_IN_KEY, Stat::Count(c)) if is_task_leaf => {
                    leaf_rows_in = Some(*c);
                }
                (BYTES_IN_KEY, Stat::Bytes(b)) if is_task_leaf => {
                    leaf_bytes_in = Some(*b);
                }
                (BYTES_READ_KEY, Stat::Bytes(b)) if is_task_leaf => {
                    leaf_bytes_read = Some(*b);
                }
                (ROWS_OUT_KEY, Stat::Count(c)) => {
                    if is_task_leaf {
                        leaf_rows_out_for_fallback = Some(*c);
                    }
                    if is_task_root {
                        self.rows_out += *c;
                    }
                }
                (BYTES_OUT_KEY, Stat::Bytes(b)) if is_task_root => {
                    self.bytes_out += *b;
                }
                _ => {}
            }
        }

        if is_task_leaf {
            self.rows_in += leaf_rows_in.or(leaf_rows_out_for_fallback).unwrap_or(0);
            self.bytes_in += leaf_bytes_in.or(leaf_bytes_read).unwrap_or(0);
        }
    }
}
