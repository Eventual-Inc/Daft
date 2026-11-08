//! Test-build-only, one-shot Bridge failpoints for P2 fault containment.
//!
//! This module is compiled only with the explicit `failure-injection` Cargo
//! feature.  It is intentionally not wired to an environment variable, SQL
//! GUC, or exported C symbol, so a production Bridge has no reachable switch.

use std::sync::atomic::{AtomicU8, Ordering};

/// One-shot failure location used by the deterministic P2 fault suite.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    /// No failure is armed.
    None = 0,
    /// Panic inside `dpg_open_v1`; the ABI boundary must contain it.
    OpenPanic = 1,
    /// Return controlled resource exhaustion before runtime allocation.
    OpenResourceExhausted = 2,
    /// Reject an input batch as corrupted before reading its metadata.
    PushCorruption = 3,
    /// Panic while polling output; cancel/close must remain usable.
    NextPanic = 4,
}

static ARMED: AtomicU8 = AtomicU8::new(FaultPoint::None as u8);

/// Arm exactly one failpoint for the next matching operation.
pub fn arm(point: FaultPoint) {
    ARMED.store(point as u8, Ordering::SeqCst);
}

/// Clear any unconsumed failpoint.
pub fn clear() {
    ARMED.store(FaultPoint::None as u8, Ordering::SeqCst);
}

pub(crate) fn take(point: FaultPoint) -> bool {
    ARMED
        .compare_exchange(
            point as u8,
            FaultPoint::None as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
        .is_ok()
}
