//! Process-wide resident-byte budget for the owned parquet reader.
//!
//! Bounds the compressed bytes concurrently resident across every parquet
//! reader in this process (one Ray Swordfish actor == one process, so this
//! is the actor-wide admission control).
//!
//! Design (see the OOM analysis docs for rationale):
//! - `reserve(bytes)` registers a waiter in strict FIFO order
//!   **synchronously** and returns a cancellation-safe future. The caller
//!   (the RG driver) must never await admission itself — it registers in RG
//!   order, hands the reservation to the RG task, and keeps draining output.
//!   Awaiting in the driver while the front RG is blocked on a full channel
//!   deadlocks (front can't finish → can't release → next permit never
//!   granted).
//! - Strict FIFO: a small request behind a blocked large request must NOT
//!   overtake it. Fairness over utilization — overtaking starves the front
//!   RG, whose output the ordered consumer is waiting on.
//! - Oversized requests (`bytes > total`): granted only when `in_use == 0`,
//!   and mark the budget `exclusive` while running. No Polars-style
//!   truncation — the ledger stays accurate; "at most one oversized RG" is
//!   the explainable bound.
//! - All bookkeeping happens inside a `std::sync::Mutex` (no await inside
//!   the critical section), so `Drop` impls can clean up synchronously.
//!   Every waiter has its own `Notify` — no lost wakeups, no thundering
//!   herd.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
};

use tokio::sync::Notify;

/// Live metrics, exported for logging/tests. Planned (reserved) bytes and
/// actually-downloaded resident bytes are tracked separately: a permit is
/// granted before the GETs run, so `reserved` leads `resident`.
#[derive(Debug, Default)]
pub(crate) struct BudgetMetrics {
    pub reserved_bytes: AtomicI64,
    pub resident_bytes: AtomicI64,
    pub resident_bytes_peak: AtomicI64,
    pub active_resident_row_groups: AtomicI64,
    pub oversized_row_groups: AtomicU64,
}

/// `DAFT_PARQUET_MEM_VERBOSE=1` → eprintln lifecycle events with live
/// totals. Test/diagnostic hook, mirrors the methodology used throughout
/// the OOM investigation.
pub(crate) fn mem_verbose() -> bool {
    static V: OnceLock<bool> = OnceLock::new();
    *V.get_or_init(|| std::env::var("DAFT_PARQUET_MEM_VERBOSE").as_deref() == Ok("1"))
}

impl BudgetMetrics {
    pub(crate) fn snapshot_line(&self) -> String {
        format!(
            "reserved={} resident={} peak={} active_rgs={}",
            self.reserved_bytes.load(Ordering::Relaxed),
            self.resident_bytes.load(Ordering::Relaxed),
            self.resident_bytes_peak.load(Ordering::Relaxed),
            self.active_resident_row_groups.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn record_resident(&self, actual: usize) {
        let now = self
            .resident_bytes
            .fetch_add(actual as i64, Ordering::Relaxed)
            + actual as i64;
        self.resident_bytes_peak.fetch_max(now, Ordering::Relaxed);
        self.active_resident_row_groups
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn release_resident(&self, actual: usize) {
        self.resident_bytes
            .fetch_sub(actual as i64, Ordering::Relaxed);
        self.active_resident_row_groups
            .fetch_sub(1, Ordering::Relaxed);
    }
}

struct Waiter {
    id: u64,
    bytes: usize,
    notify: Arc<Notify>,
    granted: bool,
}

struct State {
    in_use: usize,
    exclusive: bool,
    next_id: u64,
    waiters: VecDeque<Waiter>,
}

pub(crate) struct ByteBudget {
    /// `None` = unlimited (admission never blocks, ledger still tracked).
    total: Option<usize>,
    state: Mutex<State>,
    pub(crate) metrics: BudgetMetrics,
}

impl std::fmt::Debug for ByteBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.state.lock().unwrap();
        f.debug_struct("ByteBudget")
            .field("total", &self.total)
            .field("in_use", &s.in_use)
            .field("exclusive", &s.exclusive)
            .field("waiters", &s.waiters.len())
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl ByteBudget {
    pub(crate) fn new(total: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            total,
            state: Mutex::new(State {
                in_use: 0,
                exclusive: false,
                next_id: 0,
                waiters: VecDeque::new(),
            }),
            metrics: BudgetMetrics::default(),
        })
    }

    /// Grant as many queue-front waiters as fit. Strict FIFO: stop at the
    /// first ungranted waiter that does not fit — a smaller request behind
    /// it must not overtake. Collect the notifies and fire them after the
    /// lock is released.
    fn pump(total: Option<usize>, s: &mut State, metrics: &BudgetMetrics) -> Vec<Arc<Notify>> {
        let mut to_notify = Vec::new();
        if s.exclusive {
            return to_notify;
        }
        for w in &mut s.waiters {
            if w.granted {
                continue;
            }
            let oversized = total.is_some_and(|t| w.bytes > t);
            let admissible = match total {
                None => true,
                Some(_) if oversized => s.in_use == 0,
                Some(t) => s.in_use.saturating_add(w.bytes) <= t,
            };
            if !admissible {
                break;
            }
            w.granted = true;
            s.in_use += w.bytes;
            metrics
                .reserved_bytes
                .fetch_add(w.bytes as i64, Ordering::Relaxed);
            to_notify.push(w.notify.clone());
            if oversized {
                s.exclusive = true;
                metrics.oversized_row_groups.fetch_add(1, Ordering::Relaxed);
                break; // exclusive holder admitted; nothing else may enter
            }
        }
        to_notify
    }

    /// Synchronously register a FIFO reservation. Never blocks. The returned
    /// reservation must be awaited (usually inside the RG task) to obtain
    /// the permit; dropping it at any point deregisters cleanly.
    pub(crate) fn reserve(self: &Arc<Self>, bytes: usize) -> BudgetReservation {
        let notify = Arc::new(Notify::new());
        let id = {
            let mut s = self.state.lock().unwrap();
            let id = s.next_id;
            s.next_id = s.next_id.checked_add(1).expect("reservation id overflow");
            s.waiters.push_back(Waiter {
                id,
                bytes,
                notify: notify.clone(),
                granted: false,
            });
            let to_notify = Self::pump(self.total, &mut s, &self.metrics);
            drop(s);
            for n in to_notify {
                n.notify_one();
            }
            id
        };
        BudgetReservation {
            budget: self.clone(),
            id,
            bytes,
            notify,
            resolved: false,
        }
    }

    /// Remove a waiter (cancelled reservation). If it was already granted,
    /// roll the grant back. Either way, re-pump the queue.
    fn deregister(&self, id: u64) {
        let mut s = self.state.lock().unwrap();
        if let Some(pos) = s.waiters.iter().position(|w| w.id == id) {
            let w = s.waiters.remove(pos).unwrap();
            if w.granted {
                s.in_use -= w.bytes;
                if self.total.is_some_and(|t| w.bytes > t) {
                    s.exclusive = false;
                }
                self.metrics
                    .reserved_bytes
                    .fetch_sub(w.bytes as i64, Ordering::Relaxed);
            }
        }
        let to_notify = Self::pump(self.total, &mut s, &self.metrics);
        drop(s);
        for n in to_notify {
            n.notify_one();
        }
    }

    fn is_granted(&self, id: u64) -> bool {
        let s = self.state.lock().unwrap();
        s.waiters.iter().any(|w| w.id == id && w.granted)
    }

    /// Convert a granted reservation into a held permit: the waiter entry
    /// leaves the queue but its bytes stay accounted until `release`.
    fn commit(&self, id: u64) {
        let mut s = self.state.lock().unwrap();
        if let Some(pos) = s.waiters.iter().position(|w| w.id == id) {
            debug_assert!(s.waiters[pos].granted);
            s.waiters.remove(pos);
        }
    }

    fn release(&self, bytes: usize) {
        let mut s = self.state.lock().unwrap();
        s.in_use -= bytes;
        if self.total.is_some_and(|t| bytes > t) {
            s.exclusive = false;
        }
        self.metrics
            .reserved_bytes
            .fetch_sub(bytes as i64, Ordering::Relaxed);
        let to_notify = Self::pump(self.total, &mut s, &self.metrics);
        drop(s);
        for n in to_notify {
            n.notify_one();
        }
    }

    #[cfg(test)]
    fn in_use(&self) -> usize {
        self.state.lock().unwrap().in_use
    }

    #[cfg(test)]
    fn waiter_count(&self) -> usize {
        self.state.lock().unwrap().waiters.len()
    }
}

/// A queued admission request. Await it to obtain the [`BudgetPermit`].
/// Dropping it — before or after grant, awaited or not — deregisters and
/// rolls back cleanly (cancellation safety).
pub(crate) struct BudgetReservation {
    budget: Arc<ByteBudget>,
    id: u64,
    bytes: usize,
    notify: Arc<Notify>,
    resolved: bool,
}

impl BudgetReservation {
    pub(crate) async fn acquire(mut self) -> BudgetPermit {
        loop {
            if self.budget.is_granted(self.id) {
                self.budget.commit(self.id);
                self.resolved = true;
                return BudgetPermit {
                    budget: self.budget.clone(),
                    bytes: self.bytes,
                };
            }
            self.notify.notified().await;
        }
    }
}

impl Drop for BudgetReservation {
    fn drop(&mut self) {
        if !self.resolved {
            self.budget.deregister(self.id);
        }
    }
}

/// Held admission for one row group's planned compressed bytes. Dropping it
/// returns the bytes to the budget and wakes the queue front.
pub(crate) struct BudgetPermit {
    budget: Arc<ByteBudget>,
    bytes: usize,
}

impl BudgetPermit {
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn metrics(&self) -> &BudgetMetrics {
        &self.budget.metrics
    }
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

/// The process-wide budget used by all owned-mode parquet readers.
/// `DAFT_PARQUET_RESIDENT_BUDGET_MB`: unset → 256MB default; `0` → unlimited.
pub(crate) fn process_budget() -> &'static Arc<ByteBudget> {
    static BUDGET: OnceLock<Arc<ByteBudget>> = OnceLock::new();
    BUDGET.get_or_init(|| {
        let total = match std::env::var("DAFT_PARQUET_RESIDENT_BUDGET_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
        {
            Some(0) => None,
            Some(mb) => Some(mb.saturating_mul(1024 * 1024)),
            None => Some(256 * 1024 * 1024),
        };
        ByteBudget::new(total)
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn sequential_acquire_release() {
        let b = ByteBudget::new(Some(100));
        for _ in 0..5 {
            let p = b.reserve(60).acquire().await;
            assert_eq!(b.in_use(), 60);
            drop(p);
            assert_eq!(b.in_use(), 0);
        }
        assert_eq!(b.waiter_count(), 0);
    }

    #[tokio::test]
    async fn concurrent_fit_together() {
        let b = ByteBudget::new(Some(100));
        let p1 = b.reserve(40).acquire().await;
        let p2 = b.reserve(40).acquire().await;
        assert_eq!(b.in_use(), 80);
        drop((p1, p2));
        assert_eq!(b.in_use(), 0);
    }

    #[tokio::test]
    async fn fifo_no_overtake() {
        let b = ByteBudget::new(Some(100));
        let p1 = b.reserve(80).acquire().await;
        // Large request that doesn't fit heads the queue...
        let r_big = b.reserve(50);
        // ...a small request that WOULD fit must not overtake it.
        let r_small = b.reserve(10);
        assert!(!b.is_granted(r_big.id));
        assert!(!b.is_granted(r_small.id));
        drop(p1);
        // Now the big one is granted; small fits alongside it.
        assert!(b.is_granted(r_big.id));
        assert!(b.is_granted(r_small.id));
        let _pb = r_big.acquire().await;
        let _ps = r_small.acquire().await;
        assert_eq!(b.in_use(), 60);
    }

    #[tokio::test]
    async fn waiter_cancellation_unblocks_queue() {
        let b = ByteBudget::new(Some(100));
        let p1 = b.reserve(80).acquire().await;
        let r_blocked = b.reserve(90); // can't fit while p1 held
        let r_next = b.reserve(20); // fits, but FIFO-blocked behind r_blocked
        assert!(!b.is_granted(r_next.id));
        drop(r_blocked); // cancel the blocker
        assert!(b.is_granted(r_next.id)); // next must be pumped immediately
        let _p = r_next.acquire().await;
        drop(p1);
    }

    #[tokio::test]
    async fn granted_but_unconsumed_cancellation_rolls_back() {
        let b = ByteBudget::new(Some(100));
        let r = b.reserve(70);
        assert!(b.is_granted(r.id));
        assert_eq!(b.in_use(), 70);
        drop(r); // never awaited
        assert_eq!(b.in_use(), 0);
        assert_eq!(b.metrics.reserved_bytes.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn oversized_exclusive_singleton() {
        let b = ByteBudget::new(Some(100));
        let p_small = b.reserve(30).acquire().await;
        let r_big = b.reserve(500); // oversized: needs in_use == 0
        assert!(!b.is_granted(r_big.id));
        drop(p_small);
        assert!(b.is_granted(r_big.id));
        // While oversized runs, nothing else enters — not even tiny requests.
        let r_tiny = b.reserve(1);
        assert!(!b.is_granted(r_tiny.id));
        let p_big = r_big.acquire().await;
        assert!(!b.is_granted(r_tiny.id));
        drop(p_big);
        assert!(b.is_granted(r_tiny.id));
        let _pt = r_tiny.acquire().await;
    }

    #[tokio::test]
    async fn oversized_waiter_cancellation_unblocks() {
        let b = ByteBudget::new(Some(100));
        let p1 = b.reserve(10).acquire().await;
        let r_big = b.reserve(500); // waits for idle
        let r_after = b.reserve(10);
        assert!(!b.is_granted(r_after.id));
        drop(r_big);
        assert!(b.is_granted(r_after.id));
        drop(p1);
    }

    #[tokio::test]
    async fn unlimited_never_blocks() {
        let b = ByteBudget::new(None);
        let p1 = b.reserve(usize::MAX / 4).acquire().await;
        let p2 = b.reserve(usize::MAX / 4).acquire().await;
        drop((p1, p2));
        assert_eq!(b.in_use(), 0);
    }

    #[tokio::test]
    async fn zero_bytes_immediate() {
        let b = ByteBudget::new(Some(1));
        let _p1 = b.reserve(1).acquire().await;
        let p2 = b.reserve(0).acquire().await;
        assert_eq!(p2.bytes(), 0);
    }

    /// Regression for the v1 driver deadlock: budget=256, RG0=RG1=200,
    /// lookahead=2, channel capacity 1. The driver registers both
    /// reservations synchronously (never blocking), RG0's task acquires and
    /// streams into a full channel; RG1's task waits on its reservation.
    /// The driver keeps draining RG0, RG0 completes, drops its permit, and
    /// RG1 proceeds. With driver-side `acquire().await` this hangs forever.
    #[tokio::test]
    async fn deadlock_regression_reserve_then_await_in_task() {
        let b = ByteBudget::new(Some(256));
        let r0 = b.reserve(200); // driver: sync registration, RG order
        let r1 = b.reserve(200);

        let (tx0, mut rx0) = tokio::sync::mpsc::channel::<u32>(1);
        let t0 = tokio::spawn(async move {
            let _p = r0.acquire().await;
            for i in 0..3 {
                tx0.send(i).await.unwrap(); // blocks on full channel until drained
            }
            // permit dropped here
        });
        let t1 = tokio::spawn(async move {
            let _p = r1.acquire().await; // must eventually be granted
            true
        });

        // Driver drains RG0 while RG1 waits for budget.
        let drained = async {
            let mut got = Vec::new();
            while let Some(v) = rx0.recv().await {
                got.push(v);
            }
            got
        };
        let (got, r0_join, r1_ok) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(drained, t0, t1)
        })
        .await
        .expect("deadlock: RG1 never admitted");
        assert_eq!(got, vec![0, 1, 2]);
        r0_join.unwrap();
        assert!(r1_ok.unwrap());
        assert_eq!(b.in_use(), 0);
    }

    #[tokio::test]
    async fn multi_reader_fairness_fifo_order() {
        let b = ByteBudget::new(Some(100));
        let p_hold = b.reserve(100).acquire().await;
        // Two "readers" queue interleaved requests; grant order must be FIFO.
        let ra1 = b.reserve(50);
        let rb1 = b.reserve(50);
        let ra2 = b.reserve(50);
        drop(p_hold);
        assert!(b.is_granted(ra1.id));
        assert!(b.is_granted(rb1.id));
        assert!(!b.is_granted(ra2.id));
        let pa1 = ra1.acquire().await;
        drop(pa1);
        assert!(b.is_granted(ra2.id));
        let _ = (rb1, ra2);
    }

    #[tokio::test]
    async fn permit_drop_on_panic_returns_budget() {
        let b = ByteBudget::new(Some(100));
        let r = b.reserve(100);
        let b2 = b.clone();
        let t = tokio::spawn(async move {
            let _p = r.acquire().await;
            assert_eq!(b2.in_use(), 100);
            panic!("boom");
        });
        assert!(t.await.is_err());
        assert_eq!(b.in_use(), 0);
        let _p = b.reserve(100).acquire().await; // budget still usable
    }
}
