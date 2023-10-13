use std::sync::{atomic, Arc};

pub type IOStatsRef = Arc<IOStatsContext>;

#[derive(Default, Debug)]
pub struct IOStatsContext {
    num_get_requests: atomic::AtomicUsize,
    num_head_requests: atomic::AtomicUsize,
    num_list_requests: atomic::AtomicUsize,
    bytes_read: atomic::AtomicUsize,
}

pub(crate) struct IOStatsByteStreamContextHandle {
    // do not enable Copy or Clone on this struct
    bytes_read: usize,
    inner: IOStatsRef,
}

impl IOStatsContext {
    pub fn new() -> IOStatsRef {
        Arc::new(Default::default())
    }

    #[inline]
    pub(crate) fn mark_get_requests(&self, num_requests: usize) {
        self.num_get_requests
            .fetch_add(num_requests, atomic::Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn mark_head_requests(&self, num_requests: usize) {
        self.num_head_requests
            .fetch_add(num_requests, atomic::Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn mark_list_requests(&self, num_requests: usize) {
        self.num_list_requests
            .fetch_add(num_requests, atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn load_get_requests(&self, num_requests: usize) {
        self.num_get_requests.load(atomic::Ordering::Acquire);
    }

    #[inline]
    pub fn load_head_requests(&self, num_requests: usize) {
        self.num_head_requests.load(atomic::Ordering::Acquire);
    }

    #[inline]
    pub fn load_list_requests(&self, num_requests: usize) {
        self.num_list_requests.load(atomic::Ordering::Acquire);
    }

    #[inline]
    pub(crate) fn mark_bytes_read(&self, bytes_read: usize) {
        self.bytes_read
            .fetch_add(bytes_read, atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn load_bytes_read(&self) -> usize {
        self.bytes_read.load(atomic::Ordering::Acquire)
    }
}

impl IOStatsByteStreamContextHandle {
    pub fn new(io_stats: IOStatsRef) -> Self {
        Self {
            bytes_read: 0,
            inner: io_stats,
        }
    }

    #[inline]
    pub fn mark_bytes_read(&mut self, bytes_read: usize) {
        self.bytes_read += bytes_read;
    }
}

impl Drop for IOStatsByteStreamContextHandle {
    fn drop(&mut self) {
        self.inner.mark_bytes_read(self.bytes_read);

        log::warn!("Finished IOStats: {}", self.inner.load_bytes_read());
    }
}
