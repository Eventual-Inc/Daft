use std::{
    borrow::Cow,
    sync::{
        atomic::{self},
        Arc,
    },
};

pub type IOStatsRef = Arc<IOStatsContext>;

#[derive(Default, Debug)]
pub struct IOStatsContext {
    name: Cow<'static, str>,
    num_get_requests: atomic::AtomicUsize,
    num_head_requests: atomic::AtomicUsize,
    num_list_requests: atomic::AtomicUsize,
    num_put_requests: atomic::AtomicUsize,
    bytes_read: atomic::AtomicUsize,
    bytes_uploaded: atomic::AtomicUsize,
}

impl Drop for IOStatsContext {
    fn drop(&mut self) {
        let bytes_read = self.load_bytes_read();
        let num_gets = self.load_get_requests();
        let bytes_uploaded = self.load_bytes_uploaded();
        let num_puts = self.load_put_requests();
        let mean_get_size = (bytes_read as f64) / (num_gets as f64);
        let mean_put_size = (bytes_uploaded as f64) / (num_puts as f64);
        log::info!(
            "IOStatsContext: {}, Gets: {}, Heads: {}, Lists: {}, BytesRead: {}, AvgGetSize: {}, BytesUploaded: {}, AvgPutSize: {}",
            self.name,
            num_gets,
            self.load_head_requests(),
            self.load_list_requests(),
            bytes_read,
            mean_get_size as i64,
            bytes_uploaded,
            mean_put_size as i64,
        );
    }
}

pub(crate) struct IOStatsByteStreamContextHandle {
    // do not enable Copy or Clone on this struct
    bytes_read: usize,
    inner: IOStatsRef,
}

impl IOStatsContext {
    pub fn new<S: Into<Cow<'static, str>>>(name: S) -> IOStatsRef {
        Arc::new(IOStatsContext {
            name: name.into(),
            num_get_requests: atomic::AtomicUsize::new(0),
            num_head_requests: atomic::AtomicUsize::new(0),
            num_list_requests: atomic::AtomicUsize::new(0),
            num_put_requests: atomic::AtomicUsize::new(0),
            bytes_read: atomic::AtomicUsize::new(0),
            bytes_uploaded: atomic::AtomicUsize::new(0),
        })
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
    pub(crate) fn mark_put_requests(&self, num_requests: usize) {
        self.num_put_requests
            .fetch_add(num_requests, atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn load_get_requests(&self) -> usize {
        self.num_get_requests.load(atomic::Ordering::Acquire)
    }

    #[inline]
    pub fn load_head_requests(&self) -> usize {
        self.num_head_requests.load(atomic::Ordering::Acquire)
    }

    #[inline]
    pub fn load_list_requests(&self) -> usize {
        self.num_list_requests.load(atomic::Ordering::Acquire)
    }

    #[inline]
    pub fn load_put_requests(&self) -> usize {
        self.num_put_requests.load(atomic::Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn mark_bytes_read(&self, bytes_read: usize) {
        self.bytes_read
            .fetch_add(bytes_read, atomic::Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn mark_bytes_uploaded(&self, bytes_uploaded: usize) {
        self.bytes_uploaded
            .fetch_add(bytes_uploaded, atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn load_bytes_read(&self) -> usize {
        self.bytes_read.load(atomic::Ordering::Acquire)
    }

    #[inline]
    pub fn load_bytes_uploaded(&self) -> usize {
        self.bytes_uploaded.load(atomic::Ordering::Acquire)
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
    }
}
