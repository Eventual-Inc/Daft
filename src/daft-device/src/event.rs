use std::{ffi::c_void, fmt, ptr};

use common_error::DaftResult;

/// Synchronization contract for asynchronously produced device buffers.
///
/// This fills the role of the `sync_event` member of the Arrow C Device
/// interface and the stream/event contract of the DLPack exchange protocol: a
/// consumer must wait on the event (or make its stream wait on it) before
/// reading the buffer the event guards.
pub trait SyncEvent: Send + Sync + fmt::Debug {
    /// Blocks the calling host thread until the device work producing the
    /// guarded buffer has completed.
    fn synchronize(&self) -> DaftResult<()>;

    /// Raw backend event handle (e.g. a `cudaEvent_t`) for handoff through C
    /// ABIs such as `ArrowDeviceArray::sync_event`. Backends without a native
    /// event object return null, which those ABIs define as "no pending work".
    fn raw_handle(&self) -> *mut c_void {
        ptr::null_mut()
    }

    /// Whether the guarded work is already known complete, *without blocking*
    /// (e.g. `cudaEventQuery`). Used opportunistically — to prune finished
    /// read guards in [`crate::DeviceBuffer::guard_read`] — so `false` is
    /// always a safe answer and is the default for backends without a cheap
    /// completion query.
    fn is_complete(&self) -> bool {
        false
    }
}

/// An already-completed event: the guarded buffer is immediately readable.
///
/// Used for host-resident buffers and for device work that was synchronously
/// completed at production time.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadySyncEvent;

impl SyncEvent for ReadySyncEvent {
    fn synchronize(&self) -> DaftResult<()> {
        Ok(())
    }

    fn is_complete(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_event_is_trivially_synchronized() {
        let event = ReadySyncEvent;
        assert!(event.synchronize().is_ok());
        assert!(event.raw_handle().is_null());
        assert!(event.is_complete());
    }

    #[test]
    fn completion_query_defaults_to_pending() {
        #[derive(Debug)]
        struct NoQueryEvent;
        impl SyncEvent for NoQueryEvent {
            fn synchronize(&self) -> DaftResult<()> {
                Ok(())
            }
        }
        // A backend without a cheap completion query must report "not proven
        // complete", never "complete".
        assert!(!NoQueryEvent.is_complete());
    }
}
