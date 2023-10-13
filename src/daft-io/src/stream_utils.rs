use bytes::Bytes;

use crate::stats::{IOStatsByteStreamContextHandle, IOStatsRef};

use futures::{stream::BoxStream, StreamExt};

pub(crate) fn io_stats_on_bytestream(
    mut s: impl futures::stream::Stream<Item = super::Result<Bytes>>
        + Unpin
        + std::marker::Send
        + 'static,
    io_stats: Option<IOStatsRef>,
) -> BoxStream<'static, super::Result<Bytes>> {
    if let Some(io_stats) = io_stats {
        let mut context = IOStatsByteStreamContextHandle::new(io_stats);
        async_stream::stream! {
            while let Some(val) = s.next().await  {
                if let Ok(ref val) = val {
                    context.mark_bytes_read(val.len());
                }
                yield val
            }
        }
        .boxed()
    } else {
        s.boxed()
    }
}
