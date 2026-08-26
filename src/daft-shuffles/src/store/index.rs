//! On-disk index for a shared-placement combined map file.
//!
//! File layout:
//!
//! ```text
//! 0                            magic "DAFTSHF1"            (8 bytes)
//! 8                            num_partitions N            (u64 LE)
//! 16                           offsets[0..=N]              ((N+1) x u64 LE)
//! index_region_bytes(N)        arrow IPC stream: schema header, then batches
//! ```
//!
//! `offsets` are absolute file offsets and monotonically non-decreasing, so output
//! partition `p` occupies `[offsets[p], offsets[p+1])` and an empty partition is
//! simply a zero-length range. `offsets[0]` points just past the IPC schema header,
//! i.e. at the first batch boundary, which is what
//! [`crate::server::stream::FlightDataStreamReader::from_skipped`] expects.
//!
//! The index is a *header* rather than a trailer so that a reader can locate it
//! without first stat-ing the file for its length. On a shared mount every such
//! round trip is ~1-2 ms, and the whole point of the format is to get a reduce
//! task from "I know the input id" to "I have the bytes" in as few round trips as
//! possible: one read for the index, one for the data.
//!
//! The writer reserves the region up front, streams the IPC data after it, then
//! seeks back and fills the offsets in once they are known.

use common_error::{DaftError, DaftResult};

pub const INDEX_MAGIC: [u8; 8] = *b"DAFTSHF1";

/// Bytes before the offset array: magic + partition count.
pub const PREFIX_BYTES: usize = 16;

/// How much of the file to grab on the first read.
///
/// Sized so one read covers the index of any shuffle up to 8189 output partitions.
/// Reading this much is nearly free relative to reading only the prefix — small
/// reads on a shared mount are latency-bound, not size-bound (measured: 0.96 ms
/// for 16 B vs 2.38 ms for 64 KiB), so speculatively over-reading beats paying a
/// second round trip.
pub const PROBE_BYTES: usize = 64 * 1024;

/// Total size of the reserved index region for `num_partitions` outputs.
pub const fn index_region_bytes(num_partitions: usize) -> usize {
    PREFIX_BYTES + 8 * (num_partitions + 1)
}

/// Serialize the index region. `offsets` must hold `num_partitions + 1` entries.
pub fn encode(offsets: &[u64]) -> DaftResult<Vec<u8>> {
    if offsets.is_empty() {
        return Err(DaftError::InternalError(
            "shuffle index needs at least one offset".to_string(),
        ));
    }
    let num_partitions = offsets.len() - 1;
    let mut buf = Vec::with_capacity(index_region_bytes(num_partitions));
    buf.extend_from_slice(&INDEX_MAGIC);
    buf.extend_from_slice(&(num_partitions as u64).to_le_bytes());
    for offset in offsets {
        buf.extend_from_slice(&offset.to_le_bytes());
    }
    Ok(buf)
}

/// Read the partition count out of a buffer holding at least [`PREFIX_BYTES`].
pub fn parse_num_partitions(buf: &[u8], path: &str) -> DaftResult<usize> {
    if buf.len() < PREFIX_BYTES {
        return Err(DaftError::InternalError(format!(
            "shuffle map file {} is truncated: {} bytes, need at least {}",
            path,
            buf.len(),
            PREFIX_BYTES
        )));
    }
    if buf[..8] != INDEX_MAGIC {
        return Err(DaftError::InternalError(format!(
            "shuffle map file {} is not a shared-placement map file (bad magic)",
            path
        )));
    }
    let num_partitions = u64::from_le_bytes(buf[8..16].try_into().expect("checked length"));
    usize::try_from(num_partitions).map_err(|_| {
        DaftError::InternalError(format!(
            "shuffle map file {} declares {} partitions, which does not fit in usize",
            path, num_partitions
        ))
    })
}

/// Byte range `[start, end)` of output partition `partition_idx`.
///
/// `buf` must hold the whole index region (see [`index_region_bytes`]).
pub fn partition_range(buf: &[u8], partition_idx: usize, path: &str) -> DaftResult<(u64, u64)> {
    let num_partitions = parse_num_partitions(buf, path)?;
    if partition_idx >= num_partitions {
        return Err(DaftError::InternalError(format!(
            "shuffle map file {} holds {} partitions, asked for index {}",
            path, num_partitions, partition_idx
        )));
    }
    let needed = index_region_bytes(num_partitions);
    if buf.len() < needed {
        return Err(DaftError::InternalError(format!(
            "shuffle map file {} index is truncated: {} bytes, need {}",
            path,
            buf.len(),
            needed
        )));
    }
    let at = |i: usize| -> u64 {
        let start = PREFIX_BYTES + 8 * i;
        u64::from_le_bytes(buf[start..start + 8].try_into().expect("checked length"))
    };
    let (start, end) = (at(partition_idx), at(partition_idx + 1));
    if end < start {
        return Err(DaftError::InternalError(format!(
            "shuffle map file {} has a descending range for partition {}: {}..{}",
            path, partition_idx, start, end
        )));
    }
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<u8> {
        // 3 partitions; partition 1 is empty.
        encode(&[100, 250, 250, 900]).unwrap()
    }

    #[test]
    fn region_size_matches_encoding() {
        let buf = sample();
        assert_eq!(buf.len(), index_region_bytes(3));
        assert_eq!(parse_num_partitions(&buf, "t").unwrap(), 3);
    }

    #[test]
    fn ranges_round_trip_including_empty_partitions() {
        let buf = sample();
        assert_eq!(partition_range(&buf, 0, "t").unwrap(), (100, 250));
        assert_eq!(partition_range(&buf, 1, "t").unwrap(), (250, 250));
        assert_eq!(partition_range(&buf, 2, "t").unwrap(), (250, 900));
    }

    #[test]
    fn out_of_range_partition_is_an_error() {
        let buf = sample();
        assert!(partition_range(&buf, 3, "t").is_err());
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut buf = sample();
        buf[0] = b'X';
        assert!(parse_num_partitions(&buf, "t").is_err());
    }

    #[test]
    fn truncated_index_is_rejected() {
        let buf = sample();
        let truncated = &buf[..PREFIX_BYTES + 8];
        assert!(partition_range(truncated, 0, "t").is_err());
        assert!(parse_num_partitions(&buf[..4], "t").is_err());
    }

    #[test]
    fn probe_covers_realistic_partition_counts() {
        assert!(index_region_bytes(8189) <= PROBE_BYTES);
    }
}
