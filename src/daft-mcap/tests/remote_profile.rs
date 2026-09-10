//! Opt-in profiling of remote MCAPs. See README.md for credentials and controls.

use std::{sync::Arc, time::Instant};

use daft_io::{IOConfig, IOStatsContext, IOStatsRef, get_io_client};
use daft_mcap::{McapReadOptions, McapReadStats, NativeMcapReader};

const ABC_EPISODE: &str = "hf://datasets/XDOF/ABC-130k@75ca0b88bda489f2bd935d72454593ecb90efb52/data/val/arrange_the_flowers_into_the_vase/episode_02161a65-02b6-477b-ad3a-f4f6947041cc/episode.mcap";
const ABC_EPISODE_ROWS: usize = 288_765;

fn report(
    phase: &str,
    indexed: bool,
    stats: &McapReadStats,
    io: &IOStatsRef,
    rows: usize,
    start: Instant,
) {
    eprintln!(
        "phase={phase} rows={rows} elapsed_ms={} indexed={} gets={} heads={} lists={} bytes={} logical={:?}",
        start.elapsed().as_millis(),
        indexed,
        io.load_get_requests(),
        io.load_head_requests(),
        io.load_list_requests(),
        io.load_bytes_read(),
        stats,
    );
}

#[tokio::test]
#[ignore = "downloads remote data; ABC-130k requires approved Hugging Face access and HF_TOKEN"]
async fn profile_abc_130k() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = IOConfig::default();
    config.hf.token = std::env::var("HF_TOKEN").ok().map(Into::into);
    // HTTP range GETs give an interpretable baseline. Xet's storage-operation
    // counter does not count individual reconstruction/CAS network requests.
    config.hf.use_xet = std::env::var("MCAP_PROFILE_XET").as_deref() == Ok("1");
    let client = get_io_client(true, Arc::new(config))?;
    let uris = std::env::var("MCAP_PROFILE_URIS").unwrap_or_else(|_| ABC_EPISODE.to_string());
    let max_batches: usize = std::env::var("MCAP_PROFILE_MAX_BATCHES")
        .unwrap_or_else(|_| "1".into())
        .parse()?;
    let options = McapReadOptions {
        topics: std::env::var("MCAP_PROFILE_TOPIC")
            .ok()
            .map(|topic| vec![topic]),
        start_time: std::env::var("MCAP_PROFILE_START_TIME")
            .ok()
            .map(|v| v.parse())
            .transpose()?,
        end_time: std::env::var("MCAP_PROFILE_END_TIME")
            .ok()
            .map(|v| v.parse())
            .transpose()?,
        ..Default::default()
    };
    assert!(!uris.trim().is_empty(), "provide at least one MCAP URI");
    let mut total = (0, 0, 0, 0, 0);
    for (file, uri) in uris.split_whitespace().enumerate() {
        eprintln!("file={file}");
        let io = IOStatsContext::new("remote MCAP profile");
        let start = Instant::now();
        let mut reader =
            NativeMcapReader::new(uri, client.clone(), io.clone(), options.clone()).await?;
        let indexed = reader.indexed();
        report("open", indexed, reader.read_stats(), &io, 0, start);
        let (mut batches, mut rows) = (0, 0);
        let mut eof = false;
        while max_batches == 0 || batches < max_batches {
            let Some(batch) = reader.next_batch().await? else {
                eof = true;
                break;
            };
            rows += batch.len();
            batches += 1;
        }
        let stats = reader.read_stats().clone();
        // Streaming backends publish buffered byte counters on stream drop.
        // Flush them even when a batch cap stops an unindexed scan early.
        drop(reader);
        report(
            if eof { "eof" } else { "batch_cap" },
            indexed,
            &stats,
            &io,
            rows,
            start,
        );
        if uri == ABC_EPISODE
            && options.topics.is_none()
            && options.start_time.is_none()
            && options.end_time.is_none()
        {
            assert!(indexed);
            let expected = if max_batches == 0 {
                ABC_EPISODE_ROWS
            } else {
                max_batches
                    .saturating_mul(options.batch_size)
                    .min(ABC_EPISODE_ROWS)
            };
            assert_eq!(rows, expected, "pinned ABC-130k episode row count changed");
        }
        assert_eq!(
            stats.chunk_reads,
            stats.chunk_fetches + stats.chunk_buffer_hits
        );
        // All supplied URIs should use a remote backend with IOStats wiring.
        assert!(io.load_get_requests() > 0, "no remote GETs were recorded");
        assert!(io.load_bytes_read() > 0, "no remote bytes were recorded");
        total.0 += 1;
        total.1 += rows;
        total.2 += io.load_get_requests();
        total.3 += io.load_head_requests();
        total.4 += io.load_bytes_read();
    }
    eprintln!(
        "total files={} rows={} gets={} heads={} bytes={}",
        total.0, total.1, total.2, total.3, total.4
    );
    Ok(())
}
