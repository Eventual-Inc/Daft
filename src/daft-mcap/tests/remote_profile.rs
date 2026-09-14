//! Opt-in profiling of a public remote MCAP.
//! Run via:
//!   cargo test --locked -p daft-mcap --test remote_profile -- --ignored --nocapture

use std::{sync::Arc, time::Instant};

use daft_io::{IOConfig, IOStatsContext, IOStatsRef, get_io_client};
use daft_mcap::{McapReadOptions, McapReader};

// Public gameplay MCAP from https://huggingface.co/datasets/open-world-agents/D2E-480p (~1.4 MiB).
const D2E_RECORDING: &str = "hf://datasets/open-world-agents/D2E-480p/PEAK/recording_20250901_122320__8bd56fb0_split_02.mcap";
const D2E_RECORDING_ROWS: usize = 49_748;

fn report(phase: &str, indexed: bool, io: &IOStatsRef, rows: usize, start: Instant) {
    println!(
        "phase={phase} rows={rows} elapsed_ms={} indexed={} gets={} heads={} lists={} bytes={}",
        start.elapsed().as_millis(),
        indexed,
        io.load_get_requests(),
        io.load_head_requests(),
        io.load_list_requests(),
        io.load_bytes_read(),
    );
}

#[tokio::test]
#[ignore = "downloads remote data from Hugging Face"]
async fn profile_d2e_480p() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = IOConfig::default();
    // NOTE: HTTP mode is a better baseline than Xet.
    // Xet does individual reconstruction/CAS network requests which are not tracked
    let use_xet = std::env::var("MCAP_PROFILE_XET").as_deref() == Ok("1");
    config.hf.use_xet = use_xet;

    let client = get_io_client(true, Arc::new(config))?;
    let io = IOStatsContext::new("remote MCAP profile");
    let start = Instant::now();

    let indexed;
    let mut rows = 0;
    {
        let mut reader = McapReader::new(
            D2E_RECORDING,
            client,
            io.clone(),
            McapReadOptions::default(),
        )
        .await?;
        indexed = reader.indexed();
        report("open", indexed, &io, 0, start);
        while let Some(batch) = reader.next_batch().await? {
            rows += batch.len();
        }
        // Streaming backends publish buffered byte counters on stream drop.
    }

    report("eof", indexed, &io, rows, start);
    assert!(indexed);
    assert_eq!(
        rows, D2E_RECORDING_ROWS,
        "pinned D2E-480p recording row count changed"
    );

    assert!(
        io.load_head_requests() == usize::from(!use_xet),
        "no remote HEADs were recorded"
    );
    assert!(io.load_get_requests() > 0, "no remote GETs were recorded");
    assert!(io.load_bytes_read() > 0, "no remote bytes were recorded");
    Ok(())
}
