//! Integration tests for the Celeborn FFI shuffle client.
//!
//! These tests require a **running Celeborn cluster** and are gated behind
//! `#[ignore]`. Run them explicitly with:
//!
//! ```bash
//! CELEBORN_CPP_PREFIX=/path/to/celeborn/cpp/build/installed \
//!   cargo test -p daft-shuffles --features celeborn \
//!     --lib 'client::celeborn::integration_tests' \
//!     -- --ignored --nocapture
//! ```
//!
//! The cluster the tests connect to comes from the environment; the defaults
//! assume a LifecycleManager on the local machine:
//! - `CELEBORN_LM_HOST` (default: `127.0.0.1`)
//! - `CELEBORN_LM_PORT` (default: `9097`, Celeborn's default LM port)
//! - `CELEBORN_APP_ID`  (default: `daft-celeborn-integration-test`)

use std::sync::Arc;

use daft_core::{
    datatypes::{Float64Array, Int32Array, Utf8Array},
    series::IntoSeries,
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use futures::StreamExt;

use super::{
    client::{CelebornClient, CelebornClientConfig},
    ffi::ShuffleCelebornClient,
};

/// Helper: read Celeborn connection info from environment variables,
/// falling back to a LifecycleManager listening on localhost.
fn celeborn_test_config() -> CelebornClientConfig {
    let lm_host = std::env::var("CELEBORN_LM_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let lm_port: i32 = std::env::var("CELEBORN_LM_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9097);
    let app_id = std::env::var("CELEBORN_APP_ID")
        .unwrap_or_else(|_| "daft-celeborn-integration-test".to_string());

    CelebornClientConfig {
        lm_host,
        lm_port,
        app_id,
        properties: vec![(
            "celeborn.client.shuffle.compression.codec".to_string(),
            "NONE".to_string(),
        )],
    }
}

/// End-to-end Arrow IPC roundtrip through real Celeborn:
/// MicroPartition → IPC bytes → push_data → mapper_end → read_partition → deserialize → assert equal.
#[tokio::test]
#[ignore = "requires a running Celeborn cluster"]
async fn arrow_ipc_roundtrip_real_celeborn() {
    let config = celeborn_test_config();

    let num_mappers = 2;
    let num_partitions = 4;
    let shuffle_id: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let target_partition: u32 = 2;

    let client = ShuffleCelebornClient::connect(&config).expect("failed to connect");

    client
        .register_shuffle(shuffle_id, num_mappers, num_partitions)
        .await
        .expect("register_shuffle failed");

    // --- Mapper 0: 3 rows ---
    let string_values_m0 = vec!["alpha", "beta", "gamma"];
    let batch_m0 = RecordBatch::from_nonempty_columns(vec![
        Int32Array::from_slice("id", &[10, 20, 30]).into_series(),
        Float64Array::from_slice("score", &[1.5, 2.5, 3.5]).into_series(),
        Utf8Array::from_slice("name", string_values_m0.as_slice()).into_series(),
    ])
    .expect("failed to build batch m0");

    let mp_m0 = MicroPartition::new_loaded(
        batch_m0.schema.clone(),
        Arc::new(vec![batch_m0.clone()]),
        None,
    );
    let ipc_m0 = mp_m0.write_to_ipc_stream().expect("failed to serialize m0");

    client
        .push_data(shuffle_id, 0, 0, target_partition, &ipc_m0)
        .await
        .expect("push_data mapper 0 failed");
    println!(
        "mapper 0: pushed {} IPC bytes to partition {target_partition}",
        ipc_m0.len()
    );

    // --- Mapper 1: 2 rows (same schema) ---
    let string_values_m1 = vec!["delta", "epsilon"];
    let batch_m1 = RecordBatch::from_nonempty_columns(vec![
        Int32Array::from_slice("id", &[40, 50]).into_series(),
        Float64Array::from_slice("score", &[4.5, 5.5]).into_series(),
        Utf8Array::from_slice("name", string_values_m1.as_slice()).into_series(),
    ])
    .expect("failed to build batch m1");

    let mp_m1 = MicroPartition::new_loaded(
        batch_m1.schema.clone(),
        Arc::new(vec![batch_m1.clone()]),
        None,
    );
    let ipc_m1 = mp_m1.write_to_ipc_stream().expect("failed to serialize m1");

    client
        .push_data(shuffle_id, 1, 0, target_partition, &ipc_m1)
        .await
        .expect("push_data mapper 1 failed");
    println!(
        "mapper 1: pushed {} IPC bytes to partition {target_partition}",
        ipc_m1.len()
    );

    // --- mapper_end for both ---
    client
        .mapper_end(shuffle_id, 0, 0)
        .await
        .expect("mapper_end(0) failed");
    client
        .mapper_end(shuffle_id, 1, 0)
        .await
        .expect("mapper_end(1) failed");
    println!("both mappers ended");

    // --- Read partition ---
    let mut stream = client
        .read_partition(shuffle_id, target_partition)
        .await
        .expect("read_partition failed");

    // The client hands back the partition's raw bytes: a concatenation of one
    // self-contained IPC stream per map-side push, with no promise about how it
    // is split across chunks. Framing is the decoder's job, so join the chunks
    // and decode the whole thing.
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("stream chunk error");
        chunks.push(chunk);
    }
    let num_bytes: usize = chunks.iter().map(|c| c.len()).sum();
    println!(
        "read {} chunks from partition {target_partition} ({num_bytes} total bytes)",
        chunks.len(),
    );
    assert!(
        num_bytes > 0,
        "partition {target_partition} read back empty"
    );

    let mut partition = Vec::with_capacity(num_bytes);
    for chunk in &chunks {
        partition.extend_from_slice(chunk);
    }

    let all_batches =
        MicroPartition::read_record_batches_from_ipc_streams(bytes::Bytes::from(partition))
            .expect("failed to decode the partition's concatenated IPC streams");
    let total_rows: usize = all_batches.iter().map(|b| b.len()).sum();
    println!("decoded {} batches, {total_rows} rows", all_batches.len());

    assert_eq!(
        total_rows, 5,
        "expected 5 total rows (3 + 2), got {total_rows}"
    );

    // Verify that both mappers' data is present (order may vary).
    let has_m0 = all_batches.contains(&batch_m0);
    let has_m1 = all_batches.contains(&batch_m1);
    assert!(has_m0, "mapper 0 data not found in the decoded batches");
    assert!(has_m1, "mapper 1 data not found in the decoded batches");

    println!("Arrow IPC roundtrip through real Celeborn PASSED");
}
