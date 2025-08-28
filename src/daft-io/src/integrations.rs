use std::sync::Arc;

use bytes::Bytes;

use crate::{object_io::ObjectSource, range::GetRange};

#[allow(dead_code)]
pub async fn test_full_get(
    client: Arc<dyn ObjectSource>,
    parquet_file_path: &str,
    all_bytes: &Bytes,
) -> crate::Result<()> {
    let first_ten_bytes = client
        .get(parquet_file_path, Some((0..10).into()), None)
        .await?
        .bytes()
        .await?;
    assert_eq!(first_ten_bytes.len(), 10);
    assert_eq!(first_ten_bytes.as_ref(), &all_bytes[..10]);

    let mid_ninety_bytes = client
        .get(parquet_file_path, Some(GetRange::Bounded(10..100)), None)
        .await?
        .bytes()
        .await?;
    assert_eq!(mid_ninety_bytes.len(), 90);
    assert_eq!(mid_ninety_bytes.as_ref(), &all_bytes[10..100]);

    let last_ten_bytes = client
        .get(parquet_file_path, Some(GetRange::Suffix(10)), None)
        .await?
        .bytes()
        .await?;
    assert_eq!(last_ten_bytes.len(), 10);
    assert_eq!(
        last_ten_bytes.as_ref(),
        &all_bytes[(all_bytes.len() - 10)..]
    );

    let last_ten_bytes = client
        .get(
            parquet_file_path,
            Some(GetRange::Offset(all_bytes.len() - 10)),
            None,
        )
        .await?
        .bytes()
        .await?;
    assert_eq!(last_ten_bytes.len(), 10);
    assert_eq!(
        last_ten_bytes.as_ref(),
        &all_bytes[(all_bytes.len() - 10)..]
    );

    let invalid_range_ret = client
        .get(
            parquet_file_path,
            Some(GetRange::Offset(all_bytes.len() + 1)),
            None,
        )
        .await;

    // Check if this is a HuggingFace source.
    let is_huggingface = client
        .clone()
        .as_any_arc()
        .downcast::<crate::huggingface::HFSource>()
        .is_ok();

    match invalid_range_ret {
        Ok(result) => {
            if is_huggingface {
                // For our HuggingFace reader, we sometimes get 416 status errors for invalid ranges due to
                // CDN issues. We currently ensure the happy path works by dropping range requests when this happens,
                // which means no error is returned.
                let bytes = result.bytes().await?;
                assert_eq!(bytes.len(), all_bytes.len());
                assert_eq!(bytes.as_ref(), all_bytes.as_ref());
            } else {
                // Unexpected: other sources don't succeed with invalid ranges
                panic!("Unexpected success for invalid range request on non-HuggingFace source");
            }
        }
        Err(_) => {
            if is_huggingface {
                // Unexpected: HuggingFace should handle 416 and return full content
                panic!("Unexpected error for invalid range request on HuggingFace source");
            } else {
                // Expected: other sources should return error for invalid ranges.
            }
        }
    }

    let invalid_range_ret = client
        .get(parquet_file_path, Some(GetRange::Bounded(10..10)), None)
        .await;
    assert!(invalid_range_ret.is_err());

    let size_from_get_size = client.get_size(parquet_file_path, None).await?;
    assert_eq!(size_from_get_size, all_bytes.len());

    Ok(())
}
