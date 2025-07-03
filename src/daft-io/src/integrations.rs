use std::sync::Arc;

use bytes::Bytes;

use crate::object_io::ObjectSource;

#[allow(dead_code)]
pub async fn test_full_get(
    client: Arc<dyn ObjectSource>,
    parquet_file_path: &str,
    all_bytes: &Bytes,
) -> crate::Result<()> {
    let first_bytes = client
        .get(parquet_file_path, Some(0..10), None)
        .await?
        .bytes()
        .await?;
    assert_eq!(first_bytes.len(), 10);
    assert_eq!(first_bytes.as_ref(), &all_bytes[..10]);

    let first_bytes = client
        .get(parquet_file_path, Some(10..100), None)
        .await?
        .bytes()
        .await?;
    assert_eq!(first_bytes.len(), 90);
    assert_eq!(first_bytes.as_ref(), &all_bytes[10..100]);

    let last_bytes = client
        .get(
            parquet_file_path,
            Some((all_bytes.len() - 10)..(all_bytes.len() + 10)),
            None,
        )
        .await?
        .bytes()
        .await?;
    assert_eq!(last_bytes.len(), 10);
    assert_eq!(last_bytes.as_ref(), &all_bytes[(all_bytes.len() - 10)..]);

    let invalid_range_ret = client
        .get(
            parquet_file_path,
            Some((all_bytes.len() + 1)..(all_bytes.len() + 10)),
            None,
        )
        .await;
    assert!(invalid_range_ret.is_err());

    let size_from_get_size = client.get_size(parquet_file_path, None).await?;
    assert_eq!(size_from_get_size, all_bytes.len());

    Ok(())
}
