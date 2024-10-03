use std::{default, io::Write};

use crate::{
    object_io::{FileMetadata, FileType, ObjectSource},
    HttpSource, LocalSource, Result,
};

async fn write_remote_parquet_to_local_file(
    f: &mut tempfile::NamedTempFile,
) -> Result<bytes::Bytes> {
    let parquet_file_path = "https://daft-public-data.s3.us-west-2.amazonaws.com/test_fixtures/parquet_small/0dad4c3f-da0d-49db-90d8-98684571391b-0.parquet";
    let parquet_expected_md5 = "929674747af64a98aceaa6d895863bd3";

    let client = HttpSource::get_client(&default::Default::default()).await?;
    let parquet_file = client.get(parquet_file_path, None, None).await?;
    let bytes = parquet_file.bytes().await?;
    let all_bytes = bytes.as_ref();
    let checksum = format!("{:x}", md5::compute(all_bytes));
    assert_eq!(checksum, parquet_expected_md5);
    f.write_all(all_bytes).unwrap();
    f.flush().unwrap();
    Ok(bytes)
}

#[tokio::test]
async fn test_local_full_get() -> Result<()> {
    let mut file1 = tempfile::NamedTempFile::new().unwrap();
    let bytes = write_remote_parquet_to_local_file(&mut file1).await?;

    let parquet_file_path = format!("file://{}", file1.path().to_str().unwrap());
    let client = LocalSource::get_client().await?;

    let try_all_bytes = client
        .get(&parquet_file_path, None, None)
        .await?
        .bytes()
        .await?;
    assert_eq!(try_all_bytes.len(), bytes.len());
    assert_eq!(try_all_bytes, bytes);

    let first_bytes = client
        .get(&parquet_file_path, Some(0..10), None)
        .await?
        .bytes()
        .await?;
    assert_eq!(first_bytes.len(), 10);
    assert_eq!(first_bytes.as_ref(), &bytes[..10]);

    let first_bytes = client
        .get(&parquet_file_path, Some(10..100), None)
        .await?
        .bytes()
        .await?;
    assert_eq!(first_bytes.len(), 90);
    assert_eq!(first_bytes.as_ref(), &bytes[10..100]);

    let last_bytes = client
        .get(
            &parquet_file_path,
            Some((bytes.len() - 10)..(bytes.len() + 10)),
            None,
        )
        .await?
        .bytes()
        .await?;
    assert_eq!(last_bytes.len(), 10);
    assert_eq!(last_bytes.as_ref(), &bytes[(bytes.len() - 10)..]);

    let size_from_get_size = client.get_size(parquet_file_path.as_str(), None).await?;
    assert_eq!(size_from_get_size, bytes.len());

    Ok(())
}

#[tokio::test]
async fn test_local_full_ls() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let mut file1 = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
    write_remote_parquet_to_local_file(&mut file1).await?;
    let mut file2 = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
    write_remote_parquet_to_local_file(&mut file2).await?;
    let mut file3 = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
    write_remote_parquet_to_local_file(&mut file3).await?;
    let dir_path = format!("file://{}", dir.path().to_string_lossy().replace('\\', "/"));
    let client = LocalSource::get_client().await?;

    let ls_result = client.ls(dir_path.as_ref(), true, None, None, None).await?;
    let mut files = ls_result.files.clone();
    // Ensure stable sort ordering of file paths before comparing with expected payload.
    files.sort_by(|a, b| a.filepath.cmp(&b.filepath));
    let mut expected = vec![
        FileMetadata {
            filepath: format!(
                "file://{}/{}",
                dir.path().to_string_lossy().replace('\\', "/"),
                file1.path().file_name().unwrap().to_string_lossy(),
            ),
            size: Some(file1.as_file().metadata().unwrap().len()),
            filetype: FileType::File,
        },
        FileMetadata {
            filepath: format!(
                "file://{}/{}",
                dir.path().to_string_lossy().replace('\\', "/"),
                file2.path().file_name().unwrap().to_string_lossy(),
            ),
            size: Some(file2.as_file().metadata().unwrap().len()),
            filetype: FileType::File,
        },
        FileMetadata {
            filepath: format!(
                "file://{}/{}",
                dir.path().to_string_lossy().replace('\\', "/"),
                file3.path().file_name().unwrap().to_string_lossy(),
            ),
            size: Some(file3.as_file().metadata().unwrap().len()),
            filetype: FileType::File,
        },
    ];
    expected.sort_by(|a, b| a.filepath.cmp(&b.filepath));
    assert_eq!(files, expected);
    assert_eq!(ls_result.continuation_token, None);

    Ok(())
}
