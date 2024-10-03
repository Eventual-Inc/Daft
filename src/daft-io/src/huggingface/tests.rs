use common_error::DaftResult;

use crate::huggingface::HFPathParts;

#[test]
fn test_parse_hf_parts() -> DaftResult<()> {
    let uri = "hf://datasets/wikimedia/wikipedia/20231101.ab/*.parquet";
    let parts = uri.parse::<HFPathParts>().unwrap();
    let expected = HFPathParts {
        bucket: "datasets".to_string(),
        repository: "wikimedia/wikipedia".to_string(),
        revision: "main".to_string(),
        path: "20231101.ab/*.parquet".to_string(),
    };

    assert_eq!(parts, expected);

    Ok(())
}

#[test]
fn test_parse_hf_parts_with_revision() -> DaftResult<()> {
    let uri = "hf://datasets/wikimedia/wikipedia@dev/20231101.ab/*.parquet";
    let parts = uri.parse::<HFPathParts>().unwrap();
    let expected = HFPathParts {
        bucket: "datasets".to_string(),
        repository: "wikimedia/wikipedia".to_string(),
        revision: "dev".to_string(),
        path: "20231101.ab/*.parquet".to_string(),
    };

    assert_eq!(parts, expected);

    Ok(())
}

#[test]
fn test_parse_hf_parts_with_exact_path() -> DaftResult<()> {
    let uri = "hf://datasets/user/repo@dev/config/my_file.parquet";
    let parts = uri.parse::<HFPathParts>().unwrap();
    let expected = HFPathParts {
        bucket: "datasets".to_string(),
        repository: "user/repo".to_string(),
        revision: "dev".to_string(),
        path: "config/my_file.parquet".to_string(),
    };

    assert_eq!(parts, expected);

    Ok(())
}

#[test]
fn test_parse_hf_parts_with_wildcard() -> DaftResult<()> {
    let uri = "hf://datasets/wikimedia/wikipedia/**/*.parquet";
    let parts = uri.parse::<HFPathParts>().unwrap();
    let expected = HFPathParts {
        bucket: "datasets".to_string(),
        repository: "wikimedia/wikipedia".to_string(),
        revision: "main".to_string(),
        path: "**/*.parquet".to_string(),
    };

    assert_eq!(parts, expected);

    Ok(())
}
