from __future__ import annotations

import daft
from daft import col


def test_url_parse_basic():
    urls = [
        "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.ar/train-00004-of-00007.parquet",
        "https://user:pass@example.com:8080/path?query=value#fragment",
        "http://localhost/api",
        "ftp://files.example.com/file.txt",
    ]

    df = daft.from_pydict({"urls": urls})

    result = df.select(col("urls").url_parse()).collect()

    assert len(result) == 4
    schema = result.schema()
    assert len(schema) == 1
    field = schema["urls"]
    assert field.name == "urls"
    assert field.dtype.is_struct()

    result_data = result.to_pydict()
    assert len(result_data["urls"]) == 4
    assert all(isinstance(url_struct, dict) for url_struct in result_data["urls"] if url_struct is not None)


def test_url_parse_component_extraction():
    urls = [
        "https://user:pass@example.com:8080/path?query=value#fragment",
        "http://localhost/api",
    ]

    df = daft.from_pydict({"urls": urls})

    result = df.select(col("urls").url_parse().alias("parsed")).select(col("parsed").struct.get("*")).collect()

    data = result.to_pydict()

    assert data["scheme"][0] == "https"
    assert data["username"][0] == "user"
    assert data["password"][0] == "pass"
    assert data["host"][0] == "example.com"
    assert data["port"][0] == 8080
    assert data["path"][0] == "/path"
    assert data["query"][0] == "query=value"
    assert data["fragment"][0] == "fragment"

    assert data["scheme"][1] == "http"
    assert data["username"][1] is None
    assert data["password"][1] is None
    assert data["host"][1] == "localhost"
    assert data["port"][1] is None
    assert data["path"][1] == "/api"
    assert data["query"][1] is None
    assert data["fragment"][1] is None


def test_url_parse_individual_fields():
    urls = ["https://example.com:443/path?query=value#fragment"]

    df = daft.from_pydict({"urls": urls})

    result = (
        df.select(col("urls").url_parse())
        .select(
            col("urls").struct.get("scheme").alias("scheme"),
            col("urls").struct.get("host").alias("host"),
            col("urls").struct.get("port").alias("port"),
            col("urls").struct.get("path").alias("path"),
            col("urls").struct.get("query").alias("query"),
            col("urls").struct.get("fragment").alias("fragment"),
            col("urls").struct.get("username").alias("username"),
            col("urls").struct.get("password").alias("password"),
        )
        .collect()
    )

    data = result.to_pydict()

    assert data["scheme"][0] == "https"
    assert data["host"][0] == "example.com"
    assert data["port"][0] is None
    assert data["path"][0] == "/path"
    assert data["query"][0] == "query=value"
    assert data["fragment"][0] == "fragment"
    assert data["username"][0] is None
    assert data["password"][0] is None


def test_url_parse_null_handling():
    urls = [
        "https://example.com",
        None,
        "http://localhost",
    ]

    df = daft.from_pydict({"urls": urls})

    result = df.select(col("urls").url_parse().alias("parsed")).select(col("parsed").struct.get("*")).collect()

    data = result.to_pydict()

    assert data["scheme"][0] == "https"
    assert data["host"][0] == "example.com"

    assert data["scheme"][1] is None
    assert data["host"][1] is None
    assert data["path"][1] is None

    assert data["scheme"][2] == "http"
    assert data["host"][2] == "localhost"


def test_url_parse_invalid_urls():
    urls = [
        "https://example.com",
        "invalid-url",
        "",
        "not a url at all",
    ]

    df = daft.from_pydict({"urls": urls})

    result = df.select(col("urls").url_parse().alias("parsed")).select(col("parsed").struct.get("*")).collect()

    data = result.to_pydict()

    assert data["scheme"][0] == "https"
    assert data["host"][0] == "example.com"

    for i in [1, 2, 3]:
        assert data["scheme"][i] is None
        assert data["host"][i] is None
        assert data["path"][i] is None
        assert data["query"][i] is None
        assert data["fragment"][i] is None


def test_url_parse_edge_cases():
    urls = [
        "https://example.com",
        "https://example.com/",
        "https://example.com:443",
        "http://example.com:80",
        "https://example.com?query=value",
        "https://example.com#fragment",
        "mailto:user@example.com",
        "file:///path/to/file",
    ]

    df = daft.from_pydict({"urls": urls})

    result = df.select(col("urls").url_parse()).select(col("urls").struct.get("*")).collect()

    data = result.to_pydict()

    expected_schemes = ["https", "https", "https", "http", "https", "https", "mailto", "file"]
    assert data["scheme"] == expected_schemes

    expected_hosts = [
        "example.com",
        "example.com",
        "example.com",
        "example.com",
        "example.com",
        "example.com",
        None,
        None,
    ]
    assert data["host"] == expected_hosts

    expected_ports = [None, None, None, None, None, None, None, None]
    assert data["port"] == expected_ports


def test_url_parse_github_issue_example():
    urls = [
        "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.ar/train-00004-of-00007.parquet",
        "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.ar/train-00005-of-00007.parquet",
        "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.ar/train-00006-of-00007.parquet",
        "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.arc/train-00000-of-00001.parquet",
        "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.ary/train-00000-of-00001.parquet",
        "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.de/train-00013-of-00020.parquet",
        "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.de/train-00014-of-00020.parquet",
        "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.de/train-00015-of-00020.parquet",
        "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.de/train-00016-of-00020.parquet",
        "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.de/train-00017-of-00020.parquet",
        "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.de/train-00018-of-00020.parquet",
    ]

    df = daft.from_pydict({"urls": urls})

    result = df.select(col("urls").url_parse()).select(col("urls").struct.get("*")).collect()

    data = result.to_pydict()

    assert all(scheme == "https" for scheme in data["scheme"])
    assert all(host == "huggingface.co" for host in data["host"])

    for path in data["path"]:
        assert path.startswith("/datasets/wikimedia/wikipedia/resolve/main/")
        assert path.endswith(".parquet")

    assert all(query is None for query in data["query"])
    assert all(fragment is None for fragment in data["fragment"])
    assert all(username is None for username in data["username"])
    assert all(password is None for password in data["password"])

    assert all(port is None for port in data["port"])


def test_url_parse_with_authentication():
    urls = [
        "https://user@example.com/path",
        "https://user:pass@example.com/path",
        "ftp://anonymous:email@ftp.example.com/file",
    ]

    df = daft.from_pydict({"urls": urls})

    result = df.select(col("urls").url_parse().alias("parsed")).select(col("parsed").struct.get("*")).collect()

    data = result.to_pydict()

    assert data["username"][0] == "user"
    assert data["password"][0] is None

    assert data["username"][1] == "user"
    assert data["password"][1] == "pass"

    assert data["username"][2] == "anonymous"
    assert data["password"][2] == "email"
