from __future__ import annotations

import daft
from daft import col


class TestUrlParse:
    def test_url_parse_basic(self):
        urls = [
            "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.ar/train-00004-of-00007.parquet",
            "https://user:pass@example.com:8080/path?query=value#fragment",
            "http://localhost/api",
            "ftp://files.example.com/file.txt",
        ]

        df = daft.from_pydict({"urls": urls})

        # Test the parse functionality
        result = df.select(col("urls").url.parse()).collect()

        # Verify the result has the correct structure
        assert len(result) == 4  # 4 URLs in the test data
        schema = result.schema()
        assert len(schema) == 1
        field = schema["urls"]  # Column is auto-aliased to 'url'
        assert field.name == "urls"
        assert field.dtype.is_struct()

        # Verify that parsing creates a struct (detailed field verification is done in other tests)
        result_data = result.to_pydict()
        assert len(result_data["urls"]) == 4
        assert all(isinstance(url_struct, dict) for url_struct in result_data["urls"] if url_struct is not None)

    def test_url_parse_component_extraction(self):
        urls = [
            "https://user:pass@example.com:8080/path?query=value#fragment",
            "http://localhost/api",
        ]

        df = daft.from_pydict({"urls": urls})

        # Extract all components using wildcard
        result = df.select(col("urls").url.parse().alias("parsed")).select(col("parsed").struct.get("*")).collect()

        data = result.to_pydict()

        # Verify first URL components
        assert data["scheme"][0] == "https"
        assert data["username"][0] == "user"
        assert data["password"][0] == "pass"
        assert data["host"][0] == "example.com"
        assert data["port"][0] == 8080
        assert data["path"][0] == "/path"
        assert data["query"][0] == "query=value"
        assert data["fragment"][0] == "fragment"

        # Verify second URL components
        assert data["scheme"][1] == "http"
        assert data["username"][1] is None
        assert data["password"][1] is None
        assert data["host"][1] == "localhost"
        assert data["port"][1] is None
        assert data["path"][1] == "/api"
        assert data["query"][1] is None
        assert data["fragment"][1] is None

    def test_url_parse_individual_fields(self):
        urls = ["https://example.com:443/path?query=value#fragment"]

        df = daft.from_pydict({"urls": urls})

        result = (
            df.select(col("urls").url.parse().alias("parsed"))
            .select(
                col("parsed").struct.get("scheme").alias("scheme"),
                col("parsed").struct.get("host").alias("host"),
                col("parsed").struct.get("port").alias("port"),
                col("parsed").struct.get("path").alias("path"),
                col("parsed").struct.get("query").alias("query"),
                col("parsed").struct.get("fragment").alias("fragment"),
                col("parsed").struct.get("username").alias("username"),
                col("parsed").struct.get("password").alias("password"),
            )
            .collect()
        )

        data = result.to_pydict()

        assert data["scheme"][0] == "https"
        assert data["host"][0] == "example.com"
        assert data["port"][0] is None  # Default HTTPS port is not returned by url crate
        assert data["path"][0] == "/path"
        assert data["query"][0] == "query=value"
        assert data["fragment"][0] == "fragment"
        assert data["username"][0] is None
        assert data["password"][0] is None

    def test_url_parse_null_handling(self):
        urls = [
            "https://example.com",
            None,
            "http://localhost",
        ]

        df = daft.from_pydict({"urls": urls})

        result = df.select(col("urls").url.parse().alias("parsed")).select(col("parsed").struct.get("*")).collect()

        data = result.to_pydict()

        # First URL should be parsed correctly
        assert data["scheme"][0] == "https"
        assert data["host"][0] == "example.com"

        # Second URL (null) should have all null components
        assert data["scheme"][1] is None
        assert data["host"][1] is None
        assert data["path"][1] is None

        # Third URL should be parsed correctly
        assert data["scheme"][2] == "http"
        assert data["host"][2] == "localhost"

    def test_url_parse_invalid_urls(self):
        urls = [
            "https://example.com",  # Valid
            "invalid-url",  # Invalid
            "",  # Empty string
            "not a url at all",  # Invalid
        ]

        df = daft.from_pydict({"urls": urls})

        result = df.select(col("urls").url.parse().alias("parsed")).select(col("parsed").struct.get("*")).collect()

        data = result.to_pydict()

        # First URL should be parsed correctly
        assert data["scheme"][0] == "https"
        assert data["host"][0] == "example.com"

        # Invalid URLs should have all null components
        for i in [1, 2, 3]:
            assert data["scheme"][i] is None
            assert data["host"][i] is None
            assert data["path"][i] is None
            assert data["query"][i] is None
            assert data["fragment"][i] is None

    def test_url_parse_edge_cases(self):
        urls = [
            "https://example.com",  # No path
            "https://example.com/",  # Root path
            "https://example.com:443",  # Standard HTTPS port
            "http://example.com:80",  # Standard HTTP port
            "https://example.com?query=value",  # Query without explicit path
            "https://example.com#fragment",  # Fragment without explicit path
            "mailto:user@example.com",  # Different scheme
            "file:///path/to/file",  # File scheme
        ]

        df = daft.from_pydict({"urls": urls})

        result = df.select(col("urls").url.parse().alias("parsed")).select(col("parsed").struct.get("*")).collect()

        data = result.to_pydict()

        # Verify schemes are parsed correctly
        expected_schemes = ["https", "https", "https", "http", "https", "https", "mailto", "file"]
        assert data["scheme"] == expected_schemes

        # Verify hosts are parsed correctly
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

        # Verify ports are parsed correctly (only non-default ports are returned)
        expected_ports = [None, None, None, None, None, None, None, None]
        assert data["port"] == expected_ports

    def test_url_parse_github_issue_example(self):
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

        # Parse URLs and expand all fields (column is auto-aliased to 'url')
        result = df.select(col("urls").url.parse()).select(col("urls").struct.get("*")).collect()

        data = result.to_pydict()

        # All URLs should have https scheme and huggingface.co host
        assert all(scheme == "https" for scheme in data["scheme"])
        assert all(host == "huggingface.co" for host in data["host"])

        # All should have the expected path structure
        for path in data["path"]:
            assert path.startswith("/datasets/wikimedia/wikipedia/resolve/main/")
            assert path.endswith(".parquet")

        # No query, fragment, username, or password
        assert all(query is None for query in data["query"])
        assert all(fragment is None for fragment in data["fragment"])
        assert all(username is None for username in data["username"])
        assert all(password is None for password in data["password"])

        # No explicit ports (should be None for default HTTPS)
        assert all(port is None for port in data["port"])

    def test_url_parse_with_authentication(self):
        urls = [
            "https://user@example.com/path",  # Username only
            "https://user:pass@example.com/path",  # Username and password
            "ftp://anonymous:email@ftp.example.com/file",  # FTP with credentials
        ]

        df = daft.from_pydict({"urls": urls})

        result = df.select(col("urls").url.parse().alias("parsed")).select(col("parsed").struct.get("*")).collect()

        data = result.to_pydict()

        # First URL: username only
        assert data["username"][0] == "user"
        assert data["password"][0] is None

        # Second URL: username and password
        assert data["username"][1] == "user"
        assert data["password"][1] == "pass"

        # Third URL: FTP credentials
        assert data["username"][2] == "anonymous"
        assert data["password"][2] == "email"
