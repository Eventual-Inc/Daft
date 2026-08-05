import pytest
import pyarrow as pa
from daft import Series

@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pa.array(["hello world", "foo@bar.com", "a/b?c=d", None]),
            pa.array(["hello%20world", "foo%40bar.com", "a%2Fb%3Fc%3Dd", None]),
        ),
    ],
)
def test_series_utf8_url_encode(data, expected) -> None:
    s = Series.from_arrow(data)
    result = s.str.url_encode()
    assert result.to_arrow() == expected

@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pa.array(["hello%20world", "foo%40bar.com", "a%2Fb%3Fc%3Dd", None]),
            pa.array(["hello world", "foo@bar.com", "a/b?c=d", None]),
        ),
    ],
)
def test_series_utf8_url_decode(data, expected) -> None:
    s = Series.from_arrow(data)
    result = s.str.url_decode()
    assert result.to_arrow() == expected
