import numpy as np

import daft
from daft.sql.sql import SQLCatalog


def test_floats():
    df = daft.from_pydict(
        {
            "nans": [1.0, 2.0, np.nan, 4.0],
            "infs": [1.0, 2.0, np.inf, np.inf],
        }
    )
    catalog = SQLCatalog({"test": df})

    sql = """
    SELECT
        is_nan(nans) as is_nan,
        is_inf(infs) as is_inf,
        not_nan(nans) as not_nan,
        fill_nan(nans, 0.0) as fill_nan
    FROM test
    """
    df = daft.sql(sql, catalog=catalog).collect()
    expected = {
        "is_nan": [False, False, True, False],
        "is_inf": [False, False, True, True],
        "not_nan": [True, True, False, True],
        "fill_nan": [1.0, 2.0, 0.0, 4.0],
    }
    actual = df.to_pydict()
    assert actual == expected
