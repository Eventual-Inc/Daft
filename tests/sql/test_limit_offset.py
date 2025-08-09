from __future__ import annotations

import pytest

import daft
from daft import DataType


@pytest.fixture
def input_df():
    df = daft.range(start=0, end=1024, partitions=100)
    df = df.with_columns(
        {
            "name": df["id"].apply(func=lambda x: f"user_{x}", return_dtype=DataType.string()),
            "email": df["id"].apply(func=lambda x: f"user_{x}@getdaft.io", return_dtype=DataType.string()),
        }
    )
    return df


def test_negative_limit(input_df):
    with pytest.raises(Exception) as excinfo:
        daft.sql("SELECT name FROM input_df LIMIT -1", **{"input_df": input_df})
    assert "LIMIT <n> must be greater than or equal to 0, instead got: -1" in str(excinfo.value)


def test_limit(input_df):
    df = daft.sql("SELECT name FROM input_df LIMIT 0", **{"input_df": input_df})
    assert df.count_rows() == 0

    df = daft.sql("SELECT name FROM input_df WHERE id > 127 LIMIT 1", **{"input_df": input_df})
    assert df.count_rows() == 1

    df = daft.sql("SELECT name FROM input_df LIMIT 1024", **{"input_df": input_df})
    assert df.count_rows() == 1024

    df = daft.sql("SELECT id, name FROM input_df WHERE id >= 25 LIMIT 1024", **{"input_df": input_df})
    assert df.count_rows() == 999

    df = daft.sql("SELECT id, name FROM input_df WHERE id >= 1023 LIMIT 1024", **{"input_df": input_df})
    assert df.count_rows() == 1

    df = daft.sql("SELECT id, name FROM input_df WHERE id > 1023 LIMIT 1024", **{"input_df": input_df})
    assert df.count_rows() == 0

    df = daft.sql("SELECT name FROM input_df LIMIT 9223372036854775807", **{"input_df": input_df})
    assert df.count_rows() == 1024


def test_limit_with_sort(input_df):
    df = daft.sql("SELECT name FROM input_df ORDER BY id LIMIT 1", **{"input_df": input_df})
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(0, 1)]}

    df = daft.sql("SELECT name FROM input_df WHERE id > 17 ORDER BY id LIMIT 1", **{"input_df": input_df})
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(18, 19)]}

    df = daft.sql(
        """
        SELECT name
        FROM (SELECT *
              FROM input_df
              WHERE id > 17
              ORDER BY id LIMIT 185)
        WHERE id < 100
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(18, 100)]}

    df = daft.sql(
        """
        SELECT name
        FROM (SELECT *
              FROM (SELECT *
                    FROM input_df
                    WHERE id > 17
                    ORDER BY id LIMIT 15)
              WHERE id < 100)
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(18, 33)]}

    df = daft.sql("SELECT id, name FROM input_df ORDER BY id DESC LIMIT 1024", **{"input_df": input_df})
    assert df.to_pydict() == {
        "id": [i for i in range(1023, -1, -1)],
        "name": [f"user_{i}" for i in range(1023, -1, -1)],
    }

    df = daft.sql("SELECT id, name FROM input_df WHERE id >= 24 ORDER BY id DESC LIMIT 1024", **{"input_df": input_df})
    assert df.to_pydict() == {
        "id": [i for i in range(1023, 23, -1)],
        "name": [f"user_{i}" for i in range(1023, 23, -1)],
    }

    df = daft.sql("SELECT id, name FROM input_df WHERE id >= 24 ORDER BY id DESC LIMIT 1000", **{"input_df": input_df})
    assert df.to_pydict() == {
        "id": [i for i in range(1023, 23, -1)],
        "name": [f"user_{i}" for i in range(1023, 23, -1)],
    }

    df = daft.sql("SELECT id, name FROM input_df WHERE id >= 24  ORDER BY id DESC LIMIT 100", **{"input_df": input_df})
    assert df.to_pydict() == {
        "id": [i for i in range(1023, 923, -1)],
        "name": [f"user_{i}" for i in range(1023, 923, -1)],
    }

    df = daft.sql(
        """
        SELECT id, name
        FROM (SELECT id, name
              FROM input_df
              WHERE id >= 24
              ORDER BY id DESC LIMIT 100)
        WHERE id < 1000
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {
        "id": [i for i in range(999, 923, -1)],
        "name": [f"user_{i}" for i in range(999, 923, -1)],
    }

    df = daft.sql(
        """
        SELECT id, name
        FROM (SELECT id, name
              FROM input_df
              WHERE id >= 24
              ORDER BY id DESC LIMIT 100)
        WHERE id < 100
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {
        "id": [],
        "name": [],
    }

    df = daft.sql("SELECT name FROM input_df ORDER BY id LIMIT 9223372036854775807", **{"input_df": input_df})
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(0, 1024)]}

    df = daft.sql(
        """
        SELECT name
        FROM (SELECT name
              FROM (SELECT id, name
                    FROM input_df
                    ORDER BY id LIMIT 1024)
              WHERE id > 24 LIMIT 17) LIMIT 3
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(25, 28)]}

    df = daft.sql(
        """
        SELECT name
        FROM (SELECT id, name
              FROM (SELECT id, name
                    FROM input_df
                    ORDER BY id LIMIT 1024)
              WHERE id > 24 LIMIT 17)
        WHERE id < 25 LIMIT 3
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {"name": []}


def test_negative_offset(input_df):
    with pytest.raises(Exception) as excinfo:
        daft.sql("SELECT name FROM input_df OFFSET -1 LIMIT 1", **{"input_df": input_df})
    assert "OFFSET <n> must be greater than or equal to 0, instead got: -1" in str(excinfo.value)


def test_offset_without_limit(input_df):
    with pytest.raises(Exception) as excinfo:
        daft.sql("SELECT name FROM input_df OFFSET 17", **{"input_df": input_df}).collect()
    assert "Not Yet Implemented: Offset without limit is unsupported now!" in str(excinfo.value)


def test_limit_before_offset(input_df):
    df = daft.sql("SELECT name FROM input_df LIMIT 7 OFFSET 0", **{"input_df": input_df})
    assert df.count_rows() == 7

    df = daft.sql("SELECT name FROM input_df LIMIT 0 OFFSET 7", **{"input_df": input_df})
    assert df.count_rows() == 0

    df = daft.sql("SELECT id, name FROM input_df WHERE id > 511 LIMIT 7 OFFSET 2", **{"input_df": input_df})
    assert df.count_rows() == 7

    df = daft.sql(
        """
        SELECT *
        FROM (SELECT id, name
              FROM input_df
              WHERE id > 511 LIMIT 7
              OFFSET 2)
        WHERE id < 512
        """,
        **{"input_df": input_df},
    )
    assert df.count_rows() == 0

    df = daft.sql(
        "SELECT name FROM input_df LIMIT 7 OFFSET 7",
        **{"input_df": input_df},
    )
    assert df.count_rows() == 7

    df = daft.sql(
        """
        SELECT *
        FROM (SELECT name
              FROM (SELECT *
                    FROM input_df LIMIT 7
                    OFFSET 1) LIMIT 4
              OFFSET 2) LIMIT 1024
        """,
        **{"input_df": input_df},
    )
    assert df.count_rows() == 4

    df = daft.sql(
        """
        SELECT name
        FROM (SELECT *
              FROM (SELECT id, name FROM input_df LIMIT 1024) LIMIT 17
              OFFSET 5) LIMIT 3
        OFFSET 8
        """,
        **{"input_df": input_df},
    )
    assert df.count_rows() == 3

    df = daft.sql(
        """
        SELECT name
        FROM (SELECT *
              FROM (SELECT id, name
                    FROM input_df
                    WHERE id >= 999 LIMIT 1024) LIMIT 17
              OFFSET 5) LIMIT 3
        OFFSET 8
        """,
        **{"input_df": input_df},
    )
    assert df.count_rows() == 3


def test_limit_after_offset(input_df):
    df = daft.sql("SELECT name FROM input_df OFFSET 2 LIMIT 0", **{"input_df": input_df})
    assert df.count_rows() == 0

    df = daft.sql("SELECT name FROM input_df OFFSET 0 LIMIT 7", **{"input_df": input_df})
    assert df.count_rows() == 7

    df = daft.sql("SELECT name FROM input_df OFFSET 2 LIMIT 7", **{"input_df": input_df})
    assert df.count_rows() == 7

    df = daft.sql(
        """
        SELECT name
        FROM (SELECT id, name
              FROM input_df
              WHERE id > 997) OFFSET 2 LIMIT 7
        """,
        **{"input_df": input_df},
    )
    assert df.count_rows() == 7

    df = daft.sql(
        """
        SELECT name
        FROM (SELECT id, name
              FROM input_df
              WHERE id > 1020) OFFSET 2 LIMIT 7
        """,
        **{"input_df": input_df},
    )
    assert df.count_rows() == 1

    df = daft.sql("SELECT name FROM input_df OFFSET 7 LIMIT 7", **{"input_df": input_df})
    assert df.count_rows() == 7

    df = daft.sql(
        """
        SELECT *
        FROM (SELECT name
              FROM (SELECT id, name
                    FROM input_df OFFSET 7 LIMIT 17) OFFSET 5 LIMIT 11) OFFSET 7
        """,
        **{"input_df": input_df},
    )
    assert df.count_rows() == 4

    df = daft.sql(
        """
        SELECT *
        FROM (SELECT name
              FROM (SELECT id, name
                    FROM input_df
                    WHERE id > 1000
                    OFFSET 7 LIMIT 17) OFFSET 6 LIMIT 11) OFFSET 7
        """,
        **{"input_df": input_df},
    )
    assert df.count_rows() == 3

    df = daft.sql(
        """
        SELECT *
        FROM (SELECT name
              FROM (SELECT name
                    FROM (SELECT *
                          FROM (SELECT id, name
                                FROM input_df OFFSET 7) OFFSET 17 LIMIT 15) LIMIT 12) LIMIT 6) OFFSET 3
        """,
        **{"input_df": input_df},
    )
    assert df.count_rows() == 3


def test_limit_before_offset_with_sort(input_df):
    df = daft.sql(
        """
        SELECT *
        FROM (SELECT id, name
              FROM input_df
              ORDER BY id) LIMIT 7
        OFFSET 0
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {"id": [i for i in range(0, 7)], "name": [f"user_{i}" for i in range(0, 7)]}

    df = daft.sql(
        """
        SELECT *
        FROM (SELECT id, name
              FROM input_df
              ORDER BY id DESC) LIMIT 0
        OFFSET 7
        """,
        **{"input_df": input_df},
    )
    assert df.count_rows() == 0

    df = daft.sql(
        "SELECT id, name FROM input_df ORDER BY id DESC LIMIT 7 OFFSET 2",
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {
        "id": [i for i in range(1021, 1014, -1)],
        "name": [f"user_{i}" for i in range(1021, 1014, -1)],
    }

    df = daft.sql(
        " SELECT id, name FROM input_df WHERE id < 127 ORDER BY id DESC LIMIT 7 OFFSET 2",
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {
        "id": [i for i in range(124, 117, -1)],
        "name": [f"user_{i}" for i in range(124, 117, -1)],
    }

    df = daft.sql(
        """
        SELECT *
        FROM (SELECT id, name
              FROM input_df
              WHERE id < 127
              ORDER BY id DESC LIMIT 7
              OFFSET 2)
        WHERE id >= 122
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {
        "id": [i for i in range(124, 121, -1)],
        "name": [f"user_{i}" for i in range(124, 121, -1)],
    }

    df = daft.sql(
        "SELECT id, name FROM input_df ORDER BY id LIMIT 7 OFFSET 7",
        **{"input_df": input_df},
    )
    assert df.count_rows() == 7

    df = daft.sql(
        """
        SELECT id, name
        FROM (SELECT id, name
              FROM (SELECT id, name
                    FROM input_df
                    ORDER BY id LIMIT 7
                    OFFSET 1) LIMIT 4
              OFFSET 2) LIMIT 1024
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {"id": [i for i in range(3, 7)], "name": [f"user_{i}" for i in range(3, 7)]}

    df = daft.sql(
        """
        SELECT id, name
        FROM (SELECT id, name
              FROM (SELECT id, name
                    FROM input_df
                    WHERE id >= 700
                    ORDER BY id LIMIT 7
                    OFFSET 1) LIMIT 4
              OFFSET 2) LIMIT 1024
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {"id": [i for i in range(703, 707)], "name": [f"user_{i}" for i in range(703, 707)]}

    df = daft.sql(
        """
        SELECT name
        FROM (SELECT name
              FROM (SELECT name
                    FROM (SELECT *
                          FROM (SELECT id, name FROM input_df LIMIT 1024)
                          ORDER BY id LIMIT 17
                          OFFSET 5) OFFSET 2) OFFSET 6 LIMIT 3)
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(13, 16)]}

    df = daft.sql(
        """
        SELECT name
        FROM (SELECT name
              FROM (SELECT *
                    FROM (SELECT *
                          FROM (SELECT id, name FROM input_df ORDER BY id LIMIT 1024)
                          WHERE id >= 13 LIMIT 17
                          OFFSET 5) OFFSET 2) OFFSET 6 LIMIT 3)
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(26, 29)]}


def test_limit_after_offset_with_sort(input_df):
    df = daft.sql(
        "SELECT id, name FROM input_df ORDER BY id OFFSET 2 LIMIT 0",
        **{"input_df": input_df},
    )
    assert df.count_rows() == 0

    df = daft.sql(
        "SELECT id, name FROM input_df ORDER BY id OFFSET 0 LIMIT 7",
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {"id": [i for i in range(0, 7)], "name": [f"user_{i}" for i in range(0, 7)]}

    df = daft.sql(
        "SELECT id, name FROM input_df ORDER BY id DESC OFFSET 2 LIMIT 7",
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {
        "id": [i for i in range(1021, 1014, -1)],
        "name": [f"user_{i}" for i in range(1021, 1014, -1)],
    }

    df = daft.sql(
        "SELECT id, name FROM input_df WHERE id <= 100 ORDER BY id DESC OFFSET 2 LIMIT 7",
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {
        "id": [i for i in range(98, 91, -1)],
        "name": [f"user_{i}" for i in range(98, 91, -1)],
    }

    df = daft.sql(
        "SELECT id, name FROM input_df ORDER BY id DESC OFFSET 7 LIMIT 7",
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {
        "id": [i for i in range(1016, 1009, -1)],
        "name": [f"user_{i}" for i in range(1016, 1009, -1)],
    }

    df = daft.sql(
        """
        SELECT *
        FROM (SELECT id, name
              FROM input_df
              ORDER BY id DESC
              OFFSET 7 LIMIT 17) OFFSET 5 LIMIT 8
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {
        "id": [i for i in range(1011, 1003, -1)],
        "name": [f"user_{i}" for i in range(1011, 1003, -1)],
    }

    df = daft.sql(
        """
        SELECT id, name
        FROM (SELECT *
              FROM input_df
              WHERE id <= 1004
              ORDER BY id DESC
              OFFSET 7 LIMIT 17) OFFSET 5 LIMIT 7
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {
        "id": [i for i in range(992, 985, -1)],
        "name": [f"user_{i}" for i in range(992, 985, -1)],
    }

    df = daft.sql(
        """
        SELECT name
        FROM (SELECT name
              FROM (SELECT name
                    FROM (SELECT name
                          FROM (SELECT *
                                FROM (SELECT id, name
                                      FROM input_df
                                      ORDER BY id DESC LIMIT 1024) OFFSET 7) OFFSET 17) LIMIT 15) LIMIT 12) LIMIT 6
        OFFSET 3
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(996, 990, -1)]}

    df = daft.sql(
        """
        SELECT *
        FROM (SELECT name
              FROM (SELECT name
                    FROM (SELECT name
                          FROM (SELECT name
                                FROM (SELECT name
                                      FROM (SELECT id, name
                                            FROM input_df
                                            WHERE id <= 523
                                            ORDER BY id DESC LIMIT 1024) OFFSET 7) OFFSET 17) LIMIT 15) LIMIT 12) LIMIT 6) OFFSET 3
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(496, 493, -1)]}


def test_paging(input_df):
    offset = 0
    limit = 100

    total = input_df.count_rows()
    while offset < total:
        df = daft.sql(
            f"SELECT id, name FROM input_df ORDER BY id OFFSET {offset} LIMIT {limit}",
            **{"input_df": input_df},
        )

        assert df.to_pydict() == {
            "id": [i for i in range(offset, min(total, offset + limit))],
            "name": [f"user_{i}" for i in range(offset, min(total, offset + limit))],
        }

        offset += limit


def test_offset_limit_with_join(input_df):
    df = daft.sql(
        """
        SELECT tb1.name as name1, tb2.name as name2
        FROM tb1
                 JOIN tb2 ON tb1.id = tb2.id
        WHERE tb1.id > 5
        ORDER BY tb1.id
        OFFSET 2 LIMIT 3
        """,
        **{"tb1": input_df, "tb2": input_df},
    )
    assert df.to_pydict() == {
        "name1": [f"user_{i}" for i in range(8, 11)],
        "name2": [f"user_{i}" for i in range(8, 11)],
    }

    df = daft.sql(
        """
        SELECT id, name
        FROM input_df
                 JOIN (SELECT *
                       FROM input_df
                       ORDER BY id
                       OFFSET 7 LIMIT 10) AS join_df ON input_df.id = join_df.id
        ORDER BY id DESC LIMIT 10
        OFFSET 7
        """,
        **{"input_df": input_df},
    )
    assert df.to_pydict() == {
        "id": [i for i in range(9, 6, -1)],
        "name": [f"user_{i}" for i in range(9, 6, -1)],
    }
