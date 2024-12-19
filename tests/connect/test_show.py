from __future__ import annotations


def test_show(spark_session):
    df = spark_session.range(10)
    try:
        df.show()
    except Exception as e:
        assert False, e
