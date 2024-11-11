from __future__ import annotations


def test_read_write_config():
    """Test reading and writing Spark configurations."""
    import time

    from pyspark.sql import SparkSession

    from daft.daft import connect_start

    # Start Daft Connect server
    connect_start("sc://localhost:50051")

    # Initialize Spark Connect session
    spark_session: SparkSession = (
        SparkSession.builder.appName("DaftReadWriteConfigTest").remote("sc://localhost:50051").getOrCreate()
    )

    try:
        # Test setting and getting string configuration
        test_key = "spark.test.stringConfig"
        test_value = "test_value"
        spark_session.conf.set(test_key, test_value)
        assert spark_session.conf.get(test_key) == test_value

        # Test setting and getting boolean configuration
        bool_key = "spark.test.booleanConfig"
        bool_value = True
        spark_session.conf.set(bool_key, bool_value)
        assert spark_session.conf.get(bool_key) == str(bool_value).lower()

        # Test setting and getting integer configuration
        int_key = "spark.test.intConfig"
        int_value = 42
        spark_session.conf.set(int_key, int_value)
        assert int(spark_session.conf.get(int_key)) == int_value

        # Test getting non-existent configuration with default value
        default_key = "spark.test.nonexistent"
        default_value = "default"
        assert spark_session.conf.get(default_key, default_value) == default_value

        # Test setting multiple configurations at once
        multi_configs = {"spark.test.multi1": "value1", "spark.test.multi2": "value2", "spark.test.multi3": "value3"}
        for key, value in multi_configs.items():
            spark_session.conf.set(key, value)

        for key, value in multi_configs.items():
            assert spark_session.conf.get(key) == value

        # Test configuration overwrite
        overwrite_key = "spark.test.overwrite"
        spark_session.conf.set(overwrite_key, "initial_value")
        spark_session.conf.set(overwrite_key, "new_value")
        assert spark_session.conf.get(overwrite_key) == "new_value"

        # Test unset configuration
        unset_key = "spark.test.unset"
        spark_session.conf.set(unset_key, "temp_value")
        spark_session.conf.unset(unset_key)

        assert spark_session.conf.get(unset_key) is None

    finally:
        # Clean up Spark session
        spark_session.stop()
        time.sleep(2)  # Allow time for session cleanup
