from __future__ import annotations


def test_set_operation(spark_session):
    """Test the Set operation with various data types and edge cases."""
    configs = {
        "spark.test.string": "test_value",
        # todo: I check if non-strings are supported.
        # "spark.test.boolean": True,
        # "spark.test.integer": 42,
        # "spark.test.float": 3.14,
        "spark.test.empty": "",  # Test empty string
        "spark.test.special": "!@#$%^&*()",  # Test special characters
    }

    # Set all configurations
    for key, value in configs.items():
        spark_session.conf.set(key, value)

    # Verify all configurations
    for key, value in configs.items():
        assert str(spark_session.conf.get(key)) == str(value)


def test_get_operations(spark_session):
    """Test various Get operations including Get, GetWithDefault, and GetOption."""
    # Setup test data
    test_data = {"spark.test.existing": "value", "spark.prefix.one": "1", "spark.prefix.two": "2"}
    for key, value in test_data.items():
        spark_session.conf.set(key, value)

    # Test basic Get
    assert spark_session.conf.get("spark.test.existing") == "value"

    # Test GetWithDefault
    assert spark_session.conf.get("spark.test.nonexistent", "default") == "default"
    assert spark_session.conf.get("spark.test.existing", "default") == "value"

    # Test GetOption (if implemented)
    # Note: This might need to be adjusted based on actual GetOption implementation
    assert spark_session.conf.get("spark.test.nonexistent") is None

    # Test GetAll with prefix
    prefix_configs = {key: spark_session.conf.get(key) for key in ["spark.prefix.one", "spark.prefix.two"]}
    assert prefix_configs == {"spark.prefix.one": "1", "spark.prefix.two": "2"}


def test_unset_operation(spark_session):
    """Test the Unset operation with various scenarios."""
    # Setup test data
    spark_session.conf.set("spark.test.temp", "value")

    # Test basic unset
    spark_session.conf.unset("spark.test.temp")
    assert spark_session.conf.get("spark.test.temp") is None

    # Test unset non-existent key (should not raise error)
    spark_session.conf.unset("spark.test.nonexistent")

    # Test unset and then set again
    key = "spark.test.resettable"
    spark_session.conf.set(key, "first")
    spark_session.conf.unset(key)
    spark_session.conf.set(key, "second")
    assert spark_session.conf.get(key) == "second"


def test_edge_cases(spark_session):
    """Test various edge cases and potential error conditions."""
    # Test very long key and value
    long_key = "spark.test." + "x" * 1000
    long_value = "y" * 1000
    spark_session.conf.set(long_key, long_value)
    assert spark_session.conf.get(long_key) == long_value

    # Test unicode characters
    unicode_key = "spark.test.unicode"
    unicode_value = "测试值"
    spark_session.conf.set(unicode_key, unicode_value)
    assert spark_session.conf.get(unicode_key) == unicode_value

    # Test setting same key multiple times rapidly
    key = "spark.test.rapid"
    for i in range(100):
        spark_session.conf.set(key, f"value{i}")
    assert spark_session.conf.get(key) == "value99"

    # Test concurrent modifications (if supported)
    # Note: This might need to be adjusted based on concurrency support
    from concurrent.futures import ThreadPoolExecutor

    key = "spark.test.concurrent"

    def modify_conf(i: int):
        spark_session.conf.set(key, f"value{i}")

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(modify_conf, range(100)))

    assert spark_session.conf.get(key) is not None  # Value should be set to something
