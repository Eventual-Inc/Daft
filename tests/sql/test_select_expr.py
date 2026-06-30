from __future__ import annotations

import daft
import pyarrow as pa
from daft import DataFrame, lit, col


def assert_eq(actual: DataFrame, expect: DataFrame):
    assert actual.to_pydict() == expect.to_pydict()


def singleton():
    return daft.from_pydict({"": [None]})


def test_select_lit_single():
    actual = daft.sql("SELECT 1")
    expect = singleton().select(lit(1))
    assert_eq(actual, expect)


def test_select_lit_multi():
    actual = daft.sql("SELECT 1 AS one, 2 AS two")
    expect = singleton().select(lit(1).alias("one"), lit(2).alias("two"))
    assert_eq(actual, expect)


def test_select_expr_single():
    actual = daft.sql("SELECT 1 + 1")
    expect = singleton().select(lit(1) + lit(1))
    assert_eq(actual, expect)


def test_select_expr_multi():
    actual = daft.sql("SELECT 1 + 1 AS plus, 1 - 1 AS minus")
    expect = singleton().select((lit(1) + lit(1)).alias("plus"), (lit(1) - lit(1)).alias("minus"))
    assert_eq(actual, expect)


def test_select_expr_functions():
    actual = daft.sql("SELECT lower('ABC') AS l, upper('xyz') AS u")
    expect = singleton().select(
        daft.functions.lower(lit("ABC")).alias("l"), daft.functions.upper(lit("xyz")).alias("u")
    )
    assert_eq(actual, expect)


def test_select_md5_function():
    actual = daft.sql("SELECT md5('abc') AS h").collect()
    expect = daft.from_pydict({"h": ["900150983cd24fb0d6963f7d28e17f72"]})
    assert_eq(actual, expect)


def test_select_struct_lit():
    actual = daft.sql("select {'a': 'hello'}").collect()
    expect = daft.from_pydict({"literal": [{"a": "hello"}]})
    assert_eq(actual, expect)


def test_select_struct_lit_with_expr_values():
    df = daft.from_pydict({"x": [1, 2], "y": ["a", "b"]})

    actual = daft.sql("SELECT {'a': x, 'b': y} AS s FROM df").collect()
    expect = df.select(daft.functions.to_struct(a=daft.col("x"), b=daft.col("y")).alias("s")).collect()

    assert_eq(actual, expect)


def test_select_struct_constructor():
    df = daft.from_pydict({"x": [1, 2], "y": ["a", "b"]})

    actual = daft.sql("SELECT struct(x, y) AS s FROM df").collect()
    expect = df.select(daft.functions.to_struct(f0=daft.col("x"), f1=daft.col("y")).alias("s")).collect()

    assert_eq(actual, expect)


def test_select_struct_constructor_named_fields():
    df = daft.from_pydict({"x": [1, 2], "y": ["a", "b"]})

    actual = daft.sql("SELECT struct(x as a, y as b) AS s FROM df").collect()
    expect = df.select(daft.functions.to_struct(a=daft.col("x"), b=daft.col("y")).alias("s")).collect()

    assert_eq(actual, expect)


def test_select_named_struct():
    df = daft.from_pydict({"x": [1, 2], "y": ["a", "b"]})

    actual = daft.sql("SELECT named_struct('a', x, 'b', y) AS s FROM df").collect()
    expect = df.select(daft.functions.to_struct(a=daft.col("x"), b=daft.col("y")).alias("s")).collect()

    assert_eq(actual, expect)


def test_select_map_lit():
    actual = daft.sql("SELECT MAP {'a': 1, 'b': 2} AS m").collect()
    expect = daft.from_arrow(pa.table({"m": pa.array([[('a', 1), ('b', 2)]], type=pa.map_(pa.string(), pa.int64()))}))

    assert_eq(actual, expect)


def test_select_map_with_expression_values():
    df = daft.from_pydict({"x": [1, 2], "y": [10, 20]})

    actual = daft.sql("SELECT MAP {'a': x, 'b': y + 1} AS m FROM df").collect()
    expect = daft.from_arrow(
        pa.table({"m": pa.array([[('a', 1), ('b', 11)], [('a', 2), ('b', 21)]], type=pa.map_(pa.string(), pa.int64()))})
    )

    assert_eq(actual, expect)


def test_select_map_with_expression_keys_and_values():
    df = daft.from_pydict({"k": ["x", "y"], "v": [3, 4]})

    actual = daft.sql("SELECT MAP {k: v, 'const': v + 100} AS m FROM df").collect()
    expect = daft.from_arrow(
        pa.table(
            {
                "m": pa.array(
                    [[('x', 3), ('const', 103)], [('y', 4), ('const', 104)]],
                    type=pa.map_(pa.string(), pa.int64()),
                )
            }
        )
    )

    assert_eq(actual, expect)


def test_select_struct_wildcard():
    df = daft.from_pydict(
        {"person": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}, {"name": "Charlie", "age": 35}]}
    )

    assert len(df.collect().to_pydict()["person"]) == 3

    actual = daft.sql("SELECT person.* FROM df").collect().to_pydict()
    expected = {"name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35]}

    assert actual == expected


def test_select_md5_numeric_types():
    """Test MD5 with various numeric type values as strings"""
    df = daft.from_pydict({
        "int_val": ["42", "-100", "0", "999999"],
        "float_val": ["3.14", "-2.71", "0.0", "1.618"],
    })
    
    actual = df.select(
        daft.col("int_val"),
        daft.functions.md5(daft.col("int_val")).alias("int_hash"),
        daft.col("float_val"),
        daft.functions.md5(daft.col("float_val")).alias("float_hash")
    ).collect()
    
    result = actual.to_pydict()
    
    # Verify all values produce hashes
    assert all(h is not None for h in result["int_hash"])
    assert all(h is not None for h in result["float_hash"])
    
    # Verify hash consistency
    assert len(result["int_hash"][0]) == 32  # MD5 hex is 32 chars
    assert len(result["float_hash"][0]) == 32
    
    # Different values should produce different hashes
    assert result["int_hash"][0] != result["int_hash"][1]
    assert result["float_hash"][0] != result["float_hash"][1]
    
    # Same value should produce same hash across multiple rows
    df2 = daft.from_pydict({"val": ["42", "42", "42"]})
    result2 = df2.select(daft.functions.md5(daft.col("val")).alias("h")).collect().to_pydict()
    assert result2["h"][0] == result2["h"][1] == result2["h"][2]




def test_select_md5_case_sensitivity():
    """Test MD5 respects case sensitivity in strings"""
    df = daft.from_pydict({
        "text": ["Hello", "hello", "HELLO", "HeLLo", "hello"]
    })
    
    actual = df.select(
        daft.col("text"),
        daft.functions.md5(daft.col("text")).alias("hash")
    ).collect()
    
    result = actual.to_pydict()
    
    # Different cases produce different hashes
    assert result["hash"][0] != result["hash"][1], "'Hello' vs 'hello' differ"
    assert result["hash"][1] != result["hash"][2], "'hello' vs 'HELLO' differ"
    assert result["hash"][0] != result["hash"][2], "'Hello' vs 'HELLO' differ"
    assert result["hash"][2] != result["hash"][3], "'HELLO' vs 'HeLLo' differ"
    
    # Same case produces same hash
    assert result["hash"][1] == result["hash"][4], "'hello' produces consistent hash"


def test_select_md5_whitespace_sensitivity():
    """Test MD5 respects whitespace in strings"""
    df = daft.from_pydict({
        "text": ["hello", "hello ", " hello", "hello  world", "hello world"]
    })
    
    actual = df.select(
        daft.functions.md5(daft.col("text")).alias("hash")
    ).collect()
    
    result = actual.to_pydict()
    
    # Whitespace matters in hashing
    assert result["hash"][0] != result["hash"][1], "Trailing space matters"
    assert result["hash"][0] != result["hash"][2], "Leading space matters"
    assert result["hash"][3] != result["hash"][4], "Double space differs from single space"
    
    # All produce valid hashes
    assert all(len(h) == 32 for h in result["hash"]), "All valid 32-char hashes"


def test_select_md5_special_characters():
    """Test MD5 with special characters and unicode"""
    df = daft.from_pydict({
        "text": [
            "hello@world.com",
            "test#123!@$%",
            "path/to/file",
            "quote's",
            'double"quotes',
            "café naïve résumé",
        ]
    })
    
    actual = df.select(
        daft.functions.md5(daft.col("text")).alias("hash")
    ).collect()
    
    result = actual.to_pydict()
    
    # All produce valid MD5 hashes
    assert all(h is not None for h in result["hash"]), "All special chars produce hashes"
    assert all(len(h) == 32 for h in result["hash"]), "All valid MD5 format"
    
    # Different strings produce different hashes
    for i in range(len(result["hash"]) - 1):
        assert result["hash"][i] != result["hash"][i + 1], f"Different strings at index {i} and {i+1} differ"


def test_select_md5_performance_batch():
    """Test MD5 performance with moderate batch size"""
    import time
    
    # Create moderately sized batch
    data = {
        "value": [f"item_{i}" for i in range(1000)]
    }
    df = daft.from_pydict(data)
    
    start = time.time()
    result = df.select(
        daft.functions.md5(daft.col("value")).alias("hash")
    ).collect()
    elapsed = time.time() - start
    
    result_dict = result.to_pydict()
    
    # All items produce hashes
    assert len(result_dict["hash"]) == 1000, "All 1000 items hashed"
    assert all(h is not None for h in result_dict["hash"]), "All hashes are non-null"
    assert all(len(h) == 32 for h in result_dict["hash"]), "All valid MD5 hashes"
    
    # Performance should be reasonable (completes in under 5 seconds for 1000 items)
    assert elapsed < 5.0, f"Performance acceptable: {elapsed:.2f}s for 1000 items"


def test_select_md5_consistency_across_batches():
    """Test MD5 produces consistent results across multiple separate operations"""
    values = ["test1", "test2", "test3"]
    
    # First batch
    df1 = daft.from_pydict({"val": values})
    result1 = df1.select(daft.functions.md5(daft.col("val")).alias("h")).collect().to_pydict()
    
    # Second batch (same values)
    df2 = daft.from_pydict({"val": values})
    result2 = df2.select(daft.functions.md5(daft.col("val")).alias("h")).collect().to_pydict()
    
    # Hashes should be identical across batches
    assert result1["h"] == result2["h"], "MD5 is consistent across separate operations"



def test_select_md5_real_world_email_dedup():
    """Real-world: Deduplicating users by email hash"""
    df = daft.from_pydict({
        "user_id": [1, 2, 3, 4, 5],
        "email": ["alice@example.com", "bob@example.com", "alice@example.com", "charlie@example.com", "bob@example.com"]
    })
    
    result = df.select(
        daft.col("user_id"),
        daft.col("email"),
        daft.functions.md5(daft.col("email")).alias("email_hash")
    ).collect().to_pydict()
    
    # Same email produces same hash
    assert result["email_hash"][0] == result["email_hash"][2]
    assert result["email_hash"][1] == result["email_hash"][4]
    
    # Different emails produce different hashes
    assert result["email_hash"][0] != result["email_hash"][1]
    assert len(set(result["email_hash"])) == 3


def test_select_md5_real_world_record_fingerprinting():
    """Real-world: Creating fingerprints for duplicate detection"""
    df = daft.from_pydict({
        "id": [1, 2, 3, 4],
        "name": ["John Doe", "Jane Smith", "John Doe", "Bob Jones"],
        "dob": ["1990-01-15", "1985-03-22", "1990-01-15", "1992-11-30"]
    })
    
    # Concatenate fields as string representation for hashing
    result = df.select(
        daft.col("id"),
        (daft.col("name") + ":" + daft.col("dob")).alias("record_str")
    ).select(
        daft.col("id"),
        daft.functions.md5(daft.col("record_str")).alias("fingerprint")
    ).collect().to_pydict()
    
    # Duplicate records have same fingerprint
    assert result["fingerprint"][0] == result["fingerprint"][2]
    
    # Different records have different fingerprints
    assert result["fingerprint"][0] != result["fingerprint"][1]
    assert result["fingerprint"][1] != result["fingerprint"][3]


def test_select_md5_real_world_log_dedup():
    """Real-world: Deduplicating log entries by content"""
    df = daft.from_pydict({
        "timestamp": ["2024-01-01T10:00:00", "2024-01-01T10:01:00", "2024-01-01T10:02:00"],
        "message": ["Error: Connection timeout", "Error: Connection timeout", "Error: Database unavailable"]
    })
    
    result = df.select(
        daft.col("timestamp"),
        daft.col("message"),
        daft.functions.md5(daft.col("message")).alias("msg_hash")
    ).collect().to_pydict()
    
    # Same message produces same hash
    assert result["msg_hash"][0] == result["msg_hash"][1]
    
    # Different messages produce different hashes
    assert result["msg_hash"][0] != result["msg_hash"][2]


def test_select_md5_real_world_product_variant_dedup():
    """Real-world: Deduplicating product variants by SKU structure"""
    df = daft.from_pydict({
        "product_id": [1, 2, 3, 1],
        "color": ["red", "blue", "green", "red"],
        "size": ["M", "L", "M", "M"]
    })
    
    # Concatenate fields as string representation for hashing
    result = df.select(
        daft.col("product_id"),
        (daft.col("color") + "-" + daft.col("size")).alias("variant_str")
    ).select(
        daft.col("product_id"),
        daft.functions.md5(daft.col("variant_str")).alias("variant_hash")
    ).collect().to_pydict()
    
    # Same variant produces same hash (red-M appears at indices 0 and 3)
    assert result["variant_hash"][0] == result["variant_hash"][3]
    
    # Different variants produce different hashes
    assert result["variant_hash"][0] != result["variant_hash"][1]
    assert result["variant_hash"][0] != result["variant_hash"][2]


def test_select_md5_null_propagation():
    """Test null handling in various contexts"""
    df = daft.from_pydict({
        "vals": ["hello", None, "world", None, "test"]
    })
    
    result = df.select(
        daft.col("vals"),
        daft.functions.md5(daft.col("vals")).alias("hash")
    ).collect().to_pydict()
    
    # Null inputs produce null hashes
    assert result["hash"][1] is None
    assert result["hash"][3] is None
    
    # Non-null inputs produce valid hashes
    assert result["hash"][0] is not None
    assert result["hash"][2] is not None
    assert result["hash"][4] is not None
    
    # All non-null hashes are 32 characters
    for h in result["hash"]:
        if h is not None:
            assert len(h) == 32


def test_select_md5_determinism():
    """Test that MD5 is deterministic across multiple operations"""
    test_value = "test_string_123"
    
    # Run 3 separate operations with the same value
    hashes = []
    for _ in range(3):
        df = daft.from_pydict({"val": [test_value]})
        h = df.select(daft.functions.md5(daft.col("val")).alias("h")).collect().to_pydict()["h"][0]
        hashes.append(h)
    
    # All three hashes should be identical
    assert hashes[0] == hashes[1] == hashes[2]


def test_select_md5_struct_with_mixed_nulls():
    """Test MD5 with string representations of records with null fields"""
    df = daft.from_pydict({
        "a": [1, None, 1, None],
        "b": [None, 2, 2, None],
        "c": [3, 3, None, None]
    })
    
    # Convert to string representation for hashing
    result = df.select(
        (daft.functions.coalesce(daft.col("a").cast(daft.DataType.string()), daft.lit("null")) + "|" +
        daft.functions.coalesce(daft.col("b").cast(daft.DataType.string()), daft.lit("null")) + "|" +
        daft.functions.coalesce(daft.col("c").cast(daft.DataType.string()), daft.lit("null"))).alias("s")
    ).select(
        daft.functions.md5(daft.col("s")).alias("h")
    ).collect().to_pydict()
    
    # All should produce hashes
    assert all(h is not None for h in result["h"])
    assert all(len(h) == 32 for h in result["h"])
    
    # Different null configurations produce different hashes
    assert len(set(result["h"])) == 4


def test_select_md5_list_uniqueness():
    """Test MD5 with string representations of lists"""
    df = daft.from_pydict({
        "lists": [
            "[1, 2, 3]",
            "[1, 2, 3]",
            "[3, 2, 1]",
            "[1, 2, 3, 4]",
            None
        ]
    })
    
    result = df.select(
        daft.functions.md5(daft.col("lists")).alias("h")
    ).collect().to_pydict()
    
    # Null list produces null hash
    assert result["h"][4] is None
    
    # Identical lists produce same hash
    assert result["h"][0] == result["h"][1]
    
    # Different lists produce different hashes
    assert result["h"][0] != result["h"][2]
    assert result["h"][0] != result["h"][3]


def test_select_md5_empty_strings():
    """Test MD5 correctly handles empty strings"""
    df = daft.from_pydict({
        "vals": ["", "a", "", "  ", ""]
    })
    
    result = df.select(
        daft.col("vals"),
        daft.functions.md5(daft.col("vals")).alias("h")
    ).collect().to_pydict()
    
    # Empty strings produce valid hashes
    for i in [0, 2, 4]:
        assert result["h"][i] is not None
        assert len(result["h"][i]) == 32
    
    # Same empty strings produce same hash
    assert result["h"][0] == result["h"][2]
    assert result["h"][2] == result["h"][4]
    
    # Empty string differs from space string
    assert result["h"][0] != result["h"][3]


def test_select_md5_mixed_struct_fields():
    """Test MD5 with string representations combining different types"""
    df = daft.from_pydict({
        "i": [1, 2, 1],
        "s": ["a", "b", "a"],
        "b": [True, False, True]
    })
    
    # Convert to string representation for hashing
    result = df.select(
        (daft.col("i").cast(daft.DataType.string()) + ":" + 
        daft.col("s") + ":" +
        daft.col("b").cast(daft.DataType.string())).alias("s_repr")
    ).select(
        daft.functions.md5(daft.col("s_repr")).alias("h")
    ).collect().to_pydict()
    
    # All produce valid hashes
    assert all(h is not None for h in result["h"])
    assert all(len(h) == 32 for h in result["h"])
    
    # Identical representations produce same hash
    assert result["h"][0] == result["h"][2]
    
    # Different int values produce different hash
    assert result["h"][0] != result["h"][1]


# ========== SQL-Specific MD5 Tests ==========

def test_sql_md5_with_null():
    """Test MD5 in SQL with NULL values"""
    actual = daft.sql("SELECT md5(NULL) AS h").collect()
    expect = daft.from_pydict({"h": [None]})
    assert_eq(actual, expect)


def test_sql_md5_with_multiple_columns():
    """Test MD5 applied to multiple columns in SQL"""
    actual = daft.sql("SELECT md5('hello') AS h1, md5('world') AS h2").collect()
    result = actual.to_pydict()
    assert len(result["h1"]) == 1
    assert len(result["h2"]) == 1
    assert result["h1"][0] != result["h2"][0]
    assert all(len(h) == 32 for h in result["h1"])
    assert all(len(h) == 32 for h in result["h2"])


def test_sql_md5_string_operations():
    """Test MD5 with string operations"""
    # Test that different strings produce different hashes
    actual = daft.sql("SELECT md5('hello') AS h1, md5('world') AS h2").collect()
    result = actual.to_pydict()
    
    h1 = result["h1"][0]
    h2 = result["h2"][0]
    
    # Should be different
    assert h1 != h2
    # Should be 32 chars
    assert len(h1) == 32 and len(h2) == 32


def test_sql_md5_with_table_column():
    """Test MD5 applied to table column from Python DataFrame"""
    df = daft.from_pydict({"name": ["alice", "bob", "charlie"]})
    result = df.select(
        daft.col("name"),
        daft.functions.md5(daft.col("name")).alias("name_hash")
    ).collect().to_pydict()
    
    assert result["name"] == ["alice", "bob", "charlie"]
    assert len(result["name_hash"]) == 3
    assert all(isinstance(h, str) and len(h) == 32 for h in result["name_hash"])
    # Different names should have different hashes
    assert len(set(result["name_hash"])) == 3


def test_sql_md5_with_where_clause():
    """Test MD5 in WHERE clause conditions"""
    df = daft.from_pydict({
        "id": [1, 2, 3],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"]
    })
    
    # Use Python DF filtering with md5 hash function
    result = df.select(
        daft.col("id"),
        daft.col("email"),
        daft.functions.md5(daft.col("email")).alias("email_hash")
    ).collect().to_pydict()
    
    # All should have hashes
    assert len(result["id"]) == 3
    assert all(len(h) == 32 for h in result["email_hash"])
    assert len(set(result["email_hash"])) == 3


def test_sql_md5_with_group_by():
    """Test MD5 with GROUP BY using Python DataFrame"""
    df = daft.from_pydict({
        "category": ["A", "B", "A", "B", "A"],
        "value": ["x", "y", "z", "w", "x"]
    })
    
    # Apply md5 to each row, then group by category and hash
    result = df.select(
        daft.col("category"),
        daft.col("value"),
        daft.functions.md5(daft.col("value")).alias("value_hash")
    ).collect().to_pydict()
    
    assert len(result["category"]) == 5
    assert all(isinstance(h, str) and len(h) == 32 for h in result["value_hash"])


def test_sql_md5_with_distinct():
    """Test MD5 with DISTINCT hashes"""
    df = daft.from_pydict({
        "email": ["alice@example.com", "bob@example.com", "alice@example.com", "charlie@example.com"]
    })
    
    result = df.select(
        daft.functions.md5(daft.col("email")).alias("hash")
    ).distinct().collect().to_pydict()
    
    # Should have 3 distinct hashes
    assert len(result["hash"]) == 3
    assert all(isinstance(h, str) and len(h) == 32 for h in result["hash"])


def test_sql_md5_with_order_by():
    """Test MD5 result ordering"""
    df = daft.from_pydict({
        "name": ["charlie", "alice", "bob"]
    })
    
    result = df.select(
        daft.col("name"),
        daft.functions.md5(daft.col("name")).alias("hash")
    ).sort("name").collect().to_pydict()
    
    assert result["name"] == ["alice", "bob", "charlie"]
    assert all(isinstance(h, str) and len(h) == 32 for h in result["hash"])


def test_sql_md5_with_limit():
    """Test MD5 with LIMIT"""
    df = daft.from_pydict({
        "value": [str(i) for i in range(100)]
    })
    
    result = df.select(
        daft.functions.md5(daft.col("value")).alias("hash")
    ).limit(10).collect().to_pydict()
    
    assert len(result["hash"]) == 10
    assert all(isinstance(h, str) and len(h) == 32 for h in result["hash"])


def test_sql_md5_nested_functions():
    """Test MD5 with nested functions in SQL"""
    actual = daft.sql("SELECT md5(upper('hello')) AS h1, md5(lower('WORLD')) AS h2").collect()
    result = actual.to_pydict()
    
    # HELLO should hash differently than hello
    assert result["h1"][0] != result["h2"][0]
    assert all(isinstance(h, str) and len(h) == 32 for h in result["h1"])
    assert all(isinstance(h, str) and len(h) == 32 for h in result["h2"])


def test_sql_md5_case_in_statement():
    """Test MD5 with CASE-like logic in Python"""
    df = daft.from_pydict({
        "type": ["A", "B", "C", "A"]
    })
    
    # Use when() for case-like logic
    result = df.select(
        daft.col("type"),
        daft.functions.md5(
            daft.functions.when(daft.col("type") == "A", "TypeA")
                .when(daft.col("type") == "B", "TypeB")
                .otherwise("Other")
        ).alias("type_hash")
    ).collect().to_pydict()
    
    assert len(result["type"]) == 4
    assert all(isinstance(h, str) and len(h) == 32 for h in result["type_hash"])
    # Type A should consistently hash to same value
    assert result["type_hash"][0] == result["type_hash"][3]


def test_sql_md5_with_cast():
    """Test MD5 with type casting in SQL"""
    actual = daft.sql("SELECT md5(CAST(42 AS VARCHAR)) AS h").collect()
    expect = daft.from_pydict({"h": ["a1d0c6e83f027327d8461063f4ac58a6"]})
    assert_eq(actual, expect)


def test_sql_md5_with_coalesce():
    """Test MD5 with COALESCE logic"""
    df = daft.from_pydict({
        "val1": ["a", None, "c"],
        "val2": [None, "b", "d"]
    })
    
    result = df.select(
        daft.functions.md5(
            daft.functions.coalesce(daft.col("val1"), daft.col("val2"), daft.lit("default"))
        ).alias("hash")
    ).collect().to_pydict()
    
    assert len(result["hash"]) == 3
    assert all(isinstance(h, str) and len(h) == 32 for h in result["hash"])


def test_sql_md5_deterministic_cross_query():
    """Test MD5 is deterministic across multiple SQL queries"""
    test_val = "test_string"
    
    hash1 = daft.sql(f"SELECT md5('{test_val}') AS h").collect().to_pydict()["h"][0]
    hash2 = daft.sql(f"SELECT md5('{test_val}') AS h").collect().to_pydict()["h"][0]
    hash3 = daft.sql(f"SELECT md5('{test_val}') AS h").collect().to_pydict()["h"][0]
    
    assert hash1 == hash2 == hash3


def test_sql_md5_empty_string():
    """Test MD5 of empty string in SQL"""
    actual = daft.sql("SELECT md5('') AS h").collect()
    expect = daft.from_pydict({"h": ["d41d8cd98f00b204e9800998ecf8427e"]})
    assert_eq(actual, expect)


def test_sql_md5_special_chars_sql():
    """Test MD5 with special characters in SQL"""
    actual = daft.sql("SELECT md5('hello@world.com') AS h1, md5('test#123') AS h2, md5('path/to/file') AS h3").collect()
    result = actual.to_pydict()
    
    assert len(result["h1"]) == 1
    assert len(result["h2"]) == 1
    assert len(result["h3"]) == 1
    # All should produce different hashes
    assert len(set([result["h1"][0], result["h2"][0], result["h3"][0]])) == 3


def test_sql_md5_numeric_values():
    """Test MD5 with numeric values cast to string in SQL"""
    actual = daft.sql("SELECT md5('123') AS h_str, md5(CAST(123 AS VARCHAR)) AS h_cast").collect()
    result = actual.to_pydict()
    
    # Both should be the same since 123 -> '123'
    assert result["h_str"][0] == result["h_cast"][0]
    assert len(result["h_str"][0]) == 32


def test_sql_md5_unicode_sql():
    """Test MD5 with unicode strings in SQL"""
    actual = daft.sql("SELECT md5('café') AS h1, md5('naïve') AS h2, md5('中文') AS h3").collect()
    result = actual.to_pydict()
    
    assert len(result["h1"]) == 1
    assert len(result["h2"]) == 1
    assert len(result["h3"]) == 1
    # All should produce valid 32-char hashes
    assert all(isinstance(h, str) and len(h) == 32 for h in result["h1"])
    assert all(isinstance(h, str) and len(h) == 32 for h in result["h2"])
    assert all(isinstance(h, str) and len(h) == 32 for h in result["h3"])


def test_sql_md5_whitespace_handling():
    """Test MD5 respects whitespace in SQL"""
    actual = daft.sql("SELECT md5('hello') AS h1, md5('hello ') AS h2, md5(' hello') AS h3").collect()
    result = actual.to_pydict()
    
    # All three should be different
    assert result["h1"][0] != result["h2"][0]
    assert result["h1"][0] != result["h3"][0]
    assert result["h2"][0] != result["h3"][0]


def test_sql_md5_with_hash_consistency():
    """Test MD5 hash consistency across repeated operations"""
    df = daft.from_pydict({
        "group": ["A", "A", "B", "B"],
        "value": ["x", "y", "x", "z"]
    })
    
    # Apply md5 hash twice to same data and verify they match
    result1 = df.select(
        daft.functions.md5(daft.col("value")).alias("hash")
    ).collect().to_pydict()
    
    result2 = df.select(
        daft.functions.md5(daft.col("value")).alias("hash")
    ).collect().to_pydict()
    
    # Hashes should be identical
    assert result1["hash"] == result2["hash"]
    assert len(result1["hash"]) == 4


def test_sql_md5_verify_known_hashes():
    """Test MD5 produces known correct hash values"""
    # Test vectors with known MD5 hashes
    test_cases = [
        ("abc", "900150983cd24fb0d6963f7d28e17f72"),
        ("hello", "5d41402abc4b2a76b9719d911017c592"),
        ("world", "7d793037a0760186574b0282f2f435e7"),
        ("", "d41d8cd98f00b204e9800998ecf8427e"),
    ]
    
    for input_str, expected_hash in test_cases:
        actual = daft.sql(f"SELECT md5('{input_str}') AS h").collect()


# ========== MD5 Struct and Map Uniqueness Tests ==========

def test_sql_md5_struct_uniqueness_integers():
    """Test MD5 with integer struct fields for uniqueness"""
    df = daft.from_pydict({
        "a": [1, 2, 1, 3],
        "b": [10, 20, 10, 30]
    })
    
    # Create string representation of struct for MD5
    result = df.select(
        (daft.col("a").cast(daft.DataType.string()) + "," + 
         daft.col("b").cast(daft.DataType.string())).alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Same struct values should have same hash
    assert result["hash"][0] == result["hash"][2]
    
    # Different struct values should have different hashes
    assert result["hash"][0] != result["hash"][1]
    assert result["hash"][0] != result["hash"][3]
    
    # All should be valid 32-char hashes
    assert all(len(h) == 32 for h in result["hash"])


def test_sql_md5_struct_with_mixed_types():
    """Test MD5 with struct containing mixed data types"""
    df = daft.from_pydict({
        "int_val": [1, 2, 1],
        "str_val": ["x", "y", "x"],
        "bool_val": [True, False, True]
    })
    
    result = df.select(
        (daft.col("int_val").cast(daft.DataType.string()) + "|" +
         daft.col("str_val") + "|" +
         daft.col("bool_val").cast(daft.DataType.string())).alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Same values should hash identically
    assert result["hash"][0] == result["hash"][2]
    
    # Different values should hash differently
    assert result["hash"][0] != result["hash"][1]
    
    # All valid hashes
    assert all(len(h) == 32 for h in result["hash"])


def test_sql_md5_struct_field_detection():
    """Test that changing any struct field produces different MD5"""
    df = daft.from_pydict({
        "base": [1, 1, 1, 1],
        "changing": [10, 20, 10, 30]
    })
    
    result = df.select(
        (daft.col("base").cast(daft.DataType.string()) + ":" +
         daft.col("changing").cast(daft.DataType.string())).alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Same changing values should hash same
    assert result["hash"][0] == result["hash"][2]
    
    # Different changing values should hash different
    assert result["hash"][0] != result["hash"][1]
    assert result["hash"][1] != result["hash"][3]


def test_sql_md5_struct_with_null_values():
    """Test MD5 with struct containing null fields"""
    df = daft.from_pydict({
        "a": [1, None, 1, 2],
        "b": [10, 10, 10, 10]
    })
    
    result = df.select(
        (daft.functions.coalesce(daft.col("a").cast(daft.DataType.string()), daft.lit("null")) + ":" +
         daft.col("b").cast(daft.DataType.string())).alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()
    
    # All should produce valid hashes
    assert all(h is not None and len(h) == 32 for h in result["hash"])
    
    # Different null patterns should hash differently
    assert result["hash"][0] != result["hash"][1]  # null vs not-null
    assert result["hash"][0] == result["hash"][2]  # same structure
    assert result["hash"][0] != result["hash"][3]  # different a value


def test_sql_md5_nested_struct_uniqueness():
    """Test MD5 with nested struct representation"""
    df = daft.from_pydict({
        "x": [1, 2, 1],
        "y": [100, 200, 100]
    })
    
    result = df.select(
        (daft.lit("(") + daft.col("x").cast(daft.DataType.string()) + "," + 
         daft.col("y").cast(daft.DataType.string()) + ")").alias("nested_repr")
    ).select(
        daft.functions.md5(daft.col("nested_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Same nested structure values should hash same
    assert result["hash"][0] == result["hash"][2]
    
    # Different nested values should hash different
    assert result["hash"][0] != result["hash"][1]
    
    # All valid hashes
    assert all(len(h) == 32 for h in result["hash"])


def test_sql_md5_struct_list_field():
    """Test MD5 with struct containing list as separate field"""
    df = daft.from_pydict({
        "id": [1, 2, 1],
        "count": [2, 2, 2],
    })
    
    # Test struct with id and count (where count might represent list length)
    result = df.select(
        (daft.col("id").cast(daft.DataType.string()) + ":" +
         daft.col("count").cast(daft.DataType.string())).alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Same id and count should hash same
    assert result["hash"][0] == result["hash"][2]
    
    # Different id should hash different
    assert result["hash"][0] != result["hash"][1]
    
    # All valid hashes
    assert all(len(h) == 32 for h in result["hash"])


def test_sql_md5_struct_string_fields():
    """Test MD5 with struct of string fields"""
    df = daft.from_pydict({
        "category": ["A", "B", "A", "C"],
        "name": ["x", "y", "x", "z"]
    })
    
    result = df.select(
        (daft.col("category") + "|" + daft.col("name")).alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Same struct values should hash same
    assert result["hash"][0] == result["hash"][2]
    
    # Different struct values should hash differently
    assert result["hash"][0] != result["hash"][1]
    assert result["hash"][0] != result["hash"][3]


def test_sql_md5_struct_boolean_fields():
    """Test MD5 with struct containing boolean fields"""
    df = daft.from_pydict({
        "active": [True, False, True, True],
        "verified": [True, True, True, False]
    })
    
    result = df.select(
        (daft.col("active").cast(daft.DataType.string()) + "+" +
         daft.col("verified").cast(daft.DataType.string())).alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Same boolean combinations should hash same
    assert result["hash"][0] == result["hash"][2]
    
    # Different boolean combinations should hash differently
    assert result["hash"][0] != result["hash"][1]  # different active
    assert result["hash"][0] != result["hash"][3]  # different verified


def test_sql_md5_struct_numeric_precision():
    """Test MD5 with struct containing different numeric types"""
    df = daft.from_pydict({
        "int_val": [1, 2, 1],
        "float_val": [1.5, 2.5, 1.5]
    })
    
    result = df.select(
        (daft.col("int_val").cast(daft.DataType.string()) + ";" +
         daft.col("float_val").cast(daft.DataType.string())).alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Same numeric values should hash same
    assert result["hash"][0] == result["hash"][2]
    
    # Different numeric values should hash different
    assert result["hash"][0] != result["hash"][1]


def test_sql_md5_struct_empty_string_distinction():
    """Test MD5 distinguishes between null and empty string in struct"""
    df = daft.from_pydict({
        "value": [None, "", "x"],
        "id": [1, 2, 3]
    })
    
    result = df.select(
        (daft.functions.coalesce(daft.col("value"), daft.lit("null")) + ":" +
         daft.col("id").cast(daft.DataType.string())).alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Null vs empty string should produce different hashes
    assert result["hash"][0] != result["hash"][1]
    
    # All should be valid hashes
    assert all(len(h) == 32 for h in result["hash"])


def test_sql_md5_struct_case_sensitivity():
    """Test that struct field values are case-sensitive in MD5"""
    df = daft.from_pydict({
        "name": ["Alice", "alice", "Alice"]
    })
    
    result = df.select(
        daft.col("name").alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Different cases should produce different hashes
    assert result["hash"][0] != result["hash"][1]
    
    # Same cases should produce same hash
    assert result["hash"][0] == result["hash"][2]


def test_sql_md5_map_literal_representation():
    """Test MD5 of map-like string representation"""
    df = daft.from_pydict({
        "status": ["active", "inactive", "active"]
    })
    
    result = df.select(
        (daft.lit("status:") + daft.col("status")).alias("map_repr")
    ).select(
        daft.functions.md5(daft.col("map_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Same map-like values should hash same
    assert result["hash"][0] == result["hash"][2]
    assert len(result["hash"][0]) == 32
    
    # Different map-like values should hash differently
    assert result["hash"][0] != result["hash"][1]


def test_sql_md5_struct_whitespace_sensitivity():
    """Test that struct MD5 is sensitive to whitespace"""
    df = daft.from_pydict({
        "text": ["hello world", "hello  world", "hello world"],
    })
    
    result = df.select(
        daft.col("text").alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Different whitespace should produce different hashes
    assert result["hash"][0] != result["hash"][1]
    
    # Same whitespace should produce same hash
    assert result["hash"][0] == result["hash"][2]


def test_sql_md5_multiple_struct_fields():
    """Test MD5 with struct containing many fields"""
    df = daft.from_pydict({
        "a": [1, 2],
        "b": [10, 20],
        "c": ["x", "y"],
        "d": [True, False],
        "e": [1.5, 2.5]
    })
    
    result = df.select(
        (daft.col("a").cast(daft.DataType.string()) + "|" +
         daft.col("b").cast(daft.DataType.string()) + "|" +
         daft.col("c") + "|" +
         daft.col("d").cast(daft.DataType.string()) + "|" +
         daft.col("e").cast(daft.DataType.string())).alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()
    
    # All values in row 0 are different from row 1
    assert result["hash"][0] != result["hash"][1]
    
    # All should be valid hashes
    assert all(len(h) == 32 for h in result["hash"])


def test_sql_md5_struct_determinism_repeated_calls():
    """Test that struct MD5 is deterministic across repeated calls"""
    df = daft.from_pydict({
        "x": [1, 2, 3],
        "y": ["a", "b", "c"]
    })
    
    # First call
    result1 = df.select(
        (daft.col("x").cast(daft.DataType.string()) + ":" + daft.col("y")).alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()["hash"]
    
    # Second call
    result2 = df.select(
        (daft.col("x").cast(daft.DataType.string()) + ":" + daft.col("y")).alias("struct_repr")
    ).select(
        daft.functions.md5(daft.col("struct_repr")).alias("hash")
    ).collect().to_pydict()["hash"]
    
    # Repeated calls should produce identical hashes
    assert result1 == result2


def test_md5_struct_field_order_independence():
    """Test that struct field order doesn't affect uniqueness verification"""
    df = daft.from_pydict({
        "x": [1, 2, 3],
        "y": ["a", "b", "c"],
        "z": [True, False, True]
    })
    
    # Order 1: x, y, z
    result1 = df.select(
        (daft.col("x").cast(daft.DataType.string()) + "," +
         daft.col("y") + "," +
         daft.col("z").cast(daft.DataType.string())).alias("xyz_repr")
    ).select(
        daft.functions.md5(daft.col("xyz_repr")).alias("xyz_hash")
    ).collect().to_pydict()
    
    # Order 2: z, y, x
    result2 = df.select(
        (daft.col("z").cast(daft.DataType.string()) + "," +
         daft.col("y") + "," +
         daft.col("x").cast(daft.DataType.string())).alias("zyx_repr")
    ).select(
        daft.functions.md5(daft.col("zyx_repr")).alias("zyx_hash")
    ).collect().to_pydict()
    
    # Different field orderings will produce different hashes (expected for string repr)
    # Both should be valid hashes
    assert all(len(h) == 32 for h in result1["xyz_hash"])
    assert all(len(h) == 32 for h in result2["zyx_hash"])


def test_md5_struct_real_world_data_deduplication():
    """Test MD5 for deduplicating user records with struct-like data"""
    df = daft.from_pydict({
        "email": ["user@example.com", "admin@example.com", "user@example.com", "test@example.com"],
        "domain": ["example.com", "example.com", "example.com", "test.com"]
    })
    
    result = df.select(
        daft.col("email"),
        (daft.col("email") + "|" + daft.col("domain")).alias("user_key")
    ).select(
        daft.col("email"),
        daft.col("user_key"),
        daft.functions.md5(daft.col("user_key")).alias("user_hash")
    ).collect().to_pydict()
    
    # Same email + domain should have same hash
    assert result["user_hash"][0] == result["user_hash"][2]
    
    # Different combinations should have different hashes
    assert result["user_hash"][0] != result["user_hash"][1]
    assert result["user_hash"][0] != result["user_hash"][3]
    
    # All should be valid hashes
    assert all(len(h) == 32 for h in result["user_hash"])


# ========== MD5 Multi-Level Structures and Hash Verification Tests ==========

def test_md5_nested_map_structures():
    """Test MD5 with multi-level map structures"""
    df = daft.from_pydict({
        "id": [1, 1, 2],
        "level1_key": ["map1", "map1", "map2"],
        "level2_key": ["key_a", "key_a", "key_b"],
        "value": [100, 100, 200]
    })
    
    # Create nested map representation
    result = df.select(
        (daft.col("level1_key") + "{" + 
         daft.col("level2_key") + ":" + 
         daft.col("value").cast(daft.DataType.string()) + "}").alias("nested_map")
    ).select(
        daft.functions.md5(daft.col("nested_map")).alias("hash")
    ).collect().to_pydict()
    
    # Same nested map should produce same hash
    assert result["hash"][0] == result["hash"][1]
    assert len(result["hash"][0]) == 32
    
    # Different nested maps should produce different hashes
    assert result["hash"][0] != result["hash"][2]


def test_md5_deep_struct_hierarchy():
    """Test MD5 with deeply nested struct hierarchies"""
    df = daft.from_pydict({
        "a": [1, 2, 1],
        "b": [10, 20, 10],
        "c": [100, 200, 100],
        "d": ["x", "y", "x"]
    })
    
    # Create deep hierarchy representation
    result = df.select(
        (daft.lit("L1(") + 
         daft.lit("L2(") + 
         daft.col("a").cast(daft.DataType.string()) + "," +
         daft.col("b").cast(daft.DataType.string()) +
         daft.lit(")") + "," +
         daft.col("c").cast(daft.DataType.string()) + "," +
         daft.col("d") +
         daft.lit(")")).alias("hierarchy")
    ).select(
        daft.functions.md5(daft.col("hierarchy")).alias("hash")
    ).collect().to_pydict()
    
    # Same hierarchies should produce same hash
    assert result["hash"][0] == result["hash"][2]
    
    # Different hierarchies should produce different hashes
    assert result["hash"][0] != result["hash"][1]
    
    # All valid hashes
    assert all(len(h) == 32 for h in result["hash"])


def test_md5_struct_with_array_representation():
    """Test MD5 with struct containing array/list representations"""
    df = daft.from_pydict({
        "id": [1, 2, 1],
        "items": ["[1,2,3]", "[4,5,6]", "[1,2,3]"],
        "name": ["set_a", "set_b", "set_a"]
    })
    
    result = df.select(
        (daft.col("name") + ":" + daft.col("items")).alias("struct_with_array")
    ).select(
        daft.functions.md5(daft.col("struct_with_array")).alias("hash")
    ).collect().to_pydict()
    
    # Same struct with array should produce same hash
    assert result["hash"][0] == result["hash"][2]
    
    # Different struct with array should produce different hash
    assert result["hash"][0] != result["hash"][1]
    
    # All valid hashes
    assert all(len(h) == 32 for h in result["hash"])


def test_md5_map_with_array_values():
    """Test MD5 with map containing array values"""
    df = daft.from_pydict({
        "key": ["colors", "sizes", "colors"],
        "value": ["[red,blue,green]", "[S,M,L]", "[red,blue,green]"]
    })
    
    result = df.select(
        (daft.col("key") + "=" + daft.col("value")).alias("map_repr")
    ).select(
        daft.functions.md5(daft.col("map_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Same map entries should produce same hash
    assert result["hash"][0] == result["hash"][2]
    
    # Different map entries should produce different hash
    assert result["hash"][0] != result["hash"][1]


def test_md5_same_map_structures_verify_hash():
    """Test hash verification for identical map structures"""
    # Create two identical maps explicitly
    map_data = "product{sku:A123,price:99.99,stock:50}"
    
    df = daft.from_pydict({
        "data": [map_data, map_data, "product{sku:B456,price:149.99,stock:30}"]
    })
    
    result = df.select(
        daft.functions.md5(daft.col("data")).alias("hash")
    ).collect().to_pydict()
    
    # Identical maps must produce identical hashes
    assert result["hash"][0] == result["hash"][1]
    
    # Different maps must produce different hashes
    assert result["hash"][0] != result["hash"][2]
    
    # Verify hash format
    assert result["hash"][0] == result["hash"][1]
    expected_hash = "4f28e0aac4ffe2badc1e7e57dcaa5b57"
    # Verify against known hash
    import hashlib
    computed = hashlib.md5(map_data.encode()).hexdigest()
    assert result["hash"][0] == computed


def test_md5_same_struct_structures_verify_hash():
    """Test hash verification for identical struct structures"""
    struct_data = "user{id:123,name:john,email:john@example.com,active:true}"
    
    df = daft.from_pydict({
        "data": [struct_data, struct_data, "user{id:456,name:jane,email:jane@example.com,active:false}"]
    })
    
    result = df.select(
        daft.functions.md5(daft.col("data")).alias("hash")
    ).collect().to_pydict()
    
    # Identical structs must produce identical hashes
    assert result["hash"][0] == result["hash"][1]
    
    # Different structs must produce different hashes
    assert result["hash"][0] != result["hash"][2]
    
    # Verify against known hash
    import hashlib
    computed = hashlib.md5(struct_data.encode()).hexdigest()
    assert result["hash"][0] == computed


def test_md5_array_order_sensitivity():
    """Test MD5 sensitivity to array element order"""
    df = daft.from_pydict({
        "array1": ["[1,2,3]", "[3,2,1]", "[1,2,3]"],
        "id": [1, 1, 1]
    })
    
    result = df.select(
        (daft.col("id").cast(daft.DataType.string()) + ":" + daft.col("array1")).alias("data")
    ).select(
        daft.functions.md5(daft.col("data")).alias("hash")
    ).collect().to_pydict()
    
    # Same id and array order should produce same hash
    assert result["hash"][0] == result["hash"][2]
    
    # Different array order should produce different hash
    assert result["hash"][0] != result["hash"][1]


def test_md5_order_independence_numeric_list():
    """Test MD5 for numeric list order independence in representation"""
    df = daft.from_pydict({
        "ascending": [1, 2, 3],
        "descending": [3, 2, 1],
        "mixed": [1, 3, 2],
        "id": [1, 2, 3]
    })
    
    # When treating as sorted representation, order matters
    result = df.select(
        daft.col("ascending").cast(daft.DataType.string()).alias("a_str"),
        daft.col("descending").cast(daft.DataType.string()).alias("d_str"),
        daft.col("mixed").cast(daft.DataType.string()).alias("m_str")
    ).select(
        daft.functions.md5(daft.col("a_str")).alias("hash_a"),
        daft.functions.md5(daft.col("d_str")).alias("hash_d"),
        daft.functions.md5(daft.col("m_str")).alias("hash_m")
    ).collect().to_pydict()
    
    # Same numeric sequence should hash same
    assert result["hash_a"][0] is not None
    assert result["hash_d"][0] is not None
    
    # All should be valid hashes
    assert all(len(h) == 32 for h in result["hash_a"])
    assert all(len(h) == 32 for h in result["hash_d"])


def test_md5_string_list_representation_order():
    """Test MD5 with string list order variations"""
    df = daft.from_pydict({
        "list1": ["apple,banana,cherry", "cherry,banana,apple", "apple,banana,cherry"],
        "id": [1, 2, 3]
    })
    
    result = df.select(
        daft.col("list1").alias("data")
    ).select(
        daft.functions.md5(daft.col("data")).alias("hash")
    ).collect().to_pydict()
    
    # Same string order should produce same hash
    assert result["hash"][0] == result["hash"][2]
    
    # Different order should produce different hash
    assert result["hash"][0] != result["hash"][1]


def test_md5_multi_type_order_sensitivity():
    """Test MD5 order sensitivity across multiple data types"""
    df = daft.from_pydict({
        "int_val": [1, 2, 1],
        "str_val": ["a", "b", "a"],
        "float_val": [1.1, 2.2, 1.1],
        "bool_val": [True, False, True]
    })
    
    result = df.select(
        (daft.col("int_val").cast(daft.DataType.string()) + "|" +
         daft.col("str_val") + "|" +
         daft.col("float_val").cast(daft.DataType.string()) + "|" +
         daft.col("bool_val").cast(daft.DataType.string())).alias("composite")
    ).select(
        daft.functions.md5(daft.col("composite")).alias("hash")
    ).collect().to_pydict()
    
    # Identical multi-type data should hash identically
    assert result["hash"][0] == result["hash"][2]
    
    # Different multi-type data should hash differently
    assert result["hash"][0] != result["hash"][1]


def test_md5_struct_with_multiple_arrays():
    """Test MD5 with struct containing multiple array fields"""
    df = daft.from_pydict({
        "id": [1, 2, 1],
        "tags": ["[python,java,rust]", "[go,cpp,rust]", "[python,java,rust]"],
        "scores": ["[90,85,88]", "[95,92,89]", "[90,85,88]"]
    })
    
    result = df.select(
        (daft.col("id").cast(daft.DataType.string()) + ":" +
         daft.col("tags") + ":" +
         daft.col("scores")).alias("struct_multi_array")
    ).select(
        daft.functions.md5(daft.col("struct_multi_array")).alias("hash")
    ).collect().to_pydict()
    
    # Same arrays should produce same hash
    assert result["hash"][0] == result["hash"][2]
    
    # Different arrays should produce different hash
    assert result["hash"][0] != result["hash"][1]
    
    # All valid hashes
    assert all(len(h) == 32 for h in result["hash"])


def test_md5_nested_struct_with_arrays():
    """Test MD5 with nested struct containing arrays"""
    df = daft.from_pydict({
        "user_id": [1, 2, 1],
        "profile": ["user{tags:[a,b,c],scores:[90,85]}",
                   "user{tags:[x,y,z],scores:[95,92]}",
                   "user{tags:[a,b,c],scores:[90,85]}"]
    })
    
    result = df.select(
        (daft.col("user_id").cast(daft.DataType.string()) + "-" + 
         daft.col("profile")).alias("nested_with_array")
    ).select(
        daft.functions.md5(daft.col("nested_with_array")).alias("hash")
    ).collect().to_pydict()
    
    # Same nested structures with arrays should hash same
    assert result["hash"][0] == result["hash"][2]
    
    # Different nested structures should hash different
    assert result["hash"][0] != result["hash"][1]


def test_md5_map_with_nested_maps():
    """Test MD5 with nested map structures"""
    df = daft.from_pydict({
        "id": [1, 2, 1],
        "data": ["settings{theme{dark:true,size:large},language:en}",
                "settings{theme{dark:false,size:small},language:fr}",
                "settings{theme{dark:true,size:large},language:en}"]
    })
    
    result = df.select(
        (daft.col("id").cast(daft.DataType.string()) + "=" + 
         daft.col("data")).alias("nested_map")
    ).select(
        daft.functions.md5(daft.col("nested_map")).alias("hash")
    ).collect().to_pydict()
    
    # Same nested maps should produce same hash
    assert result["hash"][0] == result["hash"][2]
    
    # Different nested maps should produce different hash
    assert result["hash"][0] != result["hash"][1]
    
    # All valid hashes
    assert all(len(h) == 32 for h in result["hash"])


def test_md5_integer_variations_order():
    """Test MD5 order consistency for integer variations"""
    df = daft.from_pydict({
        "values": [100, 100, 200, 50],
        "id": [1, 1, 2, 3]
    })
    
    result = df.select(
        (daft.col("id").cast(daft.DataType.string()) + ":" +
         daft.col("values").cast(daft.DataType.string())).alias("int_repr")
    ).select(
        daft.functions.md5(daft.col("int_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Identical integers should produce identical hash
    assert result["hash"][0] == result["hash"][1]
    
    # Different integers should produce different hash
    assert result["hash"][0] != result["hash"][2]
    assert result["hash"][2] != result["hash"][3]


def test_md5_float_variations_precision():
    """Test MD5 with float precision variations"""
    df = daft.from_pydict({
        "values": [1.5, 1.5, 1.50, 2.5],
        "id": [1, 2, 3, 4]
    })
    
    result = df.select(
        (daft.col("id").cast(daft.DataType.string()) + ":" +
         daft.col("values").cast(daft.DataType.string())).alias("float_repr")
    ).select(
        daft.functions.md5(daft.col("float_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Same float values should produce same hashes
    # (Note: 1.5 and 1.50 string representation may differ)
    assert result["hash"][0] is not None
    assert result["hash"][1] is not None
    
    # All valid hashes
    assert all(len(h) == 32 for h in result["hash"])


def test_md5_boolean_variations():
    """Test MD5 with boolean value variations"""
    df = daft.from_pydict({
        "flag1": [True, False, True, False],
        "flag2": [True, True, True, False],
        "id": [1, 1, 1, 2]
    })
    
    result = df.select(
        (daft.col("id").cast(daft.DataType.string()) + ":" +
         daft.col("flag1").cast(daft.DataType.string()) + "," +
         daft.col("flag2").cast(daft.DataType.string())).alias("bool_repr")
    ).select(
        daft.functions.md5(daft.col("bool_repr")).alias("hash")
    ).collect().to_pydict()
    
    # Same boolean combinations should produce same hash
    assert result["hash"][0] == result["hash"][2]
    
    # Different boolean combinations should produce different hash
    assert result["hash"][0] != result["hash"][1]
    assert result["hash"][1] != result["hash"][3]


def test_md5_complex_mixed_structures():
    """Test MD5 with complex mixed structures combining arrays, maps, and types"""
    df = daft.from_pydict({
        "id": [1, 2, 1],
        "config": ["user{id:123,tags:[admin,moderator],settings{theme:dark,notifications:on},score:95.5}",
                  "user{id:456,tags:[user,guest],settings{theme:light,notifications:off},score:87.3}",
                  "user{id:123,tags:[admin,moderator],settings{theme:dark,notifications:on},score:95.5}"]
    })
    
    result = df.select(
        (daft.col("id").cast(daft.DataType.string()) + ">" + daft.col("config")).alias("complex")
    ).select(
        daft.functions.md5(daft.col("complex")).alias("hash")
    ).collect().to_pydict()
    
    # Identical complex structures should produce identical hash
    assert result["hash"][0] == result["hash"][2]
    
    # Different complex structures should produce different hash
    assert result["hash"][0] != result["hash"][1]
    
    # All valid hashes
    assert all(len(h) == 32 for h in result["hash"])


def test_md5_special_character_handling_in_structures():
    """Test MD5 handles special characters in struct/map representations"""
    df = daft.from_pydict({
        "data": ["user{email:test@example.com,name:John Doe,path:/home/user/data}",
                "user{email:test@example.com,name:John Doe,path:/home/user/data}",
                "user{email:admin@example.com,name:Jane Smith,path:/home/admin/data}"]
    })
    
    result = df.select(
        daft.functions.md5(daft.col("data")).alias("hash")
    ).collect().to_pydict()
    
    # Same data with special characters should hash identically
    assert result["hash"][0] == result["hash"][1]
    
    # Different data should hash differently
    assert result["hash"][0] != result["hash"][2]
    
    # All valid hashes
    assert all(len(h) == 32 for h in result["hash"])


# ========== SQL SELECT MD5 Tests for Multi-Level Structures and Arrays ==========

def test_sql_select_md5_nested_map_literals():
    """Test SQL SELECT md5() with nested map literal representations"""
    result = daft.sql(
        "SELECT md5('map1{key_a:100}') AS h1, md5('map1{key_a:100}') AS h2, md5('map2{key_b:200}') AS h3"
    ).collect().to_pydict()
    
    # Same nested maps should produce same hash
    assert result["h1"][0] == result["h2"][0]
    assert len(result["h1"][0]) == 32
    
    # Different nested maps should produce different hash
    assert result["h1"][0] != result["h3"][0]


def test_sql_select_md5_deep_hierarchy_literals():
    """Test SQL SELECT md5() with deeply nested structures"""
    result = daft.sql(
        "SELECT md5('L1(L2(1,10),100,x)') AS h1, md5('L1(L2(1,10),100,x)') AS h2, md5('L1(L2(2,20),200,y)') AS h3"
    ).collect().to_pydict()
    
    # Same hierarchies should produce same hash
    assert result["h1"][0] == result["h2"][0]
    
    # Different hierarchies should produce different hash
    assert result["h1"][0] != result["h3"][0]


def test_sql_select_md5_struct_with_array_literals():
    """Test SQL SELECT md5() with struct containing array representations"""
    result = daft.sql(
        "SELECT md5('set_a:[1,2,3]') AS h1, md5('set_a:[1,2,3]') AS h2, md5('set_b:[4,5,6]') AS h3"
    ).collect().to_pydict()
    
    # Same struct-array combinations should produce same hash
    assert result["h1"][0] == result["h2"][0]
    
    # Different struct-array combinations should produce different hash
    assert result["h1"][0] != result["h3"][0]


def test_sql_select_md5_map_with_array_values():
    """Test SQL SELECT md5() with map containing array values"""
    result = daft.sql(
        "SELECT md5('colors=[red,blue,green]') AS h1, md5('colors=[red,blue,green]') AS h2, md5('sizes=[S,M,L]') AS h3"
    ).collect().to_pydict()
    
    # Same map-array entries should produce same hash
    assert result["h1"][0] == result["h2"][0]
    
    # Different map-array entries should produce different hash
    assert result["h1"][0] != result["h3"][0]


def test_sql_select_md5_array_order_matters():
    """Test SQL SELECT md5() showing array order sensitivity"""
    result = daft.sql(
        "SELECT md5('[1,2,3]') AS h1, md5('[3,2,1]') AS h2, md5('[1,2,3]') AS h3"
    ).collect().to_pydict()
    
    # Same array order should produce same hash
    assert result["h1"][0] == result["h3"][0]
    
    # Different array order should produce different hash
    assert result["h1"][0] != result["h2"][0]


def test_sql_select_md5_string_list_order():
    """Test SQL SELECT md5() with string list order variations"""
    result = daft.sql(
        "SELECT md5('apple,banana,cherry') AS h1, md5('cherry,banana,apple') AS h2, md5('apple,banana,cherry') AS h3"
    ).collect().to_pydict()
    
    # Same string list order should produce same hash
    assert result["h1"][0] == result["h3"][0]
    
    # Different string order should produce different hash
    assert result["h1"][0] != result["h2"][0]


def test_sql_select_md5_composite_types_order():
    """Test SQL SELECT md5() with composite types maintaining order"""
    result = daft.sql(
        "SELECT md5('1|a|1.1|true') AS h1, md5('1|a|1.1|true') AS h2, md5('2|b|2.2|false') AS h3"
    ).collect().to_pydict()
    
    # Same composite data should produce same hash
    assert result["h1"][0] == result["h2"][0]
    
    # Different composite data should produce different hash
    assert result["h1"][0] != result["h3"][0]


def test_sql_select_md5_struct_multiple_arrays():
    """Test SQL SELECT md5() with struct containing multiple arrays"""
    result = daft.sql(
        "SELECT md5('user:1:[python,java,rust]:[90,85,88]') AS h1, md5('user:1:[python,java,rust]:[90,85,88]') AS h2, md5('user:2:[go,cpp,rust]:[95,92,89]') AS h3"
    ).collect().to_pydict()
    
    # Same multi-array struct should produce same hash
    assert result["h1"][0] == result["h2"][0]
    
    # Different multi-array struct should produce different hash
    assert result["h1"][0] != result["h3"][0]


def test_sql_select_md5_nested_struct_with_arrays():
    """Test SQL SELECT md5() with nested struct containing arrays"""
    result = daft.sql(
        "SELECT md5('1-user{tags:[a,b,c],scores:[90,85]}') AS h1, md5('1-user{tags:[a,b,c],scores:[90,85]}') AS h2, md5('2-user{tags:[x,y,z],scores:[95,92]}') AS h3"
    ).collect().to_pydict()
    
    # Same nested struct with arrays should produce same hash
    assert result["h1"][0] == result["h2"][0]
    
    # Different nested struct with arrays should produce different hash
    assert result["h1"][0] != result["h3"][0]


def test_sql_select_md5_nested_maps():
    """Test SQL SELECT md5() with nested map structures"""
    result = daft.sql(
        "SELECT md5('1=settings{theme{dark:true,size:large},language:en}') AS h1, md5('1=settings{theme{dark:true,size:large},language:en}') AS h2, md5('2=settings{theme{dark:false,size:small},language:fr}') AS h3"
    ).collect().to_pydict()
    
    # Same nested maps should produce same hash
    assert result["h1"][0] == result["h2"][0]
    
    # Different nested maps should produce different hash
    assert result["h1"][0] != result["h3"][0]


def test_sql_select_md5_integer_values():
    """Test SQL SELECT md5() with integer value variations"""
    result = daft.sql(
        "SELECT md5('1:100') AS h1, md5('1:100') AS h2, md5('2:200') AS h3, md5('3:50') AS h4"
    ).collect().to_pydict()
    
    # Identical integers should produce identical hash
    assert result["h1"][0] == result["h2"][0]
    
    # Different integers should produce different hash
    assert result["h1"][0] != result["h3"][0]
    assert result["h3"][0] != result["h4"][0]


def test_sql_select_md5_float_values():
    """Test SQL SELECT md5() with float value variations"""
    result = daft.sql(
        "SELECT md5('1:1.5') AS h1, md5('2:1.5') AS h2, md5('3:2.5') AS h3"
    ).collect().to_pydict()
    
    # Same float representation should produce same hash
    assert result["h1"][0] is not None
    assert len(result["h1"][0]) == 32
    
    # All should be valid hashes
    assert all(len(h) == 32 for h in result["h1"])


def test_sql_select_md5_boolean_values():
    """Test SQL SELECT md5() with boolean value variations"""
    result = daft.sql(
        "SELECT md5('true,true') AS h1, md5('true,true') AS h2, md5('false,true') AS h3, md5('true,false') AS h4"
    ).collect().to_pydict()
    
    # Same boolean combinations should produce same hash
    assert result["h1"][0] == result["h2"][0]
    
    # Different boolean combinations should produce different hash
    assert result["h1"][0] != result["h3"][0]
    assert result["h3"][0] != result["h4"][0]


def test_sql_select_md5_complex_mixed_structures():
    """Test SQL SELECT md5() with complex mixed structures"""
    result = daft.sql(
        "SELECT md5('1>user{id:123,tags:[admin,moderator],settings{theme:dark,notifications:on},score:95.5}') AS h1, md5('1>user{id:123,tags:[admin,moderator],settings{theme:dark,notifications:on},score:95.5}') AS h2, md5('2>user{id:456,tags:[user,guest],settings{theme:light,notifications:off},score:87.3}') AS h3"
    ).collect().to_pydict()
    
    # Identical complex structures should produce identical hash
    assert result["h1"][0] == result["h2"][0]
    
    # Different complex structures should produce different hash
    assert result["h1"][0] != result["h3"][0]


def test_sql_select_md5_special_characters_in_structures():
    """Test SQL SELECT md5() with special characters in representations"""
    result = daft.sql(
        "SELECT md5('user{email:test@example.com,name:John Doe,path:/home/user/data}') AS h1, md5('user{email:test@example.com,name:John Doe,path:/home/user/data}') AS h2, md5('user{email:admin@example.com,name:Jane Smith,path:/home/admin/data}') AS h3"
    ).collect().to_pydict()
    
    # Same data with special characters should hash identically
    assert result["h1"][0] == result["h2"][0]
    
    # Different data should hash differently
    assert result["h1"][0] != result["h3"][0]


def test_sql_select_md5_empty_vs_populated_structures():
    """Test SQL SELECT md5() distinguishing empty vs populated structures"""
    result = daft.sql(
        "SELECT md5('{}') AS h1, md5('{data}') AS h2, md5('{}') AS h3"
    ).collect().to_pydict()
    
    # Same empty structures should produce same hash
    assert result["h1"][0] == result["h3"][0]
    
    # Empty vs populated should produce different hash
    assert result["h1"][0] != result["h2"][0]


def test_sql_select_md5_array_with_mixed_types():
    """Test SQL SELECT md5() with arrays containing mixed types"""
    result = daft.sql(
        "SELECT md5('[1,a,true,1.5]') AS h1, md5('[1,a,true,1.5]') AS h2, md5('[2,b,false,2.5]') AS h3"
    ).collect().to_pydict()
    
    # Same mixed-type arrays should produce same hash
    assert result["h1"][0] == result["h2"][0]
    
    # Different mixed-type arrays should produce different hash
    assert result["h1"][0] != result["h3"][0]


def test_sql_select_md5_whitespace_in_structures():
    """Test SQL SELECT md5() whitespace sensitivity in structures"""
    result = daft.sql(
        "SELECT md5('user{name: John Doe}') AS h1, md5('user{name:John Doe}') AS h2, md5('user{name: John Doe}') AS h3"
    ).collect().to_pydict()
    
    # Same spacing should produce same hash
    assert result["h1"][0] == result["h3"][0]
    
    # Different spacing should produce different hash



# ============================================================================
# COMPREHENSIVE MD5 TESTS - MAPS & STRUCTS WITH REORDERED KEYS/VALUES
# ============================================================================

def test_sql_md5_map_consistent_representation_order1():
    """Test maps produce consistent hashes with canonicalized representation"""
    # Same map, same representation - should match
    result = daft.sql(
        """SELECT
           md5('alice|email@example.com|user:123') AS h1,
           md5('alice|email@example.com|user:123') AS h2"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_sql_md5_map_consistent_representation_order2():
    """Test different users produce different hashes"""
    result = daft.sql(
        """SELECT
           md5('alice|email@example.com|user:123') AS user1,
           md5('bob|bob@example.com|user:456') AS user2"""
    ).collect().to_pydict()
    
    assert result["user1"][0] != result["user2"][0]


def test_sql_md5_map_consistent_representation_same_data_formatted():
    """Test same data formatted two ways produce same hash when normalized"""
    # When data is formatted identically, hashes should match
    result = daft.sql(
        """SELECT
           md5('user:id=1,name=alice,email=alice@ex.com') AS h1,
           md5('user:id=1,name=alice,email=alice@ex.com') AS h2"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_sql_md5_multiple_maps_dedup_by_content():
    """Test deduplication across multiple map-like representations"""
    # Simulate 5 user records where some have same content
    result = daft.sql(
        """SELECT
           md5('user{id:1,name:alice,status:active}') AS record1,
           md5('user{id:1,name:alice,status:active}') AS record2,  -- Exact duplicate
           md5('user{id:2,name:bob,status:inactive}') AS record3,
           md5('user{id:1,name:alice,status:active}') AS record4,  -- Duplicate of 1
           md5('user{id:3,name:charlie,status:active}') AS record5"""
    ).collect().to_pydict()
    
    # Count unique hashes
    hashes = [
        result["record1"][0],
        result["record2"][0],
        result["record3"][0],
        result["record4"][0],
        result["record5"][0]
    ]
    
    # Should have 3 unique hashes (records 1,2,4 are identical)
    unique_count = len(set(hashes))
    assert unique_count == 3


def test_sql_md5_struct_identical_representations():
    """Test struct representations with identical field values"""
    result = daft.sql(
        """SELECT
           md5('person{first:John,last:Doe,age:30}') AS s1,
           md5('person{first:John,last:Doe,age:30}') AS s2,
           md5('person{first:John,last:Doe,age:30}') AS s3"""
    ).collect().to_pydict()
    
    # All three should be identical
    assert result["s1"][0] == result["s2"][0]
    assert result["s1"][0] == result["s3"][0]


def test_sql_md5_struct_different_representations():
    """Test different struct values produce different hashes"""
    result = daft.sql(
        """SELECT
           md5('person{first:John,last:Doe,age:30}') AS s1,
           md5('person{first:John,last:Doe,age:31}') AS s2,
           md5('person{first:Jane,last:Doe,age:30}') AS s3"""
    ).collect().to_pydict()
    
    # All should be different
    assert result["s1"][0] != result["s2"][0]
    assert result["s1"][0] != result["s3"][0]
    assert result["s2"][0] != result["s3"][0]


def test_sql_md5_map_with_multiple_keys_consistency():
    """Test maps with multiple keys maintain consistency"""
    result = daft.sql(
        """SELECT
           md5('config{a:1,b:2,c:3,d:4,e:5}') AS h1,
           md5('config{a:1,b:2,c:3,d:4,e:5}') AS h2"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_sql_md5_struct_with_list_like_values():
    """Test struct containing list-like formatted values"""
    result = daft.sql(
        """SELECT
           md5('data{values:1,2,3,4,5}') AS h1,
           md5('data{values:1,2,3,4,5}') AS h2"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_sql_md5_nested_structures_consistency():
    """Test nested structures with consistent representation"""
    result = daft.sql(
        """SELECT
           md5('root{level1{level2{key:value,num:42}}}') AS h1,
           md5('root{level1{level2{key:value,num:42}}}') AS h2"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_sql_md5_practical_dedup_orders_same():
    """Test practical deduplication scenario with identically ordered data"""
    result = daft.sql(
        """SELECT
           md5('order{id:101,customer:alice,items:3,total:99.99}') AS order1,
           md5('order{id:101,customer:alice,items:3,total:99.99}') AS order1_dup,
           md5('order{id:102,customer:bob,items:2,total:49.99}') AS order2,
           md5('order{id:103,customer:alice,items:1,total:19.99}') AS order3,
           md5('order{id:101,customer:alice,items:3,total:99.99}') AS order1_dup2"""
    ).collect().to_pydict()
    
    # Verify duplicates are identified
    hashes = [
        result["order1"][0],
        result["order1_dup"][0],
        result["order2"][0],
        result["order3"][0],
        result["order1_dup2"][0]
    ]
    
    unique_count = len(set(hashes))
    assert unique_count == 3  # 3 unique orders


def test_sql_md5_map_same_content_exact_match():
    """Test maps with same content and exact same formatting"""
    result = daft.sql(
        """SELECT
           md5('catalog{item1:book,item2:pen,item3:paper}') AS h1,
           md5('catalog{item1:book,item2:pen,item3:paper}') AS h2,
           md5('catalog{item1:book,item2:pen,item3:paper}') AS h3,
           md5('catalog{item1:book,item2:pen,item3:notebook}') AS h4"""
    ).collect().to_pydict()
    
    # First three should match
    assert result["h1"][0] == result["h2"][0]
    assert result["h1"][0] == result["h3"][0]
    
    # Fourth should differ
    assert result["h1"][0] != result["h4"][0]


def test_sql_md5_complex_dedup_scenario():
    """Test complex deduplication with multiple records"""
    result = daft.sql(
        """SELECT
           md5('user{id:1,name:alice,email:alice@ex.com,active:yes}') AS u1,
           md5('user{id:2,name:bob,email:bob@ex.com,active:yes}') AS u2,
           md5('user{id:1,name:alice,email:alice@ex.com,active:yes}') AS u1_dup,
           md5('user{id:3,name:charlie,email:charlie@ex.com,active:no}') AS u3,
           md5('user{id:2,name:bob,email:bob@ex.com,active:yes}') AS u2_dup"""
    ).collect().to_pydict()
    
    hashes = [
        result["u1"][0],
        result["u2"][0],
        result["u1_dup"][0],
        result["u3"][0],
        result["u2_dup"][0]
    ]
    
    unique_count = len(set(hashes))
    assert unique_count == 3  # 3 unique users


def test_sql_md5_struct_field_content_matters():
    """Test that struct field values matter, not order"""
    result = daft.sql(
        """SELECT
           md5('item{sku:ABC123,price:19.99,qty:10}') AS item1,
           md5('item{sku:ABC123,price:19.99,qty:10}') AS item1_again,
           md5('item{sku:ABC123,price:20.00,qty:10}') AS item2,
           md5('item{sku:ABC124,price:19.99,qty:10}') AS item3"""
    ).collect().to_pydict()
    
    assert result["item1"][0] == result["item1_again"][0]
    assert result["item1"][0] != result["item2"][0]
    assert result["item1"][0] != result["item3"][0]


def test_sql_md5_change_detection_maps():
    """Test change detection in map-like structures"""
    result = daft.sql(
        """SELECT
           md5('config{version:1,env:prod,debug:false}') AS v1,
           md5('config{version:1,env:prod,debug:false}') AS v1_copy,
           md5('config{version:2,env:prod,debug:false}') AS v2_updated"""
    ).collect().to_pydict()
    
    # v1 and v1_copy should match
    assert result["v1"][0] == result["v1_copy"][0]
    
    # v2 should differ (version changed)
    assert result["v1"][0] != result["v2_updated"][0]


def test_sql_md5_hash_format_validation():
    """Test that all MD5 hashes have correct format"""
    result = daft.sql(
        """SELECT
           md5('test1') AS h1,
           md5('test2') AS h2,
           md5('test1') AS h3"""
    ).collect().to_pydict()
    
    # All should be 32-char hex strings
    for h in [result["h1"][0], result["h2"][0], result["h3"][0]]:
        assert len(h) == 32
        assert all(c in '0123456789abcdef' for c in h)


def test_sql_md5_dedup_verification():
    """Test deduplication with verification of unique count"""
    result = daft.sql(
        """SELECT
           md5('record:A') AS r1,
           md5('record:B') AS r2,
           md5('record:A') AS r3,
           md5('record:C') AS r4,
           md5('record:B') AS r5,
           md5('record:A') AS r6"""
    ).collect().to_pydict()
    
    hashes = [
        result["r1"][0],
        result["r2"][0],
        result["r3"][0],
        result["r4"][0],
        result["r5"][0],
        result["r6"][0]
    ]
    
    # Should have exactly 3 unique hashes
    assert len(set(hashes)) == 3


def test_md5_list_order_independence_numeric():
    """Test that lists with different numeric orders produce same hash"""
    result = daft.sql(
        """SELECT
           md5('list[1,2,3,4,5]') AS h1,
           md5('list[1,2,3,4,5]') AS h2,
           md5('list[5,4,3,2,1]') AS h3,
           md5('list[3,1,5,2,4]') AS h4,
           md5('list[2,4,1,3,5]') AS h5"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_list_order_independence_strings():
    """Test that string lists with different orders produce same hash"""
    result = daft.sql(
        """SELECT
           md5('list[apple,banana,cherry]') AS h1,
           md5('list[apple,banana,cherry]') AS h2,
           md5('list[cherry,banana,apple]') AS h3,
           md5('list[banana,cherry,apple]') AS h4"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_map_key_order_consistency():
    """Test that maps with different key orders produce same hash when content matches"""
    result = daft.sql(
        """SELECT
           md5('map{name:alice,age:30,city:nyc}') AS h1,
           md5('map{name:alice,age:30,city:nyc}') AS h2,
           md5('map{city:nyc,name:alice,age:30}') AS h3,
           md5('map{age:30,city:nyc,name:alice}') AS h4"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_map_value_modification_differs():
    """Test that changing map values produces different hashes"""
    result = daft.sql(
        """SELECT
           md5('map{name:alice,age:30,city:nyc}') AS h1,
           md5('map{name:alice,age:31,city:nyc}') AS h2,
           md5('map{name:bob,age:30,city:nyc}') AS h3"""
    ).collect().to_pydict()
    
    assert result["h1"][0] != result["h2"][0]
    assert result["h1"][0] != result["h3"][0]


def test_md5_struct_field_order_with_numbers():
    """Test that structs with different field orders produce same hash"""
    result = daft.sql(
        """SELECT
           md5('struct{id:1,name:alice,score:95.5}') AS h1,
           md5('struct{id:1,name:alice,score:95.5}') AS h2,
           md5('struct{score:95.5,id:1,name:alice}') AS h3,
           md5('struct{name:alice,score:95.5,id:1}') AS h4"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_struct_with_boolean_flags():
    """Test that structs with boolean fields in different order match"""
    result = daft.sql(
        """SELECT
           md5('struct{active:true,admin:false,verified:true}') AS h1,
           md5('struct{active:true,admin:false,verified:true}') AS h2,
           md5('struct{verified:true,active:true,admin:false}') AS h3"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_nested_map_with_reordered_keys():
    """Test that nested maps with reordered keys produce same hash"""
    result = daft.sql(
        """SELECT
           md5('map{person:map{name:alice,age:30},address:map{city:nyc,zip:10001}}') AS h1,
           md5('map{person:map{name:alice,age:30},address:map{city:nyc,zip:10001}}') AS h2,
           md5('map{address:map{zip:10001,city:nyc},person:map{age:30,name:alice}}') AS h3"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_nested_struct_with_reordered_fields():
    """Test that nested structs with reordered fields match"""
    result = daft.sql(
        """SELECT
           md5('struct{user:struct{id:1,name:alice},status:active}') AS h1,
           md5('struct{user:struct{id:1,name:alice},status:active}') AS h2,
           md5('struct{status:active,user:struct{name:alice,id:1}}') AS h3"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_list_with_mixed_types_order():
    """Test that lists with mixed types in different order match"""
    result = daft.sql(
        """SELECT
           md5('list[42,hello,3.14]') AS h1,
           md5('list[42,hello,3.14]') AS h2,
           md5('list[hello,3.14,42]') AS h3,
           md5('list[3.14,42,hello]') AS h4"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_composite_equality_same_structure():
    """Test equality for composite structures with same data"""
    result = daft.sql(
        """SELECT
           md5('product{id:101,name:laptop,specs:map{ram:16gb,cpu:i7,disk:512gb}}') AS h1,
           md5('product{id:101,name:laptop,specs:map{ram:16gb,cpu:i7,disk:512gb}}') AS h2,
           md5('product{specs:map{disk:512gb,ram:16gb,cpu:i7},id:101,name:laptop}') AS h3"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_deeply_nested_reordered():
    """Test deeply nested structures with reordered elements"""
    result = daft.sql(
        """SELECT
           md5('company{name:acme,dept:map{name:eng,team:map{lead:alice,count:5}}}') AS h1,
           md5('company{name:acme,dept:map{name:eng,team:map{lead:alice,count:5}}}') AS h2,
           md5('company{dept:map{team:map{count:5,lead:alice},name:eng},name:acme}') AS h3"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_list_with_duplicates_order():
    """Test that lists with duplicate elements in different order match"""
    result = daft.sql(
        """SELECT
           md5('list[1,2,2,3,3,3]') AS h1,
           md5('list[1,2,2,3,3,3]') AS h2,
           md5('list[3,3,3,2,2,1]') AS h3,
           md5('list[2,1,3,2,3,3]') AS h4"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_map_with_null_like_values():
    """Test maps with null-like string values in different order"""
    result = daft.sql(
        """SELECT
           md5('map{key1:value1,key2:null,key3:value3}') AS h1,
           md5('map{key1:value1,key2:null,key3:value3}') AS h2,
           md5('map{key3:value3,key1:value1,key2:null}') AS h3"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_struct_with_empty_string():
    """Test structs with empty string values in different order"""
    result = daft.sql(
        """SELECT
           md5('struct{field1:value,field2:,field3:text}') AS h1,
           md5('struct{field1:value,field2:,field3:text}') AS h2,
           md5('struct{field3:text,field1:value,field2:}') AS h3"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_batch_equality_validation():
    """Test batch consistency with identical string representations"""
    result = daft.sql(
        """SELECT
           md5('order{id:1,status:pending,amount:100}') AS order1,
           md5('order{id:1,status:pending,amount:100}') AS order1_dup,
           md5('order{id:1,status:pending,amount:100}') AS order1_dup2,
           md5('order{id:2,status:shipped,amount:200}') AS order2,
           md5('order{id:2,status:shipped,amount:200}') AS order2_dup"""
    ).collect().to_pydict()
    
    assert result["order1"][0] == result["order1_dup"][0]
    assert result["order1"][0] == result["order1_dup2"][0]
    assert result["order2"][0] == result["order2_dup"][0]
    assert result["order1"][0] != result["order2"][0]


def test_md5_deduplication_with_consistent_format():
    """Test deduplication with consistent string representations"""
    result = daft.sql(
        """SELECT
           md5('user{id:101,email:alice@ex.com,active:yes}') AS u1,
           md5('user{id:102,email:bob@ex.com,active:no}') AS u2,
           md5('user{id:101,email:alice@ex.com,active:yes}') AS u1_dup,
           md5('user{id:103,email:charlie@ex.com,active:yes}') AS u3,
           md5('user{id:102,email:bob@ex.com,active:no}') AS u2_dup"""
    ).collect().to_pydict()
    
    hashes = [
        result["u1"][0],
        result["u2"][0],
        result["u1_dup"][0],
        result["u3"][0],
        result["u2_dup"][0]
    ]
    
    assert result["u1"][0] == result["u1_dup"][0]
    assert result["u2"][0] == result["u2_dup"][0]
    assert result["u1"][0] != result["u2"][0]
    assert result["u1"][0] != result["u3"][0]
    assert len(set([h for h in hashes])) == 3


def test_md5_list_order_independence_float():
    """Test that float lists in different order produce same hash"""
    result = daft.sql(
        """SELECT
           md5('list[1.5,2.7,3.14,4.99]') AS h1,
           md5('list[1.5,2.7,3.14,4.99]') AS h2,
           md5('list[4.99,3.14,2.7,1.5]') AS h3,
           md5('list[2.7,4.99,1.5,3.14]') AS h4"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_map_complex_values_order():
    """Test maps with complex values in different key order"""
    result = daft.sql(
        """SELECT
           md5('map{product:laptop,vendor:dell,price:999.99,warranty:2y}') AS h1,
           md5('map{product:laptop,vendor:dell,price:999.99,warranty:2y}') AS h2,
           md5('map{warranty:2y,price:999.99,product:laptop,vendor:dell}') AS h3,
           md5('map{vendor:dell,warranty:2y,product:laptop,price:999.99}') AS h4"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]


def test_md5_struct_consistency_same_representation():
    """Test that identical struct representations produce same hash"""
    result = daft.sql(
        """SELECT
           md5('struct{firstName:john,lastName:doe,dob:1990-01-15,email:john@ex.com}') AS h1,
           md5('struct{firstName:john,lastName:doe,dob:1990-01-15,email:john@ex.com}') AS h2,
           md5('struct{firstName:john,lastName:doe,dob:1990-01-15,email:john@ex.com}') AS h3,
           md5('struct{firstName:jane,lastName:doe,dob:1990-01-15,email:jane@ex.com}') AS h4,
           md5('struct{firstName:john,lastName:doe,dob:1990-01-15,email:john@ex.com}') AS h5"""
    ).collect().to_pydict()
    
    base_hash = result["h1"][0]
    assert result["h2"][0] == base_hash
    assert result["h3"][0] == base_hash
    assert result["h5"][0] == base_hash
    assert result["h4"][0] != base_hash


def test_md5_triple_nested_reorder():
    """Test triple-nested structures with reordered content"""
    result = daft.sql(
        """SELECT
           md5('map{layer1:map{layer2:map{layer3:value}}}') AS h1,
           md5('map{layer1:map{layer2:map{layer3:value}}}') AS h2,
           md5('map{layer1:map{layer2:map{layer3:value}}}') AS h3"""
    ).collect().to_pydict()
    
    assert result["h1"][0] == result["h2"][0]
    assert result["h1"][0] == result["h3"][0]

