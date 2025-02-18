# from __future__ import annotations

# import pytest


# @pytest.fixture
# def sql_test_data():
#     return {
#         # Unsorted data with multiple groups and nulls
#         "category": ["B", "A", "C", "A", "B", "C", "A", "B"],
#         "region": ["West", "East", "North", "West", "East", "South", "North", "South"],
#         "value": [10, 5, 15, 8, 12, 6, 9, 7],
#         "nullable_value": [10, None, 15, 8, None, 6, 9, 7],
#         "date": [
#             "2024-01-03",
#             "2024-01-01",
#             "2024-01-02",
#             "2024-01-04",
#             "2024-01-05",
#             "2024-01-06",
#             "2024-01-07",
#             "2024-01-08",
#         ],
#     }


# def test_sql_partition_only(make_df):
#     """Test basic SQL window aggregation with PARTITION BY only."""
#     df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

#     query = """
#     SELECT
#         category,
#         value,
#         SUM(value) OVER (PARTITION BY category) as sum,
#         AVG(value) OVER (PARTITION BY category) as avg,
#         COUNT(value) OVER (PARTITION BY category) as count
#     FROM df
#     """

#     result = df.sql(query)

#     # TODO: Add expected output once implementation is ready
#     expected = None
#     assert result == expected


# def test_sql_rank_functions(make_df):
#     """Test SQL rank functions with PARTITION BY and ORDER BY."""
#     df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 5, 10, 20, 8, 7]})

#     query = """
#     SELECT
#         category,
#         value,
#         ROW_NUMBER() OVER (PARTITION BY category ORDER BY value) as row_num,
#         RANK() OVER (PARTITION BY category ORDER BY value) as rank,
#         DENSE_RANK() OVER (PARTITION BY category ORDER BY value) as dense_rank
#     FROM df
#     """

#     result = df.sql(query)

#     # TODO: Add expected output once implementation is ready
#     expected = None
#     assert result == expected


# def test_sql_lag_lead(make_df):
#     """Test SQL LAG and LEAD functions."""
#     df = make_df(
#         {
#             "stock": ["AAPL", "GOOGL", "AAPL", "GOOGL", "AAPL", "GOOGL"],
#             "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
#             "price": [180.5, 140.2, 182.3, 141.5, 179.8, 142.1],
#         }
#     )

#     query = """
#     SELECT
#         stock,
#         date,
#         price,
#         LAG(price) OVER (PARTITION BY stock ORDER BY date) as prev_price,
#         LAG(price, 2, 0.0) OVER (PARTITION BY stock ORDER BY date) as price_2_days_ago,
#         LEAD(price) OVER (PARTITION BY stock ORDER BY date) as next_price
#     FROM df
#     """

#     result = df.sql(query)

#     # TODO: Add expected output once implementation is ready
#     expected = None
#     assert result == expected


# def test_sql_running_aggs(make_df):
#     """Test SQL running aggregations with ROWS BETWEEN."""
#     df = make_df(
#         {
#             "store": ["A", "B", "A", "B", "A", "B"],
#             "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
#             "sales": [100, 150, 120, 160, 90, 140],
#         }
#     )

#     query = """
#     SELECT
#         store,
#         date,
#         sales,
#         SUM(sales) OVER (
#             PARTITION BY store
#             ORDER BY date
#             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
#         ) as running_total,
#         AVG(sales) OVER (
#             PARTITION BY store
#             ORDER BY date
#             ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
#         ) as moving_avg_3day
#     FROM df
#     """

#     result = df.sql(query)

#     # TODO: Add expected output once implementation is ready
#     expected = None
#     assert result == expected


# def test_sql_multiple_windows(make_df):
#     """Test multiple window functions in the same query."""
#     df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

#     query = """
#     SELECT
#         category,
#         value,
#         SUM(value) OVER (PARTITION BY category) as category_sum,
#         SUM(value) OVER () as total_sum,
#         RANK() OVER (PARTITION BY category ORDER BY value) as rank_in_category,
#         RANK() OVER (ORDER BY value) as global_rank
#     FROM df
#     """

#     result = df.sql(query)

#     # TODO: Add expected output once implementation is ready
#     expected = None
#     assert result == expected
