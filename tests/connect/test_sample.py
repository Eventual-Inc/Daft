from __future__ import annotations


def test_sample(spark_session):
    # Create a range DataFrame
    df = spark_session.range(100)

    # Test sample with fraction
    sampled_df = df.sample(fraction=0.1, seed=42)
    sampled_rows = sampled_df.collect()

    # Verify sample size is roughly 10% of original
    sample_size = len(sampled_rows)
    assert 5 <= sample_size <= 15, f"Sample size {sample_size} should be roughly 10 rows"

    # Verify sampled values are within original range
    for row in sampled_rows:
        assert 0 <= row["id"] < 100, f"Sampled value {row['id']} outside valid range"
