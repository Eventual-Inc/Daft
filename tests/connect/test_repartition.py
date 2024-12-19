from __future__ import annotations

def test_repartition(spark_session):
    # Create a simple DataFrame
    df = spark_session.range(10)
    
    # Test repartitioning to 2 partitions
    repartitioned = df.repartition(2)
    
    # Verify data is preserved after repartitioning
    original_data = sorted(df.collect())
    repartitioned_data = sorted(repartitioned.collect())
    assert repartitioned_data == original_data, "Data should be preserved after repartitioning"