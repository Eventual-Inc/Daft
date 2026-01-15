from __future__ import annotations

import datetime
import random
from functools import lru_cache


@lru_cache(maxsize=32)
def generate_int_values(seed: int, count: int, min_val: int, max_val: int) -> tuple[int, ...]:
    """Generate random integers with memoization.

    Args:
        seed: Random seed for reproducibility
        count: Number of values to generate
        min_val: Minimum value (inclusive)
        max_val: Maximum value (inclusive)

    Returns:
        Tuple of random integers (immutable for caching)
    """
    random.seed(seed)
    return tuple(random.randint(min_val, max_val) for _ in range(count))


@lru_cache(maxsize=32)
def generate_float_values(seed: int, count: int, min_val: float, max_val: float) -> tuple[float, ...]:
    """Generate random floats with memoization.

    Args:
        seed: Random seed for reproducibility
        count: Number of values to generate
        min_val: Minimum value
        max_val: Maximum value

    Returns:
        Tuple of random floats (immutable for caching)
    """
    random.seed(seed)
    return tuple(random.uniform(min_val, max_val) for _ in range(count))


@lru_cache(maxsize=32)
def generate_optional_int_values(
    seed: int, count: int, none_prob: float, min_val: int, max_val: int
) -> tuple[int | None, ...]:
    """Generate random integers with None values.

    Args:
        seed: Random seed for reproducibility
        count: Number of values to generate
        none_prob: Probability (0.0-1.0) of generating None
        min_val: Minimum value (inclusive) for non-None values
        max_val: Maximum value (inclusive) for non-None values

    Returns:
        Tuple of random integers or None (immutable for caching)
    """
    random.seed(seed)
    values: list[int | None] = []
    for _ in range(count):
        if random.random() < none_prob:
            values.append(None)
        else:
            values.append(random.randint(min_val, max_val))
    return tuple(values)


@lru_cache(maxsize=32)
def generate_optional_string_values(seed: int, count: int, none_prob: float, prefix: str) -> tuple[str | None, ...]:
    """Generate random strings with None values."""
    random.seed(seed)
    values: list[str | None] = []
    for i in range(count):
        if random.random() < none_prob:
            values.append(None)
        else:
            values.append(f"{prefix}_{i}")
    return tuple(values)


@lru_cache(maxsize=32)
def generate_timestamps(
    seed: int,
    count: int,
    base_year: int,
    base_month: int,
    base_day: int,
    day_range: float,
) -> tuple[datetime.datetime, ...]:
    """Generate random timestamps with microsecond precision.

    Args:
        seed: Random seed for reproducibility
        count: Number of timestamps to generate
        base_year: Starting year
        base_month: Starting month
        base_day: Starting day
        day_range: Maximum number of days to add to base date

    Returns:
        Tuple of datetime objects with random time components (immutable for caching)
    """
    random.seed(seed)
    base_date = datetime.datetime(base_year, base_month, base_day)
    timestamps = []
    for _ in range(count):
        ts = base_date + datetime.timedelta(
            days=random.random() * day_range,
            hours=random.random() * 24,
            minutes=random.random() * 60,
            seconds=random.random() * 60,
            microseconds=random.random() * 1_000_000,
        )
        timestamps.append(ts)
    return tuple(timestamps)


@lru_cache(maxsize=32)
def generate_sampled_timestamps(seed: int, sample_count: int, range_max: int) -> tuple[int, ...]:
    """Generate sampled timestamps from a range (for sparse timestamp tests).

    Args:
        seed: Random seed for reproducibility
        sample_count: Number of samples to draw
        range_max: Maximum value in range to sample from

    Returns:
        Tuple of sorted sampled integers (immutable for caching)
    """
    random.seed(seed)
    sampled = random.sample(range(range_max), sample_count)
    return tuple(sorted(sampled))


@lru_cache(maxsize=32)
def generate_date_offsets(seed: int, count: int, min_offset: int, max_offset: int) -> tuple[int, ...]:
    """Generate random date offsets (for date-based window tests).

    Args:
        seed: Random seed for reproducibility
        count: Number of offsets to generate
        min_offset: Minimum offset in days
        max_offset: Maximum offset in days

    Returns:
        Tuple of sorted date offsets (immutable for caching)
    """
    random.seed(seed)
    offsets = [random.randint(min_offset, max_offset) for _ in range(count)]
    return tuple(sorted(offsets))


@lru_cache(maxsize=8)
def generate_shuffled_data(n=1000):
    xs = random.sample(range(1, n + 1), n)
    ys = random.sample(range(1, n + 1), n)
    values = random.sample(range(1, n + 1), n)

    return [{"x": x, "y": y, "value": value} for x, y, value in zip(xs, ys, values)]
