"""Tests for image hash functionality."""

import numpy as np
import pytest

import daft
from daft import col
from daft.datatype import DataType
from daft.functions import image_hash
from daft.series import Series


@pytest.fixture
def sample_images():
    """Create sample images for testing."""
    # Create larger 32x32 images with more distinct patterns to reduce hash collision probability
    # Image 1: Mixed pattern with specific pixel values
    img1 = np.ones((32, 32, 3), dtype=np.uint8) * 30
    img1[0:16, :] = 180  # Top half light, bottom half dark
    
    # Image 2: Different mixed pattern with different pixel values
    img2 = np.ones((32, 32, 3), dtype=np.uint8) * 220
    img2[16:32, :] = 40  # Top half light, bottom half dark (opposite of img1)
    
    # Use fixed seed for deterministic random image
    np.random.seed(42)
    img3 = np.random.randint(0, 255, (32, 32, 3), dtype=np.uint8)  # Random image
    
    # Create a distinct pattern image with clear structure
    img4 = np.zeros((32, 32, 3), dtype=np.uint8)
    img4[8:24, 8:24] = 255  # Large white square in center
    img4[0:8, :] = 100      # Top stripe
    img4[24:32, :] = 150    # Bottom stripe
    
    return [img1, img2, img3, img4]


def test_image_hash_functional_api(sample_images):
    """Test the functional API for image hashing."""
    df = daft.from_pydict({"image_data": sample_images})
    df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
    
    # Test all algorithms
    algorithms = ["average", "perceptual", "difference", "wavelet", "crop_resistant"]
    
    for algorithm in algorithms:
        result = df.with_column("hash", image_hash(col("image"), algorithm))
        hashes = result.to_pandas()["hash"].tolist()
        
        # Check that we get valid hash strings
        assert len(hashes) == 4
        for hash_val in hashes:
            assert isinstance(hash_val, str)
            assert len(hash_val) > 0


def test_image_hash_algorithms_comprehensive(sample_images):
    """Test all image hash algorithms comprehensively."""
    df = daft.from_pydict({"image_data": sample_images})
    df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
    
    # Test all algorithms using functional API
    algorithms = ["average", "perceptual", "difference", "wavelet", "crop_resistant"]
    
    for algorithm in algorithms:
        result = df.with_column("hash", image_hash(col("image"), algorithm))
        hashes = result.to_pandas()["hash"].tolist()
        
        # Check that we get valid hash strings
        assert len(hashes) == 4
        for hash_val in hashes:
            assert isinstance(hash_val, str)
            assert len(hash_val) > 0


def test_hash_consistency(sample_images):
    """Test that the same image produces the same hash."""
    df = daft.from_pydict({"image_data": sample_images})
    df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
    
    # Compute hash twice for the same image
    result1 = df.with_column("hash1", image_hash(col("image"), "average"))
    result2 = result1.with_column("hash2", image_hash(col("image"), "average"))
    
    hashes1 = result2.to_pandas()["hash1"].tolist()
    hashes2 = result2.to_pandas()["hash2"].tolist()
    
    # Hashes should be identical
    assert hashes1 == hashes2


def test_hash_different_images(sample_images):
    """Test that different images produce different hashes (for most algorithms)."""
    df = daft.from_pydict({"image_data": sample_images})
    df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
    
    # Test average hash - should be different for different images
    result = df.with_column("hash", image_hash(col("image"), "average"))
    hashes = result.to_pandas()["hash"].tolist()
    
    # Check that we have 4 hashes
    assert len(hashes) == 4
    
    # Verify that the most distinct images produce different hashes
    # Random image should be different from structured patterns
    assert hashes[2] != hashes[0], "Random image should differ from structured pattern 1"
    assert hashes[2] != hashes[1], "Random image should differ from structured pattern 2"
    assert hashes[2] != hashes[3], "Random image should differ from complex pattern"
    
    # Complex pattern should be different from simple patterns
    assert hashes[3] != hashes[0], "Complex pattern should differ from simple pattern 1"
    assert hashes[3] != hashes[1], "Complex pattern should differ from simple pattern 2"
    
    # At least some hashes should be unique (allowing for potential collisions)
    unique_hashes = len(set(hashes))
    assert unique_hashes >= 2, f"At least 2 unique hashes expected, got {unique_hashes} unique hashes: {hashes}"


def test_hash_algorithms_different(sample_images):
    """Test that different algorithms produce different hashes for the same image."""
    df = daft.from_pydict({"image_data": sample_images})
    df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
    
    # Use the random image for this test - just take the third row
    single_image_df = df.offset(2).limit(1)
    
    algorithms = ["average", "perceptual", "difference", "wavelet", "crop_resistant"]
    hashes = {}
    
    for algorithm in algorithms:
        result = single_image_df.with_column("hash", image_hash(col("image"), algorithm))
        hash_val = result.to_pandas()["hash"].iloc[0]
        hashes[algorithm] = hash_val
    
    # Different algorithms should produce different hashes
    hash_values = list(hashes.values())
    assert len(set(hash_values)) == len(hash_values), "All algorithms should produce different hashes"


def test_hash_default_algorithm(sample_images):
    """Test that the default algorithm works."""
    df = daft.from_pydict({"image_data": sample_images})
    df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
    
    # Test default algorithm (should be "average")
    result = df.with_column("hash", image_hash(col("image")))
    hashes = result.to_pandas()["hash"].tolist()
    
    # Should work without specifying algorithm
    assert len(hashes) == 4
    for hash_val in hashes:
        assert isinstance(hash_val, str)
        assert len(hash_val) > 0


def test_invalid_algorithm(sample_images):
    """Test that invalid algorithms raise appropriate errors."""
    df = daft.from_pydict({"image_data": sample_images})
    df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
    
    # This should raise an error due to invalid algorithm
    with pytest.raises(Exception):  # The exact exception type may vary
        result = df.with_column("hash", image_hash(col("image"), "invalid_algorithm"))
        result.collect()  # Force execution to trigger the error


def test_hash_with_different_image_sizes():
    """Test hashing with images of different sizes."""
    # Create images of different sizes
    img1 = np.ones((5, 5, 3), dtype=np.uint8) * 100
    img2 = np.ones((20, 20, 3), dtype=np.uint8) * 100
    img3 = np.ones((50, 50, 3), dtype=np.uint8) * 100
    
    df = daft.from_pydict({"image_data": [img1, img2, img3]})
    df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
    
    # All should work regardless of size
    result = df.with_column("hash", image_hash(col("image"), "average"))
    hashes = result.to_pandas()["hash"].tolist()
    
    assert len(hashes) == 3
    for hash_val in hashes:
        assert isinstance(hash_val, str)
        assert len(hash_val) > 0
