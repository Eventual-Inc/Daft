"""Tests for the image average hash function.

These tests use in-memory numpy arrays to avoid file I/O dependencies
and focus on testing the hash algorithm correctness and edge cases.
"""
from __future__ import annotations

import numpy as np

import daft
from daft import col
from daft.datatype import DataType


class TestImageAverageHash:
    """Test suite for image average hash functionality."""

    def test_average_hash_solid_colors(self):
        """Test average hash with solid color images."""
        # Create solid color images
        black_img = np.zeros((8, 8, 3), dtype=np.uint8)
        white_img = np.full((8, 8, 3), 255, dtype=np.uint8)
        gray_img = np.full((8, 8, 3), 128, dtype=np.uint8)

        df = daft.from_pydict({"image_data": [black_img, white_img, gray_img], "name": ["black", "white", "gray"]})

        # Cast to image type and compute hashes
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.average_hash())

        result = df.collect()
        result_dict = result.to_pydict()

        # Validate hash format
        for hash_val in result_dict["hash"]:
            assert hash_val is not None
            assert len(hash_val) == 64, f"Hash should be 64 characters, got {len(hash_val)}"
            assert all(c in "01" for c in hash_val), f"Hash should be binary, got {hash_val}"

        # Test hash properties for solid colors
        black_hash, white_hash, _ = result_dict["hash"]

        # Black image should have mostly 0s (pixels below average)
        assert black_hash.count("0") > black_hash.count(
            "1"
        ), f"Black image should have more 0s, got {black_hash.count('0')} zeros, {black_hash.count('1')} ones"

        # White image should have mostly 1s (pixels above average)
        assert white_hash.count("1") > white_hash.count(
            "0"
        ), f"White image should have more 1s, got {white_hash.count('0')} zeros, {white_hash.count('1')} ones"

    def test_average_hash_null_handling(self):
        """Test average hash with null values."""
        # Create test data with nulls
        valid_img = np.full((4, 4, 3), 100, dtype=np.uint8)

        df = daft.from_pydict({"image_data": [valid_img, None, valid_img, None], "id": [1, 2, 3, 4]})

        # Cast to image type and compute hashes
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.average_hash())

        result = df.collect()
        result_dict = result.to_pydict()

        # Check null handling
        assert result_dict["hash"][0] is not None  # Valid image
        assert result_dict["hash"][1] is None  # Null image
        assert result_dict["hash"][2] is not None  # Valid image
        assert result_dict["hash"][3] is None  # Null image

        # Valid hashes should be identical (same image)
        assert result_dict["hash"][0] == result_dict["hash"][2]
        assert len(result_dict["hash"][0]) == 64

    def test_average_hash_consistency(self):
        """Test that identical images produce identical hashes."""
        # Create identical images
        test_img = np.random.randint(0, 255, (10, 10, 3), dtype=np.uint8)
        identical_img = test_img.copy()

        df = daft.from_pydict(
            {"image_data": [test_img, identical_img, test_img], "id": ["original", "copy", "duplicate"]}
        )

        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.average_hash())

        result = df.collect()
        result_dict = result.to_pydict()

        # All hashes should be identical
        hashes = result_dict["hash"]
        hash1, hash2, hash3 = hashes[0], hashes[1], hashes[2]
        assert hash1 == hash2 == hash3
        assert len(hash1) == 64

    def test_average_hash_similarity(self):
        """Test that similar images have similar hashes."""
        # Create base image
        base_img = np.random.randint(50, 200, (16, 16, 3), dtype=np.uint8)

        # Create similar image (add small noise)
        similar_img = base_img.copy()
        noise = np.random.randint(-5, 6, similar_img.shape, dtype=np.int16)
        similar_img = np.clip(similar_img.astype(np.int16) + noise, 0, 255).astype(np.uint8)

        # Create different image
        different_img = 255 - base_img  # Invert colors

        df = daft.from_pydict(
            {"image_data": [base_img, similar_img, different_img], "type": ["base", "similar", "different"]}
        )

        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.average_hash())

        result = df.collect()
        result_dict = result.to_pydict()

        # Calculate Hamming distances
        hashes = result_dict["hash"]
        base_hash, similar_hash, different_hash = hashes[0], hashes[1], hashes[2]

        def hamming_distance(h1, h2):
            return sum(c1 != c2 for c1, c2 in zip(h1, h2))

        dist_base_similar = hamming_distance(base_hash, similar_hash)
        dist_base_different = hamming_distance(base_hash, different_hash)

        # Similar images should have lower Hamming distance than different images
        assert (
            dist_base_similar < dist_base_different
        ), f"Similar images should have lower distance: {dist_base_similar} vs {dist_base_different}"

        # Very different images (inverted) should have high distance
        assert dist_base_different > 32, f"Inverted images should have high Hamming distance, got {dist_base_different}"

    def test_average_hash_different_sizes(self):
        """Test average hash with different image sizes."""
        # Create images of different sizes but same pattern
        small_img = np.full((4, 4, 3), 100, dtype=np.uint8)
        medium_img = np.full((16, 16, 3), 100, dtype=np.uint8)
        large_img = np.full((64, 64, 3), 100, dtype=np.uint8)

        df = daft.from_pydict({"image_data": [small_img, medium_img, large_img], "size": ["4x4", "16x16", "64x64"]})

        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.average_hash())

        result = df.collect()
        result_dict = result.to_pydict()

        # All images have the same solid color, so hashes should be identical
        # (after resizing to 8x8, they all become the same)
        hashes = result_dict["hash"]
        hash1, hash2, hash3 = hashes[0], hashes[1], hashes[2]
        assert hash1 == hash2 == hash3
        assert len(hash1) == 64

    def test_average_hash_grayscale_images(self):
        """Test average hash with single-channel (grayscale) images."""
        # Create grayscale images (single channel)
        dark_gray = np.full((8, 8, 1), 64, dtype=np.uint8)
        light_gray = np.full((8, 8, 1), 192, dtype=np.uint8)

        df = daft.from_pydict({"image_data": [dark_gray, light_gray], "type": ["dark", "light"]})

        df = df.with_column("image", col("image_data").cast(DataType.image("L")))
        df = df.with_column("hash", col("image").image.average_hash())

        result = df.collect()
        result_dict = result.to_pydict()

        # Verify hashes
        hashes = result_dict["hash"]
        dark_hash, light_hash = hashes[0], hashes[1]

        assert len(dark_hash) == 64
        assert len(light_hash) == 64

        # Dark should have more 0s, light should have more 1s
        assert dark_hash.count("0") > dark_hash.count("1")
        assert light_hash.count("1") > light_hash.count("0")

    def test_average_hash_checkerboard_pattern(self):
        """Test average hash with a known pattern."""
        # Create checkerboard pattern
        checkerboard = np.zeros((8, 8, 3), dtype=np.uint8)
        for i in range(8):
            for j in range(8):
                if (i + j) % 2 == 0:
                    checkerboard[i, j] = [255, 255, 255]  # White squares
                else:
                    checkerboard[i, j] = [0, 0, 0]  # Black squares

        df = daft.from_pydict({"image_data": [checkerboard], "pattern": ["checkerboard"]})

        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.average_hash())

        result = df.collect()
        result_dict = result.to_pydict()

        hash_val = result_dict["hash"][0]
        assert len(hash_val) == 64

        # Checkerboard should have roughly equal 0s and 1s (average ~127.5)
        ones = hash_val.count("1")
        zeros = hash_val.count("0")

        # Should be reasonably balanced (within 20% of 32 each)
        assert 25 <= ones <= 39, f"Checkerboard should have balanced hash, got {ones} ones"
        assert 25 <= zeros <= 39, f"Checkerboard should have balanced hash, got {zeros} zeros"

    def test_average_hash_batch_processing(self):
        """Test average hash with larger batches."""
        # Create batch of random images
        batch_size = 20
        images = []

        for i in range(batch_size):
            # Create images with different brightness levels
            brightness = int(255 * i / batch_size)
            img = np.full((6, 6, 3), brightness, dtype=np.uint8)
            images.append(img)

        df = daft.from_pydict({"image_data": images, "brightness": list(range(batch_size))})

        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.average_hash())

        result = df.collect()
        result_dict = result.to_pydict()

        # All hashes should be valid
        hashes = result_dict["hash"]
        for i, hash_val in enumerate(hashes):
            assert hash_val is not None, f"Hash {i} should not be None"
            assert len(hash_val) == 64, f"Hash {i} should be 64 chars"
            assert all(c in "01" for c in hash_val), f"Hash {i} should be binary"

        # Darker images should have more 0s, brighter images more 1s
        first_hash = hashes[0]  # Darkest (brightness 0)
        last_hash = hashes[-1]  # Brightest (brightness 255)

        assert first_hash.count("0") > first_hash.count("1"), "Dark image should have more 0s"
        assert last_hash.count("1") > last_hash.count("0"), "Bright image should have more 1s"

    def test_average_hash_deduplication_scenario(self):
        """Test realistic deduplication use case."""
        # Create "duplicate" images (same content, different objects)
        original = np.random.randint(0, 255, (12, 12, 3), dtype=np.uint8)
        duplicate1 = original.copy()
        duplicate2 = original.copy()
        different = np.random.randint(0, 255, (12, 12, 3), dtype=np.uint8)

        df = daft.from_pydict(
            {
                "image_data": [original, duplicate1, duplicate2, different],
                "filename": ["img1.jpg", "img1_copy.jpg", "img1_backup.jpg", "img2.jpg"],
            }
        )

        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.average_hash())

        # Simple deduplication: group by hash and count
        df_with_count = df.groupby("hash").agg([col("filename").count().alias("count")])
        duplicates = df_with_count.filter(col("count") > 1)

        duplicate_result = duplicates.collect()
        duplicate_dict = duplicate_result.to_pydict()

        # Should find one group of duplicates (the first 3 images)
        assert len(duplicate_dict["count"]) == 1
        assert duplicate_dict["count"][0] == 3

    def test_average_hash_edge_cases(self):
        """Test edge cases and potential error conditions."""
        # Single pixel images
        single_pixel_black = np.zeros((1, 1, 3), dtype=np.uint8)
        single_pixel_white = np.full((1, 1, 3), 255, dtype=np.uint8)

        # Very wide image
        wide_image = np.full((1, 100, 3), 128, dtype=np.uint8)

        # Very tall image
        tall_image = np.full((100, 1, 3), 128, dtype=np.uint8)

        df = daft.from_pydict(
            {
                "image_data": [single_pixel_black, single_pixel_white, wide_image, tall_image],
                "type": ["1x1_black", "1x1_white", "1x100", "100x1"],
            }
        )

        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.average_hash())

        result = df.collect()
        result_dict = result.to_pydict()

        # All should produce valid hashes
        hashes = result_dict["hash"]
        for i, hash_val in enumerate(hashes):
            assert hash_val is not None, f"Hash {i} should not be None"
            assert len(hash_val) == 64, f"Hash {i} should be 64 chars, got {len(hash_val)}"
            assert all(c in "01" for c in hash_val), f"Hash {i} should be binary"

        # Single pixel images should have uniform hashes
        black_hash, white_hash = hashes[0], hashes[1]
        assert black_hash == "0" * 64, "Single black pixel should produce all 0s"
        assert white_hash == "1" * 64, "Single white pixel should produce all 1s"


# Quick test function for development
def test_basic_functionality():
    """Quick test to verify the function works during development."""
    print("Running basic functionality test...")

    # Create simple test data
    black_img = np.zeros((4, 4, 3), dtype=np.uint8)
    white_img = np.full((4, 4, 3), 255, dtype=np.uint8)

    df = daft.from_pydict({"image_data": [black_img, white_img], "name": ["black", "white"]})

    df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
    df = df.with_column("hash", col("image").image.average_hash())

    result = df.collect()
    result_dict = result.to_pydict()

    hashes = result_dict["hash"]
    print(f"Black hash: {hashes[0]}")
    print(f"White hash: {hashes[1]}")

    # Basic validation
    assert len(hashes[0]) == 64
    assert len(hashes[1]) == 64
    assert hashes[0].count("0") > hashes[0].count("1")  # Black should have more 0s
    assert hashes[1].count("1") > hashes[1].count("0")  # White should have more 1s

    print("Basic functionality test passed!")


if __name__ == "__main__":
    # Run basic test for development
    test_basic_functionality()
