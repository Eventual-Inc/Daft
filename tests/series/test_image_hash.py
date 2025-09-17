"""
Test the new parameterized image hash API.

This test file replaces all individual hash test files and tests the new
parameterized image.hash(algorithm="...") API for all algorithms.
"""

import numpy as np
import pytest
import daft
from daft import col
from daft.datatype import DataType


class TestImageHash:
    """Test the new parameterized image hash API."""

    def test_hash_average_basic(self):
        """Test average hash algorithm with basic functionality."""
        # Create a simple checkerboard pattern
        img = np.zeros((8, 8, 3), dtype=np.uint8)
        for y in range(8):
            for x in range(8):
                if (x + y) % 2 == 0:
                    img[y, x, :] = 255

        df = daft.from_pydict({"image_data": [img]})
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

        # Test via expression API
        result = df.with_column("hash", col("image").image.hash("average"))
        result = result.collect()
        hash_value = result.to_pydict()["hash"][0]

        assert hash_value is not None
        assert len(hash_value) == 64
        assert all(c in "01" for c in hash_value)

    def test_hash_average_solid_colors(self):
        """Test average hash with solid color images."""
        # Create solid color images
        black_img = np.zeros((8, 8, 3), dtype=np.uint8)
        white_img = np.full((8, 8, 3), 255, dtype=np.uint8)
        gray_img = np.full((8, 8, 3), 128, dtype=np.uint8)

        df = daft.from_pydict({"image_data": [black_img, white_img, gray_img], "name": ["black", "white", "gray"]})

        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.hash("average"))

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
        assert black_hash.count("0") > black_hash.count("1"), "Black image should have more 0s"

        # White image should have mostly 1s (pixels above average)
        assert white_hash.count("1") > white_hash.count("0"), "White image should have more 1s"

    def test_hash_perceptual(self):
        """Test perceptual hash algorithm."""
        # Create a gradient image
        img = np.zeros((32, 32, 3), dtype=np.uint8)
        for y in range(32):
            for x in range(32):
                img[y, x, :] = (x * 8, y * 8, (x + y) * 4)

        df = daft.from_pydict({"image_data": [img]})
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

        result = df.with_column("hash", col("image").image.hash("perceptual"))
        result = result.collect()
        hash_value = result.to_pydict()["hash"][0]

        assert hash_value is not None
        assert len(hash_value) == 64
        assert all(c in "01" for c in hash_value)

    def test_hash_difference(self):
        """Test difference hash algorithm."""
        # Create a simple pattern
        img = np.zeros((8, 8, 3), dtype=np.uint8)
        for y in range(8):
            for x in range(8):
                if x < 4:
                    img[y, x, :] = 128
                else:
                    img[y, x, :] = 192

        df = daft.from_pydict({"image_data": [img]})
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

        result = df.with_column("hash", col("image").image.hash("difference"))
        result = result.collect()
        hash_value = result.to_pydict()["hash"][0]

        assert hash_value is not None
        assert len(hash_value) == 64
        assert all(c in "01" for c in hash_value)

    def test_hash_wavelet(self):
        """Test wavelet hash algorithm."""
        # Create a more complex pattern
        img = np.zeros((64, 64, 3), dtype=np.uint8)
        for y in range(64):
            for x in range(64):
                if (x // 8 + y // 8) % 2 == 0:
                    img[y, x, :] = 200
                else:
                    img[y, x, :] = 100

        df = daft.from_pydict({"image_data": [img]})
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

        result = df.with_column("hash", col("image").image.hash("wavelet"))
        result = result.collect()
        hash_value = result.to_pydict()["hash"][0]

        assert hash_value is not None
        assert len(hash_value) == 64
        assert all(c in "01" for c in hash_value)

    def test_hash_crop_resistant(self):
        """Test crop-resistant hash algorithm."""
        # Create a detailed pattern
        img = np.zeros((256, 256, 3), dtype=np.uint8)
        for y in range(256):
            for x in range(256):
                if (x // 32 + y // 32) % 2 == 0:
                    img[y, x, :] = 150
                else:
                    img[y, x, :] = 50

        df = daft.from_pydict({"image_data": [img]})
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

        result = df.with_column("hash", col("image").image.hash("crop_resistant"))
        result = result.collect()
        hash_value = result.to_pydict()["hash"][0]

        assert hash_value is not None
        assert len(hash_value) == 64
        assert all(c in "01" for c in hash_value)

    def test_hash_default_algorithm(self):
        """Test that default algorithm is 'average'."""
        img = np.zeros((8, 8, 3), dtype=np.uint8)
        img[:, :, :] = 128

        df = daft.from_pydict({"image_data": [img]})
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

        # Test default (no algorithm specified)
        result_default = df.with_column("hash_default", col("image").image.hash())
        # Test explicit average
        result_explicit = df.with_column("hash_explicit", col("image").image.hash("average"))

        result_default = result_default.collect()
        result_explicit = result_explicit.collect()

        default_hash = result_default.to_pydict()["hash_default"][0]
        explicit_hash = result_explicit.to_pydict()["hash_explicit"][0]

        assert default_hash == explicit_hash, "Default algorithm is not 'average'"

    def test_hash_all_algorithms(self):
        """Test all algorithms produce different hashes for the same image."""
        # Create a complex test image
        img = np.random.randint(0, 256, (64, 64, 3), dtype=np.uint8)
        np.random.seed(42)  # For deterministic results

        df = daft.from_pydict({"image_data": [img]})
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

        algorithms = ["average", "perceptual", "difference", "wavelet", "crop_resistant"]
        hashes = {}

        for algorithm in algorithms:
            result = df.with_column(f"hash_{algorithm}", col("image").image.hash(algorithm))
            result = result.collect()
            hashes[algorithm] = result.to_pydict()[f"hash_{algorithm}"][0]

        # All hashes should be different
        hash_values = list(hashes.values())
        assert len(set(hash_values)) == len(hash_values), "All algorithms should produce different hashes"

    def test_hash_error_handling(self):
        """Test error handling for invalid algorithms."""
        img = np.zeros((8, 8, 3), dtype=np.uint8)
        df = daft.from_pydict({"image_data": [img]})
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

        # Test invalid algorithm
        with pytest.raises(Exception):  # Should raise an error for invalid algorithm
            result = df.with_column("hash_invalid", col("image").image.hash("invalid_algorithm"))
            result.collect()

    def test_hash_different_images(self):
        """Test that different images produce different hashes."""
        # Create two different images
        img1 = np.zeros((8, 8, 3), dtype=np.uint8)
        img1[:, :, :] = 100

        img2 = np.zeros((8, 8, 3), dtype=np.uint8)
        img2[:, :, :] = 200

        df = daft.from_pydict({"image_data": [img1, img2]})
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

        result = df.with_column("hash", col("image").image.hash("average"))
        result = result.collect()
        hashes = result.to_pydict()["hash"]

        assert len(hashes) == 2
        assert hashes[0] != hashes[1], "Different images should produce different hashes"

    def test_hash_series_api(self):
        """Test the Series API for hash functions."""
        img = np.zeros((8, 8, 3), dtype=np.uint8)
        img[:, :, :] = 128

        df = daft.from_pydict({"image_data": [img]})
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

        # Test via series API
        result = df.with_column("hash", df["image"].image.hash("average"))
        result = result.collect()
        hash_value = result.to_pydict()["hash"][0]

        assert hash_value is not None
        assert len(hash_value) == 64
        assert all(c in "01" for c in hash_value)

    def test_hash_solid_colors(self):
        """Test hash functions with solid color images."""
        colors = [
            (0, 0, 0),      # Black
            (255, 255, 255), # White
            (128, 128, 128), # Gray
            (255, 0, 0),    # Red
            (0, 255, 0),    # Green
            (0, 0, 255),    # Blue
        ]

        algorithms = ["average", "perceptual", "difference", "wavelet", "crop_resistant"]

        for color in colors:
            img = np.full((8, 8, 3), color, dtype=np.uint8)
            df = daft.from_pydict({"image_data": [img]})
            df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

            for algorithm in algorithms:
                result = df.with_column("hash", col("image").image.hash(algorithm))
                result = result.collect()
                hash_value = result.to_pydict()["hash"][0]

                assert hash_value is not None
                assert len(hash_value) == 64
                assert all(c in "01" for c in hash_value)

    def test_hash_difference_solid_colors(self):
        """Test difference hash with solid colors (should be all zeros)."""
        # Create solid color images
        black_img = np.zeros((8, 8, 3), dtype=np.uint8)
        white_img = np.full((8, 8, 3), 255, dtype=np.uint8)
        gray_img = np.full((8, 8, 3), 128, dtype=np.uint8)

        df = daft.from_pydict({"image_data": [black_img, white_img, gray_img], "name": ["black", "white", "gray"]})

        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.hash("difference"))

        result = df.collect()
        result_dict = result.to_pydict()

        # Solid color images should have all 0s (no differences between adjacent pixels)
        black_hash, white_hash, gray_hash = result_dict["hash"]
        assert black_hash == "0" * 64, f"Black image should be all zeros, got {black_hash}"
        assert white_hash == "0" * 64, f"White image should be all zeros, got {white_hash}"
        assert gray_hash == "0" * 64, f"Gray image should be all zeros, got {gray_hash}"

    def test_hash_difference_gradient(self):
        """Test difference hash with gradient images."""
        # Create horizontal gradient images
        gradient_left_to_right = np.zeros((8, 8, 3), dtype=np.uint8)
        for x in range(8):
            gradient_left_to_right[:, x, :] = x * 32

        df = daft.from_pydict({"image_data": [gradient_left_to_right]})
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.hash("difference"))

        result = df.collect()
        hash_value = result.to_pydict()["hash"][0]

        # Gradient should produce non-zero hash (differences between adjacent pixels)
        assert hash_value != "0" * 64, "Gradient should produce non-zero hash"
        assert len(hash_value) == 64
        assert all(c in "01" for c in hash_value)

    def test_hash_consistency(self):
        """Test that identical images produce identical hashes."""
        # Create identical images
        test_img = np.random.randint(0, 255, (16, 16, 3), dtype=np.uint8)
        identical_img = test_img.copy()

        df = daft.from_pydict({
            "image_data": [test_img, identical_img, test_img],
            "id": ["original", "copy", "duplicate"]
        })

        algorithms = ["average", "perceptual", "difference", "wavelet", "crop_resistant"]

        for algorithm in algorithms:
            df_with_hash = df.with_column("hash", col("image").image.hash(algorithm))
            result = df_with_hash.collect()
            hashes = result.to_pydict()["hash"]

            # All hashes should be identical
            hash1, hash2, hash3 = hashes[0], hashes[1], hashes[2]
            assert hash1 == hash2 == hash3, f"Identical images should produce identical {algorithm} hashes"
            assert len(hash1) == 64

    def test_hash_null_handling(self):
        """Test hash functions with null values."""
        # Create test data with nulls
        valid_img = np.full((8, 8, 3), 100, dtype=np.uint8)

        df = daft.from_pydict({
            "image_data": [valid_img, None, valid_img, None],
            "id": [1, 2, 3, 4]
        })

        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

        algorithms = ["average", "perceptual", "difference", "wavelet", "crop_resistant"]

        for algorithm in algorithms:
            df_with_hash = df.with_column("hash", col("image").image.hash(algorithm))
            result = df_with_hash.collect()
            hashes = result.to_pydict()["hash"]

            # Check null handling
            assert hashes[0] is not None  # Valid image
            assert hashes[1] is None  # Null image
            assert hashes[2] is not None  # Valid image
            assert hashes[3] is None  # Null image

            # Valid hashes should be identical (same image)
            assert hashes[0] == hashes[2]
            assert len(hashes[0]) == 64

    def test_hash_different_images(self):
        """Test that different images produce different hashes."""
        # Create two different images
        img1 = np.zeros((8, 8, 3), dtype=np.uint8)
        img1[:, :, :] = 100

        img2 = np.zeros((8, 8, 3), dtype=np.uint8)
        img2[:, :, :] = 200

        df = daft.from_pydict({"image_data": [img1, img2]})
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

        algorithms = ["average", "perceptual", "difference", "wavelet", "crop_resistant"]

        for algorithm in algorithms:
            df_with_hash = df.with_column("hash", col("image").image.hash(algorithm))
            result = df_with_hash.collect()
            hashes = result.to_pydict()["hash"]

            assert len(hashes) == 2
            assert hashes[0] != hashes[1], f"Different images should produce different {algorithm} hashes"

    def test_hash_deduplication_scenario(self):
        """Test realistic deduplication use case."""
        # Create "duplicate" images (same content, different objects)
        original = np.random.randint(0, 255, (12, 12, 3), dtype=np.uint8)
        duplicate1 = original.copy()
        duplicate2 = original.copy()
        different = np.random.randint(0, 255, (12, 12, 3), dtype=np.uint8)

        df = daft.from_pydict({
            "image_data": [original, duplicate1, duplicate2, different],
            "filename": ["img1.jpg", "img1_copy.jpg", "img1_backup.jpg", "img2.jpg"],
        })

        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.hash("average"))

        # Simple deduplication: group by hash and count
        df_with_count = df.groupby("hash").agg([col("filename").count().alias("count")])
        duplicates = df_with_count.filter(col("count") > 1)

        duplicate_result = duplicates.collect()
        duplicate_dict = duplicate_result.to_pydict()

        # Should find one group of duplicates (the first 3 images)
        assert len(duplicate_dict["count"]) == 1
        assert duplicate_dict["count"][0] == 3

    def test_hash_edge_cases(self):
        """Test edge cases and potential error conditions."""
        # Single pixel images
        single_pixel_black = np.zeros((1, 1, 3), dtype=np.uint8)
        single_pixel_white = np.full((1, 1, 3), 255, dtype=np.uint8)

        # Very small images
        tiny_img = np.full((2, 2, 3), 128, dtype=np.uint8)

        df = daft.from_pydict({
            "image_data": [single_pixel_black, single_pixel_white, tiny_img],
            "type": ["1x1_black", "1x1_white", "2x2"]
        })

        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))

        algorithms = ["average", "perceptual", "difference", "wavelet", "crop_resistant"]

        for algorithm in algorithms:
            df_with_hash = df.with_column("hash", col("image").image.hash(algorithm))
            result = df_with_hash.collect()
            hashes = result.to_pydict()["hash"]

            # All should produce valid hashes
            for i, hash_val in enumerate(hashes):
                assert hash_val is not None, f"Hash {i} should not be None for {algorithm}"
                assert len(hash_val) == 64, f"Hash {i} should be 64 chars for {algorithm}"
                assert all(c in "01" for c in hash_val), f"Hash {i} should be binary for {algorithm}"

            # Single pixel images should have uniform hashes for average hash
            if algorithm == "average":
                black_hash, white_hash = hashes[0], hashes[1]
                assert black_hash == "0" * 64, "Single black pixel should produce all 0s for average hash"
                assert white_hash == "1" * 64, "Single white pixel should produce all 1s for average hash"
