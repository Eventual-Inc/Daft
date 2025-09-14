"""
Tests for the image difference hash function.

These tests use in-memory numpy arrays to avoid file I/O dependencies
and focus on testing the hash algorithm correctness and edge cases.
"""

import pytest
import numpy as np
import daft
from daft import col
from daft.datatype import DataType


class TestImageDifferenceHash:
    """Test suite for image difference hash functionality."""
    
    def test_difference_hash_solid_colors(self):
        """Test difference hash with solid color images."""
        # Create solid color images
        black_img = np.zeros((8, 8, 3), dtype=np.uint8)
        white_img = np.full((8, 8, 3), 255, dtype=np.uint8)
        gray_img = np.full((8, 8, 3), 128, dtype=np.uint8)
        
        df = daft.from_pydict({
            "image_data": [black_img, white_img, gray_img],
            "name": ["black", "white", "gray"]
        })
        
        # Cast to image type and compute hashes
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.difference_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        # Validate hash format
        for hash_val in result_dict["hash"]:
            assert hash_val is not None
            assert len(hash_val) == 64, f"Hash should be 64 characters, got {len(hash_val)}"
            assert all(c in '01' for c in hash_val), f"Hash should be binary, got {hash_val}"
        
        # Test hash properties for solid colors
        black_hash, white_hash, gray_hash = result_dict["hash"]
        
        # Solid color images should have all 0s (no differences between adjacent pixels)
        assert black_hash == '0' * 64, f"Black image should be all zeros, got {black_hash}"
        assert white_hash == '0' * 64, f"White image should be all zeros, got {white_hash}"
        assert gray_hash == '0' * 64, f"Gray image should be all zeros, got {gray_hash}"
    
    def test_difference_hash_gradient(self):
        """Test difference hash with gradient images."""
        # Create horizontal gradient images
        gradient_left_to_right = np.zeros((8, 8, 3), dtype=np.uint8)
        gradient_right_to_left = np.zeros((8, 8, 3), dtype=np.uint8)
        
        for x in range(8):
            intensity = int(255 * x / 7)  # 0 to 255
            gradient_left_to_right[:, x, :] = intensity
            gradient_right_to_left[:, x, :] = 255 - intensity
        
        df = daft.from_pydict({
            "image_data": [gradient_left_to_right, gradient_right_to_left],
            "name": ["left_to_right", "right_to_left"]
        })
        
        # Cast to image type and compute hashes
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.difference_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        left_to_right_hash, right_to_left_hash = result_dict["hash"]
        
        # Validate hash format
        for hash_val in result_dict["hash"]:
            assert hash_val is not None
            assert len(hash_val) == 64, f"Hash should be 64 characters, got {len(hash_val)}"
            assert all(c in '01' for c in hash_val), f"Hash should be binary, got {hash_val}"
        
        # Left-to-right gradient should have all 1s (each pixel is brighter than the previous)
        assert left_to_right_hash == '1' * 64, f"Left-to-right gradient should be all ones, got {left_to_right_hash}"
        
        # Right-to-left gradient should have all 0s (each pixel is darker than the previous)
        assert right_to_left_hash == '0' * 64, f"Right-to-left gradient should be all zeros, got {right_to_left_hash}"
    
    def test_difference_hash_checkerboard(self):
        """Test difference hash with checkerboard pattern."""
        # Create checkerboard pattern
        checkerboard = np.zeros((8, 8, 3), dtype=np.uint8)
        for y in range(8):
            for x in range(8):
                if (x + y) % 2 == 0:
                    checkerboard[y, x, :] = 255
        
        df = daft.from_pydict({
            "image_data": [checkerboard],
            "name": ["checkerboard"]
        })
        
        # Cast to image type and compute hashes
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.difference_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        checkerboard_hash = result_dict["hash"][0]
        
        # Validate hash format
        assert checkerboard_hash is not None
        assert len(checkerboard_hash) == 64, f"Hash should be 64 characters, got {len(checkerboard_hash)}"
        assert all(c in '01' for c in checkerboard_hash), f"Hash should be binary, got {checkerboard_hash}"
        
        # Checkerboard should have alternating pattern in hash
        # The pattern depends on the exact pixel arrangement
        assert checkerboard_hash != '0' * 64, "Checkerboard should not be all zeros"
        assert checkerboard_hash != '1' * 64, "Checkerboard should not be all ones"
    
    def test_difference_hash_different_sizes(self):
        """Test difference hash with images of different sizes."""
        # Create images of different sizes
        small_img = np.random.randint(0, 256, (4, 4, 3), dtype=np.uint8)
        large_img = np.random.randint(0, 256, (32, 32, 3), dtype=np.uint8)
        tall_img = np.random.randint(0, 256, (8, 4, 3), dtype=np.uint8)
        wide_img = np.random.randint(0, 256, (4, 8, 3), dtype=np.uint8)
        
        df = daft.from_pydict({
            "image_data": [small_img, large_img, tall_img, wide_img],
            "name": ["small", "large", "tall", "wide"]
        })
        
        # Cast to image type and compute hashes
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.difference_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        # All hashes should be 64 characters regardless of input size
        for i, hash_val in enumerate(result_dict["hash"]):
            assert hash_val is not None, f"Hash should not be None for image {i}"
            assert len(hash_val) == 64, f"Hash should be 64 characters for image {i}, got {len(hash_val)}"
            assert all(c in '01' for c in hash_val), f"Hash should be binary for image {i}, got {hash_val}"
    
    def test_difference_hash_different_modes(self):
        """Test difference hash with different image modes."""
        # Create grayscale and RGB images
        rgb_img = np.random.randint(0, 256, (8, 8, 3), dtype=np.uint8)
        # For grayscale, create single-channel image
        grayscale_img = np.random.randint(0, 256, (8, 8, 1), dtype=np.uint8)
        
        df = daft.from_pydict({
            "rgb_data": [rgb_img],
            "grayscale_data": [grayscale_img],
        })
        
        # Cast to different image types and compute hashes
        df = df.with_column("rgb_image", col("rgb_data").cast(DataType.image("RGB")))
        df = df.with_column("grayscale_image", col("grayscale_data").cast(DataType.image("L")))
        df = df.with_column("rgb_hash", col("rgb_image").image.difference_hash())
        df = df.with_column("grayscale_hash", col("grayscale_image").image.difference_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        # Validate both hashes
        for hash_name in ["rgb_hash", "grayscale_hash"]:
            hash_val = result_dict[hash_name][0]
            assert hash_val is not None, f"Hash should not be None for {hash_name}"
            assert len(hash_val) == 64, f"Hash should be 64 characters for {hash_name}, got {len(hash_val)}"
            assert all(c in '01' for c in hash_val), f"Hash should be binary for {hash_name}, got {hash_val}"
    
    def test_difference_hash_null_handling(self):
        """Test difference hash with null values."""
        # Create image with null values
        valid_img = np.random.randint(0, 256, (8, 8, 3), dtype=np.uint8)
        
        df = daft.from_pydict({
            "image_data": [valid_img, None],
            "name": ["valid", "null"]
        })
        
        # Cast to image type and compute hashes
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.difference_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        # First image should have a valid hash, second should be None
        valid_hash, null_hash = result_dict["hash"]
        
        assert valid_hash is not None, "Valid image should have a hash"
        assert len(valid_hash) == 64, f"Valid hash should be 64 characters, got {len(valid_hash)}"
        assert all(c in '01' for c in valid_hash), f"Valid hash should be binary, got {valid_hash}"
        
        assert null_hash is None, "Null image should have None hash"
    
    def test_difference_hash_consistency(self):
        """Test that difference hash is consistent for identical images."""
        # Create identical images
        img1 = np.random.randint(0, 256, (8, 8, 3), dtype=np.uint8)
        img2 = img1.copy()  # Exact copy
        
        df = daft.from_pydict({
            "image_data": [img1, img2],
            "name": ["img1", "img2"]
        })
        
        # Cast to image type and compute hashes
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.difference_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        hash1, hash2 = result_dict["hash"]
        
        # Identical images should produce identical hashes
        assert hash1 == hash2, f"Identical images should produce identical hashes: {hash1} != {hash2}"
        assert hash1 is not None, "Hash should not be None"
        assert len(hash1) == 64, f"Hash should be 64 characters, got {len(hash1)}"
    
    def test_difference_hash_vs_average_hash_difference(self):
        """Test that difference hash produces different results than average hash."""
        # Create a random image
        img = np.random.randint(0, 256, (8, 8, 3), dtype=np.uint8)
        
        df = daft.from_pydict({
            "image_data": [img],
            "name": ["random"]
        })
        
        # Cast to image type and compute both hashes
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("avg_hash", col("image").image.average_hash())
        df = df.with_column("diff_hash", col("image").image.difference_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        avg_hash = result_dict["avg_hash"][0]
        diff_hash = result_dict["diff_hash"][0]
        
        # Both should be valid hashes
        assert avg_hash is not None, "Average hash should not be None"
        assert diff_hash is not None, "Difference hash should not be None"
        
        # Both should be 64 characters
        assert len(avg_hash) == 64, f"Average hash should be 64 characters, got {len(avg_hash)}"
        assert len(diff_hash) == 64, f"Difference hash should be 64 characters, got {len(diff_hash)}"
        
        # They should be different (unless by coincidence)
        # Note: There's a small chance they could be identical by coincidence
        # but it's extremely unlikely for a random image
        assert avg_hash != diff_hash, f"Average and difference hashes should be different: {avg_hash} == {diff_hash}"
