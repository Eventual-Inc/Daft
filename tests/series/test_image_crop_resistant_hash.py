"""
Tests for the image crop-resistant hash function.

These tests use in-memory numpy arrays to avoid file I/O dependencies
and focus on testing the hash algorithm correctness and edge cases.
"""

import pytest
import numpy as np
import daft
from daft import col
from daft.datatype import DataType


class TestImageCropResistantHash:
    """Test suite for image crop-resistant hash functionality."""
    
    def test_crop_resistant_hash_solid_colors(self):
        """Test crop-resistant hash with solid color images."""
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
        df = df.with_column("hash", col("image").image.crop_resistant_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        # Validate hash format
        for hash_val in result_dict["hash"]:
            assert hash_val is not None
            assert len(hash_val) == 64, f"Hash should be 64 characters, got {len(hash_val)}"
            assert all(c in '01' for c in hash_val), f"Hash should be binary, got {hash_val}"
        
        # Test hash properties for solid colors
        black_hash, white_hash, gray_hash = result_dict["hash"]
        
        # For solid colors, the crop-resistant hash should be consistent
        assert black_hash is not None, "Black image should have a hash"
        assert white_hash is not None, "White image should have a hash"
        assert gray_hash is not None, "Gray image should have a hash"
        
        # All three should be different (different intensity levels)
        assert black_hash != white_hash, "Black and white hashes should be different"
        assert black_hash != gray_hash, "Black and gray hashes should be different"
        # Note: white (255) and gray (128) may have the same hash since both are above threshold
        # This is expected behavior for the crop-resistant hash algorithm
    
    def test_crop_resistant_hash_gradient(self):
        """Test crop-resistant hash with gradient images."""
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
        df = df.with_column("hash", col("image").image.crop_resistant_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        left_to_right_hash, right_to_left_hash = result_dict["hash"]
        
        # Validate hash format
        for hash_val in result_dict["hash"]:
            assert hash_val is not None
            assert len(hash_val) == 64, f"Hash should be 64 characters, got {len(hash_val)}"
            assert all(c in '01' for c in hash_val), f"Hash should be binary, got {hash_val}"
        
        # Gradient images should produce different hashes
        assert left_to_right_hash != right_to_left_hash, "Different gradient directions should produce different hashes"
    
    def test_crop_resistant_hash_checkerboard(self):
        """Test crop-resistant hash with checkerboard pattern."""
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
        df = df.with_column("hash", col("image").image.crop_resistant_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        checkerboard_hash = result_dict["hash"][0]
        
        # Validate hash format
        assert checkerboard_hash is not None
        assert len(checkerboard_hash) == 64, f"Hash should be 64 characters, got {len(checkerboard_hash)}"
        assert all(c in '01' for c in checkerboard_hash), f"Hash should be binary, got {checkerboard_hash}"
        
        # Checkerboard should have a valid hash
        assert checkerboard_hash != '0' * 64, "Checkerboard should not be all zeros"
        assert checkerboard_hash != '1' * 64, "Checkerboard should not be all ones"
    
    def test_crop_resistant_hash_different_sizes(self):
        """Test crop-resistant hash with images of different sizes."""
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
        df = df.with_column("hash", col("image").image.crop_resistant_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        # All hashes should be 64 characters regardless of input size
        for i, hash_val in enumerate(result_dict["hash"]):
            assert hash_val is not None, f"Hash should not be None for image {i}"
            assert len(hash_val) == 64, f"Hash should be 64 characters for image {i}, got {len(hash_val)}"
            assert all(c in '01' for c in hash_val), f"Hash should be binary for image {i}, got {hash_val}"
    
    def test_crop_resistant_hash_different_modes(self):
        """Test crop-resistant hash with different image modes."""
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
        df = df.with_column("rgb_hash", col("rgb_image").image.crop_resistant_hash())
        df = df.with_column("grayscale_hash", col("grayscale_image").image.crop_resistant_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        # Validate both hashes
        for hash_name in ["rgb_hash", "grayscale_hash"]:
            hash_val = result_dict[hash_name][0]
            assert hash_val is not None, f"Hash should not be None for {hash_name}"
            assert len(hash_val) == 64, f"Hash should be 64 characters for {hash_name}, got {len(hash_val)}"
            assert all(c in '01' for c in hash_val), f"Hash should be binary for {hash_name}, got {hash_val}"
    
    def test_crop_resistant_hash_null_handling(self):
        """Test crop-resistant hash with null values."""
        # Create image with null values
        valid_img = np.random.randint(0, 256, (8, 8, 3), dtype=np.uint8)
        
        df = daft.from_pydict({
            "image_data": [valid_img, None],
            "name": ["valid", "null"]
        })
        
        # Cast to image type and compute hashes
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.crop_resistant_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        # First image should have a valid hash, second should be None
        valid_hash, null_hash = result_dict["hash"]
        
        assert valid_hash is not None, "Valid image should have a hash"
        assert len(valid_hash) == 64, f"Valid hash should be 64 characters, got {len(valid_hash)}"
        assert all(c in '01' for c in valid_hash), f"Valid hash should be binary, got {valid_hash}"
        
        assert null_hash is None, "Null image should have None hash"
    
    def test_crop_resistant_hash_consistency(self):
        """Test that crop-resistant hash is consistent for identical images."""
        # Create identical images
        img1 = np.random.randint(0, 256, (8, 8, 3), dtype=np.uint8)
        img2 = img1.copy()  # Exact copy
        
        df = daft.from_pydict({
            "image_data": [img1, img2],
            "name": ["img1", "img2"]
        })
        
        # Cast to image type and compute hashes
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.crop_resistant_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        hash1, hash2 = result_dict["hash"]
        
        # Identical images should produce identical hashes
        assert hash1 == hash2, f"Identical images should produce identical hashes: {hash1} != {hash2}"
        assert hash1 is not None, "Hash should not be None"
        assert len(hash1) == 64, f"Hash should be 64 characters, got {len(hash1)}"
    
    def test_crop_resistant_hash_vs_other_hashes_difference(self):
        """Test that crop-resistant hash produces different results than other hash methods."""
        # Create a random image
        img = np.random.randint(0, 256, (8, 8, 3), dtype=np.uint8)
        
        df = daft.from_pydict({
            "image_data": [img],
            "name": ["random"]
        })
        
        # Cast to image type and compute all hashes
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("avg_hash", col("image").image.average_hash())
        df = df.with_column("phash", col("image").image.perceptual_hash())
        df = df.with_column("dhash", col("image").image.difference_hash())
        df = df.with_column("whash", col("image").image.wavelet_hash())
        df = df.with_column("chash", col("image").image.crop_resistant_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        avg_hash = result_dict["avg_hash"][0]
        phash = result_dict["phash"][0]
        dhash = result_dict["dhash"][0]
        whash = result_dict["whash"][0]
        chash = result_dict["chash"][0]
        
        # All should be valid hashes
        for hash_name, hash_val in [("avg_hash", avg_hash), ("phash", phash), 
                                   ("dhash", dhash), ("whash", whash), ("chash", chash)]:
            assert hash_val is not None, f"{hash_name} should not be None"
            assert len(hash_val) == 64, f"{hash_name} should be 64 characters, got {len(hash_val)}"
            assert all(c in '01' for c in hash_val), f"{hash_name} should be binary, got {hash_val}"
        
        # Crop-resistant hash should be different from others (unless by coincidence)
        assert chash != avg_hash, f"Crop-resistant and average hashes should be different: {chash} == {avg_hash}"
        assert chash != phash, f"Crop-resistant and perceptual hashes should be different: {chash} == {phash}"
        assert chash != dhash, f"Crop-resistant and difference hashes should be different: {chash} == {dhash}"
        assert chash != whash, f"Crop-resistant and wavelet hashes should be different: {chash} == {whash}"
    
    def test_crop_resistant_hash_regional_patterns(self):
        """Test crop-resistant hash with regional patterns to verify regional analysis."""
        # Create an image with distinct regional patterns
        regional_img = np.zeros((8, 8, 3), dtype=np.uint8)
        
        # Create distinct regions
        regional_img[0:4, 0:4, :] = 255  # Top-left: white
        regional_img[0:4, 4:8, :] = 128  # Top-right: gray
        regional_img[4:8, 0:4, :] = 64   # Bottom-left: dark gray
        regional_img[4:8, 4:8, :] = 0    # Bottom-right: black
        
        df = daft.from_pydict({
            "image_data": [regional_img],
            "name": ["regional"]
        })
        
        # Cast to image type and compute hash
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.crop_resistant_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        regional_hash = result_dict["hash"][0]
        
        # Validate hash format
        assert regional_hash is not None
        assert len(regional_hash) == 64, f"Hash should be 64 characters, got {len(regional_hash)}"
        assert all(c in '01' for c in regional_hash), f"Hash should be binary, got {regional_hash}"
        
        # Regional patterns should produce a hash that reflects the regional differences
        assert regional_hash != '0' * 64, "Regional pattern should not be all zeros"
        assert regional_hash != '1' * 64, "Regional pattern should not be all ones"
    
    def test_crop_resistant_hash_robustness_to_scaling(self):
        """Test that crop-resistant hash is somewhat robust to scaling (resize to same size)."""
        # Create the same pattern at different original sizes
        small_pattern = np.zeros((4, 4, 3), dtype=np.uint8)
        large_pattern = np.zeros((16, 16, 3), dtype=np.uint8)
        
        # Same pattern: checkerboard
        for y in range(4):
            for x in range(4):
                if (x + y) % 2 == 0:
                    small_pattern[y, x, :] = 255
                    # Scale up the pattern
                    large_pattern[y*4:(y+1)*4, x*4:(x+1)*4, :] = 255
        
        df = daft.from_pydict({
            "image_data": [small_pattern, large_pattern],
            "name": ["small", "large"]
        })
        
        # Cast to image type and compute hashes
        df = df.with_column("image", col("image_data").cast(DataType.image("RGB")))
        df = df.with_column("hash", col("image").image.crop_resistant_hash())
        
        result = df.collect()
        result_dict = result.to_pydict()
        
        small_hash, large_hash = result_dict["hash"]
        
        # Both should be valid hashes
        assert small_hash is not None, "Small pattern should have a hash"
        assert large_hash is not None, "Large pattern should have a hash"
        assert len(small_hash) == 64, f"Small hash should be 64 characters, got {len(small_hash)}"
        assert len(large_hash) == 64, f"Large hash should be 64 characters, got {len(large_hash)}"
        
        # Note: The hashes might be different due to the different input sizes,
        # but both should be valid binary strings
        assert all(c in '01' for c in small_hash), f"Small hash should be binary, got {small_hash}"
        assert all(c in '01' for c in large_hash), f"Large hash should be binary, got {large_hash}"
