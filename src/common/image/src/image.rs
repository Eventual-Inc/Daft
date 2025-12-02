use core::slice;
use std::{hash::Hash, ops::Deref};

use common_ndarray::NdArray;
use image::{DynamicImage, ImageBuffer, Pixel, flat::SampleLayout};
use ndarray::{Array3, ShapeBuilder};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Wrapper around image::DynamicImage to implement certain traits
#[derive(Clone, Debug, PartialEq)]
pub struct Image(pub DynamicImage);

impl Image {
    pub fn into_ndarray(self) -> NdArray {
        fn into_ndarray3<P: Pixel>(buf: ImageBuffer<P, Vec<P::Subpixel>>) -> Array3<P::Subpixel> {
            let SampleLayout {
                channels,
                channel_stride,
                height,
                height_stride,
                width,
                width_stride,
            } = buf.sample_layout();
            let shape = (height as usize, width as usize, channels as usize);
            let strides = (height_stride, width_stride, channel_stride);
            Array3::from_shape_vec(shape.strides(strides), buf.into_raw()).unwrap()
        }

        match self.0 {
            DynamicImage::ImageLuma8(buf) => NdArray::U8(into_ndarray3(buf).into_dyn()),
            DynamicImage::ImageLumaA8(buf) => NdArray::U8(into_ndarray3(buf).into_dyn()),
            DynamicImage::ImageRgb8(buf) => NdArray::U8(into_ndarray3(buf).into_dyn()),
            DynamicImage::ImageRgba8(buf) => NdArray::U8(into_ndarray3(buf).into_dyn()),
            DynamicImage::ImageLuma16(buf) => NdArray::U16(into_ndarray3(buf).into_dyn()),
            DynamicImage::ImageLumaA16(buf) => NdArray::U16(into_ndarray3(buf).into_dyn()),
            DynamicImage::ImageRgb16(buf) => NdArray::U16(into_ndarray3(buf).into_dyn()),
            DynamicImage::ImageRgba16(buf) => NdArray::U16(into_ndarray3(buf).into_dyn()),
            DynamicImage::ImageRgb32F(buf) => NdArray::F32(into_ndarray3(buf).into_dyn()),
            DynamicImage::ImageRgba32F(buf) => NdArray::F32(into_ndarray3(buf).into_dyn()),
            _ => unimplemented!("unsupported DynamicImage variant"),
        }
    }
}

impl Deref for Image {
    type Target = DynamicImage;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Hash for Image {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.0 {
            DynamicImage::ImageLuma8(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageLumaA8(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageRgb8(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageRgba8(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageLuma16(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageLumaA16(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageRgb16(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageRgba16(image_buffer) => image_buffer.hash(state),
            DynamicImage::ImageRgb32F(image_buffer) => {
                image_buffer.width().hash(state);
                image_buffer.height().hash(state);
                // float can't be hashed so use the buffer as a byte slice
                let buffer_slice = unsafe {
                    slice::from_raw_parts(
                        image_buffer.as_raw().as_ptr().cast::<u8>(),
                        image_buffer.as_raw().len() * 4,
                    )
                };
                buffer_slice.hash(state);
            }
            DynamicImage::ImageRgba32F(image_buffer) => {
                image_buffer.width().hash(state);
                image_buffer.height().hash(state);
                // float can't be hashed so use the buffer as a byte slice
                let buffer_slice = unsafe {
                    slice::from_raw_parts(
                        image_buffer.as_raw().as_ptr().cast::<u8>(),
                        image_buffer.as_raw().len() * 4,
                    )
                };
                buffer_slice.hash(state);
            }
            _ => unimplemented!("unsupported DynamicImage variant"),
        }
    }
}

// Helper struct for serialization
#[derive(Serialize, Deserialize)]
struct SerializedImageBufferWrapper {
    variant: u8,
    width: u32,
    height: u32,
    data: Vec<u8>,
}

impl Serialize for Image {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let (variant, width, height, data) = match &self.0 {
            DynamicImage::ImageLuma8(img) => (0u8, img.width(), img.height(), img.as_raw().clone()),
            DynamicImage::ImageLumaA8(img) => {
                (1u8, img.width(), img.height(), img.as_raw().clone())
            }
            DynamicImage::ImageRgb8(img) => (2u8, img.width(), img.height(), img.as_raw().clone()),
            DynamicImage::ImageRgba8(img) => (3u8, img.width(), img.height(), img.as_raw().clone()),
            DynamicImage::ImageLuma16(img) => {
                let bytes: Vec<u8> = img.as_raw().iter().flat_map(|&x| x.to_le_bytes()).collect();
                (4u8, img.width(), img.height(), bytes)
            }
            DynamicImage::ImageLumaA16(img) => {
                let bytes: Vec<u8> = img.as_raw().iter().flat_map(|&x| x.to_le_bytes()).collect();
                (5u8, img.width(), img.height(), bytes)
            }
            DynamicImage::ImageRgb16(img) => {
                let bytes: Vec<u8> = img.as_raw().iter().flat_map(|&x| x.to_le_bytes()).collect();
                (6u8, img.width(), img.height(), bytes)
            }
            DynamicImage::ImageRgba16(img) => {
                let bytes: Vec<u8> = img.as_raw().iter().flat_map(|&x| x.to_le_bytes()).collect();
                (7u8, img.width(), img.height(), bytes)
            }
            DynamicImage::ImageRgb32F(img) => {
                let bytes: Vec<u8> = img.as_raw().iter().flat_map(|&x| x.to_le_bytes()).collect();
                (8u8, img.width(), img.height(), bytes)
            }
            DynamicImage::ImageRgba32F(img) => {
                let bytes: Vec<u8> = img.as_raw().iter().flat_map(|&x| x.to_le_bytes()).collect();
                (9u8, img.width(), img.height(), bytes)
            }
            _ => {
                return Err(serde::ser::Error::custom(
                    "Unsupported DynamicImage variant",
                ));
            }
        };

        let serialized = SerializedImageBufferWrapper {
            variant,
            width,
            height,
            data,
        };

        serialized.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Image {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serialized = SerializedImageBufferWrapper::deserialize(deserializer)?;

        let dynamic_image = match serialized.variant {
            0 => {
                // ImageLuma8
                let img =
                    ImageBuffer::from_raw(serialized.width, serialized.height, serialized.data)
                        .ok_or_else(|| {
                            serde::de::Error::custom("Failed to create ImageLuma8 from raw data")
                        })?;
                DynamicImage::ImageLuma8(img)
            }
            1 => {
                // ImageLumaA8
                let img =
                    ImageBuffer::from_raw(serialized.width, serialized.height, serialized.data)
                        .ok_or_else(|| {
                            serde::de::Error::custom("Failed to create ImageLumaA8 from raw data")
                        })?;
                DynamicImage::ImageLumaA8(img)
            }
            2 => {
                // ImageRgb8
                let img =
                    ImageBuffer::from_raw(serialized.width, serialized.height, serialized.data)
                        .ok_or_else(|| {
                            serde::de::Error::custom("Failed to create ImageRgb8 from raw data")
                        })?;
                DynamicImage::ImageRgb8(img)
            }
            3 => {
                // ImageRgba8
                let img =
                    ImageBuffer::from_raw(serialized.width, serialized.height, serialized.data)
                        .ok_or_else(|| {
                            serde::de::Error::custom("Failed to create ImageRgba8 from raw data")
                        })?;
                DynamicImage::ImageRgba8(img)
            }
            4 => {
                // ImageLuma16
                if serialized.data.len() % 2 != 0 {
                    return Err(serde::de::Error::custom(
                        "Invalid data length for u16 pixels",
                    ));
                }
                let u16_data: Vec<u16> = serialized
                    .data
                    .chunks_exact(2)
                    .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                    .collect();
                let img = ImageBuffer::from_raw(serialized.width, serialized.height, u16_data)
                    .ok_or_else(|| {
                        serde::de::Error::custom("Failed to create ImageLuma16 from raw data")
                    })?;
                DynamicImage::ImageLuma16(img)
            }
            5 => {
                // ImageLumaA16
                if serialized.data.len() % 2 != 0 {
                    return Err(serde::de::Error::custom(
                        "Invalid data length for u16 pixels",
                    ));
                }
                let u16_data: Vec<u16> = serialized
                    .data
                    .chunks_exact(2)
                    .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                    .collect();
                let img = ImageBuffer::from_raw(serialized.width, serialized.height, u16_data)
                    .ok_or_else(|| {
                        serde::de::Error::custom("Failed to create ImageLumaA16 from raw data")
                    })?;
                DynamicImage::ImageLumaA16(img)
            }
            6 => {
                // ImageRgb16
                if serialized.data.len() % 2 != 0 {
                    return Err(serde::de::Error::custom(
                        "Invalid data length for u16 pixels",
                    ));
                }
                let u16_data: Vec<u16> = serialized
                    .data
                    .chunks_exact(2)
                    .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                    .collect();
                let img = ImageBuffer::from_raw(serialized.width, serialized.height, u16_data)
                    .ok_or_else(|| {
                        serde::de::Error::custom("Failed to create ImageRgb16 from raw data")
                    })?;
                DynamicImage::ImageRgb16(img)
            }
            7 => {
                // ImageRgba16
                if serialized.data.len() % 2 != 0 {
                    return Err(serde::de::Error::custom(
                        "Invalid data length for u16 pixels",
                    ));
                }
                let u16_data: Vec<u16> = serialized
                    .data
                    .chunks_exact(2)
                    .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                    .collect();
                let img = ImageBuffer::from_raw(serialized.width, serialized.height, u16_data)
                    .ok_or_else(|| {
                        serde::de::Error::custom("Failed to create ImageRgba16 from raw data")
                    })?;
                DynamicImage::ImageRgba16(img)
            }
            8 => {
                // ImageRgb32F
                if serialized.data.len() % 4 != 0 {
                    return Err(serde::de::Error::custom(
                        "Invalid data length for f32 pixels",
                    ));
                }
                let f32_data: Vec<f32> = serialized
                    .data
                    .chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();
                let img = ImageBuffer::from_raw(serialized.width, serialized.height, f32_data)
                    .ok_or_else(|| {
                        serde::de::Error::custom("Failed to create ImageRgb32F from raw data")
                    })?;
                DynamicImage::ImageRgb32F(img)
            }
            9 => {
                // ImageRgba32F
                if serialized.data.len() % 4 != 0 {
                    return Err(serde::de::Error::custom(
                        "Invalid data length for f32 pixels",
                    ));
                }
                let f32_data: Vec<f32> = serialized
                    .data
                    .chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();
                let img = ImageBuffer::from_raw(serialized.width, serialized.height, f32_data)
                    .ok_or_else(|| {
                        serde::de::Error::custom("Failed to create ImageRgba32F from raw data")
                    })?;
                DynamicImage::ImageRgba32F(img)
            }
            _ => return Err(serde::de::Error::custom("Unknown DynamicImage variant")),
        };

        Ok(Self(dynamic_image))
    }
}

#[cfg(test)]
mod tests {
    use image::{Luma, Rgb, Rgba};

    use super::*;

    #[test]
    fn test_serialize_deserialize_luma8() {
        // Create a simple 2x2 grayscale image
        let data = vec![0u8, 64, 128, 255];
        let img = ImageBuffer::<Luma<u8>, Vec<u8>>::from_raw(2, 2, data.clone()).unwrap();
        let wrapper = Image(DynamicImage::ImageLuma8(img));

        // Serialize
        let serialized =
            bincode::serde::encode_to_vec(&wrapper, bincode::config::legacy()).unwrap();

        // Deserialize
        let deserialized: Image =
            bincode::serde::decode_from_slice(&serialized, bincode::config::legacy())
                .unwrap()
                .0;

        // Check that it matches the original
        if let DynamicImage::ImageLuma8(result_img) = deserialized.0 {
            assert_eq!(result_img.dimensions(), (2, 2));
            assert_eq!(result_img.as_raw(), &data);
        } else {
            panic!("Expected ImageLuma8 variant");
        }
    }

    #[test]
    fn test_serialize_deserialize_rgb8() {
        // Create a simple 2x1 RGB image
        let data = vec![255u8, 0, 0, 0, 255, 0]; // Red pixel, Green pixel
        let img = ImageBuffer::<Rgb<u8>, Vec<u8>>::from_raw(2, 1, data.clone()).unwrap();
        let wrapper = Image(DynamicImage::ImageRgb8(img));

        // Serialize
        let serialized =
            bincode::serde::encode_to_vec(&wrapper, bincode::config::legacy()).unwrap();

        // Deserialize
        let deserialized: Image =
            bincode::serde::decode_from_slice(&serialized, bincode::config::legacy())
                .unwrap()
                .0;

        // Check that it matches the original
        if let DynamicImage::ImageRgb8(result_img) = deserialized.0 {
            assert_eq!(result_img.dimensions(), (2, 1));
            assert_eq!(result_img.as_raw(), &data);
        } else {
            panic!("Expected ImageRgb8 variant");
        }
    }

    #[test]
    fn test_serialize_deserialize_rgba16() {
        // Create a simple 1x1 RGBA16 image
        let data = vec![65535u16, 32768, 16384, 8192]; // R, G, B, A
        let img = ImageBuffer::<Rgba<u16>, Vec<u16>>::from_raw(1, 1, data.clone()).unwrap();
        let wrapper = Image(DynamicImage::ImageRgba16(img));

        // Serialize
        let serialized =
            bincode::serde::encode_to_vec(&wrapper, bincode::config::legacy()).unwrap();

        // Deserialize
        let deserialized: Image =
            bincode::serde::decode_from_slice(&serialized, bincode::config::legacy())
                .unwrap()
                .0;

        // Check that it matches the original
        if let DynamicImage::ImageRgba16(result_img) = deserialized.0 {
            assert_eq!(result_img.dimensions(), (1, 1));
            assert_eq!(result_img.as_raw(), &data);
        } else {
            panic!("Expected ImageRgba16 variant");
        }
    }

    #[test]
    fn test_serialize_deserialize_rgb32f() {
        // Create a simple 1x1 RGB32F image
        let data = vec![1.0f32, 0.5, 0.0]; // R, G, B
        let img = ImageBuffer::<Rgb<f32>, Vec<f32>>::from_raw(1, 1, data.clone()).unwrap();
        let wrapper = Image(DynamicImage::ImageRgb32F(img));

        // Serialize
        let serialized =
            bincode::serde::encode_to_vec(&wrapper, bincode::config::legacy()).unwrap();

        // Deserialize
        let deserialized: Image =
            bincode::serde::decode_from_slice(&serialized, bincode::config::legacy())
                .unwrap()
                .0;

        // Check that it matches the original
        if let DynamicImage::ImageRgb32F(result_img) = deserialized.0 {
            assert_eq!(result_img.dimensions(), (1, 1));
            assert_eq!(result_img.as_raw(), &data);
        } else {
            panic!("Expected ImageRgb32F variant");
        }
    }

    #[test]
    fn test_roundtrip_equality() {
        // Test that serialization -> deserialization preserves equality
        let data = vec![100u8, 150, 200, 255];
        let img = ImageBuffer::<Rgba<u8>, Vec<u8>>::from_raw(1, 1, data).unwrap();
        let original = Image(DynamicImage::ImageRgba8(img));

        let serialized =
            bincode::serde::encode_to_vec(&original, bincode::config::legacy()).unwrap();
        let deserialized: Image =
            bincode::serde::decode_from_slice(&serialized, bincode::config::legacy())
                .unwrap()
                .0;

        assert_eq!(original, deserialized);
    }
}
