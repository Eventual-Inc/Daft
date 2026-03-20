use std::{borrow::Cow, time::Duration};

use common_image::CowImage;
use criterion::{Criterion, criterion_group, criterion_main};
use daft_core::{
    array::ops::image::{fixed_image_array_from_img_buffers, image_array_from_img_buffers},
    prelude::*,
};
use daft_image::series;
use daft_schema::prelude::{ImageFormat, ImageMode};

const BATCH_SIZE: usize = 100;

// ---------------------------------------------------------------------------
// Test data generators
// ---------------------------------------------------------------------------

/// Generate deterministic pseudo-random pixel data for an image.
fn make_pixel_data(width: u32, height: u32, channels: u8, seed: usize) -> Vec<u8> {
    let size = (width as usize) * (height as usize) * (channels as usize);
    let mut data = Vec::with_capacity(size);
    // Simple LCG for deterministic, cheap pseudo-random fill.
    let mut state: u64 = seed as u64 ^ 0xDEAD_BEEF;
    for _ in 0..size {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        data.push((state >> 33) as u8);
    }
    data
}

/// Build an `ImageArray` (variable-shape) Series of RGB images.
fn make_rgb_image_series(width: u32, height: u32, count: usize) -> Series {
    let buffers = (0..count).map(|i| {
        let pixels = make_pixel_data(width, height, 3, i);
        Some(CowImage::from_raw(
            &ImageMode::RGB,
            width,
            height,
            Cow::Owned(pixels),
        ))
    });

    image_array_from_img_buffers("image", buffers, Some(ImageMode::RGB))
        .unwrap()
        .into_series()
}

/// Build a `FixedShapeImageArray` Series of RGB images.
fn make_fixed_rgb_image_series(width: u32, height: u32, count: usize) -> Series {
    let buffers: Vec<Option<CowImage>> = (0..count)
        .map(|i| {
            let pixels = make_pixel_data(width, height, 3, i);
            Some(CowImage::from_raw(
                &ImageMode::RGB,
                width,
                height,
                Cow::Owned(pixels),
            ))
        })
        .collect();
    fixed_image_array_from_img_buffers("image", &buffers, &ImageMode::RGB, height, width)
        .unwrap()
        .into_series()
}

/// Build an `ImageArray` Series of RGBA images.
fn make_rgba_image_series(width: u32, height: u32, count: usize) -> Series {
    let buffers = (0..count).map(|i| {
        let pixels = make_pixel_data(width, height, 4, i);
        Some(CowImage::from_raw(
            &ImageMode::RGBA,
            width,
            height,
            Cow::Owned(pixels),
        ))
    });

    image_array_from_img_buffers("image", buffers, Some(ImageMode::RGBA))
        .unwrap()
        .into_series()
}

/// Encode images to a binary format and return as a Binary Series (for decode benchmarks).
fn make_encoded_series(width: u32, height: u32, count: usize, format: ImageFormat) -> Series {
    let encoded: Vec<Option<Vec<u8>>> = (0..count)
        .map(|i| {
            let pixels = make_pixel_data(width, height, 3, i);
            let img = CowImage::from_raw(&ImageMode::RGB, width, height, Cow::Owned(pixels));
            let mut buf = Vec::new();
            let mut writer = std::io::BufWriter::new(std::io::Cursor::new(&mut buf));
            img.encode(format, &mut writer).unwrap();
            drop(writer);
            Some(buf)
        })
        .collect();
    let refs = encoded.iter().map(|v| v.as_deref());
    BinaryArray::from_iter("image", refs).into_series()
}

/// Build a bounding box Series for crop benchmarks.
/// Returns a FixedSizeList(UInt32, 4) with [x, y, width, height] per row.
fn make_bbox_series(x: u32, y: u32, w: u32, h: u32, count: usize) -> Series {
    // Build flat UInt32 child: [x, y, w, h, x, y, w, h, ...]
    let flat_data: Vec<u32> = (0..count).flat_map(|_| [x, y, w, h]).collect();
    let flat_child = UInt32Array::from_vec("item", flat_data).into_series();
    let field = Field::new(
        "bbox",
        DataType::FixedSizeList(Box::new(DataType::UInt32), 4),
    );
    FixedSizeListArray::new(field, flat_child, None).into_series()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    let jpeg_series = make_encoded_series(224, 224, BATCH_SIZE, ImageFormat::JPEG);
    group.bench_function("jpeg_224x224", |b| {
        b.iter(|| series::decode(&jpeg_series, true, Some(ImageMode::RGB)).unwrap());
    });

    let png_series = make_encoded_series(224, 224, BATCH_SIZE, ImageFormat::PNG);
    group.bench_function("png_224x224", |b| {
        b.iter(|| series::decode(&png_series, true, Some(ImageMode::RGB)).unwrap());
    });

    group.finish();
}

fn bench_resize(c: &mut Criterion) {
    let mut group = c.benchmark_group("resize");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    let images_512 = make_rgb_image_series(512, 512, BATCH_SIZE);
    group.bench_function("512_to_224", |b| {
        b.iter(|| series::resize(&images_512, 224, 224).unwrap());
    });

    let images_224 = make_rgb_image_series(224, 224, BATCH_SIZE);
    group.bench_function("224_to_64", |b| {
        b.iter(|| series::resize(&images_224, 64, 64).unwrap());
    });

    group.finish();
}

fn bench_crop(c: &mut Criterion) {
    let mut group = c.benchmark_group("crop");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    let images = make_rgb_image_series(512, 512, BATCH_SIZE);
    // Center crop: (144, 144, 224, 224)
    let bboxes = make_bbox_series(144, 144, 224, 224, BATCH_SIZE);

    group.bench_function("512_center_crop_224", |b| {
        b.iter(|| series::crop(&images, &bboxes).unwrap());
    });

    group.finish();
}

fn bench_to_mode(c: &mut Criterion) {
    let mut group = c.benchmark_group("to_mode");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    let rgba_images = make_rgba_image_series(224, 224, BATCH_SIZE);
    group.bench_function("rgba_to_rgb", |b| {
        b.iter(|| series::to_mode(&rgba_images, ImageMode::RGB).unwrap());
    });

    let rgb_images = make_rgb_image_series(224, 224, BATCH_SIZE);
    group.bench_function("rgb_to_l", |b| {
        b.iter(|| series::to_mode(&rgb_images, ImageMode::L).unwrap());
    });

    group.finish();
}

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    let images = make_rgb_image_series(224, 224, BATCH_SIZE);

    group.bench_function("png_224x224", |b| {
        b.iter(|| series::encode(&images, ImageFormat::PNG).unwrap());
    });

    group.bench_function("jpeg_224x224", |b| {
        b.iter(|| series::encode(&images, ImageFormat::JPEG).unwrap());
    });

    group.finish();
}

fn bench_to_tensor(c: &mut Criterion) {
    let mut group = c.benchmark_group("to_tensor");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    let images = make_fixed_rgb_image_series(224, 224, BATCH_SIZE);

    group.bench_function("fixed_rgb_224x224", |b| {
        b.iter(|| series::to_tensor(&images).unwrap());
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_decode,
    bench_resize,
    bench_crop,
    bench_to_mode,
    bench_encode,
    bench_to_tensor
);
criterion_main!(benches);
