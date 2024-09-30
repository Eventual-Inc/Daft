use super::SQLModule;
use crate::functions::SQLFunctions;
pub mod crop;
pub mod decode;
pub mod encode;
pub mod resize;
pub mod to_mode;

pub struct SQLModuleImage;

impl SQLModule for SQLModuleImage {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("image_crop", crop::SQLImageCrop {}, "TODO: Docstring", &[]);
        parent.add_fn(
            "image_decode",
            decode::SQLImageDecode {},
            "TODO: Docstring",
            &[],
        );
        parent.add_fn(
            "image_encode",
            encode::SQLImageEncode {},
            "TODO: Docstring",
            &[],
        );
        parent.add_fn(
            "image_resize",
            resize::SQLImageResize {},
            "TODO: Docstring",
            &[],
        );
        parent.add_fn(
            "image_to_mode",
            to_mode::SQLImageToMode {},
            "TODO: Docstring",
            &[],
        );
    }
}
