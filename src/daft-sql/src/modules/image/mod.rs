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
        parent.add_fn("image_crop", crop::SQLImageCrop {});
        parent.add_fn("image_decode", decode::SQLImageDecode {});
        parent.add_fn("image_encode", encode::SQLImageEncode {});
        parent.add_fn("image_resize", resize::SQLImageResize {});
        parent.add_fn("image_to_mode", to_mode::SQLImageToMode {});
    }
}
