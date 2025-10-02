use daft_dsl::functions::FunctionModule;

pub mod attribute;
pub mod crop;
pub mod decode;
pub mod encode;
pub mod resize;
pub mod to_mode;

pub struct ImageFunctions;

impl FunctionModule for ImageFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(crop::ImageCrop);
        parent.add_fn(decode::ImageDecode);
        parent.add_fn(encode::ImageEncode);
        parent.add_fn(resize::ImageResize);
        parent.add_fn(to_mode::ImageToMode);
        parent.add_fn(attribute::ImageAttribute);
    }
}
