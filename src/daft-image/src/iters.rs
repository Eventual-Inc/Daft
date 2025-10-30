use common_image::CowImage;
use daft_core::array::ops::image::AsImageObj;

pub struct ImageBufferIter<'a, Arr>
where
    Arr: AsImageObj,
{
    cursor: usize,
    image_array: &'a Arr,
}

impl<'a, Arr> ImageBufferIter<'a, Arr>
where
    Arr: AsImageObj,
{
    pub fn new(image_array: &'a Arr) -> Self {
        Self {
            cursor: 0usize,
            image_array,
        }
    }
}

impl<'a, Arr> Iterator for ImageBufferIter<'a, Arr>
where
    Arr: AsImageObj,
{
    type Item = Option<CowImage<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.image_array.len() {
            None
        } else {
            let image_obj = self.image_array.as_image_obj(self.cursor);
            self.cursor += 1;
            Some(image_obj)
        }
    }
}
impl<Arr> ExactSizeIterator for ImageBufferIter<'_, Arr>
where
    Arr: AsImageObj,
{
    fn len(&self) -> usize {
        self.image_array.len()
    }
}
