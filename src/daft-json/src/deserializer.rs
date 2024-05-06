use std::borrow::Cow;

use indexmap::IndexMap;
use simd_json::{Deserializer, Node, StaticNode};
/// Representation of a JSON object
pub type Object<'value> = IndexMap<Cow<'value, str>, Value<'value>>;
/// Borrowed JSON-DOM Value, consider using the `ValueTrait`
/// to access its content
#[derive(Debug, Clone)]
pub(crate) enum Value<'value> {
    /// Static values
    Static(StaticNode),
    /// string type
    String(Cow<'value, str>),
    /// array type
    Array(Vec<Value<'value>>),
    /// object type
    Object(Object<'value>),
}

struct OrderPreservingDeserializer<'de>(Deserializer<'de>);

impl<'de> OrderPreservingDeserializer<'de> {
    pub fn from_deserializer(de: Deserializer<'de>) -> Self {
        Self(de)
    }

    pub fn parse(&mut self) -> Value<'de> {
        match unsafe { self.0.next_() } {
            Node::Static(s) => Value::Static(s),
            Node::String(s) => Value::String(Cow::Borrowed(s)),
            Node::Array { len, count: _ } => self.parse_array(len),
            Node::Object { len, count: _ } => self.parse_map(len),
        }
    }

    fn parse_array(&mut self, len: usize) -> Value<'de> {
        // Rust doesn't optimize the normal loop away here
        // so we write our own avoiding the length
        // checks during push
        let mut res: Vec<Value<'de>> = Vec::with_capacity(len);
        let res_ptr = res.as_mut_ptr();
        unsafe {
            for i in 0..len {
                res_ptr.add(i).write(self.parse());
            }
            res.set_len(len);
        }
        Value::Array(res)
    }

    fn parse_map(&mut self, len: usize) -> Value<'de> {
        let mut res = IndexMap::with_capacity(len);

        // Since we checked if it's empty we know that we at least have one
        // element so we eat this
        for _ in 0..len {
            if let Node::String(key) = unsafe { self.0.next_() } {
                res.insert(key.into(), self.parse());
            } else {
                unreachable!();
            }
        }
        Value::Object(res)
    }
}

pub fn to_value(s: &mut [u8]) -> simd_json::Result<Value> {
    match Deserializer::from_slice(s) {
        Ok(de) => Ok(OrderPreservingDeserializer::from_deserializer(de).parse()),
        Err(e) => Err(e),
    }
}
