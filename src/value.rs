use std::borrow::Cow;

#[derive(Debug, Clone)]
pub enum Value<'p> {
    Null,
    String(Cow<'p, str>),
    Blob(Cow<'p, [u8]>),
    Int(i64),
    Float(f64),
}

impl<'p> Value<'p> {
    pub fn as_str(&self) -> Option<&str> {
        if let Value::String(s) = self {
            Some(s.as_ref())
        } else {
            None
        }
    }
}
