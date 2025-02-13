use std::{borrow::Cow, rc::Rc};

#[derive(Debug, Clone)]
pub enum Value<'p> {
    Null,
    String(Cow<'p, str>),
    Blob(Cow<'p, [u8]>),
    Int(i64),
    Float(f64),
}

impl Value<'_> {
    pub fn as_str(&self) -> Option<&str> {
        if let Value::String(s) = self {
            Some(s.as_ref())
        } else {
            None
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        if let Value::Int(i) = self {
            Some(*i)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub enum OwnedValue {
    Null,
    String(Rc<String>),
    Blob(Rc<Vec<u8>>),
    Int(i64),
    Float(f64),
}

impl<'p> From<Value<'p>> for OwnedValue {
    fn from(value: Value<'p>) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Int(i) => Self::Int(i),
            Value::Float(f) => Self::Float(f),
            Value::Blob(b) => Self::Blob(Rc::new(b.into_owned())),
            Value::String(s) => Self::String(Rc::new(s.into_owned())),
        }
    }
}

impl std::fmt::Display for OwnedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OwnedValue::Null => write!(f, "null"),
            OwnedValue::String(s) => s.fmt(f),
            OwnedValue::Blob(items) => {
                write!(
                    f,
                    "{}",
                    items
                        .iter()
                        .filter_map(|&n| char::from_u32(n as u32).filter(char::is_ascii))
                        .collect::<String>()
                )
            }
            OwnedValue::Int(i) => i.fmt(f),
            OwnedValue::Float(x) => x.fmt(f),
        }
    }
}
