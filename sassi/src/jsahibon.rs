//! Portable JSON values for Sassi cache and wire boundaries.

use std::cmp::Ordering;

use thiserror::Error;

/// Errors produced while constructing or converting [`JSahibON`] values.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum JSahibONError {
    /// A non-finite floating point value was rejected.
    #[error("JSahibON only accepts finite f64 values")]
    NonFiniteF64,

    /// A serde_json number could not fit into any JSahibON numeric carrier.
    #[cfg(feature = "serde-json-bridge")]
    #[error("serde_json number is outside JSahibON's supported numeric range: {0}")]
    NumberOutOfRange(String),

    /// The serde_json bridge failed to serialize or deserialize a value.
    #[cfg(feature = "serde-json-bridge")]
    #[error("serde_json bridge error: {0}")]
    SerdeJson(String),
}

/// Finite-only `f64` carrier for [`JSahibON::F64`].
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub struct JFiniteF64(f64);

impl JFiniteF64 {
    /// Construct a finite float wrapper.
    pub fn try_new(value: f64) -> Result<Self, JSahibONError> {
        if value.is_finite() {
            Ok(Self(value))
        } else {
            Err(JSahibONError::NonFiniteF64)
        }
    }

    /// Return the wrapped finite float.
    pub fn get(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for JFiniteF64 {
    type Error = JSahibONError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl PartialEq for JFiniteF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for JFiniteF64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_f64(self.0)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for JFiniteF64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

/// Duplicate-key-free, insertion-ordered JSON object storage.
#[derive(Clone, Debug)]
pub struct JObject {
    entries: Vec<(String, JSahibON)>,
}

impl JObject {
    /// Construct an empty JSON object.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Construct an object from key/value pairs.
    ///
    /// Duplicate keys replace the previous value without moving the key's
    /// original insertion position.
    pub fn from_entries<I>(entries: I) -> Self
    where
        I: IntoIterator<Item = (String, JSahibON)>,
    {
        let mut object = Self::new();
        for (key, value) in entries {
            object.insert(key, value);
        }
        object
    }

    /// Insert or replace a key/value pair.
    pub fn insert(&mut self, key: String, value: JSahibON) -> Option<JSahibON> {
        if let Some((_, existing)) = self
            .entries
            .iter_mut()
            .find(|(existing_key, _)| existing_key == &key)
        {
            return Some(std::mem::replace(existing, value));
        }

        self.entries.push((key, value));
        None
    }

    /// Return the value for `key`, if present.
    pub fn get(&self, key: &str) -> Option<&JSahibON> {
        self.entries
            .iter()
            .find(|(existing_key, _)| existing_key == key)
            .map(|(_, value)| value)
    }

    /// Iterate in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &JSahibON)> {
        self.entries.iter().map(|(key, value)| (key, value))
    }

    /// Return the number of object keys.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return true when the object has no keys.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for JObject {
    fn default() -> Self {
        Self::new()
    }
}

impl FromIterator<(String, JSahibON)> for JObject {
    fn from_iter<T: IntoIterator<Item = (String, JSahibON)>>(iter: T) -> Self {
        Self::from_entries(iter)
    }
}

impl IntoIterator for JObject {
    type Item = (String, JSahibON);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl PartialEq for JObject {
    fn eq(&self, other: &Self) -> bool {
        self.entries.len() == other.entries.len()
            && self.entries.iter().all(|(key, value)| {
                other
                    .entries
                    .iter()
                    .find(|(other_key, _)| other_key == key)
                    .is_some_and(|(_, other_value)| value == other_value)
            })
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for JObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.entries.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for JObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let entries = Vec::<(String, JSahibON)>::deserialize(deserializer)?;
        Ok(Self::from_entries(entries))
    }
}

/// Sassi-owned portable JSON value.
///
/// Do not reorder variants without a Sassi wire-major bump: derived serde
/// encodes enum variants by declaration order in postcard bytes.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum JSahibON {
    /// JSON null.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// Exact signed integer.
    I64(i64),
    /// Exact unsigned integer.
    U64(u64),
    /// Finite binary64 number.
    F64(JFiniteF64),
    /// JSON string.
    String(String),
    /// JSON array.
    Array(Vec<JSahibON>),
    /// JSON object with insertion-order-preserving storage.
    Object(JObject),
}

impl JSahibON {
    /// Construct a finite floating-point JSON value.
    pub fn try_f64(value: f64) -> Result<Self, JSahibONError> {
        JFiniteF64::try_new(value).map(Self::F64)
    }
}

impl PartialEq for JSahibON {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::I64(left), Self::I64(right)) => left == right,
            (Self::U64(left), Self::U64(right)) => left == right,
            (Self::F64(left), Self::F64(right)) => left == right,
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Array(left), Self::Array(right)) => left == right,
            (Self::Object(left), Self::Object(right)) => left == right,
            (Self::I64(left), Self::U64(right)) => i64_u64_eq(*left, *right),
            (Self::U64(left), Self::I64(right)) => i64_u64_eq(*right, *left),
            (Self::I64(left), Self::F64(right)) => i64_f64_eq(*left, right.get()),
            (Self::F64(left), Self::I64(right)) => i64_f64_eq(*right, left.get()),
            (Self::U64(left), Self::F64(right)) => u64_f64_eq(*left, right.get()),
            (Self::F64(left), Self::U64(right)) => u64_f64_eq(*right, left.get()),
            _ => false,
        }
    }
}

pub(crate) fn compare_jsahibon_numbers(left: &JSahibON, right: &JSahibON) -> Option<Ordering> {
    match (left, right) {
        (JSahibON::I64(left), JSahibON::I64(right)) => Some(left.cmp(right)),
        (JSahibON::U64(left), JSahibON::U64(right)) => Some(left.cmp(right)),
        (JSahibON::F64(left), JSahibON::F64(right)) => left.get().partial_cmp(&right.get()),
        (JSahibON::I64(left), JSahibON::U64(right)) => Some(compare_i64_u64(*left, *right)),
        (JSahibON::U64(left), JSahibON::I64(right)) => {
            Some(compare_i64_u64(*right, *left).reverse())
        }
        (JSahibON::I64(left), JSahibON::F64(right)) => Some(compare_i64_f64(*left, right.get())),
        (JSahibON::F64(left), JSahibON::I64(right)) => {
            Some(compare_i64_f64(*right, left.get()).reverse())
        }
        (JSahibON::U64(left), JSahibON::F64(right)) => Some(compare_u64_f64(*left, right.get())),
        (JSahibON::F64(left), JSahibON::U64(right)) => {
            Some(compare_u64_f64(*right, left.get()).reverse())
        }
        _ => None,
    }
}

fn i64_u64_eq(left: i64, right: u64) -> bool {
    u64::try_from(left) == Ok(right)
}

fn i64_f64_eq(left: i64, right: f64) -> bool {
    compare_i64_f64(left, right).is_eq()
}

fn u64_f64_eq(left: u64, right: f64) -> bool {
    compare_u64_f64(left, right).is_eq()
}

fn compare_i64_u64(left: i64, right: u64) -> Ordering {
    match u64::try_from(left) {
        Ok(left) => left.cmp(&right),
        Err(_) => Ordering::Less,
    }
}

fn compare_i64_f64(left: i64, right: f64) -> Ordering {
    debug_assert!(right.is_finite());

    const I64_MIN_F64: f64 = -9_223_372_036_854_775_808.0;
    const I64_MAX_PLUS_ONE_F64: f64 = 9_223_372_036_854_775_808.0;

    if right < I64_MIN_F64 {
        return Ordering::Greater;
    }
    if right >= I64_MAX_PLUS_ONE_F64 {
        return Ordering::Less;
    }
    if right.fract() == 0.0 {
        return left.cmp(&(right as i64));
    }

    if right.is_sign_positive() {
        let floor = right.floor() as i64;
        if left <= floor {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    } else {
        let ceil = right.ceil() as i64;
        if left < ceil {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}

fn compare_u64_f64(left: u64, right: f64) -> Ordering {
    debug_assert!(right.is_finite());

    const U64_MAX_PLUS_ONE_F64: f64 = 18_446_744_073_709_551_616.0;

    if right < 0.0 {
        return Ordering::Greater;
    }
    if right >= U64_MAX_PLUS_ONE_F64 {
        return Ordering::Less;
    }
    if right.fract() == 0.0 {
        return left.cmp(&(right as u64));
    }

    let floor = right.floor() as u64;
    if left <= floor {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}

#[cfg(feature = "serde-json-bridge")]
impl TryFrom<serde_json::Value> for JSahibON {
    type Error = JSahibONError;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::Null => Ok(Self::Null),
            serde_json::Value::Bool(value) => Ok(Self::Bool(value)),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    Ok(Self::I64(value))
                } else if let Some(value) = value.as_u64() {
                    Ok(Self::U64(value))
                } else if let Some(value) = value.as_f64() {
                    Self::try_f64(value)
                } else {
                    Err(JSahibONError::NumberOutOfRange(value.to_string()))
                }
            }
            serde_json::Value::String(value) => Ok(Self::String(value)),
            serde_json::Value::Array(values) => values
                .into_iter()
                .map(Self::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Array),
            serde_json::Value::Object(values) => values
                .into_iter()
                .map(|(key, value)| Self::try_from(value).map(|value| (key, value)))
                .collect::<Result<JObject, _>>()
                .map(Self::Object),
        }
    }
}

#[cfg(feature = "serde-json-bridge")]
impl From<JSahibON> for serde_json::Value {
    fn from(value: JSahibON) -> Self {
        match value {
            JSahibON::Null => Self::Null,
            JSahibON::Bool(value) => Self::Bool(value),
            JSahibON::I64(value) => Self::Number(value.into()),
            JSahibON::U64(value) => Self::Number(value.into()),
            JSahibON::F64(value) => {
                Self::Number(serde_json::Number::from_f64(value.get()).expect("finite f64"))
            }
            JSahibON::String(value) => Self::String(value),
            JSahibON::Array(values) => Self::Array(values.into_iter().map(Self::from).collect()),
            JSahibON::Object(values) => {
                let mut object = serde_json::Map::new();
                for (key, value) in values {
                    object.insert(key, Self::from(value));
                }
                Self::Object(object)
            }
        }
    }
}

#[cfg(feature = "serde-json-bridge")]
impl JSahibON {
    /// Serialize a typed value through serde_json and project it into JSahibON.
    pub fn try_from_serializable<T: serde::Serialize>(value: &T) -> Result<Self, JSahibONError> {
        let value =
            serde_json::to_value(value).map_err(|err| JSahibONError::SerdeJson(err.to_string()))?;
        Self::try_from(value)
    }

    /// Convert JSahibON into a typed value through serde_json.
    pub fn try_into_typed<T: serde::de::DeserializeOwned>(self) -> Result<T, JSahibONError> {
        serde_json::from_value(self.into()).map_err(|err| JSahibONError::SerdeJson(err.to_string()))
    }
}
