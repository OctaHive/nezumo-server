//! JSON ↔ Yrs `Any` mapping with a strict, fallible numeric domain (protocol v1 /
//! ADR-2). No silent lossy conversion: anything without an unambiguous JSON-V1
//! representation is a `MappingError`.

use serde_json::Value;
use std::collections::HashMap;
use thiserror::Error;
use yrs::Any;

/// Largest integer exactly representable in f64. Integers with `|v| <= 2^53`
/// round-trip losslessly through `Any::Number(f64)`.
const SAFE_INT: u64 = 1 << 53;

#[derive(Debug, Error, PartialEq)]
pub enum MappingError {
    #[error("non-finite number (NaN/Inf) has no JSON V1 representation")]
    NonFiniteNumber,
    #[error(
        "integer {0} exceeds the safe f64 range (|v| <= 2^53); needs schema-level string encoding"
    )]
    UnsafeInteger(String),
    #[error("yrs Any value has no JSON V1 representation: {0}")]
    UnsupportedAny(&'static str),
}

/// serde_json `Value` -> yrs `Any`. Fallible on the unsafe numeric domain.
pub fn json_to_any(value: &Value) -> Result<Any, MappingError> {
    Ok(match value {
        Value::Null => Any::Null,
        Value::Bool(b) => Any::Bool(*b),
        Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                if u <= SAFE_INT {
                    Any::Number(u as f64)
                } else {
                    return Err(MappingError::UnsafeInteger(u.to_string()));
                }
            } else if let Some(i) = n.as_i64() {
                if i.unsigned_abs() <= SAFE_INT {
                    Any::Number(i as f64)
                } else {
                    return Err(MappingError::UnsafeInteger(i.to_string()));
                }
            } else {
                let f = n.as_f64().ok_or(MappingError::NonFiniteNumber)?;
                if f.is_finite() {
                    Any::Number(f)
                } else {
                    return Err(MappingError::NonFiniteNumber);
                }
            }
        }
        Value::String(s) => Any::String(s.as_str().into()),
        Value::Array(arr) => {
            let items: Result<Vec<Any>, _> = arr.iter().map(json_to_any).collect();
            Any::Array(items?.into())
        }
        Value::Object(obj) => {
            let mut map = HashMap::with_capacity(obj.len());
            for (k, v) in obj {
                map.insert(k.clone(), json_to_any(v)?);
            }
            Any::Map(map.into())
        }
    })
}

/// yrs `Any` -> serde_json `Value`. Integer-valued finite numbers project back to
/// JSON integers (byte-stable round-trip); fractional to floats.
pub fn any_to_json(value: &Any) -> Result<Value, MappingError> {
    Ok(match value {
        Any::Null => Value::Null,
        Any::Undefined => return Err(MappingError::UnsupportedAny("Undefined")),
        Any::Bool(b) => Value::Bool(*b),
        Any::Number(f) => number_to_json(*f)?,
        Any::BigInt(i) => {
            if i.unsigned_abs() <= SAFE_INT {
                Value::Number((*i).into())
            } else {
                return Err(MappingError::UnsafeInteger(i.to_string()));
            }
        }
        Any::String(s) => Value::String(s.to_string()),
        Any::Buffer(_) => return Err(MappingError::UnsupportedAny("Buffer")),
        Any::Array(arr) => {
            let items: Result<Vec<Value>, _> = arr.iter().map(any_to_json).collect();
            Value::Array(items?)
        }
        Any::Map(map) => {
            let mut obj = serde_json::Map::with_capacity(map.len());
            for (k, v) in map.iter() {
                obj.insert(k.clone(), any_to_json(v)?);
            }
            Value::Object(obj)
        }
    })
}

fn number_to_json(f: f64) -> Result<Value, MappingError> {
    if !f.is_finite() {
        return Err(MappingError::NonFiniteNumber);
    }
    if f.fract() == 0.0 && f.abs() <= SAFE_INT as f64 {
        if f >= 0.0 {
            return Ok(Value::Number((f as u64).into()));
        }
        return Ok(Value::Number((f as i64).into()));
    }
    serde_json::Number::from_f64(f)
        .map(Value::Number)
        .ok_or(MappingError::NonFiniteNumber)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn roundtrip_ok(v: Value) {
        let any = json_to_any(&v).expect("json_to_any");
        let back = any_to_json(&any).expect("any_to_json");
        assert_eq!(back, v, "round-trip changed the value");
    }

    #[test]
    fn primitives_and_nesting_roundtrip() {
        roundtrip_ok(Value::Null);
        roundtrip_ok(json!(true));
        roundtrip_ok(json!("hello"));
        roundtrip_ok(json!(0));
        roundtrip_ok(json!(-1));
        roundtrip_ok(json!(1.5));
        roundtrip_ok(json!([1, "a", null, {"k": 2}]));
        roundtrip_ok(json!({"color": "red", "w": 3, "nested": {"x": 1.5, "arr": [true, 0]}}));
    }

    #[test]
    fn unsafe_integers_error_not_coerce() {
        assert!(matches!(
            json_to_any(&json!(SAFE_INT + 1)),
            Err(MappingError::UnsafeInteger(_))
        ));
        assert!(matches!(
            json_to_any(&json!(u64::MAX)),
            Err(MappingError::UnsafeInteger(_))
        ));
    }

    #[test]
    fn integer_valued_float_projects_to_integer() {
        assert_eq!(any_to_json(&Any::Number(5.0)).unwrap(), json!(5));
        assert_eq!(any_to_json(&Any::Number(-3.0)).unwrap(), json!(-3));
        assert_eq!(any_to_json(&Any::Number(1.5)).unwrap(), json!(1.5));
    }
}
