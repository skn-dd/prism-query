//! SIMD-optimized string operations on Arrow StringArrays.
//!
//! Replaces Trino's `VariableWidthBlock` string operations. Leverages
//! Arrow's string kernels which use SSE4.2/AVX2 intrinsics for byte scanning.

use arrow_array::{
    builder::StringBuilder, Array, ArrayRef, BooleanArray, StringArray,
};
use arrow_string::like as arrow_like;
use arrow_string::length as arrow_length;
use arrow_string::substring as arrow_substring;

use crate::Result;

/// Evaluate SQL LIKE pattern on a StringArray.
pub fn string_like(array: &StringArray, pattern: &str) -> Result<BooleanArray> {
    let scalar = StringArray::new_scalar(pattern);
    Ok(arrow_like::like(array, &scalar)?)
}

/// Evaluate SQL ILIKE (case-insensitive LIKE).
pub fn string_ilike(array: &StringArray, pattern: &str) -> Result<BooleanArray> {
    let scalar = StringArray::new_scalar(pattern);
    Ok(arrow_like::ilike(array, &scalar)?)
}

/// Substring extraction: SUBSTRING(str FROM start FOR length).
/// `start` is 1-based (SQL convention). `length` is optional.
pub fn string_substring(
    array: &StringArray,
    start: i64,
    length: Option<u64>,
) -> Result<StringArray> {
    // Arrow's substring is 0-based; convert from SQL 1-based
    let result = arrow_substring::substring(array, start - 1, length)?;
    Ok(result
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone())
}

/// Concatenate two StringArrays element-wise.
pub fn string_concat(left: &StringArray, right: &StringArray) -> Result<StringArray> {
    let mut builder = StringBuilder::with_capacity(left.len(), 0);
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            let mut s = String::with_capacity(left.value(i).len() + right.value(i).len());
            s.push_str(left.value(i));
            s.push_str(right.value(i));
            builder.append_value(&s);
        }
    }
    Ok(builder.finish())
}

/// Compute string length for each element.
pub fn string_length(array: &StringArray) -> Result<ArrayRef> {
    Ok(arrow_length::length(array)?)
}

/// Convert to uppercase.
pub fn string_upper(array: &StringArray) -> Result<StringArray> {
    let mut builder = StringBuilder::with_capacity(array.len(), 0);
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(array.value(i).to_uppercase());
        }
    }
    Ok(builder.finish())
}

/// Convert to lowercase.
pub fn string_lower(array: &StringArray) -> Result<StringArray> {
    let mut builder = StringBuilder::with_capacity(array.len(), 0);
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(array.value(i).to_lowercase());
        }
    }
    Ok(builder.finish())
}

/// Check if string starts with prefix.
pub fn string_starts_with(array: &StringArray, prefix: &str) -> Result<BooleanArray> {
    let scalar = StringArray::new_scalar(prefix);
    Ok(arrow_like::starts_with(array, &scalar)?)
}

/// Check if string ends with suffix.
pub fn string_ends_with(array: &StringArray, suffix: &str) -> Result<BooleanArray> {
    let scalar = StringArray::new_scalar(suffix);
    Ok(arrow_like::ends_with(array, &scalar)?)
}

/// Check if string contains substring.
pub fn string_contains(array: &StringArray, substring: &str) -> Result<BooleanArray> {
    let scalar = StringArray::new_scalar(substring);
    Ok(arrow_like::contains(array, &scalar)?)
}

/// Replace occurrences of `from` with `to` in each string.
pub fn string_replace(array: &StringArray, from: &str, to: &str) -> Result<StringArray> {
    let mut builder = StringBuilder::with_capacity(array.len(), 0);
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(array.value(i).replace(from, to));
        }
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_like() {
        let arr = StringArray::from(vec!["hello", "world", "help", "held"]);
        let result = string_like(&arr, "hel%").unwrap();
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), Some(true), Some(true)]
        );
    }

    #[test]
    fn test_substring() {
        let arr = StringArray::from(vec!["Hello World"]);
        let result = string_substring(&arr, 1, Some(5)).unwrap();
        assert_eq!(result.value(0), "Hello");
    }

    #[test]
    fn test_concat() {
        let a = StringArray::from(vec!["foo", "bar"]);
        let b = StringArray::from(vec!["_1", "_2"]);
        let result = string_concat(&a, &b).unwrap();
        assert_eq!(result.value(0), "foo_1");
        assert_eq!(result.value(1), "bar_2");
    }

    #[test]
    fn test_upper_lower() {
        let arr = StringArray::from(vec!["Hello"]);
        assert_eq!(string_upper(&arr).unwrap().value(0), "HELLO");
        assert_eq!(string_lower(&arr).unwrap().value(0), "hello");
    }

    #[test]
    fn test_contains() {
        let arr = StringArray::from(vec!["Hello World", "Goodbye"]);
        let result = string_contains(&arr, "World").unwrap();
        assert!(result.value(0));
        assert!(!result.value(1));
    }

    #[test]
    fn test_replace() {
        let arr = StringArray::from(vec!["foo_bar_baz"]);
        let result = string_replace(&arr, "_", "-").unwrap();
        assert_eq!(result.value(0), "foo-bar-baz");
    }

    #[test]
    fn test_starts_ends_with() {
        let arr = StringArray::from(vec!["hello_world"]);
        assert!(string_starts_with(&arr, "hello").unwrap().value(0));
        assert!(string_ends_with(&arr, "world").unwrap().value(0));
        assert!(!string_starts_with(&arr, "world").unwrap().value(0));
    }
}
