use crate::pattern_cache::PatternCache;
use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, LargeStringArray, LargeStringBuilder, StringArray,
    StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use regex::Regex;
use std::sync::Arc;

/// Core loop for Utf8 strings with Utf8 patterns
pub fn run_utf8_utf8(
    strings: &StringArray,
    patterns: &StringArray,
    idx_i64: Option<&Int64Array>,
    idx_i32: Option<&Int32Array>,
    _out_dt: &DataType,
) -> Result<ArrayRef> {
    let n = strings.len();
    let mut b = StringBuilder::with_capacity(n, n * 4);
    let mut cache = PatternCache::new(32);

    let (idx_is_scalar, idx_scalar) = match (idx_i64, idx_i32) {
        (Some(i64s), None) if i64s.len() == 1 => (true, i64s.value(0)),
        (None, Some(i32s)) if i32s.len() == 1 => (true, i32s.value(0) as i64),
        _ => (false, 0),
    };

    // When patterns is scalar array (length 1 repeated), compile once
    let mut compiled_scalar: Option<Regex> = None;
    let pat_scalar = patterns.len() == 1;

    for i in 0..n {
        if strings.is_null(i) || patterns.is_null(i) {
            b.append_null();
            continue;
        }
        let s = strings.value(i);
        let idx = if idx_is_scalar {
            idx_scalar
        } else if let Some(i64s) = idx_i64 {
            if i64s.is_null(i) {
                b.append_null();
                continue;
            }
            i64s.value(i)
        } else if let Some(i32s) = idx_i32 {
            if i32s.is_null(i) {
                b.append_null();
                continue;
            }
            i32s.value(i) as i64
        } else {
            return Err(DataFusionError::Execution(
                "idx array missing (internal)".into(),
            ));
        };

        if idx < 0 {
            return Err(DataFusionError::Execution(format!(
                "regexp_extract: idx must be >= 0, got {idx}"
            )));
        }

        let re = if pat_scalar {
            if compiled_scalar.is_none() {
                compiled_scalar = Some(Regex::new(patterns.value(0)).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "regexp_extract: invalid regex pattern: {e}"
                    ))
                })?);
            }
            compiled_scalar.as_ref().unwrap()
        } else {
            cache.get_or_compile(patterns.value(i)).map_err(|e| {
                DataFusionError::Execution(format!("regexp_extract: invalid regex pattern: {e}"))
            })?
        };

        let out = if let Some(caps) = re.captures(s) {
            // idx==0 -> whole match
            if idx == 0 {
                caps.get(0).map(|m| m.as_str()).unwrap_or("").to_string()
            } else {
                let i = idx as usize;
                caps.get(i).map(|m| m.as_str()).unwrap_or("").to_string()
            }
        } else {
            "".to_string()
        };

        b.append_value(out);
    }

    Ok(Arc::new(b.finish()) as ArrayRef)
}

/// LargeUtf8 strings with Utf8 patterns
pub fn run_large_utf8_utf8(
    strings: &LargeStringArray,
    patterns: &StringArray,
    idx_i64: Option<&Int64Array>,
    idx_i32: Option<&Int32Array>,
    _out_dt: &DataType,
) -> Result<ArrayRef> {
    let n = strings.len();
    let mut b = LargeStringBuilder::with_capacity(n, n * 4);
    let mut cache = PatternCache::new(32);

    let (idx_is_scalar, idx_scalar) = match (idx_i64, idx_i32) {
        (Some(i64s), None) if i64s.len() == 1 => (true, i64s.value(0)),
        (None, Some(i32s)) if i32s.len() == 1 => (true, i32s.value(0) as i64),
        _ => (false, 0),
    };

    let mut compiled_scalar: Option<Regex> = None;
    let pat_scalar = patterns.len() == 1;

    for i in 0..n {
        if strings.is_null(i) || patterns.is_null(i) {
            b.append_null();
            continue;
        }
        let s = strings.value(i);
        let idx = if idx_is_scalar {
            idx_scalar
        } else if let Some(i64s) = idx_i64 {
            if i64s.is_null(i) {
                b.append_null();
                continue;
            }
            i64s.value(i)
        } else if let Some(i32s) = idx_i32 {
            if i32s.is_null(i) {
                b.append_null();
                continue;
            }
            i32s.value(i) as i64
        } else {
            return Err(DataFusionError::Execution(
                "idx array missing (internal)".into(),
            ));
        };

        if idx < 0 {
            return Err(DataFusionError::Execution(format!(
                "regexp_extract: idx must be >= 0, got {idx}"
            )));
        }

        let re = if pat_scalar {
            if compiled_scalar.is_none() {
                compiled_scalar = Some(Regex::new(patterns.value(0)).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "regexp_extract: invalid regex pattern: {e}"
                    ))
                })?);
            }
            compiled_scalar.as_ref().unwrap()
        } else {
            cache.get_or_compile(patterns.value(i)).map_err(|e| {
                DataFusionError::Execution(format!("regexp_extract: invalid regex pattern: {e}"))
            })?
        };

        let out = if let Some(caps) = re.captures(s) {
            if idx == 0 {
                caps.get(0).map(|m| m.as_str()).unwrap_or("").to_string()
            } else {
                let i = idx as usize;
                caps.get(i).map(|m| m.as_str()).unwrap_or("").to_string()
            }
        } else {
            "".to_string()
        };

        b.append_value(out);
    }

    Ok(Arc::new(b.finish()) as ArrayRef)
}

/// Utf8 strings with LargeUtf8 patterns
pub fn run_utf8_largeutf8(
    strings: &StringArray,
    patterns: &LargeStringArray,
    idx_i64: Option<&Int64Array>,
    idx_i32: Option<&Int32Array>,
    _out_dt: &DataType,
) -> Result<ArrayRef> {
    let n = strings.len();
    let mut b = StringBuilder::with_capacity(n, n * 4);
    let mut cache = PatternCache::new(32);

    let (idx_is_scalar, idx_scalar) = match (idx_i64, idx_i32) {
        (Some(i64s), None) if i64s.len() == 1 => (true, i64s.value(0)),
        (None, Some(i32s)) if i32s.len() == 1 => (true, i32s.value(0) as i64),
        _ => (false, 0),
    };

    // Note: arrays expanded from scalars will have len == n, so we rely on cache
    let mut compiled_scalar: Option<Regex> = None;
    let pat_scalar = patterns.len() == 1;

    for i in 0..n {
        if strings.is_null(i) || patterns.is_null(i) {
            b.append_null();
            continue;
        }
        let s = strings.value(i);

        let idx = if idx_is_scalar {
            idx_scalar
        } else if let Some(i64s) = idx_i64 {
            if i64s.is_null(i) {
                b.append_null();
                continue;
            }
            i64s.value(i)
        } else if let Some(i32s) = idx_i32 {
            if i32s.is_null(i) {
                b.append_null();
                continue;
            }
            i32s.value(i) as i64
        } else {
            return Err(DataFusionError::Execution(
                "idx array missing (internal)".into(),
            ));
        };

        if idx < 0 {
            return Err(DataFusionError::Execution(format!(
                "regexp_extract: idx must be >= 0, got {idx}"
            )));
        }

        let re = if pat_scalar {
            if compiled_scalar.is_none() {
                compiled_scalar = Some(Regex::new(patterns.value(0)).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "regexp_extract: invalid regex pattern: {e}"
                    ))
                })?);
            }
            compiled_scalar.as_ref().unwrap()
        } else {
            cache.get_or_compile(patterns.value(i)).map_err(|e| {
                DataFusionError::Execution(format!("regexp_extract: invalid regex pattern: {e}"))
            })?
        };

        let out = if let Some(caps) = re.captures(s) {
            if idx == 0 {
                caps.get(0).map(|m| m.as_str()).unwrap_or("").to_string()
            } else {
                let gi = idx as usize;
                caps.get(gi).map(|m| m.as_str()).unwrap_or("").to_string()
            }
        } else {
            "".to_string()
        };

        b.append_value(out);
    }

    Ok(Arc::new(b.finish()) as ArrayRef)
}

/// LargeUtf8 strings with LargeUtf8 patterns
pub fn run_large_utf8_largeutf8(
    strings: &LargeStringArray,
    patterns: &LargeStringArray,
    idx_i64: Option<&Int64Array>,
    idx_i32: Option<&Int32Array>,
    _out_dt: &DataType,
) -> Result<ArrayRef> {
    let n = strings.len();
    let mut b = LargeStringBuilder::with_capacity(n, n * 4);
    let mut cache = PatternCache::new(32);

    let (idx_is_scalar, idx_scalar) = match (idx_i64, idx_i32) {
        (Some(i64s), None) if i64s.len() == 1 => (true, i64s.value(0)),
        (None, Some(i32s)) if i32s.len() == 1 => (true, i32s.value(0) as i64),
        _ => (false, 0),
    };

    let mut compiled_scalar: Option<Regex> = None;
    let pat_scalar = patterns.len() == 1;

    for i in 0..n {
        if strings.is_null(i) || patterns.is_null(i) {
            b.append_null();
            continue;
        }
        let s = strings.value(i);

        let idx = if idx_is_scalar {
            idx_scalar
        } else if let Some(i64s) = idx_i64 {
            if i64s.is_null(i) {
                b.append_null();
                continue;
            }
            i64s.value(i)
        } else if let Some(i32s) = idx_i32 {
            if i32s.is_null(i) {
                b.append_null();
                continue;
            }
            i32s.value(i) as i64
        } else {
            return Err(DataFusionError::Execution(
                "idx array missing (internal)".into(),
            ));
        };

        if idx < 0 {
            return Err(DataFusionError::Execution(format!(
                "regexp_extract: idx must be >= 0, got {idx}"
            )));
        }

        let re = if pat_scalar {
            if compiled_scalar.is_none() {
                compiled_scalar = Some(Regex::new(patterns.value(0)).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "regexp_extract: invalid regex pattern: {e}"
                    ))
                })?);
            }
            compiled_scalar.as_ref().unwrap()
        } else {
            cache.get_or_compile(patterns.value(i)).map_err(|e| {
                DataFusionError::Execution(format!("regexp_extract: invalid regex pattern: {e}"))
            })?
        };

        let out = if let Some(caps) = re.captures(s) {
            if idx == 0 {
                caps.get(0).map(|m| m.as_str()).unwrap_or("").to_string()
            } else {
                let gi = idx as usize;
                caps.get(gi).map(|m| m.as_str()).unwrap_or("").to_string()
            }
        } else {
            "".to_string()
        };

        b.append_value(out);
    }

    Ok(Arc::new(b.finish()) as ArrayRef)
}
