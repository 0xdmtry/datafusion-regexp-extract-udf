use crate::pattern_cache::PatternCache;
use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, LargeStringArray, LargeStringBuilder, StringArray,
    StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use regex::Regex;
use std::sync::Arc;

/// Minimal traits to unify Utf8 and LargeUtf8 arrays/builders without runtime dispatch.
trait StrArray: Array {
    type Builder: StrBuilder;
    fn value(&self, i: usize) -> &str;
    fn builder_with_capacity(len: usize, bytes: usize) -> Self::Builder;
}

trait StrBuilder {
    fn append_null(&mut self);
    fn append_value(&mut self, v: &str);
    fn finish_array(&mut self) -> ArrayRef; // renamed to avoid name clash with inherent `finish`
}

// --- Trait impls for Utf8 ---

impl StrArray for StringArray {
    type Builder = StringBuilder;

    #[inline]
    fn value(&self, i: usize) -> &str {
        StringArray::value(self, i)
    }

    #[inline]
    fn builder_with_capacity(len: usize, bytes: usize) -> Self::Builder {
        StringBuilder::with_capacity(len, bytes)
    }
}

impl StrBuilder for StringBuilder {
    #[inline]
    fn append_null(&mut self) {
        StringBuilder::append_null(self)
    }

    #[inline]
    fn append_value(&mut self, v: &str) {
        StringBuilder::append_value(self, v)
    }

    #[inline]
    fn finish_array(&mut self) -> ArrayRef {
        let arr: StringArray = self.finish(); // call inherent method
        Arc::new(arr) as ArrayRef
    }
}
// --- Trait impls for LargeUtf8 ---

impl StrArray for LargeStringArray {
    type Builder = LargeStringBuilder;

    #[inline]
    fn value(&self, i: usize) -> &str {
        LargeStringArray::value(self, i)
    }

    #[inline]
    fn builder_with_capacity(len: usize, bytes: usize) -> Self::Builder {
        LargeStringBuilder::with_capacity(len, bytes)
    }
}

impl StrBuilder for LargeStringBuilder {
    #[inline]
    fn append_null(&mut self) {
        LargeStringBuilder::append_null(self)
    }

    #[inline]
    fn append_value(&mut self, v: &str) {
        LargeStringBuilder::append_value(self, v)
    }

    #[inline]
    fn finish_array(&mut self) -> ArrayRef {
        let arr: LargeStringArray = self.finish(); // call inherent method
        Arc::new(arr) as ArrayRef
    }
}

/// Generic kernel: works for any string width combinations and returns an array
/// whose width matches `strings` (builder is `S::Builder`).
fn run_generic<S, P>(
    strings: &S,
    patterns: &P,
    idx_i64: Option<&Int64Array>,
    idx_i32: Option<&Int32Array>,
) -> Result<ArrayRef>
where
    S: StrArray,
    P: StrArray,
{
    let n = strings.len();
    let mut b = S::builder_with_capacity(n, n * 4);
    let mut cache = PatternCache::new(32);

    // Detect scalar idx once
    let (idx_is_scalar, idx_scalar) = match (idx_i64, idx_i32) {
        (Some(i64s), None) if i64s.len() == 1 => (true, i64s.value(0)),
        (None, Some(i32s)) if i32s.len() == 1 => (true, i32s.value(0) as i64),
        _ => (false, 0),
    };

    // Scalar pattern (length 1) → compile once
    let mut compiled_scalar: Option<Regex> = None;
    let pat_scalar = patterns.len() == 1;

    for i in 0..n {
        let pat_is_null = if pat_scalar {
            patterns.is_null(0)
        } else {
            patterns.is_null(i)
        };
        if strings.is_null(i) || pat_is_null {
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

        let out: &str = if let Some(caps) = re.captures(s) {
            if idx == 0 {
                caps.get(0).map(|m| m.as_str()).unwrap_or("")
            } else {
                let gi = idx as usize;
                caps.get(gi).map(|m| m.as_str()).unwrap_or("")
            }
        } else {
            ""
        };

        b.append_value(out);
    }

    Ok(b.finish_array())
}

// -------------------- Public wrappers (stable API) --------------------

/// Utf8 strings with Utf8 patterns
pub fn run_utf8_utf8(
    strings: &StringArray,
    patterns: &StringArray,
    idx_i64: Option<&Int64Array>,
    idx_i32: Option<&Int32Array>,
    _out_dt: &DataType,
) -> Result<ArrayRef> {
    run_generic(strings, patterns, idx_i64, idx_i32)
}

/// LargeUtf8 strings with Utf8 patterns
pub fn run_large_utf8_utf8(
    strings: &LargeStringArray,
    patterns: &StringArray,
    idx_i64: Option<&Int64Array>,
    idx_i32: Option<&Int32Array>,
    _out_dt: &DataType,
) -> Result<ArrayRef> {
    run_generic(strings, patterns, idx_i64, idx_i32)
}

/// Utf8 strings with LargeUtf8 patterns
pub fn run_utf8_largeutf8(
    strings: &StringArray,
    patterns: &LargeStringArray,
    idx_i64: Option<&Int64Array>,
    idx_i32: Option<&Int32Array>,
    _out_dt: &DataType,
) -> Result<ArrayRef> {
    run_generic(strings, patterns, idx_i64, idx_i32)
}

/// LargeUtf8 strings with LargeUtf8 patterns
pub fn run_large_utf8_largeutf8(
    strings: &LargeStringArray,
    patterns: &LargeStringArray,
    idx_i64: Option<&Int64Array>,
    idx_i32: Option<&Int32Array>,
    _out_dt: &DataType,
) -> Result<ArrayRef> {
    run_generic(strings, patterns, idx_i64, idx_i32)
}
