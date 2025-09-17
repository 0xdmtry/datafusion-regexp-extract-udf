use crate::config::InvalidPatternMode;
use crate::error::RegexpExtractError;
use crate::pattern_cache::PatternCache;
use crate::re::{Regex, captures, compile};
use datafusion::arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, LargeStringArray, LargeStringBuilder, StringArray,
    StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
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
    cache_cap: usize,
    mode: InvalidPatternMode,
) -> Result<ArrayRef, RegexpExtractError>
where
    S: StrArray,
    P: StrArray,
{
    let n = strings.len();

    // Detect scalar idx once (moved above estimate_bytes)
    let (idx_is_scalar, idx_scalar) = match (idx_i64, idx_i32) {
        (Some(i64s), None) if i64s.len() == 1 => (true, i64s.value(0)),
        (None, Some(i32s)) if i32s.len() == 1 => (true, i32s.value(0) as i64),
        _ => (false, 0),
    };

    let bytes_hint = estimate_bytes(strings, idx_i64, idx_i32, idx_is_scalar, idx_scalar);
    let mut b = S::builder_with_capacity(n, bytes_hint);
    let mut cache = PatternCache::new(cache_cap);

    let mut compiled_scalar: Option<Regex> = None;
    let mut scalar_pat_invalid = false;

    let pat_scalar = patterns.len() == 1;

    let str_no_nulls = strings.null_count() == 0;
    let pat_no_nulls = if pat_scalar {
        !patterns.is_null(0)
    } else {
        patterns.null_count() == 0
    };
    let idx_no_nulls = if idx_is_scalar {
        match (idx_i64, idx_i32) {
            (Some(i64s), None) => !i64s.is_null(0),
            (None, Some(i32s)) => !i32s.is_null(0),
            _ => false, // shouldn't happen
        }
    } else if let Some(i64s) = idx_i64 {
        i64s.null_count() == 0
    } else if let Some(i32s) = idx_i32 {
        i32s.null_count() == 0
    } else {
        false
    };

    let use_no_nulls_fast_path = str_no_nulls && pat_no_nulls && idx_no_nulls;

    if use_no_nulls_fast_path {
        // compile scalar pattern once (or use cache for column patterns)
        if pat_scalar && compiled_scalar.is_none() {
            match compile(patterns.value(0)) {
                Ok(re) => compiled_scalar = Some(re),
                Err(e) => {
                    if let InvalidPatternMode::EmptyString = mode {
                        scalar_pat_invalid = true;
                    } else {
                        return Err(e.into());
                    }
                }
            }
        }

        for i in 0..n {
            // idx is guaranteed non-null here
            let idx = if idx_is_scalar {
                idx_scalar
            } else if let Some(i64s) = idx_i64 {
                i64s.value(i)
            } else {
                idx_i32.unwrap().value(i) as i64
            };

            if idx < 0 {
                return Err(RegexpExtractError::NegativeIndex(idx));
            }

            // select regex
            let re = if pat_scalar {
                if scalar_pat_invalid {
                    b.append_value("");
                    continue;
                }
                compiled_scalar.as_ref().unwrap()
            } else {
                cache.get_or_compile(patterns.value(i))?
            };

            // match
            let s = strings.value(i);
            let out: &str = match captures(re, s) {
                Ok(Some(caps)) => {
                    if idx == 0 {
                        caps.get(0).map(|m| m.as_str()).unwrap_or("")
                    } else {
                        caps.get(idx as usize).map(|m| m.as_str()).unwrap_or("")
                    }
                }
                Ok(None) => "",
                Err(e) => {
                    if let InvalidPatternMode::EmptyString = mode {
                        ""
                    } else {
                        return Err(RegexpExtractError::MatchError(e.to_string()));
                    }
                }
            };

            b.append_value(out);
        }

        #[cfg(feature = "debug-logging")]
        {
            let st = cache.stats();
            eprintln!(
                "regexp_extract: rows={} hits={} misses={} compiled={} hit_rate={:.1}%",
                n,
                st.hits,
                st.misses,
                st.compiled,
                if st.hits + st.misses == 0 {
                    100.0
                } else {
                    100.0 * (st.hits as f64) / ((st.hits + st.misses) as f64)
                }
            );
        }

        return Ok(b.finish_array());
    }

    if pat_scalar && compiled_scalar.is_none() {
        match compile(patterns.value(0)) {
            Ok(re) => {
                compiled_scalar = Some(re);
                scalar_pat_invalid = false;
            }
            Err(e) => {
                if let InvalidPatternMode::EmptyString = mode {
                    scalar_pat_invalid = true; // emit "" per row below
                } else {
                    return Err(e.into());
                }
            }
        }
    }

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
            return Err(RegexpExtractError::MissingIdxArray);
        };

        if idx < 0 {
            return Err(RegexpExtractError::NegativeIndex(idx));
        }

        let re: &Regex = if pat_scalar {
            if scalar_pat_invalid {
                b.append_value("");
                continue;
            }
            compiled_scalar.as_ref().expect("compiled scalar regex")
        } else {
            match cache.get_or_compile(patterns.value(i)) {
                Ok(r) => r,
                Err(e) => {
                    if let InvalidPatternMode::EmptyString = mode {
                        b.append_value("");
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            }
        };

        let out: &str = match captures(re, s) {
            Ok(Some(caps)) => {
                if idx == 0 {
                    caps.get(0).map(|m| m.as_str()).unwrap_or("")
                } else {
                    let gi = idx as usize;
                    caps.get(gi).map(|m| m.as_str()).unwrap_or("")
                }
            }
            Ok(None) => "",
            Err(e) => {
                if let InvalidPatternMode::EmptyString = mode {
                    ""
                } else {
                    return Err(RegexpExtractError::MatchError(e.to_string()));
                }
            }
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
    cache_cap: usize,
    mode: InvalidPatternMode,
) -> Result<ArrayRef, RegexpExtractError> {
    run_generic(strings, patterns, idx_i64, idx_i32, cache_cap, mode)
}

/// LargeUtf8 strings with Utf8 patterns
pub fn run_large_utf8_utf8(
    strings: &LargeStringArray,
    patterns: &StringArray,
    idx_i64: Option<&Int64Array>,
    idx_i32: Option<&Int32Array>,
    _out_dt: &DataType,
    cache_cap: usize,
    mode: InvalidPatternMode,
) -> Result<ArrayRef, RegexpExtractError> {
    run_generic(strings, patterns, idx_i64, idx_i32, cache_cap, mode)
}

/// Utf8 strings with LargeUtf8 patterns
pub fn run_utf8_largeutf8(
    strings: &StringArray,
    patterns: &LargeStringArray,
    idx_i64: Option<&Int64Array>,
    idx_i32: Option<&Int32Array>,
    _out_dt: &DataType,
    cache_cap: usize,
    mode: InvalidPatternMode,
) -> Result<ArrayRef, RegexpExtractError> {
    run_generic(strings, patterns, idx_i64, idx_i32, cache_cap, mode)
}

/// LargeUtf8 strings with LargeUtf8 patterns
pub fn run_large_utf8_largeutf8(
    strings: &LargeStringArray,
    patterns: &LargeStringArray,
    idx_i64: Option<&Int64Array>,
    idx_i32: Option<&Int32Array>,
    _out_dt: &DataType,
    cache_cap: usize,
    mode: InvalidPatternMode,
) -> Result<ArrayRef, RegexpExtractError> {
    run_generic(strings, patterns, idx_i64, idx_i32, cache_cap, mode)
}

#[inline]
fn estimate_bytes<S: StrArray>(
    strings: &S,
    idx_i64: Option<&Int64Array>,
    idx_i32: Option<&Int32Array>,
    idx_is_scalar: bool,
    idx_scalar: i64,
) -> usize {
    let n = strings.len();

    // Base lower bound (what we used before).
    let mut bytes = n.saturating_mul(4);

    let mut sum_input = 0usize;
    for i in 0..n {
        if strings.is_null(i) {
            continue;
        }
        if !idx_is_scalar {
            if let Some(i64s) = idx_i64 {
                if i64s.is_null(i) {
                    continue;
                }
            }
            if let Some(i32s) = idx_i32 {
                if i32s.is_null(i) {
                    continue;
                }
            }
        }
        sum_input = sum_input.saturating_add(strings.value(i).len());
    }

    let group_factor = if idx_is_scalar && idx_scalar == 0 {
        1.0
    } else {
        0.25
    };
    let est = (sum_input as f64 * group_factor) as usize;

    bytes = bytes.max(est);
    bytes
}
