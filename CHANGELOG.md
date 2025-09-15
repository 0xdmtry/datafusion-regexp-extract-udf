# Changelog

## [0.1.0] — Initial release
- Spark-compatible `regexp_extract` as a DataFusion external Scalar UDF.
- Full width coverage: `str` (Utf8/LargeUtf8) × `pattern` (Utf8/LargeUtf8).
- `idx` as Int32/Int64 (scalar or column).
- Semantics: `idx=0` whole match; no match or missing group → `""`; NULL propagation; negative `idx` → error.
- Per-batch compiled-pattern cache.
