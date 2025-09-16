# Semantics: `regexp_extract`

**Signature**  
`regexp_extract(str: Utf8|LargeUtf8, pattern: Utf8|LargeUtf8, idx: Int32|Int64) -> Utf8|LargeUtf8`

**Return width**
- Returns `Utf8` if `str` is `Utf8`; returns `LargeUtf8` if `str` is `LargeUtf8`.

**Determinism & volatility**
- Deterministic, immutable: output depends only on inputs.

**Null behavior**
- If **any** of `str`, `pattern`, or `idx` is `NULL` at a row → result is `NULL` at that row.

**Index (`idx`) rules**
- `idx = 0` → entire match.
- `idx > group_count` → empty string `""`.
- `idx < 0` → error (execution error).
- `idx` may be a scalar or a column.

**Match behavior**
- If the regex **does not match** the input string → `""`.
- If the specified **group exists but did not match** (e.g., optional group) → `""`.

**Types**
- `str`: `Utf8` or `LargeUtf8`
- `pattern`: `Utf8` or `LargeUtf8` (scalar or column)
- `idx`: `Int32` or `Int64` (scalar or column)

**Unicode**

- Uses Rust `regex` with Unicode support; group boundaries respect UTF-8.

**Performance notes**

- Compiles a scalar `pattern` once per batch.
- For `pattern` as a column, uses a small per-batch cache to avoid repeated compilations.

**Errors**

Errors are represented internally via a structured `RegexpExtractError` enum (e.g., `InvalidPattern`, `NegativeIndex`) and are mapped to `DataFusionError::Execution` at the UDF boundary.


**Invalid pattern & engine behavior**

- **Default (mode = `Error`)**: a syntactically invalid pattern results in an error (`DataFusionError::Execution`) for the batch.
- **Optional (mode = `EmptyString`)**: invalid patterns yield `""` for affected rows; processing continues.
- With the **`fancy-regex`** feature, look-around/backreferences are supported; compilation or match-time errors are still surfaced/handled according to the selected mode.


  **Unicode**

- Uses a Unicode-aware engine; capture groups operate on UTF-8 codepoint boundaries (results are substrings, not byte ranges).