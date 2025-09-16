# Datafusion `regexp_extract` UDF

Spark-compatible `regexp_extract` implemented as an external **Scalar UDF** for **DataFusion**, callable via the **Expr/DataFrame API** (no SQL).

## Installation

Add to `Cargo.toml`:

```toml
[dependencies]
datafusion = { version = "49.0.2", default-features = false }
datafusion-regexp-extract-udf = { version = "0.1.0", path = "." } # replace path with crates.io once published
```

## Compatibility

* DataFusion: tested with `49.0.2`
* Rust edition: 2024
* MSRV: 1.85
* Regex engine: [`regex`](https://crates.io/crates/regex) (Rust)

## Features

* Full width coverage

    * `str`: `Utf8` and `LargeUtf8`
    * `pattern`: `Utf8` and `LargeUtf8`
    * `idx`: `Int32` or `Int64` (scalar or column)
* Vectorized over Arrow arrays
* Deterministic / immutable UDF
* Per-batch compiled-pattern cache
* Return width follows `str` (`Utf8` → `Utf8`, `LargeUtf8` → `LargeUtf8`).

## Semantics (aligned with Spark)

* `idx = 0` → entire match
* `idx > group_count` → empty string `""`
* No regex match or optional group not matched → `""`
* Any NULL input at a row → NULL result at that row
* Negative `idx` → error
* Invalid regex pattern → error with diagnostic


## Usage (Expr/DataFrame API; no SQL)

```rust
use std::sync::Arc;
use datafusion::prelude::{SessionContext, col, lit};
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;

use datafusion_regexp_extract_udf::regexp_extract_udf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Session + UDF registration
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    // In-memory table
    let s = Arc::new(StringArray::from(vec![Some("100-200"), Some("foo")])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![s]).unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(table))?;

    // Call via Expr/DataFrame API
    let f = regexp_extract_udf();
    let df = ctx.table("t").await?
        .select(vec![
            col("s"),
            f.call(vec![col("s"), lit(r"(\d+)-(\d+)"), lit(1)]).alias("left"),
            f.call(vec![col("s"), lit(r"(\d+)"), lit(1)]).alias("digits"),
            f.call(vec![col("s"), lit(r"(\d+)-(\d+)"), lit(0)]).alias("whole"),
        ])?;

    let batches = df.collect().await?;
    datafusion::arrow::util::pretty::print_batches(&batches)?;
    Ok(())
}
```

Example output:

```
+---------+-------+--------+---------+
| s       | left  | digits | whole   |
+---------+-------+--------+---------+
| 100-200 | 100   | 100    | 100-200 |
| foo     |       |        |         |
+---------+-------+--------+---------+
```

## Regex engine differences (Spark vs DataFusion)

* Spark uses **Java regex**; this UDF uses Rust’s `regex` engine.
* Look-behind and some advanced constructs are not supported by Rust `regex`.
* Inline flags such as `(?i)` (case-insensitive) are supported.
* When a Java-only feature is required, adjust the pattern or pre-process data accordingly.

## Testing

```bash
cargo test
```

```bash
cargo test --features fancy-regex -q 
```

Integration tests exercise:

* Spark documentation examples
* All width combinations (Utf8/LargeUtf8 × Utf8/LargeUtf8)
* NULL propagation and error paths
* Unicode strings

Run the example:

```bash
cargo run --example df_api
```

## Configuration

```text
use datafusion_regexp_extract_udf::{
    regexp_extract_udf_with, RegexpExtractConfig, InvalidPatternMode,
};

let cfg = RegexpExtractConfig::new()
    .cache_size(128)
    .invalid_pattern_mode(InvalidPatternMode::EmptyString);

let udf = regexp_extract_udf_with(cfg);
```
## Feature flags

- `fancy-regex` — enables look-around/backreferences via `fancy-regex`
  (higher cost; keep off unless needed).
- `debug-logging` — prints per-batch cache stats (hits/misses/compiled) to **stderr**.
  In tests, use `-- --nocapture` to see the output.

Examples:
```bash
# Tests
cargo test --features fancy-regex
cargo test --features debug-logging -- --nocapture

# Run example with logging
cargo run --features debug-logging --example df_api

# Run example with both flags
cargo run --features "fancy-regex debug-logging" --example df_api
```

### Sanity checklist

- Defaults unchanged: `regexp_extract_udf()` still works as before.
- Configured path: `regexp_extract_udf_with(cfg)` captures and threads `cache_size` into the kernels.
- Tests & clippy should pass after updating benches (if they call kernels directly).

## Additional commands

```bash
cargo check
```

```bash
cargo fmt -- --check
```

```bash 
cargo clippy --all-targets --all-features -- -D warnings
```

 ```bash
cargo test -q
 ```

```bash
cargo test --features debug-logging -- --nocapture
```

```bash
cargo test --features fancy-regex -q 
```

```bash
cargo bench --bench regexp_extract
```

```bash
cargo run --example ping
 ```

```bash 
cargo run --example df_api
```

```bash
cargo run --example df_api --features debug-logging
```

## Future improvements

- **Custom internal error type + UDF mapping**  
  Structured variants (e.g., `InvalidPattern`, `NegativeIndex`) internally; mapped to `DataFusionError` at the boundary for consistent external behavior.
- **Configurable behavior for invalid patterns**  
  Optional mode to return `""` on pattern compilation failure instead of error, to mimic certain Spark workflows.
- **Optional alternative regex engine**  
  Feature-flag `fancy-regex` for look-around/backreferences; default remains `regex` for performance and footprint.
- **Lightweight observability**  
  Counters for cache hits/misses and compile counts; optional debug logging to aid tuning.
- **User-facing builder for tuning**  
  Knobs for `cache_size`, `invalid_pattern_mode`, `engine` (regex/fancy), and compatibility warnings.

---

- **Vectorized NULL iteration**  
  Iterate validity bitmaps to reduce per-row `is_null()` checks on large batches.
- **Better builder sizing**  
  Heuristics to pre-estimate output bytes (beyond `n*4`) to lower reallocations for long outputs.
- **Global LRU across batches (guarded)**  
  Small shared LRU behind a lock to reuse stable patterns across batches; enable only if profiling shows regex compile cost dominates.

---

- **Property-based / fuzz testing**  
  Broaden input coverage (random patterns/inputs) to harden behavior under edge cases.
- **Spark-compat suite (optional)**  
  Cross-check selected patterns against a Spark runner to track engine-difference impacts.
- **Benchmark scenarios**  
  Add cases for mixed-match rates and highly nested alternations; include long-string Unicode workloads.
- **Documentation**  
  Expand compatibility notes with Java↔Rust regex migration tips and pattern rewrite examples.


## Documentation

- [Semantics](docs/SEMANTICS.md)
- [Compatibility (Spark ↔︎ Rust regex)](docs/COMPATIBILITY.md)
- [Performance](docs/PERFORMANCE.md)
- [Changelog](CHANGELOG.md)
- Example source: [`examples/df_api.rs`](examples/df_api.rs)

## License

[Apache-2.0](LICENSE)
