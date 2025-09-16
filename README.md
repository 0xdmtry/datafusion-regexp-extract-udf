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

Integration tests exercise:

* Spark documentation examples
* All width combinations (Utf8/LargeUtf8 × Utf8/LargeUtf8)
* NULL propagation and error paths
* Unicode strings

Run the example:

```bash
cargo run --example df_api
```

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
cargo bench --bench regexp_extract
```

```bash
cargo run --example ping
 ```

```bash 
cargo run --example df_api
```

## License

Apache-2.0.
