# Performance: `regexp_extract` UDF

## Environment
- DataFusion: 49.0.2
- Rust: 1.85 (edition 2024)
- Regex engine: `regex`
- Benchmark tool: `criterion`
- Rows per run: 20,000
- Notes: release build, single process

## Method
Three micro-benchmarks over short strings:
1. **Utf8 / scalar pattern / scalar idx**
2. **Utf8 / column pattern (repeated value) / scalar idx**
3. **LargeUtf8 / scalar pattern / scalar idx**

Command:
```bash
cargo bench --bench regexp_extract
```

## After scalar fast-path + zero-alloc appends

**Changes**
- Scalar fast-path in `eval`: preserve scalar `pattern`/`idx` as length-1 arrays, enabling scalar path in kernels.
- Kernels: direct `&str` appends (avoid per-row `String` allocation) and scalar-aware null checks.

**Environment**
- Same as baseline (DataFusion 49.0.2; Rust 1.85; 20,000 rows; criterion)

**Results**
| Case                                      | Baseline Time (ms) | New Time (ms) | Time Δ        | Rows/s (baseline) | Rows/s (new) |
|-------------------------------------------|--------------------:|--------------:|:--------------|------------------:|-------------:|
| Utf8 / scalar pattern / scalar idx        | 2.74               | 2.3117        | −15.6%        | 7.30M            | 8.65M        |
| Utf8 / column pattern (repeated) / scalar idx | 3.50           | 3.0977        | −11.5%        | 5.71M            | 6.46M        |
| LargeUtf8 / scalar pattern / scalar idx   | 2.74               | 2.3196        | −15.3%        | 7.30M            | 8.62M        |

_Throughput computed as 20,000 rows ÷ time._

**Criterion notes**
- Reported median changes:
    - Utf8 scalar/scalar: −15.6%
    - Utf8 column(repeated)/scalar: −11.5%
    - LargeUtf8 scalar/scalar: −15.4%
- Outliers within normal jitter bounds.

**Baselines**
```bash
# Save baseline before optimizations
cargo bench --bench regexp_extract -- --save-baseline v0_1_0

# Compare after optimizations
cargo bench --bench regexp_extract -- --baseline v0_1_0

# Optionally save new baseline
cargo bench --bench regexp_extract -- --save-baseline fastpath_v1
``````
