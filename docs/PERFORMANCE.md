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

**Cache size rationale**
- Default per-batch cache size is 64 entries. Typical analytical queries use a small set of repeated patterns (dozens, not thousands) per batch; 64 balances hit rate and memory.
- Tune this constant if workloads show either (a) many distinct patterns per batch or (b) very stable patterns across batches.

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


## After per-batch LRU pattern cache

**Change**
- Replaced HashMap+clear eviction with a per-batch **LRU cache** (using `lru`) to avoid thrashing when pattern columns contain repeated values.

**Environment**
- Same as above (DataFusion 49.0.2; Rust 1.85; 20,000 rows; criterion)

**Results (absolute times)**

| Case                                      | Time (ms) | Rows/s      |
|-------------------------------------------|----------:|------------:|
| Utf8 / scalar pattern / scalar idx        | 2.3556    | 8.49M       |
| Utf8 / column pattern (repeated) / scalar idx | 2.7625 | 7.24M       |
| LargeUtf8 / scalar pattern / scalar idx   | 2.3426    | 8.54M       |


**Criterion reported deltas vs previous baseline**
- Utf8 scalar/scalar: **≈ +0.8% … +1.9%** (within noise)
- Utf8 column(repeated)/scalar: **≈ −13.9%** (clear improvement)
- LargeUtf8 scalar/scalar: **≈ −1.0% … +0.2%** (no material change)

**Notes**
- The LRU cache primarily benefits workloads with **pattern-as-column** and repeated values.
- Scalar-pattern cases were already optimized via the scalar fast-path.


## Benchmark matrix expansion

**Setup**
- Same environment and row count (20,000) as above.

**Results (median)**

| Case                                          | Time (ms) | Rows/s   |
|-----------------------------------------------|----------:|---------:|
| Utf8 / scalar pattern / scalar idx            | 2.2976    | 8.70M    |
| Utf8 / column pattern (repeated) / scalar idx | 2.7140    | 7.37M    |
| LargeUtf8 / scalar pattern / scalar idx       | 2.3671    | 8.46M    |
| Utf8 / unique pattern per row / scalar idx    | 235.15    | 0.085M   |
| Utf8 / long strings 10/10 / scalar pat+idx    | 2.0106    | 9.95M    |
| Utf8 / long strings 100/100 / scalar pat+idx  | 5.1552    | 3.88M    |
| Utf8 / long strings 1000/1000 / scalar pat+idx| 37.309    | 0.54M    |
| Utf8 / no-match / scalar pat+idx              | 0.9757    | 20.5M    |
| Utf8 / heavy alternation / scalar pat+idx     | 3.9616    | 5.05M    |

**Interpretation**
- **Repeated-column patterns** benefit from the per-batch **LRU cache** (see earlier section).
- **Unique pattern per row** is the **worst case** (regex compiled per row); this validates correctness under cache-miss workloads.
- **Longer strings** scale roughly with input length (as expected).
- **No-match** can be faster than “match” due to early failure in the regex engine.
- **Heavy alternation** stresses the engine more than simple digit captures.

> Note: Criterion warned that “unique pattern per row” needs a longer target time; results are still representative for relative comparisons.
