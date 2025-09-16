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
