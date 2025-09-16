use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{ArrayRef, Int32Array, LargeStringArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use std::hint::black_box;

use datafusion_regexp_extract_udf::kernel::{run_large_utf8_utf8, run_utf8_utf8};

fn bench_utf8_scalar_pattern(c: &mut Criterion) {
    let n = 20_000;

    let strings = Arc::new(StringArray::from(vec![Some("100-200"); n])) as ArrayRef;
    let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();

    // scalar pattern / idx (len == 1)
    let pattern_scalar = StringArray::from(vec![Some(r"(\d+)-(\d+)")]);
    let idx_scalar = Int32Array::from(vec![Some(1)]);

    c.bench_function("utf8 / scalar pattern / scalar idx", |b| {
        b.iter(|| {
            let out = run_utf8_utf8(
                black_box(strings),
                black_box(&pattern_scalar),
                None,
                Some(&idx_scalar),
                &DataType::Utf8,
            )
            .unwrap();
            black_box(out);
        });
    });
}

fn bench_utf8_column_pattern_repeated(c: &mut Criterion) {
    let n = 20_000;

    let strings = Arc::new(StringArray::from(vec![Some("100-200"); n])) as ArrayRef;
    let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();

    // column pattern, but same value repeated (exercises cache hit path)
    let pattern_col = StringArray::from(vec![Some(r"(\d+)-(\d+)"); n]);
    let idx_scalar = Int32Array::from(vec![Some(1)]);

    c.bench_function("utf8 / column pattern (repeated) / scalar idx", |b| {
        b.iter(|| {
            let out = run_utf8_utf8(
                black_box(strings),
                black_box(&pattern_col),
                None,
                Some(&idx_scalar),
                &DataType::Utf8,
            )
            .unwrap();
            black_box(out);
        });
    });
}

fn bench_largeutf8_scalar_pattern(c: &mut Criterion) {
    let n = 20_000;

    let strings = Arc::new(LargeStringArray::from(vec![Some("100-200"); n])) as ArrayRef;
    let strings = strings.as_any().downcast_ref::<LargeStringArray>().unwrap();

    // reuse Utf8 pattern; kernel handles LargeUtf8 strings + Utf8 patterns
    let pattern_scalar = StringArray::from(vec![Some(r"(\d+)-(\d+)")]);
    let idx_scalar = Int32Array::from(vec![Some(1)]);

    c.bench_function("largeutf8 / scalar pattern / scalar idx", |b| {
        b.iter(|| {
            let out = run_large_utf8_utf8(
                black_box(strings),
                black_box(&pattern_scalar),
                None,
                Some(&idx_scalar),
                &DataType::LargeUtf8,
            )
            .unwrap();
            black_box(out);
        });
    });
}

criterion_group!(
    benches,
    bench_utf8_scalar_pattern,
    bench_utf8_column_pattern_repeated,
    bench_largeutf8_scalar_pattern
);
criterion_main!(benches);
