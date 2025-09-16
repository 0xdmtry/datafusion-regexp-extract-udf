use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, LargeStringArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionContext, col, lit};
use datafusion::scalar::ScalarValue;

use datafusion_regexp_extract_udf::{regexp_extract_udf, regexp_extract_udf_with};

use datafusion_regexp_extract_udf::{InvalidPatternMode, RegexpExtractConfig};

fn make_memtable(
    s: ArrayRef,
    p_opt: Option<ArrayRef>, // Optional pattern column (used for pattern-as-column tests)
    idx_opt: Option<ArrayRef>, // Optional idx column
    s_name: &str,
    p_name: &str,
    idx_name: &str,
) -> MemTable {
    let mut fields = vec![Field::new(
        s_name,
        match s.data_type() {
            DataType::Utf8 => DataType::Utf8,
            DataType::LargeUtf8 => DataType::LargeUtf8,
            dt => dt.clone(),
        },
        true,
    )];

    let mut cols = vec![s];

    if let Some(p) = p_opt {
        fields.push(Field::new(
            p_name,
            match p.data_type() {
                DataType::Utf8 => DataType::Utf8,
                DataType::LargeUtf8 => DataType::LargeUtf8,
                dt => dt.clone(),
            },
            true,
        ));
        cols.push(p);
    }

    if let Some(i) = idx_opt {
        fields.push(Field::new(
            idx_name,
            match i.data_type() {
                DataType::Int32 => DataType::Int32,
                DataType::Int64 => DataType::Int64,
                dt => dt.clone(),
            },
            true,
        ));
        cols.push(i);
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    MemTable::try_new(schema, vec![vec![batch]]).unwrap()
}

#[tokio::test]
async fn utf8_str_utf8_pattern_literal_idx_scalar() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    // s: Utf8
    let s = Arc::new(StringArray::from(vec![
        Some("100-200"),
        Some("foo"),
        Some("aaaac"),
    ])) as ArrayRef;

    // Register table with only 's' column
    let table = make_memtable(s, None, None, "s", "p", "i");
    ctx.register_table("t", Arc::new(table)).unwrap();

    let f = regexp_extract_udf();
    // Use Spark examples:
    // 1) (\d+)-(\d+), idx=1 --> "100"
    // 2) (\d+), idx=1 over "foo" --> ""
    // 3) (a+)(b)?(c), idx=2 over "aaaac" --> ""
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            col("s"),
            f.call(vec![col("s"), lit(r"(\d+)-(\d+)"), lit(1)])
                .alias("g1"),
            f.call(vec![col("s"), lit(r"(\d+)"), lit(1)])
                .alias("g1_miss"),
            f.call(vec![col("s"), lit("(a+)(b)?(c)"), lit(2)])
                .alias("opt_miss"),
            f.call(vec![col("s"), lit(r"(\d+)-(\d+)"), lit(0)])
                .alias("whole"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let out = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(out.value(0), "100"); // from "100-200"
    let miss = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(miss.value(1), ""); // "foo" with (\d+) -> ""
    let opt = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(opt.value(2), ""); // (a+)(b)?(c) group 2 missing -> ""
    let whole = batches[0]
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(whole.value(0), "100-200"); // idx=0 -> whole match
}

#[tokio::test]
async fn largeutf8_str_utf8_pattern_literal_idx_column() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    // s: LargeUtf8
    let s = Arc::new(LargeStringArray::from(vec![Some("100-200"), Some("foo")])) as ArrayRef;
    // idx as a column: [1, 1]
    let idx = Arc::new(Int32Array::from(vec![Some(1), Some(1)])) as ArrayRef;

    let table = make_memtable(s, None, Some(idx), "s", "p", "i");
    ctx.register_table("t", Arc::new(table)).unwrap();

    let f = regexp_extract_udf();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            col("s"),
            f.call(vec![col("s"), lit(r"(\d+)-(\d+)"), col("i")])
                .alias("g1"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let g1 = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(g1.value(0), "100");
    assert_eq!(g1.value(1), ""); // "foo" doesn't match
}

#[tokio::test]
async fn utf8_str_largeutf8_pattern_literal_idx_scalar() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    let s = Arc::new(StringArray::from(vec![Some("42-7")])) as ArrayRef;

    // Pattern as LargeUtf8 *scalar* literal
    let pat = ScalarValue::LargeUtf8(Some(r"(\d+)-(\d+)".to_string()));
    let df = ctx
        .read_table(Arc::new(make_memtable(s, None, None, "s", "p", "i")))
        .unwrap()
        .select(vec![
            regexp_extract_udf()
                .call(vec![col("s"), lit(pat), lit(2)])
                .alias("g2"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let g2 = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(g2.value(0), "7");
}

#[tokio::test]
async fn largeutf8_str_largeutf8_pattern_column_idx_scalar() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    let s = Arc::new(LargeStringArray::from(vec![Some("abc123"), Some("xyz")])) as ArrayRef;
    // Pattern as LargeUtf8 *column*
    let p = Arc::new(LargeStringArray::from(vec![
        Some(r"([A-Za-z]+)(\d+)"),
        Some(r"(\d+)"),
    ])) as ArrayRef;

    let table = make_memtable(s, Some(p), None, "s", "p", "i");
    ctx.register_table("t", Arc::new(table)).unwrap();

    let f = regexp_extract_udf();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![f.call(vec![col("s"), col("p"), lit(2)]).alias("g2")])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let g2 = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(g2.value(0), "123");
    assert_eq!(g2.value(1), "");
}

#[tokio::test]
async fn negative_idx_errors() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    let s = Arc::new(StringArray::from(vec![Some("100-200")])) as ArrayRef;
    let table = make_memtable(s, None, None, "s", "p", "i");
    ctx.register_table("t", Arc::new(table)).unwrap();

    let f = regexp_extract_udf();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            f.call(vec![col("s"), lit(r"(\d+)-(\d+)"), lit(-1)])
                .alias("bad"),
        ])
        .unwrap();

    let err = df
        .collect()
        .await
        .expect_err("should error on negative idx");
    let msg = format!("{err:?}");
    assert!(msg.contains("idx must be >= 0"));
}

#[tokio::test]
async fn null_input_propagates() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    // s has a NULL
    let s = Arc::new(StringArray::from(vec![Some("100-200"), None])) as ArrayRef;
    let table = make_memtable(s, None, None, "s", "p", "i");
    ctx.register_table("t", Arc::new(table)).unwrap();

    let f = regexp_extract_udf();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            f.call(vec![col("s"), lit(r"(\d+)-(\d+)"), lit(1)])
                .alias("g1"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let out = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(out.value(0), "100");
    assert!(out.is_null(1)); // NULL input -> NULL output
}

#[tokio::test]
async fn invalid_regex_errors() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    let s = Arc::new(StringArray::from(vec![Some("ab")])) as ArrayRef;
    let table = make_memtable(s, None, None, "s", "p", "i");
    ctx.register_table("t", Arc::new(table)).unwrap();

    // Rust regex doesn't support look-behind; this should error at compile time
    let f = regexp_extract_udf();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            f.call(vec![col("s"), lit(r"(?<=a)b"), lit(0)])
                .alias("whole"),
        ])
        .unwrap();

    let err = df
        .collect()
        .await
        .expect_err("should error on invalid pattern");
    let msg = format!("{err:?}");
    assert!(msg.contains("invalid regex pattern"));
}

#[tokio::test]
async fn invalid_pattern_lenient_returns_empty_string() {
    let mut cfg = RegexpExtractConfig::new();
    cfg.invalid_pattern_mode = InvalidPatternMode::EmptyString;

    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf_with(cfg));

    let s = StringArray::from(vec![Some("ab")]);
    let s = Arc::new(s) as ArrayRef;

    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![s]).unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    // invalid in Rust regex (look-behind)
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            col("s"),
            regexp_extract_udf_with(
                RegexpExtractConfig::new().invalid_pattern_mode(InvalidPatternMode::EmptyString),
            )
            .call(vec![col("s"), lit("(?<=a)b"), lit(0)])
            .alias("whole"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let out = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(out.value(0), "");
}
