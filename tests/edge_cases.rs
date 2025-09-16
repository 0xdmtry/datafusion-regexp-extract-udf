use datafusion::arrow::array::{ArrayRef, LargeStringArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionContext, col, lit};
use std::sync::Arc;

use datafusion_regexp_extract_udf::regexp_extract_udf;

fn memtable(cols: Vec<(&str, DataType, ArrayRef)>) -> MemTable {
    let fields = cols
        .iter()
        .map(|(n, dt, _)| Field::new(*n, dt.clone(), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let arrays = cols.into_iter().map(|(_, _, a)| a).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    MemTable::try_new(schema, vec![vec![batch]]).unwrap()
}

#[tokio::test]
async fn empty_input_string() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    let s = Arc::new(StringArray::from(vec![Some("")])) as ArrayRef;
    let t = memtable(vec![("s", DataType::Utf8, s)]);
    ctx.register_table("t", Arc::new(t)).unwrap();

    let f = regexp_extract_udf();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            f.call(vec![col("s"), lit(r"(\d+)"), lit(1)]).alias("g1"), // no match -> ""
            f.call(vec![col("s"), lit(r""), lit(0)])
                .alias("whole_empty_pat"), // "" matches "" -> ""
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let g1 = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(g1.value(0), "");
    let whole = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(whole.value(0), "");
}

#[tokio::test]
async fn empty_pattern_behaviour() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    let s = Arc::new(StringArray::from(vec![Some("abc")])) as ArrayRef;
    let t = memtable(vec![("s", DataType::Utf8, s)]);
    ctx.register_table("t", Arc::new(t)).unwrap();

    let f = regexp_extract_udf();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            f.call(vec![col("s"), lit(r""), lit(0)]).alias("whole"), // empty match
            f.call(vec![col("s"), lit(r""), lit(1)]).alias("g1"),    // no group -> ""
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let whole = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(whole.value(0), "");
    let g1 = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(g1.value(0), "");
}

#[tokio::test]
async fn unique_pattern_per_row_cache_miss_case() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    let s = Arc::new(StringArray::from(vec![Some("a1"), Some("b2"), Some("c3")])) as ArrayRef;
    let p = Arc::new(StringArray::from(vec![
        Some(r"(a)(\d)"),
        Some(r"(b)(\d)"),
        Some(r"(c)(\d)"),
    ])) as ArrayRef;
    let t = memtable(vec![("s", DataType::Utf8, s), ("p", DataType::Utf8, p)]);
    ctx.register_table("t", Arc::new(t)).unwrap();

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
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(g2.value(0), "1");
    assert_eq!(g2.value(1), "2");
    assert_eq!(g2.value(2), "3");
}

#[tokio::test]
async fn long_strings_varying_lengths() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    // rows: "aaaa...a123bbbb", with a/b lengths 10, 100, 1000
    let mk = |a: usize, b: usize| -> String {
        let mut s = String::with_capacity(a + 3 + b);
        s.push_str(&"a".repeat(a));
        s.push_str("123");
        s.push_str(&"b".repeat(b));
        s
    };
    let s = Arc::new(LargeStringArray::from(vec![
        Some(mk(10, 10)),
        Some(mk(100, 100)),
        Some(mk(1000, 1000)),
    ])) as ArrayRef;
    let t = memtable(vec![("s", DataType::LargeUtf8, s)]);
    ctx.register_table("t", Arc::new(t)).unwrap();

    let f = regexp_extract_udf();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            f.call(vec![col("s"), lit(r"(\d+)"), lit(1)])
                .alias("digits"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let digits = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(digits.value(0), "123");
    assert_eq!(digits.value(1), "123");
    assert_eq!(digits.value(2), "123");
}
