use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionContext, col, lit};
use datafusion_regexp_extract_udf::regexp_extract_udf;

fn memtable(cols: Vec<(&str, ArrayRef)>) -> MemTable {
    let fields = cols
        .iter()
        .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let arrays = cols.into_iter().map(|(_, a)| a).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    MemTable::try_new(schema, vec![vec![batch]]).unwrap()
}

#[tokio::test]
async fn unicode_extracts_properly() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    let s = Arc::new(StringArray::from(vec![Some("Köln99"), Some("東京123")])) as ArrayRef;
    let t = memtable(vec![("s", s)]);
    ctx.register_table("t", Arc::new(t)).unwrap();

    let f = regexp_extract_udf();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            f.call(vec![col("s"), lit(r"([^\d]+)(\d+)"), lit(2)])
                .alias("num"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let out = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(out.value(0), "99");
    assert_eq!(out.value(1), "123");
}

#[tokio::test]
async fn nulls_in_pattern_and_idx_propagate() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    let s = Arc::new(StringArray::from(vec![Some("100-200"), Some("300-400")])) as ArrayRef;
    let p = Arc::new(StringArray::from(vec![Some(r"(\d+)-(\d+)"), None])) as ArrayRef;
    let i = Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef;

    let t = memtable(vec![("s", s), ("p", p), ("i", i)]);
    ctx.register_table("t", Arc::new(t)).unwrap();

    let f = regexp_extract_udf();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![f.call(vec![col("s"), col("p"), col("i")]).alias("g1")])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let g1 = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(g1.value(0), "100"); // ok
    assert!(g1.is_null(1)); // NULL pattern/idx -> NULL output
}

#[tokio::test]
async fn too_large_idx_returns_empty() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    let s = Arc::new(StringArray::from(vec![Some("100-200")])) as ArrayRef;
    let t = memtable(vec![("s", s)]);
    ctx.register_table("t", Arc::new(t)).unwrap();

    let f = regexp_extract_udf();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            f.call(vec![col("s"), lit(r"(\d+)-(\d+)"), lit(3)])
                .alias("g3"), // only 2 groups exist
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let g3 = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(g3.value(0), ""); // out-of-range group -> ""
}

#[tokio::test]
async fn negative_idx_in_column_errors() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    let s = Arc::new(StringArray::from(vec![Some("100-200")])) as ArrayRef;
    let i = Arc::new(Int32Array::from(vec![Some(-1)])) as ArrayRef;
    let t = memtable(vec![("s", s), ("i", i)]);
    ctx.register_table("t", Arc::new(t)).unwrap();

    let f = regexp_extract_udf();
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            f.call(vec![col("s"), lit(r"(\d+)-(\d+)"), col("i")])
                .alias("bad"),
        ])
        .unwrap();

    let err = df.collect().await.expect_err("negative idx should error");
    assert!(format!("{err:?}").contains("idx must be >= 0"));
}
