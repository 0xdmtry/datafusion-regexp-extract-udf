#![cfg(feature = "fancy-regex")]

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionContext, col, lit};

use datafusion_regexp_extract_udf::{RegexpExtractConfig, regexp_extract_udf_with};

fn make_table_utf8(col_name: &str, vals: Vec<Option<&str>>) -> MemTable {
    let s = Arc::new(StringArray::from(vals)) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new(
        col_name,
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![s]).unwrap();
    MemTable::try_new(schema, vec![vec![batch]]).unwrap()
}

#[tokio::test]
async fn fancy_regex_lookbehind_works() {
    // (?<=a)b matches the 'b' only if preceded by 'a'
    let ctx = SessionContext::new();
    let udf = regexp_extract_udf_with(RegexpExtractConfig::new()); // default mode; feature enables engine
    ctx.register_udf(udf);

    let table = make_table_utf8("s", vec![Some("ab")]);
    ctx.register_table("t", Arc::new(table)).unwrap();

    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            col("s"),
            // idx=0 => whole match ("b"), note: lookbehind doesn't create a capture group
            datafusion_regexp_extract_udf::regexp_extract_udf()
                .call(vec![col("s"), lit("(?<=a)b"), lit(0)])
                .alias("m0"),
            // idx=1 => non-existent group -> ""
            datafusion_regexp_extract_udf::regexp_extract_udf()
                .call(vec![col("s"), lit("(?<=a)b"), lit(1)])
                .alias("m1"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let m0 = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let m1 = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(m0.value(0), "b");
    assert_eq!(m1.value(0), "");
}

#[tokio::test]
async fn fancy_regex_backreference_works() {
    // (ab)\1 matches "abab"; group 1 == "ab"
    let ctx = SessionContext::new();
    let udf = regexp_extract_udf_with(RegexpExtractConfig::new());
    ctx.register_udf(udf);

    let table = make_table_utf8("s", vec![Some("abab")]);
    ctx.register_table("t", Arc::new(table)).unwrap();

    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            datafusion_regexp_extract_udf::regexp_extract_udf()
                .call(vec![col("s"), lit(r"(ab)\1"), lit(0)])
                .alias("whole"),
            datafusion_regexp_extract_udf::regexp_extract_udf()
                .call(vec![col("s"), lit(r"(ab)\1"), lit(1)])
                .alias("g1"),
        ])
        .unwrap();

    let batches = df.collect().await.unwrap();
    let whole = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let g1 = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(whole.value(0), "abab");
    assert_eq!(g1.value(0), "ab");
}
