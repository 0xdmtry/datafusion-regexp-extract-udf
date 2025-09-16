#![cfg(not(feature = "fancy-regex"))]

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionContext, col, lit};

use datafusion_regexp_extract_udf::regexp_extract_udf;

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
async fn lookbehind_is_invalid_in_default_engine_and_errors() {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf()); // default: strict error

    let table = make_table_utf8("s", vec![Some("ab")]);
    ctx.register_table("t", Arc::new(table)).unwrap();

    let df = ctx
        .table("t")
        .await
        .unwrap()
        .select(vec![
            regexp_extract_udf()
                .call(vec![col("s"), lit("(?<=a)b"), lit(0)])
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
