use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionContext, col, lit};
use datafusion_regexp_extract_udf::regexp_extract_udf;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_udf(regexp_extract_udf());

    // Build a small in-memory table
    let s = Arc::new(StringArray::from(vec![Some("100-200"), Some("foo")])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![s]).unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(table))?;

    let f = regexp_extract_udf();
    let df = ctx.table("t").await?.select(vec![
        col("s"),
        f.call(vec![col("s"), lit(r"(\d+)-(\d+)"), lit(1)])
            .alias("left_num"),
        f.call(vec![col("s"), lit(r"(\d+)"), lit(1)])
            .alias("digits"),
        f.call(vec![col("s"), lit(r"(\d+)-(\d+)"), lit(0)])
            .alias("whole"),
    ])?;

    let batches = df.collect().await?;
    datafusion::arrow::util::pretty::print_batches(&batches)?;
    Ok(())
}
