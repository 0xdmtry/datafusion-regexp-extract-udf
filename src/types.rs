use datafusion::arrow::array::ArrayRef;
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;

pub fn to_array(cv: &ColumnarValue, rows: usize) -> Result<ArrayRef> {
    match cv {
        ColumnarValue::Array(a) => Ok(a.clone()),
        ColumnarValue::Scalar(sv) => sv.to_array_of_size(rows),
    }
}
