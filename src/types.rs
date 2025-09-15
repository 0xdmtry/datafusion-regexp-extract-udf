use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, LargeStringArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;

pub fn to_array(cv: &ColumnarValue, rows: usize) -> Result<ArrayRef> {
    match cv {
        ColumnarValue::Array(a) => Ok(a.clone()),
        ColumnarValue::Scalar(sv) => sv.to_array_of_size(rows),
    }
}

pub enum StringWidth<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
}

pub fn as_string_width(arr: &ArrayRef) -> Result<StringWidth<'_>> {
    match arr.data_type() {
        DataType::Utf8 => Ok(StringWidth::Utf8(
            arr.as_any().downcast_ref::<StringArray>().unwrap(),
        )),
        DataType::LargeUtf8 => Ok(StringWidth::LargeUtf8(
            arr.as_any().downcast_ref::<LargeStringArray>().unwrap(),
        )),
        other => Err(DataFusionError::Execution(format!(
            "expected Utf8 or LargeUtf8, got {other:?}"
        ))),
    }
}

pub enum IndexWidth<'a> {
    I32(&'a Int32Array),
    I64(&'a Int64Array),
}

pub fn as_index_width(arr: &ArrayRef) -> Result<IndexWidth<'_>> {
    match arr.data_type() {
        DataType::Int32 => Ok(IndexWidth::I32(
            arr.as_any().downcast_ref::<Int32Array>().unwrap(),
        )),
        DataType::Int64 => Ok(IndexWidth::I64(
            arr.as_any().downcast_ref::<Int64Array>().unwrap(),
        )),
        other => Err(DataFusionError::Execution(format!(
            "expected Int32 or Int64 for idx, got {other:?}"
        ))),
    }
}

/// Convenience: make a ScalarValue::Utf8 or LargeUtf8 NULL of the right width
pub fn null_string_of_width(dt: &DataType) -> ScalarValue {
    match dt {
        DataType::Utf8 => ScalarValue::Utf8(None),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
        _ => ScalarValue::Utf8(None),
    }
}

pub fn string_builder_for(dt: &DataType, len: usize) -> Result<ArrayRef> {
    Err(DataFusionError::Internal(
        "string_builder_for not used directly".into(),
    ))
}
