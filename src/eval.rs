use crate::config::RegexpExtractConfig;
use crate::kernel::{
    run_large_utf8_largeutf8, run_large_utf8_utf8, run_utf8_largeutf8, run_utf8_utf8,
};
use crate::types::to_array;
use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, LargeStringArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};

pub fn evaluate_regexp_extract_with(
    args: ScalarFunctionArgs,
    cfg: &RegexpExtractConfig,
) -> Result<ColumnarValue> {
    let rows = args.number_rows;
    let a = &args.args;

    if a.len() != 3 {
        return Err(DataFusionError::Execution(format!(
            "regexp_extract expects 3 arguments, got {}",
            a.len()
        )));
    }

    // Materialize to arrays (handles scalars by expanding to length `rows`)
    let s_arr = to_array(&a[0], rows)?;

    let p_arr = match &a[1] {
        ColumnarValue::Scalar(sv) => sv.to_array()?,
        _ => to_array(&a[1], rows)?,
    };
    let i_arr = match &a[2] {
        ColumnarValue::Scalar(sv) => sv.to_array()?,
        _ => to_array(&a[2], rows)?,
    };

    // Determine output width from first arg
    let out_dt = s_arr.data_type().clone();

    // Downcast pattern and idx
    let pat_utf8: Option<&StringArray> = (p_arr.data_type() == &DataType::Utf8)
        .then(|| p_arr.as_any().downcast_ref::<StringArray>().unwrap());

    let pat_lutf8: Option<&LargeStringArray> = (p_arr.data_type() == &DataType::LargeUtf8)
        .then(|| p_arr.as_any().downcast_ref::<LargeStringArray>().unwrap());

    if pat_utf8.is_none() && pat_lutf8.is_none() {
        return Err(DataFusionError::Execution(
            "regexp_extract pattern must be Utf8 or LargeUtf8".into(),
        ));
    }

    let (idx_i64, idx_i32): (Option<&Int64Array>, Option<&Int32Array>) = match i_arr.data_type() {
        DataType::Int64 => (
            Some(i_arr.as_any().downcast_ref::<Int64Array>().unwrap()),
            None,
        ),
        DataType::Int32 => (
            None,
            Some(i_arr.as_any().downcast_ref::<Int32Array>().unwrap()),
        ),
        other => {
            return Err(DataFusionError::Execution(format!(
                "regexp_extract idx must be Int32 or Int64, got {other:?}"
            )));
        }
    };

    let out: ArrayRef = match (s_arr.data_type(), p_arr.data_type()) {
        (DataType::Utf8, DataType::Utf8) => {
            let s = s_arr.as_any().downcast_ref::<StringArray>().unwrap();
            let p = pat_utf8.unwrap();
            run_utf8_utf8(
                s,
                p,
                idx_i64,
                idx_i32,
                &out_dt,
                cfg.cache_size,
                cfg.invalid_pattern_mode,
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?
        }
        (DataType::LargeUtf8, DataType::Utf8) => {
            let s = s_arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let p = pat_utf8.unwrap();
            run_large_utf8_utf8(
                s,
                p,
                idx_i64,
                idx_i32,
                &out_dt,
                cfg.cache_size,
                cfg.invalid_pattern_mode,
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?
        }
        (DataType::Utf8, DataType::LargeUtf8) => {
            let s = s_arr.as_any().downcast_ref::<StringArray>().unwrap();
            let p = pat_lutf8.unwrap();
            run_utf8_largeutf8(
                s,
                p,
                idx_i64,
                idx_i32,
                &out_dt,
                cfg.cache_size,
                cfg.invalid_pattern_mode,
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let s = s_arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let p = pat_lutf8.unwrap();
            run_large_utf8_largeutf8(
                s,
                p,
                idx_i64,
                idx_i32,
                &out_dt,
                cfg.cache_size,
                cfg.invalid_pattern_mode,
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?
        }
        (other_s, _) => {
            return Err(DataFusionError::Execution(format!(
                "regexp_extract expects first argument Utf8 or LargeUtf8, got {other_s:?}"
            )));
        }
    };

    Ok(ColumnarValue::Array(out))
}

pub fn evaluate_regexp_extract(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    evaluate_regexp_extract_with(args, &RegexpExtractConfig::default())
}
