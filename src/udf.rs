//! UDF construction: logical surface only

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

use std::any::Any;

/// Public factory: returns the logical UDF handle users will call from the Expr/DataFrame API
pub fn regexp_extract_udf() -> ScalarUDF {
    ScalarUDF::from(RegexpExtractUdf::new())
}

/// Internal implementation of the `regexp_extract` UDF
#[derive(Debug)]
struct RegexpExtractUdf {
    signature: Signature,
}

impl RegexpExtractUdf {
    fn new() -> Self {
        // Accept (Utf8|LargeUtf8, Utf8|LargeUtf8, Int32|Int64)
        let combos = [
            (DataType::Utf8, DataType::Utf8, DataType::Int32),
            (DataType::Utf8, DataType::Utf8, DataType::Int64),
            (DataType::Utf8, DataType::LargeUtf8, DataType::Int32),
            (DataType::Utf8, DataType::LargeUtf8, DataType::Int64),
            (DataType::LargeUtf8, DataType::Utf8, DataType::Int32),
            (DataType::LargeUtf8, DataType::Utf8, DataType::Int64),
            (DataType::LargeUtf8, DataType::LargeUtf8, DataType::Int32),
            (DataType::LargeUtf8, DataType::LargeUtf8, DataType::Int64),
        ];

        let sig = Signature::one_of(
            combos
                .into_iter()
                .map(|(a, b, c)| TypeSignature::Exact(vec![a, b, c]))
                .collect(),
            Volatility::Immutable,
        );

        Self { signature: sig }
    }
}

impl ScalarUDFImpl for RegexpExtractUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.get(0) {
            Some(DataType::Utf8) => Ok(DataType::Utf8),
            Some(DataType::LargeUtf8) => Ok(DataType::LargeUtf8),
            other => Err(DataFusionError::Plan(format!(
                "regexp_extract expects first argument Utf8 or LargeUtf8, got: {other:?}"
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        crate::eval::evaluate_regexp_extract(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType;

    #[test]
    fn udf_shape_is_correct() {
        let f = regexp_extract_udf();
        assert_eq!(f.name(), "regexp_extract");

        // Check return type resolution based on first arg width
        let utf = f
            .return_type(&[DataType::Utf8, DataType::Utf8, DataType::Int32])
            .unwrap();
        assert_eq!(utf, DataType::Utf8);

        let lutf = f
            .return_type(&[DataType::LargeUtf8, DataType::Utf8, DataType::Int64])
            .unwrap();
        assert_eq!(lutf, DataType::LargeUtf8);
    }
}
