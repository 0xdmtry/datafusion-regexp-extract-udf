//! datafusion-regexp-extract-udf

// Public API surface (will export the UDF factory once implmented)
mod config;
pub mod error;
pub mod eval;
pub mod kernel;
pub mod pattern_cache;
pub mod re;
pub mod types;
pub mod udf;

pub use config::{InvalidPatternMode, RegexpExtractConfig};
pub use udf::{regexp_extract_udf, regexp_extract_udf_with};

/// Returns a ping message; used by the smoke test
pub fn ping() -> &'static str {
    "pong"
}

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(crate::ping(), "pong")
    }
}
