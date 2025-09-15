//! datafusion-regexp-extract-udf

// Public API surface (will export the UDF factory once implmented)
pub mod udf;
pub use udf::regexp_extract_udf; // will re-export `regexp_extract_udf()` later 


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