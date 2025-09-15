//! datafusion-regexp-extract-udf

pub mod udf;
pub use udf::*;


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