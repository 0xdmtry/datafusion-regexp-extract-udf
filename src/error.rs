use regex::Error as RegexError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RegexpExtractError {
    #[error("regexp_extract: invalid regex pattern: {0}")]
    InvalidPattern(#[from] RegexError),

    #[error("regexp_extract: idx must be >= 0, got {0}")]
    NegativeIndex(i64),

    #[error("regexp_extract: idx array missing (internal)")]
    MissingIdxArray,
}
