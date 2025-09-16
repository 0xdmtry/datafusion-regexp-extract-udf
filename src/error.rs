use crate::re::RegexError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RegexpExtractError {
    #[error("regexp_extract: invalid regex pattern: {0}")]
    InvalidPattern(#[from] Box<RegexError>),

    #[error("regexp_extract: idx must be >= 0, got {0}")]
    NegativeIndex(i64),

    #[error("regexp_extract: idx array missing (internal)")]
    MissingIdxArray,

    #[error("regexp_extract: match error: {0}")]
    MatchError(String),
}

impl From<RegexError> for RegexpExtractError {
    fn from(e: RegexError) -> Self {
        Self::InvalidPattern(Box::new(e))
    }
}
