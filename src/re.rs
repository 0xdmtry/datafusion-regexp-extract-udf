#[cfg(feature = "fancy-regex")]
pub use fancy_regex as engine;
#[cfg(not(feature = "fancy-regex"))]
pub use regex as engine;

pub type Regex = engine::Regex;

#[cfg(feature = "fancy-regex")]
pub type Captures<'a> = fancy_regex::Captures<'a>;
#[cfg(not(feature = "fancy-regex"))]
pub type Captures<'a> = regex::Captures<'a>;

#[cfg(feature = "fancy-regex")]
pub type RegexError = fancy_regex::Error;
#[cfg(not(feature = "fancy-regex"))]
pub type RegexError = regex::Error;

#[inline]
pub fn compile(pat: &str) -> Result<Regex, Box<RegexError>> {
    engine::Regex::new(pat).map_err(Box::new)
}

/// Always return `Result<Option<Captures>>` so callers handle both engines uniformly.
#[inline]
pub fn captures<'a>(re: &'a Regex, s: &'a str) -> Result<Option<Captures<'a>>, Box<RegexError>> {
    #[cfg(feature = "fancy-regex")]
    {
        re.captures(s).map_err(Box::new)
    }
    #[cfg(not(feature = "fancy-regex"))]
    {
        Ok(re.captures(s))
    }
}
