#[derive(Debug, Clone, Copy)]
pub enum InvalidPatternMode {
    Error,
    EmptyString,
}

#[derive(Debug, Clone)]
pub struct RegexpExtractConfig {
    pub cache_size: usize,
    pub invalid_pattern_mode: InvalidPatternMode,
}

impl Default for RegexpExtractConfig {
    fn default() -> Self {
        Self {
            cache_size: 64,
            invalid_pattern_mode: InvalidPatternMode::Error,
        }
    }
}

impl RegexpExtractConfig {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cache_size(mut self, n: usize) -> Self {
        self.cache_size = n;
        self
    }

    pub fn invalid_pattern_mode(mut self, m: InvalidPatternMode) -> Self {
        self.invalid_pattern_mode = m;
        self
    }
}
