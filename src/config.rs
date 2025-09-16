#[derive(Debug, Clone)]
pub struct RegexpExtractConfig {
    pub cache_size: usize,
}

impl Default for RegexpExtractConfig {
    fn default() -> Self {
        Self { cache_size: 64 }
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
}
