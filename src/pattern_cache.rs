use crate::error::RegexpExtractError;
use crate::re::{Regex, compile};
use lru::LruCache;
use std::num::NonZeroUsize;

#[derive(Debug, Default, Clone, Copy)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub compiled: u64,
}

pub struct PatternCache {
    lru: LruCache<String, Regex>,
    stats: CacheStats,
}

impl PatternCache {
    pub fn new(cap: usize) -> Self {
        // LruCache requires NonZeroUsize; clamp 0 to 1
        let cap_nz = NonZeroUsize::new(cap.max(1)).unwrap();
        Self {
            lru: LruCache::new(cap_nz),
            stats: CacheStats::default(),
        }
    }

    pub fn stats(&self) -> CacheStats {
        self.stats
    }

    pub fn reset_stats(&mut self) {
        self.stats = CacheStats::default();
    }

    pub fn get_or_compile(&mut self, pat: &str) -> Result<&Regex, RegexpExtractError> {
        if self.lru.contains(pat) {
            self.stats.hits += 1;
            return Ok(self.lru.get(pat).unwrap());
        }
        self.stats.misses += 1;
        let re = compile(pat)?;
        self.stats.compiled += 1;
        self.lru.put(pat.to_string(), re);
        Ok(self.lru.get(pat).expect("entry just inserted"))
    }
}
