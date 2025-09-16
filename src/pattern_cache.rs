use crate::error::RegexpExtractError;
use lru::LruCache;
use regex::Regex;
use std::num::NonZeroUsize;

pub struct PatternCache {
    lru: LruCache<String, Regex>,
}

impl PatternCache {
    pub fn new(cap: usize) -> Self {
        // LruCache requires NonZeroUsize; clamp 0 to 1
        let cap_nz = NonZeroUsize::new(cap.max(1)).unwrap();
        Self {
            lru: LruCache::new(cap_nz),
        }
    }

    pub fn get_or_compile(&mut self, pat: &str) -> Result<&Regex, RegexpExtractError> {
        if self.lru.contains(pat) {
            return Ok(self.lru.get(pat).unwrap());
        }
        let re = Regex::new(pat)?;
        self.lru.put(pat.to_string(), re);
        Ok(self.lru.get(pat).expect("entry just inserted"))
    }
}
