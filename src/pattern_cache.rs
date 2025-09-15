use regex::Regex;
use std::collections::HashMap;

pub struct PatternCache {
    map: HashMap<String, Regex>,
    cap: usize,
}

impl PatternCache {
    pub fn new(cap: usize) -> Self {
        Self {
            map: HashMap::new(),
            cap,
        }
    }

    pub fn get_or_compile(&mut self, pat: &str) -> Result<&Regex, regex::Error> {
        if self.map.len() >= self.cap && !self.map.contains_key(pat) {
            self.map.clear();
        }
        if !self.map.contains_key(pat) {
            self.map.insert(pat.to_string(), Regex::new(pat)?);
        }
        Ok(self.map.get(pat).unwrap())
    }
}
