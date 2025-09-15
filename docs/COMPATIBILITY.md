# Compatibility: Spark (Java regex) vs Rust `regex`

`regexp_extract` matches Spark semantics for grouping and return values, but the **regex engines differ**.

## Engine differences
- **Look-around**: Java supports `(?=...)`, `(?!...)`, `(?<=...)`, `(?<!...)`.  
  Rust `regex` **does not support any look-around**.
    - Example that fails here: `(?<=a)b`
- **Backreferences**: Java supports backreferences like `\1`.  
  Rust `regex` **does not support backreferences**.
    - Example that fails here: `(a)\1`
- **Inline flags**: Both support inline flags such as `(?i)` (case-insensitive).
- **Unicode**: Both support Unicode categories and boundaries, but class names and details may differ.

## Practical guidance
- Prefer patterns that avoid look-around and backreferences; rewrite with alternation, grouping, or simple captures.
- To approximate a positive look-behind for fixed-width prefixes, capture the prefix and select the needed group via `idx`.
- Validate patterns at development time; invalid patterns surface as execution errors with engine diagnostics.
