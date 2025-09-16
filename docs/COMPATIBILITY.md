# Compatibility: Spark (Java regex) vs Rust `regex`

`regexp_extract` matches Spark-style grouping/return semantics, but **regex engines differ**.

## Engine differences
- **Look-around**: Java supports `(?=...)`, `(?!...)`, `(?<=...)`, `(?<!...)`.  
  Rust `regex` **does not support look-around**.
  - Example that fails here: `(?<=a)b`
- **Backreferences**: Java supports `\1`, `\2`, …  
  Rust `regex` **does not support backreferences**.
  - Example that fails here: `(a)\1`
- **Inline flags**: Both support flags like `(?i)` (case-insensitive), `(?m)` (multi-line), `(?s)` (dot matches newline).
- **Unicode**: Both engines are Unicode-aware, but class names and edge behavior may differ.

### Feature flag: `fancy-regex`
Enabling the **`fancy-regex`** feature narrows gaps with Java:
- Adds **look-around** and **backreferences** support.
- Uses a backtracking engine; patterns with heavy alternation or nested groups may be slower and can backtrack deeply.
- Error handling/semantics follow this crate’s modes (see **Invalid pattern** in `SEMANTICS.md`).

### Quick reference


| Feature                       | Java regex | Rust `regex` | `fancy-regex` (feature) |
|------------------------------|-----------:|-------------:|-------------------------:|
| Look-ahead/behind            | ✅         | ❌           | ✅                       |
| Backreferences (`\1`)        | ✅         | ❌           | ✅                       |
| Inline flags `(?i)(?m)(?s)`  | ✅         | ✅           | ✅                       |
| Named groups                 | ✅ `(?<n>)`| ✅ `(?P<n>)` | ✅ (`(?P<n>)`)           |
| Atomic/possessive groups     | ⚠️ partial | ❌           | ⚠️ partial               |

> Symbols: ✅ supported · ❌ not supported · ⚠️ nuanced/partial

## Practical guidance
- Prefer patterns that avoid look-around/backrefs for portability and speed; rewrite with alternation or explicit captures when feasible.
- For fixed-prefix look-behind, capture the prefix and select the desired group via `idx`.
- Validate patterns during development; invalid patterns produce clear errors (or `""` if configured) and differences versus Java should be documented in queries.
