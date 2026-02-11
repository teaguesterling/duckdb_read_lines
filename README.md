# DuckDB read_lines Extension

A DuckDB extension for reading line-based text files with line numbers and efficient subset extraction.

## Quick Start

```sql
-- Read all lines
SELECT * FROM read_lines('app.log');

-- Read specific lines (error message style)
SELECT * FROM read_lines('src/file.py:42 +/-5');

-- Read a range with context
SELECT * FROM read_lines('app.log', lines := '100-200', context := 3);
```

## Functions

| Function | Description |
|----------|-------------|
| `read_lines(path, ...)` | Read lines from file(s), supports glob patterns |
| `read_lines_lateral(path)` | Lateral join variant for per-row file paths |
| `parse_lines(text, ...)` | Parse lines from a string value |

### Output Columns

| Column | Type | Description |
|--------|------|-------------|
| `line_number` | BIGINT | 1-indexed line number |
| `content` | VARCHAR | Line content (preserves line endings) |
| `byte_offset` | BIGINT | Byte position of line start |
| `file_path` | VARCHAR | Source file path (file functions only) |

## Line Selection

Lines can be selected using the `lines` parameter or embedded in the file path.

### Line Spec Syntax

A line spec is a mini-language for selecting lines:

| Syntax | Meaning | Example |
|--------|---------|---------|
| `N` | Single line | `42` |
| `N-M` | Range (inclusive) | `10-20` |
| `N...M` | Range (alternative) | `10...20` |
| `-N` or `...N` | First N lines (head) | `-100` |
| `N-` or `N...` | From line N to end (tail) | `100-` |
| `spec +/-C` | With C lines context | `42 +/-3` |
| `spec -B +A` | With B before, A after | `42 -2 +5` |

### Path-Embedded Selection

Line specs can be embedded in the file path after a colon:

```sql
read_lines('file.py:42')           -- line 42
read_lines('file.py:10-20')        -- lines 10-20
read_lines('file.py:42 +/-3')      -- line 42 with 3 lines context
read_lines('file.py:-50')          -- first 50 lines
read_lines('file.py:100-')         -- from line 100 to end
```

If a file literally named `file.py:42` exists, it takes precedence.

### Lines Parameter

The `lines` parameter accepts integers, strings, or structs:

```sql
-- Integer: single line or list
lines := 42
lines := [1, 5, 10, 20]

-- String: line spec syntax
lines := '100-200'
lines := '42 +/-3'
lines := ['-10', '100-']        -- first 10 and from 100 to end
```

### Struct Format

Structs provide named fields for complex selections:

| Field | Type | Description |
|-------|------|-------------|
| `start` | INT | Range start (with `stop`) |
| `stop` | INT | Range end (with `start`) |
| `line` | INT | Single line number |
| `lines` | INT[] | Multiple line numbers |
| `before` | INT | Lines of context before |
| `after` | INT | Lines of context after |
| `context` | INT | Symmetric context (before and after) |
| `inclusive` | BOOL | Include stop line (default: true) |

```sql
-- Range
lines := {start: 100, stop: 200}

-- Single line with context
lines := {line: 42, context: 3}

-- Multiple lines with context
lines := {lines: [10, 20, 30], before: 2, after: 5}

-- Head/tail
lines := {stop: 100}              -- first 100 lines
lines := {start: 100}             -- from line 100 to end

-- Exclusive stop (like Python range)
lines := {start: 1, stop: 11, inclusive: false}   -- lines 1-10
```

DuckDB unifies struct types, so you can mix forms in a list:

```sql
lines := [{line: 5}, {start: 10, stop: 20}, {lines: [30, 40]}]
```

## Global Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `lines` | ANY | Line selection (see above) |
| `before` | BIGINT | Context lines before each selection |
| `after` | BIGINT | Context lines after each selection |
| `context` | BIGINT | Symmetric context (sets both before and after) |
| `ignore_errors` | BOOL | Skip unreadable files in glob patterns |

## Examples

### View error location from stack trace

```sql
SELECT line_number, content
FROM read_lines('src/module.py:142 +/-5');
```

### Extract log section

```sql
SELECT line_number, content
FROM read_lines('app.log', lines := '1000-1100');
```

### Head and tail

```sql
-- First 20 lines
SELECT * FROM read_lines('data.csv', lines := '-20');

-- Last section (from line 500 onward)
SELECT * FROM read_lines('data.csv', lines := '500-');
```

### Find errors with context

```sql
WITH error_lines AS (
    SELECT line_number
    FROM read_lines('app.log')
    WHERE content LIKE '%ERROR%'
)
SELECT l.line_number, l.content
FROM read_lines('app.log',
    lines := (SELECT list(line_number) FROM error_lines),
    context := 2
) l;
```

### Search across files

```sql
SELECT file_path, line_number, content
FROM read_lines('logs/*.log')
WHERE content LIKE '%Exception%';
```

### Lateral join for per-row files

```sql
SELECT t.id, l.line_number, l.content
FROM my_table t,
     read_lines_lateral(t.file_path) l;
```

## Design Notes

- **Line numbering**: 1-indexed (matches editors, grep, error messages)
- **Range bounds**: Inclusive on both ends
- **Line endings**: Preserved as-is (`\n`, `\r\n`, `\r`)
- **Context clamping**: Context before line 1 or after EOF is clamped
- **Short-circuit**: Scanning stops after passing all selected ranges
- **Encoding**: UTF-8

## License

MIT
