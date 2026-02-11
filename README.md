# DuckDB read_lines Extension

A DuckDB extension for reading line-based text files with line numbers and efficient subset extraction.

## Functions

### `read_text_lines(file_path, ...)`

Read lines from file(s) with glob pattern support.

```sql
-- Read all lines from a file
SELECT * FROM read_text_lines('server.log');

-- Read specific lines
SELECT * FROM read_text_lines('server.log', lines := '100-200');

-- Read lines with context
SELECT * FROM read_text_lines('error.log', lines := 42, context := 3);

-- Glob pattern
SELECT * FROM read_text_lines('logs/*.log', lines := '1-10');
```

### `read_text_lines_lateral(file_path)`

Lateral join variant for reading lines from file paths stored in a table column.

```sql
-- Read lines from files listed in a table
SELECT f.name, l.line_number, l.content
FROM files f,
     read_text_lines_lateral(f.path) AS l;

-- Read from multiple file paths using VALUES
SELECT v.path, l.line_number, l.content
FROM (VALUES ('a.txt'), ('b.txt')) AS v(path),
     read_text_lines_lateral(v.path) AS l;
```

**Note:** Named parameters (lines, context, etc.) are not supported in the lateral version due to DuckDB limitations. Use the regular `read_text_lines` function for line selection features.

### `parse_text_lines(text, ...)`

Parse lines from a string.

```sql
-- Parse inline text
SELECT * FROM parse_text_lines('line1
line2
line3');

-- With line selection
SELECT * FROM parse_text_lines(my_column, lines := [1, 5, 10]);
```

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `line_number` | BIGINT | 1-indexed line number |
| `content` | VARCHAR | Line content (preserves original line endings) |
| `byte_offset` | BIGINT | Byte position of line start in file/string |
| `file_path` | VARCHAR | Source file path (read_text_lines only) |

## Parameters

### Line Selection (`lines`)

Flexible line selection supporting multiple formats:

```sql
-- Single line number
lines := 42

-- Range string (inclusive)
lines := '100-200'

-- List of line numbers
lines := [1, 5, 10]

-- List of ranges (use strings for ranges)
lines := ['1-10', '50-60', '100']
```

### Context Parameters

```sql
-- Lines before each match
before := 3

-- Lines after each match
after := 3

-- Shorthand for before and after
context := 3
```

### File Parameters (read_text_lines only)

```sql
-- Skip files that can't be read
ignore_errors := true
```

## Building

```bash
make
```

## Testing

```bash
make test
```

## Example Use Cases

### Extract specific lines from a log file

```sql
SELECT line_number, content
FROM read_text_lines('app.log', lines := '100-150');
```

### Get error lines with context

```sql
-- First find error line numbers, then extract with context
WITH error_lines AS (
    SELECT line_number
    FROM read_text_lines('app.log')
    WHERE content LIKE '%ERROR%'
)
SELECT l.line_number, l.content
FROM read_text_lines('app.log',
    lines := (SELECT list(line_number) FROM error_lines),
    context := 2
) l;
```

### Process multiple log files

```sql
SELECT file_path, line_number, content
FROM read_text_lines('logs/*.log')
WHERE content LIKE '%Exception%';
```

### Parse multi-line string column

```sql
SELECT id, lines.line_number, lines.content
FROM my_table,
     parse_text_lines(my_table.text_column) AS lines;
```

## Design Decisions

- **Line numbering**: 1-indexed (matches editors, grep, error messages)
- **Range syntax**: Inclusive on both ends (`'100-200'` = lines 100 through 200)
- **Line endings**: Preserved as-is (`\n`, `\r\n`, `\r`)
- **Empty lines**: Included (no filtering)
- **Encoding**: UTF-8

## License

MIT
