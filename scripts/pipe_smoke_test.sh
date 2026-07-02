#!/usr/bin/env bash
# Smoke-test the non-seekable (pipe/stream) read paths of read_lines against a
# built extension binary, using shellfs pipes as the non-seekable source.
#
# The sqllogictest suite cannot cover these paths in CI: its runner skips
# test/sql/read_lines_shellfs.test unless the shellfs extension is loadable
# ("require shellfs" is a silent skip). This script provides that coverage by
# loading the built extension into a stock DuckDB CLI, installing shellfs from
# the community repository, and asserting the pipe behaviors exactly
# (terminators included — no rtrim masking).
#
# Usage: scripts/pipe_smoke_test.sh <duckdb-cli> <read_lines.duckdb_extension>
# Local example:
#   scripts/pipe_smoke_test.sh build/release/duckdb \
#       build/release/extension/read_lines/read_lines.duckdb_extension
set -euo pipefail

DUCKDB="${1:?usage: pipe_smoke_test.sh <duckdb-cli> <extension-file>}"
EXTENSION="${2:?usage: pipe_smoke_test.sh <duckdb-cli> <extension-file>}"

run_sql() {
	"$DUCKDB" -unsigned -noheader -list -c "
LOAD '$EXTENSION';
INSTALL shellfs FROM community;
LOAD shellfs;
$1"
}

FAILURES=0
expect() {
	local label="$1" sql="$2" expected="$3" actual
	if ! actual="$(run_sql "$sql" 2>&1)"; then
		echo "FAIL: $label (query errored)"
		echo "$actual" | sed 's/^/    /'
		FAILURES=$((FAILURES + 1))
		return
	fi
	if [[ "$actual" != "$expected" ]]; then
		echo "FAIL: $label"
		printf -- '--- expected ---\n%s\n--- actual ---\n%s\n' "$expected" "$actual"
		FAILURES=$((FAILURES + 1))
		return
	fi
	echo "PASS: $label"
}

expect "basic pipe read" \
	"SELECT line_number || ':' || replace(content, chr(10), '<LF>')
	 FROM read_lines('printf \"line1\nline2\nline3\" |') ORDER BY line_number;" \
	$'1:line1<LF>\n2:line2<LF>\n3:line3'

expect "blank line mid-stream is not EOF" \
	"SELECT count(*) FROM read_lines('printf \"a\n\nc\" |');" \
	"3"

expect "trailing empty line survives" \
	"SELECT count(*) FROM read_lines('printf \"a\n\n\" |');" \
	"2"

expect "terminators preserved exactly" \
	"SELECT line_number || ':' || replace(replace(content, chr(13), '<CR>'), chr(10), '<LF>')
	 FROM read_lines('printf \"a\r\nb\" |') ORDER BY line_number;" \
	$'1:a<CR><LF>\n2:b'

expect "BOM stripped, offsets true to stream" \
	"SELECT line_number || ':' || replace(content, chr(10), '<LF>') || '@' || byte_offset
	 FROM read_lines('printf \"\\357\\273\\277hello\nworld\n\" |') ORDER BY line_number;" \
	$'1:hello<LF>@3\n2:world<LF>@9'

expect "from-end selection on a pipe" \
	"SELECT line_number || ':' || replace(content, chr(10), '<LF>')
	 FROM read_lines('printf \"a\nb\nc\" |', '+2');" \
	"2:b<LF>"

expect "correlated lateral: one pipe per input row" \
	"SELECT v.tag || ':' || l.line_number || ':' || replace(l.content, chr(10), '<LF>')
	 FROM (VALUES ('one', 'printf \"a\nb\" |'), ('two', 'printf \"c\" |')) v(tag, cmd),
	      read_lines_lateral(v.cmd) l
	 ORDER BY v.tag, l.line_number;" \
	$'one:1:a<LF>\none:2:b\ntwo:1:c'

expect "lateral multi-chunk output (5000 lines)" \
	"SELECT count(*) FROM (VALUES ('seq 1 5000 |')) v(cmd), read_lines_lateral(v.cmd) l;" \
	"5000"

expect "lateral over empty input relation" \
	"SELECT count(*) FROM (SELECT 'x' WHERE 1=0) v(cmd), read_lines_lateral(v.cmd) l;" \
	"0"

if [[ "$FAILURES" -gt 0 ]]; then
	echo "$FAILURES pipe smoke test(s) FAILED"
	exit 1
fi
echo "All pipe smoke tests passed"
