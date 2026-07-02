#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

class ReadLinesExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

// =============================================================================
// Shared text line-splitting helpers (defined in parse_lines.cpp)
//
// These implement the canonical line-splitting semantics used across the
// extension: a line includes its terminator (\n, \r\n, or \r), a final line
// without a trailing terminator is still counted, and byte offsets are
// positions within the buffer. read_lines / read_lines_lateral reuse these
// when a source is non-seekable and the whole stream has been buffered, so
// that buffered streams behave identically to parse_lines on the same bytes.
// =============================================================================

// Count total lines in text (used to resolve from-end line references).
int64_t CountLinesInText(const string &text);

// Extract one line from `text` starting at `position`, including its line
// ending. Advances `position` to the start of the next line and returns the
// line content (with the terminator preserved). Returns "" when at the end.
string ExtractLine(const string &text, idx_t &position);

} // namespace duckdb
