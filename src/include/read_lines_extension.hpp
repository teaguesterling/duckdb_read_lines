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
// positions within the buffer. read_lines / read_lines_lateral split every
// source (file or pipe) through these, so all three functions behave
// identically on the same bytes.
// =============================================================================

// Count total lines in text starting at byte `start` (used to resolve
// from-end line references; `start` lets buffered readers skip a BOM).
int64_t CountLinesInText(const string &text, idx_t start = 0);

// Extract one line from `text` starting at `position`, including its line
// ending. Advances `position` to the start of the next line and returns the
// line content (with the terminator preserved). Returns "" when at the end.
string ExtractLine(const string &text, idx_t &position);

// =============================================================================
// Content trimming (the `trim` argument of read_lines / read_lines_lateral /
// parse_lines). A pure content transform applied after splitting: line
// numbers, byte offsets, line counting, and selection always operate on the
// raw bytes, so `trim` never changes which rows appear or where they start.
// =============================================================================

enum class LineTrimMode : uint8_t {
	NONE,    // NULL / false / 'none': preserve exactly (default)
	ENDINGS, // true / 'endings': strip the line terminator only (chomp)
	LEFT,    // 'left': strip leading spaces/tabs; terminator kept
	RIGHT,   // 'right': strip the terminator and trailing spaces/tabs
	BOTH     // 'both': LEFT + RIGHT
};

// Parse a trim argument (BOOLEAN, NULL, or one of the strings above);
// throws InvalidInputException on anything else.
LineTrimMode ParseLineTrimMode(const Value &value);

// Apply a trim mode to one split line (whose content includes its terminator).
string ApplyLineTrim(const string &line, LineTrimMode mode);

} // namespace duckdb
