#pragma once

#include "duckdb.hpp"
#include <vector>
#include <string>

namespace duckdb {

// Represents a range of lines (inclusive on both ends)
struct LineRange {
	int64_t start;
	int64_t end;

	LineRange(int64_t start, int64_t end) : start(start), end(end) {
	}

	bool Contains(int64_t line) const {
		return line >= start && line <= end;
	}
};

// Parsed line selection - a list of sorted, merged ranges
class LineSelection {
public:
	// Create selection that matches all lines
	static LineSelection All();

	// Parse from a Value (can be integer, string, or list)
	static LineSelection Parse(const Value &value);

	// Check if a specific line number should be included
	bool ShouldIncludeLine(int64_t line_number) const;

	// Check if we've passed all ranges (can stop scanning)
	bool PastAllRanges(int64_t line_number) const;

	// Check if this selection matches all lines
	bool IsAll() const {
		return match_all_;
	}

	// Check if selection has any "from end" references (negative line numbers)
	bool HasFromEndReferences() const;

	// Resolve "from end" references given the total line count
	// Converts negative line numbers to positive (e.g., -10 with 100 lines -> 91)
	void ResolveFromEnd(int64_t total_lines);

	// Get the minimum and maximum line numbers in selection
	int64_t MinLine() const;
	int64_t MaxLine() const;

	// Expand ranges to include context lines
	void AddContext(int64_t before, int64_t after);

private:
	LineSelection() : match_all_(true) {
	}
	explicit LineSelection(vector<LineRange> ranges);

	bool match_all_;
	vector<LineRange> ranges_;

	// Merge overlapping ranges and sort them
	static vector<LineRange> MergeRanges(vector<LineRange> ranges);

	// Parse a single range string like "100-200" or "13 -2 +3" (with per-entry context)
	static LineRange ParseRangeString(const string &str);

public:
	// Parse a path that may contain an embedded line spec (e.g., "file.py:13-14")
	// Returns the actual file path and a LineSelection
	// If no line spec is found, returns the original path and LineSelection::All()
	static std::pair<string, LineSelection> ParsePathWithLineSpec(const string &path);
};

} // namespace duckdb
