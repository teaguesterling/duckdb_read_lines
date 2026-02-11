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

	// Parse a single range string like "100-200"
	static LineRange ParseRangeString(const string &str);
};

} // namespace duckdb
