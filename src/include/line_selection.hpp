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

// Where a path-embedded line spec came from.
enum class LineSpecSource : uint8_t {
	NONE,    // the path carries no line spec
	COLON,   // legacy suffix: "file.py:10-20"
	FRAGMENT // URI fragment:  "file.py#L10-L20"
};

struct ParsedPathSpec {
	string path;             // the locator, with the line spec removed
	LineSelection selection; // LineSelection::All() when source == NONE
	LineSpecSource source;
};

// Split a path into the locator a filesystem should see and the line selection
// it carries. Recognises, in this order:
//   1. the URI fragment        "path#L10-L20" / "path#L12-24"
//   2. the legacy colon suffix "path:10-20"
// Returns source == NONE (and the path unchanged) when none of them applies.
// Throws if the path carries more than one spec.
//
// NOTE: only the syntax is parsed here; the caller is responsible for checking
// that the literal path does not resolve *before* asking for an interpretation.
ParsedPathSpec ParsePathLineSpec(const string &path);

// Human-readable name of a spec form, for error messages.
const char *LineSpecSourceName(LineSpecSource source);

} // namespace duckdb
