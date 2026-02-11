#include "line_selection.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <algorithm>
#include <limits>

namespace duckdb {

// Forward declarations
static vector<LineRange> ParseLineStruct(const Value &value);
static bool IsLineStruct(const LogicalType &type);
static bool ParseContextSuffix(const string &str, size_t line_spec_end, int64_t &before, int64_t &after);
static bool IsGlobalContextString(const string &str, int64_t &context);
static void ApplyContextToRange(LineRange &range, int64_t before, int64_t after);

LineSelection LineSelection::All() {
	return LineSelection();
}

LineSelection::LineSelection(vector<LineRange> ranges) : match_all_(false), ranges_(MergeRanges(std::move(ranges))) {
}

LineSelection LineSelection::Parse(const Value &value) {
	if (value.IsNull()) {
		return All();
	}

	vector<LineRange> ranges;
	auto &type = value.type();

	if (type.id() == LogicalTypeId::BIGINT || type.id() == LogicalTypeId::INTEGER ||
	    type.id() == LogicalTypeId::SMALLINT || type.id() == LogicalTypeId::TINYINT ||
	    type.id() == LogicalTypeId::UBIGINT || type.id() == LogicalTypeId::UINTEGER ||
	    type.id() == LogicalTypeId::USMALLINT || type.id() == LogicalTypeId::UTINYINT) {
		// Single line number: lines := 42
		auto line = value.GetValue<int64_t>();
		if (line < 1) {
			throw InvalidInputException("Line number must be >= 1, got %lld", line);
		}
		ranges.emplace_back(line, line);
	} else if (type.id() == LogicalTypeId::VARCHAR) {
		// Range string: lines := '100-200' or '42 +/-3'
		ranges.push_back(ParseRangeString(value.GetValue<string>()));
	} else if (type.id() == LogicalTypeId::STRUCT && IsLineStruct(type)) {
		// Struct: lines := {start: 10, stop: 100} or {line: 42} or {lines: [1,2,3]}
		auto struct_ranges = ParseLineStruct(value);
		ranges.insert(ranges.end(), struct_ranges.begin(), struct_ranges.end());
	} else if (type.id() == LogicalTypeId::LIST) {
		// List of numbers, strings, or structs
		auto &list_values = ListValue::GetChildren(value);
		for (auto &item : list_values) {
			auto &item_type = item.type();
			if (item_type.id() == LogicalTypeId::VARCHAR) {
				ranges.push_back(ParseRangeString(item.GetValue<string>()));
			} else if (item_type.id() == LogicalTypeId::STRUCT && IsLineStruct(item_type)) {
				auto struct_ranges = ParseLineStruct(item);
				ranges.insert(ranges.end(), struct_ranges.begin(), struct_ranges.end());
			} else {
				auto line = item.GetValue<int64_t>();
				if (line < 1) {
					throw InvalidInputException("Line number must be >= 1, got %lld", line);
				}
				ranges.emplace_back(line, line);
			}
		}
	} else {
		throw InvalidInputException(
		    "Invalid type for 'lines' parameter: expected integer, string, struct, or list");
	}

	if (ranges.empty()) {
		return All();
	}
	return LineSelection(std::move(ranges));
}

// Check if a type is a line selection struct (has start, stop, line, or lines field)
static bool IsLineStruct(const LogicalType &type) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto &children = StructType::GetChildTypes(type);
	bool has_start = false;
	bool has_stop = false;
	bool has_line = false;
	bool has_lines = false;
	for (auto &child : children) {
		if (child.first == "start") {
			has_start = true;
		} else if (child.first == "stop") {
			has_stop = true;
		} else if (child.first == "line") {
			has_line = true;
		} else if (child.first == "lines") {
			has_lines = true;
		}
	}
	// Valid if has start (tail), stop (head), start+stop (range), line, or lines
	return has_start || has_stop || has_line || has_lines;
}

// Parse a unified line selection struct into one or more LineRanges
// Struct fields: start, stop, line, lines, before, after, context, inclusive
static vector<LineRange> ParseLineStruct(const Value &value) {
	auto &children = StructValue::GetChildren(value);
	auto &struct_type = value.type();
	auto &child_types = StructType::GetChildTypes(struct_type);

	// Field values (use optional-like semantics with sentinel values)
	int64_t start = -1;      // -1 means not specified
	int64_t stop = -1;       // -1 means not specified
	int64_t line = -1;       // -1 means not specified
	vector<int64_t> lines;   // empty means not specified
	bool inclusive = true;   // Default: inclusive (like SQL BETWEEN)
	int64_t before = 0;
	int64_t after = 0;
	int64_t context = -1;    // -1 means not specified

	for (idx_t i = 0; i < child_types.size(); i++) {
		auto &field_name = child_types[i].first;
		auto &field_value = children[i];

		if (field_name == "start") {
			if (!field_value.IsNull()) {
				start = field_value.GetValue<int64_t>();
			}
		} else if (field_name == "stop") {
			if (!field_value.IsNull()) {
				stop = field_value.GetValue<int64_t>();
			}
		} else if (field_name == "line") {
			if (!field_value.IsNull()) {
				line = field_value.GetValue<int64_t>();
			}
		} else if (field_name == "lines") {
			if (!field_value.IsNull()) {
				auto &list_children = ListValue::GetChildren(field_value);
				for (auto &item : list_children) {
					lines.push_back(item.GetValue<int64_t>());
				}
			}
		} else if (field_name == "inclusive") {
			if (!field_value.IsNull()) {
				inclusive = field_value.GetValue<bool>();
			}
		} else if (field_name == "before") {
			if (!field_value.IsNull()) {
				before = field_value.GetValue<int64_t>();
				if (before < 0) {
					throw InvalidInputException("Struct 'before' must be >= 0, got %lld", before);
				}
			}
		} else if (field_name == "after") {
			if (!field_value.IsNull()) {
				after = field_value.GetValue<int64_t>();
				if (after < 0) {
					throw InvalidInputException("Struct 'after' must be >= 0, got %lld", after);
				}
			}
		} else if (field_name == "context") {
			if (!field_value.IsNull()) {
				context = field_value.GetValue<int64_t>();
				if (context < 0) {
					throw InvalidInputException("Struct 'context' must be >= 0, got %lld", context);
				}
			}
		}
	}

	// Context is shorthand for before=after=context
	if (context >= 0) {
		before = context;
		after = context;
	}

	vector<LineRange> result;

	// Determine which form was used
	if (start >= 0 && stop >= 0) {
		// Full range form: {start: N, stop: M}
		if (start < 1) {
			throw InvalidInputException("Line range start must be >= 1, got %lld", start);
		}
		int64_t effective_end = inclusive ? stop : stop - 1;
		if (effective_end < start) {
			throw InvalidInputException("Line range stop must be %s start, got %lld-%lld", inclusive ? ">=" : ">",
			                            start, stop);
		}
		LineRange range(start, effective_end);
		ApplyContextToRange(range, before, after);
		result.push_back(range);
	} else if (start >= 0 && stop < 0) {
		// Tail form: {start: N} without stop → N to EOF
		if (start < 1) {
			throw InvalidInputException("Line range start must be >= 1, got %lld", start);
		}
		LineRange range(start, std::numeric_limits<int64_t>::max());
		ApplyContextToRange(range, before, after);
		result.push_back(range);
	} else if (stop >= 0 && start < 0) {
		// Head form: {stop: N} without start → 1 to N
		int64_t effective_end = inclusive ? stop : stop - 1;
		if (effective_end < 1) {
			throw InvalidInputException("Line range stop must be >= 1, got %lld", stop);
		}
		LineRange range(1, effective_end);
		ApplyContextToRange(range, before, after);
		result.push_back(range);
	} else if (line >= 0) {
		// Single line form: {line: N}
		if (line < 1) {
			throw InvalidInputException("Line number must be >= 1, got %lld", line);
		}
		LineRange range(line, line);
		ApplyContextToRange(range, before, after);
		result.push_back(range);
	} else if (!lines.empty()) {
		// Multiple lines form: {lines: [N, M, ...]}
		for (auto l : lines) {
			if (l < 1) {
				throw InvalidInputException("Line number must be >= 1, got %lld", l);
			}
			LineRange range(l, l);
			ApplyContextToRange(range, before, after);
			result.push_back(range);
		}
	} else {
		throw InvalidInputException(
		    "Line selection struct must have 'start'+'stop', 'line', or 'lines' field");
	}

	return result;
}

// Apply context (before/after) to a range
static void ApplyContextToRange(LineRange &range, int64_t before, int64_t after) {
	range.start = std::max(int64_t(1), range.start - before);
	range.end = range.end + after;
}

// Check if string is a global context specifier like "+/-3" or "-/+3"
static bool IsGlobalContextString(const string &str, int64_t &context) {
	string trimmed = str;
	StringUtil::Trim(trimmed);

	// Check for +/-N or -/+N pattern
	if (trimmed.length() >= 4) {
		if ((trimmed.substr(0, 3) == "+/-" || trimmed.substr(0, 3) == "-/+")) {
			try {
				context = std::stoll(trimmed.substr(3));
				if (context >= 0) {
					return true;
				}
			} catch (...) {
				return false;
			}
		}
	}
	return false;
}

// Parse context suffix from a range string
// Formats: " -B", " +A", " -B+A", " -B +A", " +/-C", " -/+C"
// Returns true if context was found, sets before/after values
static bool ParseContextSuffix(const string &str, size_t line_spec_end, int64_t &before, int64_t &after) {
	before = 0;
	after = 0;

	if (line_spec_end >= str.length()) {
		return false;
	}

	string suffix = str.substr(line_spec_end);
	StringUtil::Trim(suffix);

	if (suffix.empty()) {
		return false;
	}

	// Check for +/-N or -/+N (symmetric context)
	if (suffix.length() >= 4) {
		if (suffix.substr(0, 3) == "+/-" || suffix.substr(0, 3) == "-/+") {
			try {
				int64_t ctx = std::stoll(suffix.substr(3));
				if (ctx < 0) {
					throw InvalidInputException("Context value must be >= 0, got %lld", ctx);
				}
				before = after = ctx;
				return true;
			} catch (const InvalidInputException &) {
				throw;
			} catch (...) {
				throw InvalidInputException("Invalid context specifier: '%s'", suffix);
			}
		}
	}

	// Parse -B and/or +A
	size_t pos = 0;
	bool found_before = false;
	bool found_after = false;

	while (pos < suffix.length()) {
		// Skip whitespace
		while (pos < suffix.length() && std::isspace(suffix[pos])) {
			pos++;
		}
		if (pos >= suffix.length()) {
			break;
		}

		char sign = suffix[pos];
		if (sign == '-' && !found_before) {
			pos++;
			size_t num_start = pos;
			while (pos < suffix.length() && std::isdigit(suffix[pos])) {
				pos++;
			}
			if (pos == num_start) {
				throw InvalidInputException("Invalid context specifier: expected number after '-'");
			}
			before = std::stoll(suffix.substr(num_start, pos - num_start));
			if (before < 0) {
				throw InvalidInputException("Before context must be >= 0, got %lld", before);
			}
			found_before = true;
		} else if (sign == '+' && !found_after) {
			pos++;
			size_t num_start = pos;
			while (pos < suffix.length() && std::isdigit(suffix[pos])) {
				pos++;
			}
			if (pos == num_start) {
				throw InvalidInputException("Invalid context specifier: expected number after '+'");
			}
			after = std::stoll(suffix.substr(num_start, pos - num_start));
			if (after < 0) {
				throw InvalidInputException("After context must be >= 0, got %lld", after);
			}
			found_after = true;
		} else {
			throw InvalidInputException("Invalid context specifier: '%s'", suffix);
		}
	}

	return found_before || found_after;
}

LineRange LineSelection::ParseRangeString(const string &str) {
	string trimmed = str;
	StringUtil::Trim(trimmed);

	// Find where the line spec ends and context begins
	// Context starts with a space followed by - or + (but not part of a range)
	size_t line_spec_end = trimmed.length();

	// Check for ... separator first (takes precedence)
	size_t ellipsis_pos = trimmed.find("...");

	// Find dash position for range separator (but not at start which means head)
	size_t dash_pos = string::npos;
	if (ellipsis_pos == string::npos) {
		for (size_t i = 0; i < trimmed.length(); i++) {
			if (trimmed[i] == '-') {
				// Dash at position 0 means head (-100)
				// Dash after a digit followed by digit or end means range
				if (i == 0) {
					// Check if followed by digits (head form: -100)
					if (i + 1 < trimmed.length() && std::isdigit(trimmed[i + 1])) {
						dash_pos = i;
						break;
					}
				} else if (std::isdigit(trimmed[i - 1])) {
					// After a digit - could be range end or tail form
					if (i + 1 < trimmed.length() && std::isdigit(trimmed[i + 1])) {
						// Followed by digit: range (100-200)
						dash_pos = i;
						break;
					} else if (i + 1 >= trimmed.length() || trimmed[i + 1] == ' ') {
						// End of string or space: tail form (100-)
						dash_pos = i;
						break;
					}
				}
			}
		}
	}

	// Determine separator position (ellipsis or dash)
	size_t sep_pos = string::npos;
	size_t sep_len = 1;
	if (ellipsis_pos != string::npos) {
		sep_pos = ellipsis_pos;
		sep_len = 3;
	} else if (dash_pos != string::npos) {
		sep_pos = dash_pos;
		sep_len = 1;
	}

	// Find where context suffix starts (space followed by - or +)
	size_t search_start = (sep_pos != string::npos) ? sep_pos + sep_len : 0;
	for (size_t i = search_start; i < trimmed.length(); i++) {
		if (trimmed[i] == ' ') {
			// Look ahead for - or +
			size_t j = i + 1;
			while (j < trimmed.length() && trimmed[j] == ' ') {
				j++;
			}
			if (j < trimmed.length() && (trimmed[j] == '-' || trimmed[j] == '+')) {
				line_spec_end = i;
				break;
			}
		}
	}

	string line_spec = trimmed.substr(0, line_spec_end);
	StringUtil::Trim(line_spec);

	// Parse context suffix if present
	int64_t before = 0, after = 0;
	ParseContextSuffix(trimmed, line_spec_end, before, after);

	// Parse the line spec (single number, range, or half-bounded range)
	LineRange range(0, 0);

	if (sep_pos != string::npos && sep_pos < line_spec_end) {
		// Range or half-bounded range
		string start_str, end_str;
		if (ellipsis_pos != string::npos) {
			start_str = line_spec.substr(0, ellipsis_pos);
			end_str = line_spec.substr(ellipsis_pos + 3);
		} else {
			start_str = line_spec.substr(0, dash_pos);
			end_str = line_spec.substr(dash_pos + 1);
		}
		StringUtil::Trim(start_str);
		StringUtil::Trim(end_str);

		int64_t start, end;

		if (start_str.empty() && !end_str.empty()) {
			// Head form: -100 or ...100
			start = 1;
			try {
				end = std::stoll(end_str);
			} catch (...) {
				throw InvalidInputException("Invalid line range: '%s'", str);
			}
		} else if (!start_str.empty() && end_str.empty()) {
			// Tail form: 100- or 100...
			try {
				start = std::stoll(start_str);
			} catch (...) {
				throw InvalidInputException("Invalid line range: '%s'", str);
			}
			end = std::numeric_limits<int64_t>::max();
		} else if (!start_str.empty() && !end_str.empty()) {
			// Full range: 100-200 or 100...200
			try {
				start = std::stoll(start_str);
				end = std::stoll(end_str);
			} catch (...) {
				throw InvalidInputException("Invalid line range: '%s'", str);
			}
		} else {
			throw InvalidInputException("Invalid line range: '%s'", str);
		}

		if (start < 1) {
			throw InvalidInputException("Line range start must be >= 1, got %lld", start);
		}
		if (end < start) {
			throw InvalidInputException("Line range end must be >= start, got %lld-%lld", start, end);
		}

		range = LineRange(start, end);
	} else {
		// Single number
		int64_t line;
		try {
			line = std::stoll(line_spec);
		} catch (...) {
			throw InvalidInputException("Invalid line number: '%s'", str);
		}
		if (line < 1) {
			throw InvalidInputException("Line number must be >= 1, got %lld", line);
		}
		range = LineRange(line, line);
	}

	// Apply per-entry context
	ApplyContextToRange(range, before, after);

	return range;
}

vector<LineRange> LineSelection::MergeRanges(vector<LineRange> ranges) {
	if (ranges.empty()) {
		return ranges;
	}

	// Sort by start position
	std::sort(ranges.begin(), ranges.end(), [](const LineRange &a, const LineRange &b) { return a.start < b.start; });

	vector<LineRange> merged;
	merged.push_back(ranges[0]);

	for (size_t i = 1; i < ranges.size(); i++) {
		auto &last = merged.back();
		auto &current = ranges[i];

		// Check if ranges overlap or are adjacent
		if (current.start <= last.end + 1) {
			// Merge by extending end if needed
			last.end = std::max(last.end, current.end);
		} else {
			merged.push_back(current);
		}
	}

	return merged;
}

bool LineSelection::ShouldIncludeLine(int64_t line_number) const {
	if (match_all_) {
		return true;
	}

	for (const auto &range : ranges_) {
		if (range.Contains(line_number)) {
			return true;
		}
		// Early exit: ranges are sorted, if we're past this range's end
		// and haven't matched yet, keep looking
		if (line_number < range.start) {
			return false; // Line is before this range and all subsequent ranges
		}
	}
	return false;
}

bool LineSelection::PastAllRanges(int64_t line_number) const {
	if (match_all_) {
		return false;
	}
	if (ranges_.empty()) {
		return true;
	}
	return line_number > ranges_.back().end;
}

int64_t LineSelection::MinLine() const {
	if (match_all_ || ranges_.empty()) {
		return 1;
	}
	return ranges_.front().start;
}

int64_t LineSelection::MaxLine() const {
	if (match_all_ || ranges_.empty()) {
		return std::numeric_limits<int64_t>::max();
	}
	return ranges_.back().end;
}

void LineSelection::AddContext(int64_t before, int64_t after) {
	if (match_all_) {
		return;
	}

	for (auto &range : ranges_) {
		range.start = std::max(int64_t(1), range.start - before);
		range.end = range.end + after;
	}

	// Re-merge in case context caused overlaps
	ranges_ = MergeRanges(std::move(ranges_));
}

std::pair<string, LineSelection> LineSelection::ParsePathWithLineSpec(const string &path) {
	// Look for a colon followed by a line spec
	// Format: path:line_spec where line_spec can be:
	//   - Single number: file.py:42
	//   - Range: file.py:10-20 or file.py:10...20
	//   - Half-bounded: file.py:10- or file.py:-20
	//   - With context: file.py:42 +/-3
	//
	// NOTE: Actual file existence check is done at bind time.
	// This function only parses the syntax - the caller should verify
	// the extracted path exists before using the line selection.

	// Find the last colon that could start a line spec
	// A line spec starts with: digit, '-' followed by digit, or '...'
	size_t colon_pos = string::npos;

	for (size_t i = path.length(); i > 0; i--) {
		if (path[i - 1] == ':') {
			// Check what follows the colon
			if (i < path.length()) {
				char next = path[i];
				// Valid line spec starts: digit, '-', or '.'
				if (std::isdigit(next) || next == '-' || next == '.') {
					// Additional check: if '-', must be followed by digit (head form)
					// or if '.', must be followed by '..' (ellipsis)
					if (next == '-') {
						if (i + 1 < path.length() && std::isdigit(path[i + 1])) {
							colon_pos = i - 1;
							break;
						}
					} else if (next == '.') {
						if (i + 2 < path.length() && path[i + 1] == '.' && path[i + 2] == '.') {
							colon_pos = i - 1;
							break;
						}
					} else {
						// Digit - valid start
						colon_pos = i - 1;
						break;
					}
				}
			}
		}
	}

	if (colon_pos == string::npos) {
		// No line spec found
		return {path, LineSelection::All()};
	}

	// Windows drive letter check: if colon is at position 1 and preceded by a letter,
	// it's likely a drive letter (e.g., C:\path), not a line spec
	if (colon_pos == 1 && std::isalpha(path[0])) {
		// Check if this looks like a Windows path
		if (colon_pos + 1 < path.length() && (path[colon_pos + 1] == '\\' || path[colon_pos + 1] == '/')) {
			return {path, LineSelection::All()};
		}
	}

	string file_path = path.substr(0, colon_pos);
	string line_spec = path.substr(colon_pos + 1);

	// Try to parse the line spec
	try {
		LineRange range = ParseRangeString(line_spec);
		vector<LineRange> ranges;
		ranges.push_back(range);
		return {file_path, LineSelection(std::move(ranges))};
	} catch (...) {
		// If parsing fails, treat the whole thing as a path
		return {path, LineSelection::All()};
	}
}

} // namespace duckdb
