#include "line_selection.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <algorithm>
#include <limits>

namespace duckdb {

// Forward declarations
static LineRange ParseRangeStruct(const Value &value);
static bool IsRangeStruct(const LogicalType &type);

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
		// Range string: lines := '100-200'
		ranges.push_back(ParseRangeString(value.GetValue<string>()));
	} else if (type.id() == LogicalTypeId::STRUCT && IsRangeStruct(type)) {
		// Struct range: lines := {start: 10, stop: 100}
		ranges.push_back(ParseRangeStruct(value));
	} else if (type.id() == LogicalTypeId::LIST) {
		// List of numbers/ranges/structs: lines := [1, 5, '10-20', {start: 30, stop: 40}]
		auto &list_values = ListValue::GetChildren(value);
		for (auto &item : list_values) {
			auto &item_type = item.type();
			if (item_type.id() == LogicalTypeId::VARCHAR) {
				ranges.push_back(ParseRangeString(item.GetValue<string>()));
			} else if (item_type.id() == LogicalTypeId::STRUCT && IsRangeStruct(item_type)) {
				ranges.push_back(ParseRangeStruct(item));
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
		    "Invalid type for 'lines' parameter: expected integer, string, struct with start/stop, or list");
	}

	if (ranges.empty()) {
		return All();
	}
	return LineSelection(std::move(ranges));
}

// Check if a type is a struct with 'start' and 'stop' fields
static bool IsRangeStruct(const LogicalType &type) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto &children = StructType::GetChildTypes(type);
	bool has_start = false;
	bool has_stop = false;
	for (auto &child : children) {
		if (child.first == "start") {
			has_start = true;
		} else if (child.first == "stop") {
			has_stop = true;
		}
	}
	return has_start && has_stop;
}

// Parse a struct value with 'start', 'stop', and optional 'inclusive' fields into a LineRange
static LineRange ParseRangeStruct(const Value &value) {
	auto &children = StructValue::GetChildren(value);
	auto &struct_type = value.type();
	auto &child_types = StructType::GetChildTypes(struct_type);

	int64_t start = -1;
	int64_t stop = -1;
	bool inclusive = true; // Default: inclusive (like SQL BETWEEN)

	for (idx_t i = 0; i < child_types.size(); i++) {
		auto &field_name = child_types[i].first;
		auto &field_value = children[i];

		if (field_name == "start") {
			if (field_value.IsNull()) {
				throw InvalidInputException("Range struct 'start' field cannot be NULL");
			}
			start = field_value.GetValue<int64_t>();
		} else if (field_name == "stop") {
			if (field_value.IsNull()) {
				throw InvalidInputException("Range struct 'stop' field cannot be NULL");
			}
			stop = field_value.GetValue<int64_t>();
		} else if (field_name == "inclusive") {
			if (!field_value.IsNull()) {
				inclusive = field_value.GetValue<bool>();
			}
		}
	}

	if (start < 1) {
		throw InvalidInputException("Line range start must be >= 1, got %lld", start);
	}

	// Calculate effective end based on inclusive flag
	int64_t effective_end = inclusive ? stop : stop - 1;

	if (effective_end < start) {
		throw InvalidInputException("Line range stop must be %s start, got %lld-%lld", inclusive ? ">=" : ">", start,
		                            stop);
	}

	return LineRange(start, effective_end);
}

LineRange LineSelection::ParseRangeString(const string &str) {
	string trimmed = str;
	StringUtil::Trim(trimmed);
	auto dash_pos = trimmed.find('-');

	if (dash_pos == string::npos) {
		// Single number as string
		int64_t line;
		try {
			line = std::stoll(trimmed);
		} catch (...) {
			throw InvalidInputException("Invalid line number: '%s'", str);
		}
		if (line < 1) {
			throw InvalidInputException("Line number must be >= 1, got %lld", line);
		}
		return LineRange(line, line);
	}

	// Parse range like "100-200"
	string start_str = trimmed.substr(0, dash_pos);
	string end_str = trimmed.substr(dash_pos + 1);
	StringUtil::Trim(start_str);
	StringUtil::Trim(end_str);

	int64_t start, end;
	try {
		start = std::stoll(start_str);
		end = std::stoll(end_str);
	} catch (...) {
		throw InvalidInputException("Invalid line range: '%s'", str);
	}

	if (start < 1) {
		throw InvalidInputException("Line range start must be >= 1, got %lld", start);
	}
	if (end < start) {
		throw InvalidInputException("Line range end must be >= start, got %lld-%lld", start, end);
	}

	return LineRange(start, end);
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

} // namespace duckdb
