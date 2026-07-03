#include "read_lines_extension.hpp"
#include "line_selection.hpp"
#include "duckdb_compat.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

struct ParseTextLinesBindData : public TableFunctionData {
	string text;
	LineSelection line_selection;
	LineTrimMode trim_mode;

	ParseTextLinesBindData(string text, LineSelection selection, LineTrimMode trim_mode)
	    : text(std::move(text)), line_selection(std::move(selection)), trim_mode(trim_mode) {
	}
};

struct ParseTextLinesGlobalState : public GlobalTableFunctionState {
	idx_t position; // Current position in text
	int64_t current_line_number;
	bool finished;
	bool initialized;
	LineSelection resolved_selection;

	ParseTextLinesGlobalState()
	    : position(0), current_line_number(0), finished(false), initialized(false),
	      resolved_selection(LineSelection::All()) {
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> ParseTextLinesBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto text = input.inputs[0].GetValue<string>();

	// Parse named parameters
	LineSelection line_selection = LineSelection::All();
	LineTrimMode trim_mode = LineTrimMode::NONE;
	int64_t before_context = 0;
	int64_t after_context = 0;

	for (auto &param : input.named_parameters) {
		auto &name = param.first;
		auto &value = param.second;

		if (name == "lines") {
			line_selection = LineSelection::Parse(value);
		} else if (name == "trim") {
			trim_mode = ParseLineTrimMode(value);
		} else if (name == "before") {
			before_context = value.GetValue<int64_t>();
		} else if (name == "after") {
			after_context = value.GetValue<int64_t>();
		} else if (name == "context") {
			before_context = value.GetValue<int64_t>();
			after_context = before_context;
		}
	}

	// Apply context to line selection
	if (before_context > 0 || after_context > 0) {
		line_selection.AddContext(before_context, after_context);
	}

	// Define output columns (no file_path for parse_lines)
	return_types.push_back(LogicalType::BIGINT); // line_number
	names.push_back("line_number");

	return_types.push_back(LogicalType::VARCHAR); // content
	names.push_back("content");

	return_types.push_back(LogicalType::BIGINT); // byte_offset
	names.push_back("byte_offset");

	return make_uniq<ParseTextLinesBindData>(std::move(text), std::move(line_selection), trim_mode);
}

static unique_ptr<GlobalTableFunctionState> ParseTextLinesInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ParseTextLinesGlobalState>();
}

// Count total lines in text
// Shared with read_lines.cpp (declared in read_lines_extension.hpp) so that
// buffered non-seekable streams resolve from-end references identically.
int64_t CountLinesInText(const string &text, idx_t start) {
	int64_t count = 0;
	idx_t pos = start;
	while (pos < text.size()) {
		char c = text[pos];
		if (c == '\n') {
			count++;
			pos++;
		} else if (c == '\r') {
			count++;
			pos++;
			if (pos < text.size() && text[pos] == '\n') {
				pos++;
			}
		} else {
			pos++;
		}
	}
	// Count last line if text doesn't end with newline
	if (text.size() > start && text.back() != '\n' && text.back() != '\r') {
		count++;
	}
	return count;
}

// Extract a line from text starting at position, handling \n, \r\n, and \r line endings
// Returns the line content (including line ending) and updates position to after the line
// Shared with read_lines.cpp (declared in read_lines_extension.hpp) so that
// buffered non-seekable streams split lines identically to parse_lines.
string ExtractLine(const string &text, idx_t &position) {
	if (position >= text.size()) {
		return "";
	}

	idx_t start = position;
	idx_t end = position;

	// Find end of line
	while (end < text.size()) {
		char c = text[end];
		if (c == '\n') {
			end++; // Include \n
			break;
		} else if (c == '\r') {
			end++;
			// Check for \r\n
			if (end < text.size() && text[end] == '\n') {
				end++;
			}
			break;
		}
		end++;
	}

	position = end;
	return text.substr(start, end - start);
}

LineTrimMode ParseLineTrimMode(const Value &value) {
	if (value.IsNull()) {
		return LineTrimMode::NONE;
	}
	if (value.type().id() == LogicalTypeId::BOOLEAN) {
		return value.GetValue<bool>() ? LineTrimMode::ENDINGS : LineTrimMode::NONE;
	}
	auto str = StringUtil::Lower(value.GetValue<string>());
	if (str == "none" || str == "false") {
		return LineTrimMode::NONE;
	}
	if (str == "endings" || str == "true") {
		return LineTrimMode::ENDINGS;
	}
	if (str == "left") {
		return LineTrimMode::LEFT;
	}
	if (str == "right") {
		return LineTrimMode::RIGHT;
	}
	if (str == "both") {
		return LineTrimMode::BOTH;
	}
	throw InvalidInputException(
	    "Invalid trim mode \"%s\": expected true/false, NULL, 'endings', 'left', 'right', 'both', or 'none'", str);
}

// Spaces and tabs only; terminator bytes are handled separately so 'left' can
// trim indentation while keeping the line ending.
static bool IsHorizontalWhitespace(char c) {
	return c == ' ' || c == '\t';
}

string ApplyLineTrim(const string &line, LineTrimMode mode) {
	if (mode == LineTrimMode::NONE || line.empty()) {
		return line;
	}
	idx_t begin = 0;
	idx_t end = line.size();
	if (mode != LineTrimMode::LEFT) {
		// Strip the single trailing terminator (\n, \r\n, or \r)
		if (line[end - 1] == '\n') {
			end--;
			if (end > 0 && line[end - 1] == '\r') {
				end--;
			}
		} else if (line[end - 1] == '\r') {
			end--;
		}
	}
	if (mode == LineTrimMode::RIGHT || mode == LineTrimMode::BOTH) {
		while (end > begin && IsHorizontalWhitespace(line[end - 1])) {
			end--;
		}
	}
	if (mode == LineTrimMode::LEFT || mode == LineTrimMode::BOTH) {
		while (begin < end && IsHorizontalWhitespace(line[begin])) {
			begin++;
		}
	}
	return line.substr(begin, end - begin);
}

static void ParseTextLinesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ParseTextLinesBindData>();
	auto &state = data_p.global_state->Cast<ParseTextLinesGlobalState>();

	if (state.finished) {
		CompatSetOutputCardinality(output, 0);
		return;
	}

	const string &text = bind_data.text;

	// Initialize resolved selection on first call
	if (!state.initialized) {
		state.initialized = true;
		if (bind_data.line_selection.HasFromEndReferences()) {
			int64_t total_lines = CountLinesInText(text);
			state.resolved_selection = bind_data.line_selection;
			state.resolved_selection.ResolveFromEnd(total_lines);
		} else {
			state.resolved_selection = bind_data.line_selection;
		}
	}

	idx_t output_row = 0;

	while (output_row < STANDARD_VECTOR_SIZE && state.position < text.size()) {
		auto line_start_offset = static_cast<int64_t>(state.position);
		string line = ExtractLine(text, state.position);

		state.current_line_number++;

		// Check if we should include this line
		if (!state.resolved_selection.ShouldIncludeLine(state.current_line_number)) {
			// Check if we've passed all ranges
			if (state.resolved_selection.PastAllRanges(state.current_line_number)) {
				state.finished = true;
				break;
			}
			continue;
		}

		// Output this line
		output.data[0].SetValue(output_row, Value::BIGINT(state.current_line_number));
		output.data[1].SetValue(output_row, Value(ApplyLineTrim(line, bind_data.trim_mode)));
		output.data[2].SetValue(output_row, Value::BIGINT(line_start_offset));

		output_row++;
	}

	if (state.position >= text.size()) {
		state.finished = true;
	}

	CompatSetOutputCardinality(output, output_row);
}

TableFunction ParseLinesFunction() {
	TableFunction func("parse_lines", {LogicalType::VARCHAR}, ParseTextLinesFunction, ParseTextLinesBind,
	                   ParseTextLinesInit);

	// Named parameters
	func.named_parameters["lines"] = LogicalType::ANY; // Can be int, string, or list
	func.named_parameters["trim"] = LogicalType::ANY;  // BOOLEAN or 'endings'/'left'/'right'/'both'/'none'
	func.named_parameters["before"] = LogicalType::BIGINT;
	func.named_parameters["after"] = LogicalType::BIGINT;
	func.named_parameters["context"] = LogicalType::BIGINT;

	return func;
}

} // namespace duckdb
