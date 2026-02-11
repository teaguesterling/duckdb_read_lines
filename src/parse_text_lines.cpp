#include "read_lines_extension.hpp"
#include "line_selection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

struct ParseTextLinesBindData : public TableFunctionData {
    string text;
    LineSelection line_selection;

    ParseTextLinesBindData(string text, LineSelection selection)
        : text(std::move(text)), line_selection(std::move(selection)) {
    }
};

struct ParseTextLinesGlobalState : public GlobalTableFunctionState {
    idx_t position;           // Current position in text
    int64_t current_line_number;
    bool finished;

    ParseTextLinesGlobalState() : position(0), current_line_number(0), finished(false) {
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
    int64_t before_context = 0;
    int64_t after_context = 0;

    for (auto &param : input.named_parameters) {
        auto &name = param.first;
        auto &value = param.second;

        if (name == "lines") {
            line_selection = LineSelection::Parse(value);
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

    // Define output columns (no file_path for parse_text_lines)
    return_types.push_back(LogicalType::BIGINT);   // line_number
    names.push_back("line_number");

    return_types.push_back(LogicalType::VARCHAR);  // content
    names.push_back("content");

    return_types.push_back(LogicalType::BIGINT);   // byte_offset
    names.push_back("byte_offset");

    return make_uniq<ParseTextLinesBindData>(std::move(text), std::move(line_selection));
}

static unique_ptr<GlobalTableFunctionState> ParseTextLinesInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<ParseTextLinesGlobalState>();
}

// Extract a line from text starting at position, handling \n, \r\n, and \r line endings
// Returns the line content (including line ending) and updates position to after the line
static string ExtractLine(const string &text, idx_t &position) {
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

static void ParseTextLinesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<ParseTextLinesBindData>();
    auto &state = data_p.global_state->Cast<ParseTextLinesGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    idx_t output_row = 0;
    const string &text = bind_data.text;

    while (output_row < STANDARD_VECTOR_SIZE && state.position < text.size()) {
        auto line_start_offset = static_cast<int64_t>(state.position);
        string line = ExtractLine(text, state.position);

        state.current_line_number++;

        // Check if we should include this line
        if (!bind_data.line_selection.ShouldIncludeLine(state.current_line_number)) {
            // Check if we've passed all ranges
            if (bind_data.line_selection.PastAllRanges(state.current_line_number)) {
                state.finished = true;
                break;
            }
            continue;
        }

        // Output this line
        output.data[0].SetValue(output_row, Value::BIGINT(state.current_line_number));
        output.data[1].SetValue(output_row, Value(line));
        output.data[2].SetValue(output_row, Value::BIGINT(line_start_offset));

        output_row++;
    }

    if (state.position >= text.size()) {
        state.finished = true;
    }

    output.SetCardinality(output_row);
}

TableFunction ParseTextLinesFunction() {
    TableFunction func("parse_text_lines", {LogicalType::VARCHAR}, ParseTextLinesFunction, ParseTextLinesBind,
                       ParseTextLinesInit);

    // Named parameters
    func.named_parameters["lines"] = LogicalType::ANY;  // Can be int, string, or list
    func.named_parameters["before"] = LogicalType::BIGINT;
    func.named_parameters["after"] = LogicalType::BIGINT;
    func.named_parameters["context"] = LogicalType::BIGINT;

    return func;
}

} // namespace duckdb
