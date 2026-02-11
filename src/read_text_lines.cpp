#include "read_lines_extension.hpp"
#include "line_selection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

struct ReadTextLinesBindData : public TableFunctionData {
    vector<OpenFileInfo> files;
    LineSelection line_selection;
    bool ignore_errors;

    ReadTextLinesBindData(vector<OpenFileInfo> files, LineSelection selection, bool ignore_errors)
        : files(std::move(files)), line_selection(std::move(selection)), ignore_errors(ignore_errors) {
    }
};

struct ReadTextLinesGlobalState : public GlobalTableFunctionState {
    idx_t file_index;
    unique_ptr<FileHandle> current_file;
    int64_t current_line_number;
    int64_t current_byte_offset;
    string current_file_path;
    bool file_finished;
    FileSystem *fs;

    ReadTextLinesGlobalState() : file_index(0), current_line_number(0), current_byte_offset(0), file_finished(true), fs(nullptr) {
    }

    idx_t MaxThreads() const override {
        return 1; // Single-threaded for now
    }
};

static unique_ptr<FunctionData> ReadTextLinesBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    // Parse file path/glob pattern
    auto &fs = FileSystem::GetFileSystem(context);
    auto glob_pattern = input.inputs[0].GetValue<string>();

    // Expand glob pattern
    auto files = fs.GlobFiles(glob_pattern, context, FileGlobOptions::ALLOW_EMPTY);

    // Parse named parameters
    LineSelection line_selection = LineSelection::All();
    int64_t before_context = 0;
    int64_t after_context = 0;
    bool ignore_errors = false;

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
        } else if (name == "ignore_errors") {
            ignore_errors = value.GetValue<bool>();
        }
    }

    // Apply context to line selection
    if (before_context > 0 || after_context > 0) {
        line_selection.AddContext(before_context, after_context);
    }

    // Define output columns
    return_types.push_back(LogicalType::BIGINT);   // line_number
    names.push_back("line_number");

    return_types.push_back(LogicalType::VARCHAR);  // content
    names.push_back("content");

    return_types.push_back(LogicalType::BIGINT);   // byte_offset
    names.push_back("byte_offset");

    return_types.push_back(LogicalType::VARCHAR);  // file_path
    names.push_back("file_path");

    return make_uniq<ReadTextLinesBindData>(std::move(files), std::move(line_selection), ignore_errors);
}

static unique_ptr<GlobalTableFunctionState> ReadTextLinesInit(ClientContext &context, TableFunctionInitInput &input) {
    auto result = make_uniq<ReadTextLinesGlobalState>();
    result->fs = &FileSystem::GetFileSystem(context);
    return std::move(result);
}

static bool OpenNextFile(ReadTextLinesGlobalState &state, const ReadTextLinesBindData &bind_data) {
    while (state.file_index < bind_data.files.size()) {
        auto &file_info = bind_data.files[state.file_index];
        state.file_index++;

        try {
            state.current_file = state.fs->OpenFile(file_info.path, FileFlags::FILE_FLAGS_READ);
            state.current_file_path = file_info.path;
            state.current_line_number = 0;
            state.current_byte_offset = 0;
            state.file_finished = false;
            return true;
        } catch (std::exception &e) {
            if (!bind_data.ignore_errors) {
                throw;
            }
            // Skip this file and try next
            continue;
        }
    }
    return false;
}

static void ReadTextLinesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<ReadTextLinesBindData>();
    auto &state = data_p.global_state->Cast<ReadTextLinesGlobalState>();

    idx_t output_row = 0;

    while (output_row < STANDARD_VECTOR_SIZE) {
        // Open next file if needed
        if (state.file_finished) {
            if (!OpenNextFile(state, bind_data)) {
                break; // No more files
            }
        }

        // Read lines from current file
        while (output_row < STANDARD_VECTOR_SIZE && !state.file_finished) {
            string line;
            auto line_start_offset = state.current_byte_offset;

            try {
                line = state.current_file->ReadLine();
            } catch (...) {
                state.file_finished = true;
                break;
            }

            // Check for EOF - ReadLine returns empty string at EOF
            // but we also need to handle empty lines in the file
            if (line.empty()) {
                // Check if we're at EOF by comparing position
                auto current_pos = state.current_file->SeekPosition();
                auto file_size = state.current_file->GetFileSize();
                if (current_pos >= file_size) {
                    state.file_finished = true;
                    break;
                }
            }

            state.current_line_number++;
            state.current_byte_offset = state.current_file->SeekPosition();

            // Check if we should include this line
            if (!bind_data.line_selection.ShouldIncludeLine(state.current_line_number)) {
                // Check if we've passed all ranges
                if (bind_data.line_selection.PastAllRanges(state.current_line_number)) {
                    state.file_finished = true;
                    break;
                }
                continue;
            }

            // Output this line
            output.data[0].SetValue(output_row, Value::BIGINT(state.current_line_number));
            output.data[1].SetValue(output_row, Value(line));
            output.data[2].SetValue(output_row, Value::BIGINT(line_start_offset));
            output.data[3].SetValue(output_row, Value(state.current_file_path));

            output_row++;
        }
    }

    output.SetCardinality(output_row);
}

TableFunction ReadTextLinesFunction() {
    TableFunction func("read_text_lines", {LogicalType::VARCHAR}, ReadTextLinesFunction, ReadTextLinesBind,
                       ReadTextLinesInit);

    // Named parameters
    func.named_parameters["lines"] = LogicalType::ANY;  // Can be int, string, or list
    func.named_parameters["before"] = LogicalType::BIGINT;
    func.named_parameters["after"] = LogicalType::BIGINT;
    func.named_parameters["context"] = LogicalType::BIGINT;
    func.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;

    return func;
}

} // namespace duckdb
