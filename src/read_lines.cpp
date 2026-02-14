#include "read_lines_extension.hpp"
#include "line_selection.hpp"
#include "compat.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
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
	LineSelection resolved_selection; // Per-file resolved selection (handles from-end refs)

	ReadTextLinesGlobalState()
	    : file_index(0), current_line_number(0), current_byte_offset(0), file_finished(true), fs(nullptr),
	      resolved_selection(LineSelection::All()) {
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> ReadTextLinesBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto input_path = input.inputs[0].GetValue<string>();

	// Try the original path first - if it exists or matches files, use it as-is
	// This handles cases where filenames contain colons (e.g., "file:2.txt")
	auto files = compat::GlobFilesCompat(fs, input_path, context, FileGlobOptions::ALLOW_EMPTY);

	string glob_pattern = input_path;
	LineSelection path_line_selection = LineSelection::All();

	if (files.empty()) {
		// No files found with original path - try parsing for embedded line spec
		auto parsed_result = LineSelection::ParsePathWithLineSpec(input_path);
		if (parsed_result.first != input_path) {
			// Path was parsed differently, try globbing with the extracted path
			files = compat::GlobFilesCompat(fs, parsed_result.first, context, FileGlobOptions::ALLOW_EMPTY);
			if (!files.empty()) {
				glob_pattern = parsed_result.first;
				path_line_selection = std::move(parsed_result.second);
			}
		}
	}

	LineSelection line_selection = LineSelection::All();
	bool has_explicit_lines = false;
	int64_t before_context = 0;
	int64_t after_context = 0;
	bool ignore_errors = false;

	// Check for second positional argument (lines)
	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		line_selection = LineSelection::Parse(input.inputs[1]);
		has_explicit_lines = true;
	}

	for (auto &param : input.named_parameters) {
		auto &name = param.first;
		auto &value = param.second;

		if (name == "lines") {
			line_selection = LineSelection::Parse(value);
			has_explicit_lines = true;
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

	// If no explicit lines param, use path-embedded selection
	if (!has_explicit_lines && !path_line_selection.IsAll()) {
		line_selection = std::move(path_line_selection);
	}

	if (before_context > 0 || after_context > 0) {
		line_selection.AddContext(before_context, after_context);
	}

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("line_number");

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("content");

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("byte_offset");

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("file_path");

	return make_uniq<ReadTextLinesBindData>(std::move(files), std::move(line_selection), ignore_errors);
}

static unique_ptr<GlobalTableFunctionState> ReadTextLinesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<ReadTextLinesGlobalState>();
	result->fs = &FileSystem::GetFileSystem(context);
	return std::move(result);
}

// Count total lines in a file (for resolving from-end references)
static int64_t CountLinesInFile(FileHandle &file) {
	int64_t count = 0;
	file.Seek(0);
	while (true) {
		try {
			string line = file.ReadLine();
			if (line.empty()) {
				auto current_pos = file.SeekPosition();
				auto file_size = file.GetFileSize();
				if (current_pos >= file_size) {
					break;
				}
			}
			count++;
		} catch (...) {
			break;
		}
	}
	file.Seek(0);
	return count;
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

			// Handle from-end references (e.g., +10 meaning 10th line from end)
			if (bind_data.line_selection.HasFromEndReferences()) {
				// Count lines first, then resolve
				int64_t total_lines = CountLinesInFile(*state.current_file);
				state.resolved_selection = bind_data.line_selection;
				state.resolved_selection.ResolveFromEnd(total_lines);
			} else {
				state.resolved_selection = bind_data.line_selection;
			}

			return true;
		} catch (std::exception &e) {
			if (!bind_data.ignore_errors) {
				throw;
			}
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
		if (state.file_finished) {
			if (!OpenNextFile(state, bind_data)) {
				break;
			}
		}

		while (output_row < STANDARD_VECTOR_SIZE && !state.file_finished) {
			string line;
			auto line_start_offset = state.current_byte_offset;

			try {
				line = state.current_file->ReadLine();
			} catch (...) {
				state.file_finished = true;
				break;
			}

			if (line.empty()) {
				auto current_pos = state.current_file->SeekPosition();
				auto file_size = state.current_file->GetFileSize();
				if (current_pos >= file_size) {
					state.file_finished = true;
					break;
				}
			}

			state.current_line_number++;
			state.current_byte_offset = state.current_file->SeekPosition();

			if (!state.resolved_selection.ShouldIncludeLine(state.current_line_number)) {
				if (state.resolved_selection.PastAllRanges(state.current_line_number)) {
					state.file_finished = true;
					break;
				}
				continue;
			}

			output.data[0].SetValue(output_row, Value::BIGINT(state.current_line_number));
			output.data[1].SetValue(output_row, Value(line));
			output.data[2].SetValue(output_row, Value::BIGINT(line_start_offset));
			output.data[3].SetValue(output_row, Value(state.current_file_path));

			output_row++;
		}
	}

	output.SetCardinality(output_row);
}

TableFunctionSet ReadLinesFunction() {
	TableFunctionSet set("read_lines");

	// Single argument: read_lines(path)
	TableFunction func1("read_lines", {LogicalType::VARCHAR}, ReadTextLinesFunction, ReadTextLinesBind,
	                    ReadTextLinesInit);
	func1.named_parameters["lines"] = LogicalType::ANY;
	func1.named_parameters["before"] = LogicalType::BIGINT;
	func1.named_parameters["after"] = LogicalType::BIGINT;
	func1.named_parameters["context"] = LogicalType::BIGINT;
	func1.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	set.AddFunction(func1);

	// Two arguments: read_lines(path, lines)
	TableFunction func2("read_lines", {LogicalType::VARCHAR, LogicalType::ANY}, ReadTextLinesFunction,
	                    ReadTextLinesBind, ReadTextLinesInit);
	func2.named_parameters["before"] = LogicalType::BIGINT;
	func2.named_parameters["after"] = LogicalType::BIGINT;
	func2.named_parameters["context"] = LogicalType::BIGINT;
	func2.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	set.AddFunction(func2);

	return set;
}

// =============================================================================
// Lateral join version: read_lines_lateral
// =============================================================================

struct ReadTextLinesLateralBindData : public TableFunctionData {
	LineSelection line_selection;
	bool ignore_errors;

	ReadTextLinesLateralBindData(LineSelection selection, bool ignore_errors)
	    : line_selection(std::move(selection)), ignore_errors(ignore_errors) {
	}
};

struct ReadTextLinesLateralState : public LocalTableFunctionState {
	FileSystem *fs;
	unique_ptr<FileHandle> current_file;
	string current_file_path;
	int64_t current_line_number;
	int64_t current_byte_offset;
	bool file_open;
	idx_t current_row;
	LineSelection resolved_selection; // Per-file resolved selection

	ReadTextLinesLateralState()
	    : fs(nullptr), current_line_number(0), current_byte_offset(0), file_open(false), current_row(0),
	      resolved_selection(LineSelection::All()) {
	}
};

static unique_ptr<FunctionData> ReadTextLinesLateralBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	LineSelection line_selection = LineSelection::All();
	bool ignore_errors = false;

	// For in_out functions, additional positional arguments appear in input_table_names.
	// The second argument (lines selection) is at index 1.
	if (input.input_table_names.size() > 1) {
		// The argument value is stored as the "column name"
		string lines_arg = input.input_table_names[1];

		// Strip surrounding quotes if present (string literals come with quotes)
		if (lines_arg.size() >= 2 && lines_arg.front() == '\'' && lines_arg.back() == '\'') {
			lines_arg = lines_arg.substr(1, lines_arg.size() - 2);
		}

		if (!lines_arg.empty()) {
			// Parse as string - LineSelection::Parse handles both integers and line specs
			line_selection = LineSelection::Parse(Value(lines_arg));
		}
	}

	// Check for second positional argument (lines)
	// Note: Named parameters don't work with in_out functions, so we only support positional.
	// Context can be embedded in the lines spec (e.g., '42 +/-3').
	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		line_selection = LineSelection::Parse(input.inputs[1]);
	}

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("line_number");

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("content");

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("byte_offset");

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("file_path");

	return make_uniq<ReadTextLinesLateralBindData>(std::move(line_selection), ignore_errors);
}

static unique_ptr<LocalTableFunctionState> ReadTextLinesLateralLocalInit(ExecutionContext &context,
                                                                         TableFunctionInitInput &input,
                                                                         GlobalTableFunctionState *global_state) {
	auto result = make_uniq<ReadTextLinesLateralState>();
	result->fs = &FileSystem::GetFileSystem(context.client);
	return std::move(result);
}

static OperatorResultType ReadTextLinesLateralInOut(ExecutionContext &context, TableFunctionInput &data_p,
                                                    DataChunk &input, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ReadTextLinesLateralBindData>();
	auto &state = data_p.local_state->Cast<ReadTextLinesLateralState>();

	if (input.size() == 0) {
		output.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	idx_t output_row = 0;

	while (output_row < STANDARD_VECTOR_SIZE) {
		// Need to open a new file?
		if (!state.file_open) {
			if (state.current_row >= input.size()) {
				// Done with all input rows
				output.SetCardinality(output_row);
				state.current_row = 0;
				return OperatorResultType::NEED_MORE_INPUT;
			}

			// Get file path from input
			auto path_value = input.GetValue(0, state.current_row);
			if (path_value.IsNull()) {
				state.current_row++;
				continue;
			}

			auto file_path = path_value.GetValue<string>();

			// Try to open the file
			try {
				state.current_file = state.fs->OpenFile(file_path, FileFlags::FILE_FLAGS_READ);
				state.current_file_path = file_path;
				state.current_line_number = 0;
				state.current_byte_offset = 0;
				state.file_open = true;

				// Handle from-end references (e.g., +10 meaning 10th line from end)
				if (bind_data.line_selection.HasFromEndReferences()) {
					int64_t total_lines = CountLinesInFile(*state.current_file);
					state.resolved_selection = bind_data.line_selection;
					state.resolved_selection.ResolveFromEnd(total_lines);
				} else {
					state.resolved_selection = bind_data.line_selection;
				}
			} catch (std::exception &e) {
				if (!bind_data.ignore_errors) {
					throw;
				}
				state.current_row++;
				continue;
			}
		}

		// Read lines from current file
		while (output_row < STANDARD_VECTOR_SIZE && state.file_open) {
			string line;
			auto line_start_offset = state.current_byte_offset;

			try {
				line = state.current_file->ReadLine();
			} catch (...) {
				state.file_open = false;
				state.current_row++;
				break;
			}

			// Check for EOF
			if (line.empty()) {
				auto current_pos = state.current_file->SeekPosition();
				auto file_size = state.current_file->GetFileSize();
				if (current_pos >= file_size) {
					state.file_open = false;
					state.current_row++;
					break;
				}
			}

			state.current_line_number++;
			state.current_byte_offset = state.current_file->SeekPosition();

			// Check line selection
			if (!state.resolved_selection.ShouldIncludeLine(state.current_line_number)) {
				if (state.resolved_selection.PastAllRanges(state.current_line_number)) {
					state.file_open = false;
					state.current_row++;
					break;
				}
				continue;
			}

			// Output the line
			output.data[0].SetValue(output_row, Value::BIGINT(state.current_line_number));
			output.data[1].SetValue(output_row, Value(line));
			output.data[2].SetValue(output_row, Value::BIGINT(line_start_offset));
			output.data[3].SetValue(output_row, Value(state.current_file_path));

			output_row++;
		}

		// If file closed and more rows, continue to next file
		if (!state.file_open && state.current_row < input.size()) {
			continue;
		}

		// Exit if we have output or no more work
		if (output_row > 0 || state.current_row >= input.size()) {
			break;
		}
	}

	output.SetCardinality(output_row);

	// More output from current file?
	if (state.file_open) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}

	// More input rows to process?
	if (state.current_row < input.size()) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}

	state.current_row = 0;
	return OperatorResultType::NEED_MORE_INPUT;
}

TableFunctionSet ReadLinesLateralFunction() {
	TableFunctionSet set("read_lines_lateral");

	// Single argument: read_lines_lateral(path)
	TableFunction func1("read_lines_lateral", {LogicalType::VARCHAR}, nullptr, ReadTextLinesLateralBind, nullptr,
	                    ReadTextLinesLateralLocalInit);
	func1.in_out_function = ReadTextLinesLateralInOut;
	set.AddFunction(func1);

	// Two arguments: read_lines_lateral(path, lines)
	// Note: Named parameters don't work with in_out functions, so we only support positional.
	// Context can be embedded in the lines spec (e.g., '+5 +/-2').
	TableFunction func2("read_lines_lateral", {LogicalType::VARCHAR, LogicalType::ANY}, nullptr,
	                    ReadTextLinesLateralBind, nullptr, ReadTextLinesLateralLocalInit);
	func2.in_out_function = ReadTextLinesLateralInOut;
	set.AddFunction(func2);

	return set;
}

} // namespace duckdb
