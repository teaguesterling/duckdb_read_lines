#include "read_lines_extension.hpp"
#include "line_selection.hpp"
#include "compat.hpp"
#include "duckdb_compat.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

struct ReadTextLinesBindData : public TableFunctionData {
	vector<OpenFileInfo> files;
	LineSelection line_selection;
	LineTrimMode trim_mode;
	bool ignore_errors;

	ReadTextLinesBindData(vector<OpenFileInfo> files, LineSelection selection, LineTrimMode trim_mode,
	                      bool ignore_errors)
	    : files(std::move(files)), line_selection(std::move(selection)), trim_mode(trim_mode),
	      ignore_errors(ignore_errors) {
	}
};

// Forward declaration; defined below with the shared reading helpers.
class BufferedLineReader;

struct ReadTextLinesGlobalState : public GlobalTableFunctionState {
	idx_t file_index;
	unique_ptr<FileHandle> current_file;
	unique_ptr<BufferedLineReader> reader;
	int64_t current_line_number;
	string current_file_path;
	bool file_finished;
	FileSystem *fs;
	LineSelection resolved_selection; // Per-file resolved selection (handles from-end refs)

	ReadTextLinesGlobalState()
	    : file_index(0), current_line_number(0), file_finished(true), fs(nullptr),
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
	LineTrimMode trim_mode = LineTrimMode::NONE;
	bool has_explicit_lines = false;
	int64_t before_context = 0;
	int64_t after_context = 0;
	bool ignore_errors = false;

	// Check for second positional argument (lines)
	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		line_selection = LineSelection::Parse(input.inputs[1]);
		has_explicit_lines = true;
	}

	// Check for third positional argument (trim)
	if (input.inputs.size() > 2) {
		trim_mode = ParseLineTrimMode(input.inputs[2]);
	}

	for (auto &param : input.named_parameters) {
		auto &name = param.first;
		auto &value = param.second;

		if (name == "lines") {
			line_selection = LineSelection::Parse(value);
			has_explicit_lines = true;
		} else if (name == "trim") {
			trim_mode = ParseLineTrimMode(value);
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

	if (files.empty() && !ignore_errors) {
		throw IOException("No files found that match the pattern \"%s\"", input_path);
	}

	return make_uniq<ReadTextLinesBindData>(std::move(files), std::move(line_selection), trim_mode, ignore_errors);
}

static unique_ptr<GlobalTableFunctionState> ReadTextLinesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<ReadTextLinesGlobalState>();
	result->fs = &FileSystem::GetFileSystem(context);
	return std::move(result);
}

// =============================================================================
// BufferedLineReader
//
// The single reading path for every source, seekable or not. Reads the source
// in large chunks (one FileHandle::Read call per chunk instead of the previous
// syscall-per-byte ReadLine loop) and splits lines with the shared ExtractLine,
// so files, pipes, and parse_lines all agree on the same bytes:
//   - terminators (\n, \r\n, lone \r) are separators AND are preserved in the
//     returned content, as the README documents;
//   - a terminator-final stream does not grow a phantom trailing line, and a
//     trailing empty line ("a\n\n") is not silently dropped;
//   - a UTF-8 BOM at the start of the stream is skipped, not leaked into the
//     first line's content (byte offsets remain true source offsets, so the
//     first line of a BOM'd file starts at offset 3);
//   - end of stream is a 0-byte Read(), which works for pipes and virtual
//     URIs where SeekPosition()/GetFileSize() throw.
// Memory stays bounded (longest line + one chunk) because consumed bytes are
// compacted away on refill — except after SlurpAll(), which deliberately
// buffers the whole stream to resolve from-end references on sources that
// cannot rewind.
// =============================================================================
class BufferedLineReader {
public:
	explicit BufferedLineReader(FileHandle &file) : file(file) {
	}

	// Extract the next line, including its terminator. Returns false at end of
	// stream. start_offset is the byte offset of the line's first content byte
	// in the source.
	bool NextLine(string &line, int64_t &start_offset) {
		if (!EnsureLineBuffered()) {
			return false;
		}
		start_offset = buffer_base + static_cast<int64_t>(pos);
		line = ExtractLine(buffer, pos);
		return true;
	}

	// Buffer the whole remaining stream. Needed before CountBufferedLines() on
	// non-seekable sources, which cannot be rewound after counting.
	void SlurpAll() {
		SkipBOM();
		while (!eof) {
			Fill();
		}
	}

	// Lines from the current position to end of stream. Call after SlurpAll()
	// and before any NextLine().
	int64_t CountBufferedLines() const {
		return CountLinesInText(buffer, pos);
	}

private:
	static constexpr idx_t FILL_CHUNK_SIZE = 65536;

	// Ensure the buffer holds a complete line starting at pos (or the final
	// unterminated line once eof is reached). Returns false at end of stream.
	bool EnsureLineBuffered() {
		SkipBOM();
		while (true) {
			auto term = buffer.find_first_of("\r\n", pos);
			if (term != string::npos) {
				// A '\r' as the last buffered byte may be the first half of a
				// '\r\n' spanning a read boundary; decide after the next fill.
				if (buffer[term] == '\r' && term + 1 == buffer.size() && !eof) {
					Fill();
					continue;
				}
				return true;
			}
			if (eof) {
				return pos < buffer.size();
			}
			Fill();
		}
	}

	// Skip a UTF-8 byte-order mark at the very start of the stream. Runs
	// before the first line is parsed and never again.
	void SkipBOM() {
		while (!bom_checked) {
			if (buffer.size() >= 3) {
				if (buffer.compare(0, 3, "\xEF\xBB\xBF") == 0) {
					pos = 3;
				}
				bom_checked = true;
			} else if (eof) {
				bom_checked = true;
			} else {
				Fill();
			}
		}
	}

	void Fill() {
		if (eof) {
			return;
		}
		// Compact consumed bytes so streaming reads don't accumulate the whole
		// source. pos stays 0-based within the buffer; buffer_base keeps
		// start offsets equal to true source offsets.
		if (pos > 0) {
			buffer.erase(0, pos);
			buffer_base += static_cast<int64_t>(pos);
			pos = 0;
		}
		char chunk[FILL_CHUNK_SIZE];
		int64_t bytes_read = file.Read(chunk, FILL_CHUNK_SIZE);
		if (bytes_read <= 0) {
			eof = true;
			return;
		}
		buffer.append(chunk, static_cast<size_t>(bytes_read));
	}

	FileHandle &file;
	string buffer;
	idx_t pos = 0;
	int64_t buffer_base = 0;
	bool eof = false;
	bool bom_checked = false;
};

// Count total lines by scanning the stream through a reader (for resolving
// from-end references on seekable sources; the caller rewinds afterwards).
static int64_t CountLinesInStream(BufferedLineReader &reader) {
	string line;
	int64_t offset;
	int64_t count = 0;
	while (reader.NextLine(line, offset)) {
		count++;
	}
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
			state.file_finished = false;
			state.reader = make_uniq<BufferedLineReader>(*state.current_file);

			if (bind_data.line_selection.HasFromEndReferences()) {
				// From-end references (e.g. '+2' = 2nd line from the end) need
				// the total line count before any line can be emitted.
				int64_t total_lines;
				if (state.current_file->CanSeek()) {
					total_lines = CountLinesInStream(*state.reader);
					state.current_file->Seek(0);
					state.reader = make_uniq<BufferedLineReader>(*state.current_file);
				} else {
					// Pipes and streams cannot rewind after counting: buffer
					// the whole stream and serve lines from the buffer.
					state.reader->SlurpAll();
					total_lines = state.reader->CountBufferedLines();
				}
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
			int64_t line_start_offset;
			bool have_line;
			try {
				have_line = state.reader->NextLine(line, line_start_offset);
			} catch (std::exception &) {
				// A genuine mid-read I/O error (EOF is a 0-byte read, not an
				// exception). Skip the rest of the file only if asked to.
				if (!bind_data.ignore_errors) {
					throw;
				}
				have_line = false;
			}
			if (!have_line) {
				state.file_finished = true;
				break;
			}

			state.current_line_number++;

			if (!state.resolved_selection.ShouldIncludeLine(state.current_line_number)) {
				if (state.resolved_selection.PastAllRanges(state.current_line_number)) {
					state.file_finished = true;
					break;
				}
				continue;
			}

			// VARCHAR requires valid UTF-8; a bad byte must not abort the whole
			// scan when the user opted into ignore_errors (the line keeps its
			// number so subsequent line numbers stay true to the file).
			if (Utf8Proc::Analyze(line.c_str(), line.size()) == UnicodeType::INVALID) {
				if (bind_data.ignore_errors) {
					continue;
				}
				throw InvalidInputException(
				    "read_lines: line %lld of \"%s\" is not valid UTF-8; set ignore_errors=true to skip such lines",
				    state.current_line_number, state.current_file_path);
			}

			output.data[0].SetValue(output_row, Value::BIGINT(state.current_line_number));
			output.data[1].SetValue(output_row, Value(ApplyLineTrim(line, bind_data.trim_mode)));
			output.data[2].SetValue(output_row, Value::BIGINT(line_start_offset));
			output.data[3].SetValue(output_row, Value(state.current_file_path));

			output_row++;
		}
	}

	CompatSetOutputCardinality(output, output_row);
}

TableFunctionSet ReadLinesFunction() {
	TableFunctionSet set("read_lines");

	// Single argument: read_lines(path)
	TableFunction func1("read_lines", {LogicalType::VARCHAR}, ReadTextLinesFunction, ReadTextLinesBind,
	                    ReadTextLinesInit);
	func1.named_parameters["lines"] = LogicalType::ANY;
	func1.named_parameters["trim"] = LogicalType::ANY;
	func1.named_parameters["before"] = LogicalType::BIGINT;
	func1.named_parameters["after"] = LogicalType::BIGINT;
	func1.named_parameters["context"] = LogicalType::BIGINT;
	func1.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	set.AddFunction(func1);

	// Two arguments: read_lines(path, lines)
	TableFunction func2("read_lines", {LogicalType::VARCHAR, LogicalType::ANY}, ReadTextLinesFunction,
	                    ReadTextLinesBind, ReadTextLinesInit);
	func2.named_parameters["trim"] = LogicalType::ANY;
	func2.named_parameters["before"] = LogicalType::BIGINT;
	func2.named_parameters["after"] = LogicalType::BIGINT;
	func2.named_parameters["context"] = LogicalType::BIGINT;
	func2.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	set.AddFunction(func2);

	// Three arguments: read_lines(path, lines, trim)
	TableFunction func3("read_lines", {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, ReadTextLinesFunction,
	                    ReadTextLinesBind, ReadTextLinesInit);
	func3.named_parameters["before"] = LogicalType::BIGINT;
	func3.named_parameters["after"] = LogicalType::BIGINT;
	func3.named_parameters["context"] = LogicalType::BIGINT;
	func3.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	set.AddFunction(func3);

	return set;
}

// =============================================================================
// Lateral join version: read_lines_lateral
// =============================================================================

struct ReadTextLinesLateralBindData : public TableFunctionData {
	LineSelection line_selection;
	LineTrimMode trim_mode;
	bool ignore_errors;

	ReadTextLinesLateralBindData(LineSelection selection, LineTrimMode trim_mode, bool ignore_errors)
	    : line_selection(std::move(selection)), trim_mode(trim_mode), ignore_errors(ignore_errors) {
	}
};

struct ReadTextLinesLateralState : public LocalTableFunctionState {
	FileSystem *fs;
	unique_ptr<FileHandle> current_file;
	// The InOut operator is re-entered (HAVE_MORE_OUTPUT) until the current
	// source is exhausted, so the reader — and with it any buffered stream
	// content and the parse position — must persist in operator state across
	// re-invocations. Each correlated input row opens its own source and gets
	// its own reader.
	unique_ptr<BufferedLineReader> reader;
	string current_file_path;
	int64_t current_line_number;
	bool file_open;
	idx_t current_row;
	LineSelection resolved_selection; // Per-file resolved selection

	ReadTextLinesLateralState()
	    : fs(nullptr), current_line_number(0), file_open(false), current_row(0),
	      resolved_selection(LineSelection::All()) {
	}
};

static unique_ptr<FunctionData> ReadTextLinesLateralBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	LineSelection line_selection = LineSelection::All();
	LineTrimMode trim_mode = LineTrimMode::NONE;
	bool ignore_errors = false;

	// For in_out functions, additional positional arguments appear in input_table_names.
	// The argument value is stored as the "column name"; string literals come
	// with surrounding quotes that must be stripped.
	auto table_name_arg = [&](idx_t index) -> string {
		string arg = input.input_table_names[index];
		if (arg.size() >= 2 && arg.front() == '\'' && arg.back() == '\'') {
			arg = arg.substr(1, arg.size() - 2);
		}
		return arg;
	};

	// The second argument (lines selection) is at index 1. A literal NULL
	// arrives as the unquoted text "NULL" and means "all lines" (useful for
	// skipping to the third argument).
	if (input.input_table_names.size() > 1) {
		string lines_arg = table_name_arg(1);
		if (!lines_arg.empty() && !StringUtil::CIEquals(lines_arg, "null")) {
			// Parse as string - LineSelection::Parse handles both integers and line specs
			line_selection = LineSelection::Parse(Value(lines_arg));
		}
	}

	// The third argument (trim) is at index 2. A bare boolean literal binds as
	// a cast expression and arrives as its stringification, so map those back;
	// quoted string modes ('endings', 'both', ...) arrive as plain text.
	if (input.input_table_names.size() > 2) {
		string trim_arg = table_name_arg(2);
		auto lowered = StringUtil::Lower(trim_arg);
		if (lowered == "cast('t' as boolean)") {
			trim_arg = "true";
		} else if (lowered == "cast('f' as boolean)") {
			trim_arg = "false";
		}
		if (!trim_arg.empty() && !StringUtil::CIEquals(trim_arg, "null")) {
			trim_mode = ParseLineTrimMode(Value(trim_arg));
		}
	}

	// Positional arguments (lines, trim) may also come through as constants.
	// Note: Named parameters don't work with in_out functions, so we only support positional.
	// Context can be embedded in the lines spec (e.g., '42 +/-3').
	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		line_selection = LineSelection::Parse(input.inputs[1]);
	}
	if (input.inputs.size() > 2) {
		trim_mode = ParseLineTrimMode(input.inputs[2]);
	}

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("line_number");

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("content");

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("byte_offset");

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("file_path");

	return make_uniq<ReadTextLinesLateralBindData>(std::move(line_selection), trim_mode, ignore_errors);
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
		CompatSetOutputCardinality(output, 0);
		return OperatorResultType::FINISHED;
	}

	idx_t output_row = 0;

	while (output_row < STANDARD_VECTOR_SIZE) {
		// Need to open a new file?
		if (!state.file_open) {
			if (state.current_row >= input.size()) {
				// This input chunk is exhausted. Flush whatever we produced and
				// ask for the next input chunk. We must NOT return FINISHED here:
				// FINISHED requires an empty output chunk (the pipeline executor
				// asserts current_chunk.size() == 0), and we may be carrying
				// output rows. The pipeline terminates when the upstream source
				// is exhausted (we then receive input.size() == 0 above).
				CompatSetOutputCardinality(output, output_row);
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
				state.file_open = true;
				state.reader = make_uniq<BufferedLineReader>(*state.current_file);

				if (bind_data.line_selection.HasFromEndReferences()) {
					// From-end references need the total line count up front.
					int64_t total_lines;
					if (state.current_file->CanSeek()) {
						total_lines = CountLinesInStream(*state.reader);
						state.current_file->Seek(0);
						state.reader = make_uniq<BufferedLineReader>(*state.current_file);
					} else {
						// Pipes (e.g. a per-row shellfs command) cannot rewind
						// after counting: buffer the whole stream and serve
						// lines from the buffer.
						state.reader->SlurpAll();
						total_lines = state.reader->CountBufferedLines();
					}
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

		// Read lines from the current source. The reader persists in operator
		// state, so the parse position survives this operator's
		// HAVE_MORE_OUTPUT re-invocations.
		while (output_row < STANDARD_VECTOR_SIZE && state.file_open) {
			string line;
			int64_t line_start_offset;
			bool have_line;
			try {
				have_line = state.reader->NextLine(line, line_start_offset);
			} catch (std::exception &) {
				if (!bind_data.ignore_errors) {
					throw;
				}
				have_line = false;
			}
			if (!have_line) {
				state.file_open = false;
				state.current_row++;
				break;
			}

			state.current_line_number++;

			// Check line selection
			if (!state.resolved_selection.ShouldIncludeLine(state.current_line_number)) {
				if (state.resolved_selection.PastAllRanges(state.current_line_number)) {
					state.file_open = false;
					state.current_row++;
					break;
				}
				continue;
			}

			// VARCHAR requires valid UTF-8; see ReadTextLinesFunction.
			if (Utf8Proc::Analyze(line.c_str(), line.size()) == UnicodeType::INVALID) {
				if (bind_data.ignore_errors) {
					continue;
				}
				throw InvalidInputException("read_lines_lateral: line %lld of \"%s\" is not valid UTF-8",
				                            state.current_line_number, state.current_file_path);
			}

			// Output the line
			output.data[0].SetValue(output_row, Value::BIGINT(state.current_line_number));
			output.data[1].SetValue(output_row, Value(ApplyLineTrim(line, bind_data.trim_mode)));
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

	CompatSetOutputCardinality(output, output_row);

	// More output from the current source? Re-invoke with the same input chunk.
	if (state.file_open) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}

	// More input rows in this chunk to process? Re-invoke with the same input.
	if (state.current_row < input.size()) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}

	// This input chunk is fully consumed. Flush any output and request the next
	// chunk. We never return FINISHED with output rows still in the chunk: the
	// pipeline executor asserts FINISHED implies an empty chunk. Termination is
	// driven by the upstream source delivering input.size() == 0 (handled at the
	// top of this function).
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

	// Three arguments: read_lines_lateral(path, lines, trim)
	TableFunction func3("read_lines_lateral", {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY}, nullptr,
	                    ReadTextLinesLateralBind, nullptr, ReadTextLinesLateralLocalInit);
	func3.in_out_function = ReadTextLinesLateralInOut;
	set.AddFunction(func3);

	return set;
}

} // namespace duckdb
