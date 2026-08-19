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
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/error_manager.hpp"
#include "utf8proc_wrapper.hpp"
#include <algorithm>
#include <exception>

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

	// Try the original path first - if it exists or matches files, use it as-is.
	// This keeps the rule monotone: any path that resolved before still resolves,
	// so a file whose name genuinely contains ':' ("file:2.txt") or '#'
	// ("weird#name.txt") always wins over a decorated interpretation of it.
	//
	// Not every filesystem answers "nothing matches" with an empty list: a VFS
	// that parses the path itself (duck_tails' git:// resolves the revision in
	// Glob, for one) throws instead, because a decorated path is not a path it
	// can parse. That must not make the line spec unreachable, so remember the
	// error and only surface it if no interpretation of the path resolves - the
	// unchanged error is what a path with no line spec still gets.
	vector<OpenFileInfo> files;
	std::exception_ptr literal_path_error;
	try {
		files = compat::GlobFilesCompat(fs, input_path, context, FileGlobOptions::ALLOW_EMPTY);
	} catch (std::exception &) {
		literal_path_error = std::current_exception();
	}

	string glob_pattern = input_path;
	LineSelection path_line_selection = LineSelection::All();
	LineSpecSource path_spec_source = LineSpecSource::NONE;

	if (files.empty()) {
		// No files found with the original path - try parsing an embedded line spec
		auto parsed_result = ParsePathLineSpec(input_path);
		if (parsed_result.source != LineSpecSource::NONE) {
			// Path was parsed differently, try globbing with the extracted path
			files = compat::GlobFilesCompat(fs, parsed_result.path, context, FileGlobOptions::ALLOW_EMPTY);
			if (!files.empty()) {
				glob_pattern = parsed_result.path;
				path_line_selection = std::move(parsed_result.selection);
				path_spec_source = parsed_result.source;
			} else if (parsed_result.source != LineSpecSource::COLON) {
				// The locator still does not resolve. If the leftover locator also
				// carries a legacy ':' spec that *does* resolve, the path names its
				// lines twice - report that instead of silently picking one. The
				// ':' interpretation must really resolve before we call it a spec,
				// otherwise a URI port ("host:8080/f.txt") would look like one.
				auto colon_result = LineSelection::ParsePathWithLineSpec(parsed_result.path);
				if (colon_result.first != parsed_result.path) {
					bool colon_resolves = false;
					try {
						colon_resolves =
						    !compat::GlobFilesCompat(fs, colon_result.first, context, FileGlobOptions::ALLOW_EMPTY)
						         .empty();
					} catch (...) { // NOLINT: a filesystem that cannot glob simply is not a conflict
						colon_resolves = false;
					}
					if (colon_resolves) {
						throw InvalidInputException(
						    "read_lines: path \"%s\" carries both a %s line spec and a %s line spec; use only one",
						    input_path, LineSpecSourceName(parsed_result.source),
						    LineSpecSourceName(LineSpecSource::COLON));
					}
				}
			}
		}

		if (files.empty() && literal_path_error) {
			// Nothing resolved: the path is simply broken, so report exactly the
			// error the filesystem gave for it.
			std::rethrow_exception(literal_path_error);
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

	// A URI-form spec plus an explicit 'lines' argument names the lines twice:
	// reject it rather than silently picking one. The legacy ':' form keeps its
	// documented behaviour (the explicit argument wins) for back-compatibility.
	if (has_explicit_lines && path_spec_source != LineSpecSource::NONE && path_spec_source != LineSpecSource::COLON) {
		throw InvalidInputException(
		    "read_lines: path \"%s\" carries a %s line spec and a 'lines' argument was also given; use only one",
		    input_path, LineSpecSourceName(path_spec_source));
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
	// start_offset is the source offset the handle is currently positioned at.
	// A non-zero one means the caller seeked into the middle of the source, so
	// the byte-order mark (which only ever sits at offset 0) is already behind
	// us and must not be looked for again.
	explicit BufferedLineReader(FileHandle &file, int64_t start_offset = 0)
	    : file(file), buffer_base(start_offset), bom_checked(start_offset != 0) {
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

	// Count the lines from the current position to end of stream without
	// materialising any of them, and remember where the last `tail_window`
	// of them start (ascending, in tail_starts). A seekable caller resolving a
	// from-end reference can then jump straight to the first line it needs
	// instead of reading the whole source a second time.
	//
	// The line rule is exactly ExtractLine's / CountLinesInText's: '\n', '\r\n'
	// and a lone '\r' each end a line, and a final unterminated run of bytes is
	// a line of its own.
	int64_t CountRemainingLines(idx_t tail_window, vector<int64_t> &tail_starts) {
		SkipBOM();
		tail_starts.clear();
		idx_t ring_head = 0; // next slot to overwrite once the ring is full
		auto remember = [&](int64_t offset) {
			if (tail_window == 0) {
				return;
			}
			if (tail_starts.size() < tail_window) {
				tail_starts.push_back(offset);
			} else {
				tail_starts[ring_head] = offset;
				ring_head = (ring_head + 1) % tail_window;
			}
		};

		int64_t count = 0;
		int64_t line_start = buffer_base + static_cast<int64_t>(pos);
		bool line_open = false; // bytes seen since the last terminator
		while (true) {
			while (pos < buffer.size()) {
				char c = buffer[pos];
				if (c == '\n') {
					pos++;
				} else if (c == '\r') {
					pos++;
					// A '\r' as the last buffered byte may be the first half of
					// a '\r\n' spanning a read boundary; refill before deciding.
					if (pos == buffer.size() && !eof) {
						Fill();
					}
					if (pos < buffer.size() && buffer[pos] == '\n') {
						pos++;
					}
				} else {
					pos++;
					line_open = true;
					continue;
				}
				count++;
				remember(line_start);
				line_start = buffer_base + static_cast<int64_t>(pos);
				line_open = false;
			}
			if (eof) {
				break;
			}
			Fill();
		}
		if (line_open) {
			count++;
			remember(line_start);
		}
		if (tail_starts.size() == tail_window && ring_head != 0) {
			std::rotate(tail_starts.begin(), tail_starts.begin() + static_cast<int64_t>(ring_head), tail_starts.end());
		}
		return count;
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

// How many line starts we are willing to remember while counting, so that a
// from-end selection can be served without re-reading the head of the source.
// 1M offsets is 8MB; past that the plain rewind is the cheaper trade.
static constexpr idx_t MAX_TAIL_MEMO_LINES = 1u << 20;

// Resolve a selection's from-end references and leave `reader` positioned so
// that the next line it yields is the first one the selection wants, with
// `line_number` set to the number of the line before it.
//
// line_number is the true line number in the source, so the lines ahead of the
// selection have to be counted whichever way this is done. What can be avoided
// is reading them twice: counting no longer materialises the lines it walks
// past, and it remembers where the last few lines start, so a tail selection
// resumes by seeking straight to its first line. A selection that also names a
// head range, or that reaches further back than the remembered window, rewinds
// to the start as before.
//
// Non-seekable sources (pipes, streams) cannot rewind at all: they buffer the
// whole stream during the count and then serve every line from that buffer,
// which is unchanged behaviour.
static LineSelection PrepareFromEndScan(FileHandle &file, unique_ptr<BufferedLineReader> &reader,
                                        const LineSelection &selection, int64_t &line_number) {
	auto resolved = selection;
	if (!file.CanSeek()) {
		reader->SlurpAll();
		resolved.ResolveFromEnd(reader->CountBufferedLines());
		return resolved;
	}

	auto from_end_distance = selection.MaxFromEndDistance();
	idx_t tail_window = 0;
	if (from_end_distance > 0 && from_end_distance <= static_cast<int64_t>(MAX_TAIL_MEMO_LINES)) {
		tail_window = static_cast<idx_t>(from_end_distance);
	}
	vector<int64_t> tail_starts;
	int64_t total_lines = reader->CountRemainingLines(tail_window, tail_starts);
	resolved.ResolveFromEnd(total_lines);

	// The remembered offsets cover lines [first_memo .. total_lines].
	int64_t first_memo = total_lines - static_cast<int64_t>(tail_starts.size()) + 1;
	int64_t min_line = resolved.MinLine();
	if (!tail_starts.empty() && min_line >= first_memo && min_line <= total_lines) {
		int64_t resume_offset = tail_starts[static_cast<idx_t>(min_line - first_memo)];
		file.Seek(static_cast<idx_t>(resume_offset));
		reader = make_uniq<BufferedLineReader>(file, resume_offset);
		line_number = min_line - 1;
	} else {
		file.Seek(0);
		reader = make_uniq<BufferedLineReader>(file);
		line_number = 0;
	}
	return resolved;
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
				state.resolved_selection = PrepareFromEndScan(*state.current_file, state.reader,
				                                              bind_data.line_selection, state.current_line_number);
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

// =============================================================================
// LineOutputWriter
//
// Writes the four output columns (line_number, content, byte_offset,
// file_path) straight into the chunk's vectors.
//
// The obvious `Vector::SetValue(row, Value(...))` costs a heap-allocated
// StringValueInfo per string cell and re-runs the UTF-8 check the scan has
// already done, and it did that four times per line -- on a 2M-line file that
// was more than half the scan. Writing the vectors directly costs one copy of
// the line into the chunk's string heap and nothing else.
//
// file_path is the same string for every line of a source, so it is added to
// the heap once per source rather than once per row. The heap is reset with
// the chunk, so the writer is per-invocation and re-adds the path each time.
// =============================================================================
class LineOutputWriter {
public:
	explicit LineOutputWriter(DataChunk &output)
	    : content_vector(output.data[1]), path_vector(output.data[3]),
	      line_numbers(FlatVector::GetData<int64_t>(output.data[0])),
	      contents(FlatVector::GetData<string_t>(output.data[1])),
	      byte_offsets(FlatVector::GetData<int64_t>(output.data[2])),
	      paths(FlatVector::GetData<string_t>(output.data[3])) {
	}

	// Point subsequent rows at `file_path`. `source_token` is anything that
	// changes when the source does (the file index, the lateral input row), so
	// that the path is only re-added when it actually differs.
	void SetSource(const string &file_path, idx_t source_token) {
		if (has_path && source_token == cached_token) {
			return;
		}
		// Building a Value used to reject a path that is not valid UTF-8; keep
		// rejecting it, but once per source instead of once per line.
		if (!Value::StringIsValid(file_path.c_str(), file_path.size())) {
			throw ErrorManager::InvalidUnicodeError(file_path, "value construction");
		}
		cached_path = StringVector::AddString(path_vector, file_path);
		cached_token = source_token;
		has_path = true;
	}

	void Write(idx_t row, int64_t line_number, const string &content, int64_t byte_offset) {
		line_numbers[row] = line_number;
		contents[row] = StringVector::AddString(content_vector, content);
		byte_offsets[row] = byte_offset;
		paths[row] = cached_path;
	}

private:
	Vector &content_vector;
	Vector &path_vector;
	int64_t *line_numbers;
	string_t *contents;
	int64_t *byte_offsets;
	string_t *paths;
	string_t cached_path;
	idx_t cached_token = 0;
	bool has_path = false;
};

static void ReadTextLinesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ReadTextLinesBindData>();
	auto &state = data_p.global_state->Cast<ReadTextLinesGlobalState>();

	idx_t output_row = 0;
	LineOutputWriter writer(output);

	while (output_row < STANDARD_VECTOR_SIZE) {
		if (state.file_finished) {
			if (!OpenNextFile(state, bind_data)) {
				break;
			}
		}
		writer.SetSource(state.current_file_path, state.file_index);

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

			writer.Write(output_row, state.current_line_number, ApplyLineTrim(line, bind_data.trim_mode),
			             line_start_offset);

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
	LineOutputWriter writer(output);

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
					state.resolved_selection = PrepareFromEndScan(*state.current_file, state.reader,
					                                              bind_data.line_selection, state.current_line_number);
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
		writer.SetSource(state.current_file_path, state.current_row);
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
			writer.Write(output_row, state.current_line_number, ApplyLineTrim(line, bind_data.trim_mode),
			             line_start_offset);

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
