#define DUCKDB_EXTENSION_MAIN

#include "read_lines_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

// Forward declarations - defined in separate files
TableFunctionSet ReadLinesFunction();
TableFunctionSet ReadLinesLateralFunction();
TableFunction ParseLinesFunction();

void ReadLinesExtension::Load(ExtensionLoader &loader) {
	// Register read_lines table function
	{
		CreateTableFunctionInfo info(ReadLinesFunction());
		info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;

		FunctionDescription desc1;
		desc1.parameter_types = {LogicalType::VARCHAR};
		desc1.parameter_names = {"path"};
		desc1.description = "Read line-based text files with line numbers and subset extraction.";
		desc1.examples = {"SELECT * FROM read_lines('server.log')"};
		desc1.categories = {"read_lines"};
		info.descriptions.push_back(desc1);

		FunctionDescription desc2;
		desc2.parameter_types = {LogicalType::VARCHAR, LogicalType::ANY};
		desc2.parameter_names = {"path", "lines"};
		desc2.description = "Read selected lines from line-based text files.";
		desc2.examples = {"SELECT * FROM read_lines('server.log', '100-200')"};
		desc2.categories = {"read_lines"};
		info.descriptions.push_back(desc2);

		FunctionDescription desc3;
		desc3.parameter_types = {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY};
		desc3.parameter_names = {"path", "lines", "trim"};
		desc3.description = "Read selected lines with trimming mode from line-based text files.";
		desc3.examples = {"SELECT * FROM read_lines('server.log', '100-200', 'both')"};
		desc3.categories = {"read_lines"};
		info.descriptions.push_back(desc3);

		loader.RegisterFunction(std::move(info));
	}

	// Register read_lines_lateral for lateral join support
	{
		CreateTableFunctionInfo info(ReadLinesLateralFunction());
		info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;

		FunctionDescription desc1;
		desc1.parameter_types = {LogicalType::VARCHAR};
		desc1.parameter_names = {"path"};
		desc1.description = "Read lines in a correlated lateral join for paths from table columns.";
		desc1.examples = {"SELECT * FROM paths_table p, read_lines_lateral(p.path)"};
		desc1.categories = {"read_lines"};
		info.descriptions.push_back(desc1);

		FunctionDescription desc2;
		desc2.parameter_types = {LogicalType::VARCHAR, LogicalType::ANY};
		desc2.parameter_names = {"path", "lines"};
		desc2.description = "Read selected lines in a correlated lateral join.";
		desc2.examples = {"SELECT * FROM paths_table p, read_lines_lateral(p.path, '100-200')"};
		desc2.categories = {"read_lines"};
		info.descriptions.push_back(desc2);

		FunctionDescription desc3;
		desc3.parameter_types = {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY};
		desc3.parameter_names = {"path", "lines", "trim"};
		desc3.description = "Read selected lines with trimming mode in a correlated lateral join.";
		desc3.examples = {"SELECT * FROM paths_table p, read_lines_lateral(p.path, '100-200', 'both')"};
		desc3.categories = {"read_lines"};
		info.descriptions.push_back(desc3);

		loader.RegisterFunction(std::move(info));
	}

	// Register parse_lines table function
	{
		CreateTableFunctionInfo info(ParseLinesFunction());
		info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;

		FunctionDescription desc;
		desc.parameter_names = {"text"};
		desc.description = "Parse lines from a text string with line numbers and subset extraction.";
		desc.examples = {"SELECT * FROM parse_lines('hello\\nworld')"};
		desc.categories = {"read_lines"};
		info.descriptions.push_back(desc);

		loader.RegisterFunction(std::move(info));
	}
}

std::string ReadLinesExtension::Name() {
	return "read_lines";
}

std::string ReadLinesExtension::Version() const {
#ifdef EXT_VERSION_READ_LINES
	return EXT_VERSION_READ_LINES;
#else
	return "";
#endif
}

} // namespace duckdb

// Use the new DuckDB C++ extension entry point (for loadable extension).
//
// Deliberately NOT guarded on DUCKDB_BUILD_LOADABLE_EXTENSION. v1.5.x defines
// that macro from extension/extension_build_tools.cmake; v2.0-cyanoptera still
// reads it in duckdb.h but no longer defines it anywhere, so the guard silently
// compiled this block away and the extension exported no entry point. Linux and
// Windows linked anyway; wasm (-Wundefined -Werror) and macOS
// (-Wl,-exported_symbol) did not:
//
//   emcc: error: undefined exported symbol: "_read_lines_duckdb_cpp_init"
//   Undefined symbols for architecture arm64: "_read_lines_duckdb_cpp_init"
extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(read_lines, loader) {
	duckdb::ReadLinesExtension extension;
	extension.Load(loader);
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
