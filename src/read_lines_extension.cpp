#define DUCKDB_EXTENSION_MAIN

#include "read_lines_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

// Forward declarations - defined in separate files
TableFunctionSet ReadLinesFunction();
TableFunctionSet ReadLinesLateralFunction();
TableFunction ParseLinesFunction();

void ReadLinesExtension::Load(ExtensionLoader &loader) {
	// Register read_lines table function
	loader.RegisterFunction(ReadLinesFunction());

	// Register read_lines_lateral for lateral join support
	loader.RegisterFunction(ReadLinesLateralFunction());

	// Register parse_lines table function
	loader.RegisterFunction(ParseLinesFunction());
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

// Use the new DuckDB C++ extension entry point (for loadable extension)
#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(read_lines, loader) {
	duckdb::ReadLinesExtension extension;
	extension.Load(loader);
}
}
#endif

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
