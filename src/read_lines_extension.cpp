#define DUCKDB_EXTENSION_MAIN

#include "read_lines_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Forward declarations - defined in separate files
TableFunction ReadTextLinesFunction();
TableFunction ReadTextLinesLateralFunction();
TableFunction ParseTextLinesFunction();

void ReadLinesExtension::Load(ExtensionLoader &loader) {
    // Register read_text_lines table function
    loader.RegisterFunction(ReadTextLinesFunction());

    // Register read_text_lines_lateral for lateral join support
    loader.RegisterFunction(ReadTextLinesLateralFunction());

    // Register parse_text_lines table function
    loader.RegisterFunction(ParseTextLinesFunction());
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

extern "C" {

DUCKDB_EXTENSION_API void read_lines_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadStaticExtension<duckdb::ReadLinesExtension>();
}

DUCKDB_EXTENSION_API const char *read_lines_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
