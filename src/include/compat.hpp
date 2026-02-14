#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/enums/file_glob_options.hpp"
#include "duckdb/common/open_file_info.hpp"

namespace duckdb {

// Compatibility layer for GlobFiles API changes
// Older DuckDB: GlobFiles(path, context, FileGlobInput)
// Newer DuckDB: GlobFiles(path, FileGlobInput)

namespace compat {

// C++11-compatible void_t implementation
template <typename...>
struct void_t_impl {
	typedef void type;
};

template <typename... T>
using void_t = typename void_t_impl<T...>::type;

// SFINAE helper to detect new GlobFiles signature (2 args without ClientContext)
template <typename FS, typename = void>
struct has_new_glob_api : std::false_type {};

template <typename FS>
struct has_new_glob_api<FS, void_t<decltype(std::declval<FS>().GlobFiles(std::declval<const string &>(),
                                                                         std::declval<const FileGlobInput &>()))>>
    : std::true_type {};

// New API (2 args): GlobFiles(path, FileGlobInput)
template <typename FS>
typename std::enable_if<has_new_glob_api<FS>::value, vector<OpenFileInfo>>::type
GlobFilesCompat(FS &fs, const string &path, ClientContext & /*context*/, FileGlobOptions options) {
	return fs.GlobFiles(path, FileGlobInput(options));
}

// Old API (3 args): GlobFiles(path, context, FileGlobInput)
template <typename FS>
typename std::enable_if<!has_new_glob_api<FS>::value, vector<OpenFileInfo>>::type
GlobFilesCompat(FS &fs, const string &path, ClientContext &context, FileGlobOptions options) {
	return fs.GlobFiles(path, context, FileGlobInput(options));
}

} // namespace compat

} // namespace duckdb
