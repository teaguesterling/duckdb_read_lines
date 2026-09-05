#pragma once

#include "duckdb.hpp"
#include <type_traits>

// duckdb_compat.hpp — fleet-standard cross-version shim for DuckDB extensions.
//
// Pattern established by @bendrucker in teaguesterling/duckdb_webbed#76 (May 2026):
// detect the new API via __has_include of headers that moved in the same DuckDB
// refactor ([duckdb/duckdb#22377](https://github.com/duckdb/duckdb/pull/22377) —
// "mandatory per-vector size tracking" landed alongside the vector-buffer header
// reshuffle), then dispatch via a single #ifdef block.
//
// Cross-version coverage:
//   - duckdb v1.4.x / v1.5.x: old API everywhere
//   - duckdb main / v1.6.x:   new API everywhere
//
// See teaguesterling/duckdb_markdown's docs/DUCKDB_API_MIGRATION.md for the
// long-form rationale + upgrade checklist for other extensions.

#if __has_include("duckdb/common/vector/list_vector.hpp")
#define DUCKDB_HAS_NEW_VECTOR_HEADERS 1
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#endif

// duckdb::Identifier replaced std::string as the name type in table-function and
// COPY bind signatures on the v2.0 line. Identifier compares case-insensitively,
// and construction from a RUNTIME string is explicit by design -- promoting a
// string to an identifier is meant to be a deliberate act at the call site -- so
// a boundary helper is needed rather than an implicit conversion.
//
// This probe gates ONLY whether an Identifier overload of CompatNameStr can be
// declared. It deliberately does NOT decide CompatName -- see below.
#if __has_include("duckdb/common/identifier.hpp")
#define DUCKDB_HAS_IDENTIFIER 1
#include "duckdb/common/identifier.hpp"
#endif

#include "duckdb/function/table_function.hpp"
#include <utility>

namespace duckdb {

// --- bind-signature name type -------------------------------------------------
// Used wherever a bind callback receives or fills a vector of column names.
//
// DERIVED FROM DUCKDB, NOT PROBED FOR. `__has_include(identifier.hpp)` is the
// obvious test and it is WRONG here, because identifier.hpp was backported to
// the stable branch WITHOUT changing table_function_bind_t. Verified against
// three heads:
//
//   v1.5-variegata @ b155d6f63c (our pin)  no identifier.hpp   bind: vector<string>
//   v1.5-variegata @ branch tip            HAS identifier.hpp  bind: vector<string>
//   main (v2.0)                            HAS identifier.hpp  bind: vector<Identifier>
//
// So the header probe is right today only by the accident that our pin predates
// the backport; the next routine submodule bump would flip CompatName to
// Identifier on a DuckDB that still wants strings, and every bind signature in
// this extension would stop compiling at once.
//
// TableFunctionBindInput::input_table_names has the same element type as the
// bind out-parameter on both lines (table_function.hpp :110/:288 on the pin,
// :123/:318 on main), so asking DuckDB what its own name type IS cannot drift --
// it is the very thing that changed.
using CompatName = typename std::remove_reference<decltype(
    std::declval<TableFunctionBindInput &>().input_table_names)>::type::value_type;

// String LITERALS convert implicitly on both lines (Identifier's const char*
// constructor is intentionally implicit), so `names.push_back("content")` is
// unchanged -- only the signatures and the RUNTIME-string boundaries move.
inline string CompatNameStr(const string &name) {
	return name;
}
#ifdef DUCKDB_HAS_IDENTIFIER
// Not ambiguous with the overload above even when both exist and CompatName is
// still string: Identifier's constructor from string is explicit, so a string
// argument has exactly one viable candidate.
inline string CompatNameStr(const Identifier &id) {
	return id.GetIdentifierName();
}
#endif

inline CompatName CompatMakeName(string name) {
	return CompatName(std::move(name));
}

// --- LogicalType alias ---------------------------------------------------------
// v1.5: void SetAlias(string)               -- mutates in place
// v2.0: LogicalType WithAlias(string) const -- returns a copy, never mutating a
//       type whose type-info is shared. SetAlias is REMOVED, not deprecated.
//
// Detected by PROBING for the member rather than by a version macro.
//
// NOTE ON DIALECT: the fleet reference shim (duckdb_markdown) writes these as
// `if constexpr`, which is C++17. That extension sets CXX_STANDARD 17 on its own
// targets; this one does not -- it inherits DuckDB's `CMAKE_CXX_STANDARD 11`, so
// `if constexpr` here is only a GCC/Clang extension (-Wc++17-extensions) and MSVC
// rejects it outright. The same discard-the-untaken-branch effect is obtained in
// C++11 by TAG DISPATCH: the impl overload that is not selected is declared but
// never instantiated, so its body is never type-checked against the wrong API.
// This is the idiom this repo's own compat.hpp already uses.
template <class T, class = void>
struct CompatHasWithAlias : std::false_type {};
template <class T>
struct CompatHasWithAlias<T, decltype(void(std::declval<const T &>().WithAlias(string())))> : std::true_type {};

template <class TYPE>
inline LogicalType CompatWithAliasImpl(TYPE type, string alias, std::true_type) {
	return type.WithAlias(std::move(alias)); // v2.0: returns a copy
}
template <class TYPE>
inline LogicalType CompatWithAliasImpl(TYPE type, string alias, std::false_type) {
	type.SetAlias(std::move(alias)); // v1.5: mutates in place
	return type;
}

// The ENTRY POINT is deliberately NOT a template. A `template <class TYPE =
// LogicalType>` form looks equivalent but is not: the default template argument
// is inert because deduction wins, so the very common call
//
//     CompatWithAlias(LogicalType::VARCHAR, "md")
//
// deduces TYPE = LogicalTypeId -- LogicalType::VARCHAR is a static constexpr
// LogicalTypeId, not a LogicalType -- and hard-errors inside the shim with
// "request for member 'SetAlias' in 'type', which is of non-class type
// 'duckdb::LogicalTypeId'", on the PINNED build. A concrete parameter restores
// the implicit LogicalTypeId -> LogicalType conversion at the call site. Only
// the Impl overloads stay templated, which is all the tag dispatch needs.
inline LogicalType CompatWithAlias(LogicalType type, string alias) {
	return CompatWithAliasImpl(std::move(type), std::move(alias), CompatHasWithAlias<LogicalType>());
}

// --- Vector::ToUnifiedFormat ---------------------------------------------------
// v1.5: ToUnifiedFormat(count, data)  -- the only overload
// v2.0: ToUnifiedFormat(data)         -- plus the count form kept as [[deprecated]]
//
// PROBE FOR THE COUNT-FREE OVERLOAD, not the count-taking one. v2.0 did not
// remove the count form, it deprecated it, so a probe for the count form is true
// on BOTH versions and the shim would always take the deprecated path -- silently
// never reaching the new API it exists to call. The count-free form is the one
// that exists only on v2.0, so it is the one that discriminates.
template <class T, class = void>
struct CompatToUnifiedWithoutCount : std::false_type {};
template <class T>
struct CompatToUnifiedWithoutCount<T, decltype(void(std::declval<T &>().ToUnifiedFormat(
                                          std::declval<UnifiedVectorFormat &>())))> : std::true_type {};

template <class VEC>
inline void CompatToUnifiedFormatImpl(VEC &vec, idx_t, UnifiedVectorFormat &data, std::true_type) {
	vec.ToUnifiedFormat(data);
}
template <class VEC>
inline void CompatToUnifiedFormatImpl(VEC &vec, idx_t count, UnifiedVectorFormat &data, std::false_type) {
	vec.ToUnifiedFormat(count, data);
}

template <class VEC = Vector>
inline void CompatToUnifiedFormat(VEC &vec, idx_t count, UnifiedVectorFormat &data) {
	CompatToUnifiedFormatImpl(vec, count, data, CompatToUnifiedWithoutCount<VEC>());
}

// --- FlatVector mutable data ---------------------------------------------------
// v1.5: FlatVector::GetData<T>(vec)         returns T*
// v2.0: FlatVector::GetData<T>(vec)         returns const T*
//       FlatVector::GetDataMutable<T>(vec)  returns T*
// Writing through the v2.0 GetData is a compile error, which is the point of the
// split -- so the WRITE path must ask for mutability explicitly.
template <class T, class = void>
struct CompatHasFlatGetDataMutable : std::false_type {};
template <class T>
struct CompatHasFlatGetDataMutable<T, decltype(void(T::template GetDataMutable<bool>(std::declval<Vector &>())))>
    : std::true_type {};

template <class VALUE, class FV>
inline VALUE *CompatFlatDataMutableImpl(Vector &vec, std::true_type) {
	return FV::template GetDataMutable<VALUE>(vec);
}
template <class VALUE, class FV>
inline VALUE *CompatFlatDataMutableImpl(Vector &vec, std::false_type) {
	return FV::template GetData<VALUE>(vec);
}

template <class VALUE, class FV = FlatVector>
inline VALUE *CompatFlatDataMutable(Vector &vec) {
	return CompatFlatDataMutableImpl<VALUE, FV>(vec, CompatHasFlatGetDataMutable<FV>());
}

#ifdef DUCKDB_HAS_NEW_VECTOR_HEADERS

// --- Output chunk finalization ---
// DuckDB main mandates per-vector Size() tracking; DataChunk::SetCardinality only
// updates chunk.count. SetChildCardinality additionally calls FlatVector::SetSize
// on every column so query operators reading vec.Size() see the right value.
// Without this, VariadicExecutor (and similar) reports:
//   "Mismatch in input vector sizes ... expected 0 rows but got N"
inline void CompatSetOutputCardinality(DataChunk &chunk, idx_t count) {
	chunk.SetChildCardinality(count);
}

#else // Old API (v1.4.x / v1.5.x)

inline void CompatSetOutputCardinality(DataChunk &chunk, idx_t count) {
	chunk.SetCardinality(count);
}

#endif

} // namespace duckdb
