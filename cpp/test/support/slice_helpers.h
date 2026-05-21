#ifndef FALCON_TESTS_SLICE_HELPERS_H
#define FALCON_TESTS_SLICE_HELPERS_H

#include <rocksdb/slice.h>

#include <cstring>
#include <string>
#include <string_view>

namespace falcon_test {

/**
 * Build a rocksdb::Slice that owns its backing buffer via `new char[]`.
 *
 * FalconCache's API is built around the convention that the caller hands
 * over heap-allocated key / value buffers (allocated with `new[]`) and the
 * cache itself runs `delete[]` later. Tests need to construct slices with
 * matching ownership semantics — this helper does exactly that.
 *
 * Don't `delete[]` the returned slice's data yourself; either pass it into
 * the cache (which will free it) or ensure the test path frees it.
 */
inline ROCKSDB_NAMESPACE::Slice make_owned_slice(std::string_view bytes) {
    char* data = new char[bytes.size() == 0 ? 1 : bytes.size()];
    if (!bytes.empty()) {
        std::memcpy(data, bytes.data(), bytes.size());
    }
    return ROCKSDB_NAMESPACE::Slice(data, bytes.size());
}

/** Convenience for `make_owned_slice` + std::string. */
inline ROCKSDB_NAMESPACE::Slice make_owned_slice(const std::string& s) {
    return make_owned_slice(std::string_view(s));
}

/** Free a slice's `new[]`-allocated buffer (mirrors what FalconCache does). */
inline void free_owned_slice(ROCKSDB_NAMESPACE::Slice& slice) {
    delete[] slice.data_;
    slice.data_ = nullptr;
}

}  // namespace falcon_test

#endif  // FALCON_TESTS_SLICE_HELPERS_H
