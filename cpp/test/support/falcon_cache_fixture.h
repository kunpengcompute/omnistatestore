#ifndef FALCON_TESTS_FALCON_CACHE_FIXTURE_H
#define FALCON_TESTS_FALCON_CACHE_FIXTURE_H

#include <gtest/gtest.h>

#include <jni.h>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>

#include <memory>
#include <string>
#include <vector>

#include "FalconCache.h"
#include "MockJniEnv.h"
#include "rocksdb_fixture.h"
#include "slice_helpers.h"

namespace falcon_test {

/**
 * Fixture base for tests that exercise FalconCache against a real RocksDB
 * instance. Inherits the temp-dir RocksDB setup from RocksDBFixture and adds
 * the cache instance, MockJniEnv, and a handful of common helpers
 * (CachePut / CacheGet / RocksDBPut / DirectRocksDBDelete / ArrayToString).
 *
 * Each test gets a fresh FalconCache so hitCnt / accessCnt always start at 0.
 */
class FalconCacheFixture : public RocksDBFixture {
protected:
    void SetUp() override {
        RocksDBFixture::SetUp();
        cache_ = std::make_unique<FalconCache>(/*hitThreshold=*/0.5);
    }

    void TearDown() override {
        // clearAll() before destroying the cache avoids any flush attempt into
        // a closed db. The FalconCache destructor also calls clearAll(), but
        // we do it explicitly first so we control ordering relative to db_.
        if (cache_) {
            cache_->clearAll();
            cache_.reset();
        }
        RocksDBFixture::TearDown();
    }

    // Cache helpers — every Slice handed in is heap-allocated via
    // make_owned_slice() so the cache's delete[] is safe.
    void CachePut(const std::string& key, const std::string& value) {
        cache_->put(jni_.env(), dbHandle(), cfHandle(), writeOptionsHandle(),
                    make_owned_slice(key), make_owned_slice(value));
    }

    jbyteArray CacheGet(const std::string& key) {
        return cache_->get(jni_.env(), dbHandle(), cfHandle(), writeOptionsHandle(),
                           make_owned_slice(key));
    }

    void CacheRemove(const std::string& key) {
        cache_->remove(jni_.env(), dbHandle(), cfHandle(), writeOptionsHandle(),
                       make_owned_slice(key));
    }

    // Convenience: cache.get + decode to std::string. "" for nullptr (miss).
    std::string CacheGet_String(const std::string& key) {
        jbyteArray r = CacheGet(key);
        return r == nullptr ? std::string() : ArrayToString(r);
    }

    // Read back the bytes of a jbyteArray as std::string.
    std::string ArrayToString(jbyteArray arr) {
        std::vector<jbyte> bytes = jni_.arrayBytes(arr);
        return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }

    // Direct RocksDB access (bypasses cache).
    std::string RocksDBGet(const std::string& key, bool* found = nullptr) {
        std::string value;
        rocksdb::Status s = db_->Get(rocksdb::ReadOptions(),
                                     db_->DefaultColumnFamily(),
                                     rocksdb::Slice(key),
                                     &value);
        if (found) *found = s.ok();
        if (s.IsNotFound()) return "";
        EXPECT_TRUE(s.ok()) << "Unexpected RocksDB status: " << s.ToString();
        return value;
    }

    void RocksDBPut(const std::string& key, const std::string& value) {
        rocksdb::Status s = db_->Put(write_options_,
                                     db_->DefaultColumnFamily(),
                                     rocksdb::Slice(key),
                                     rocksdb::Slice(value));
        ASSERT_TRUE(s.ok()) << "RocksDB put failed: " << s.ToString();
    }

    void DirectRocksDBDelete(const std::string& key) {
        rocksdb::Status s = db_->Delete(write_options_,
                                        db_->DefaultColumnFamily(),
                                        rocksdb::Slice(key));
        ASSERT_TRUE(s.ok()) << "RocksDB delete failed: " << s.ToString();
    }

    bool RocksDBHas(const std::string& key) {
        bool found = false;
        RocksDBGet(key, &found);
        return found;
    }

    MockJniEnv                   jni_;
    std::unique_ptr<FalconCache> cache_;
};

}  // namespace falcon_test

#endif  // FALCON_TESTS_FALCON_CACHE_FIXTURE_H
