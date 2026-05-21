/*
 * Tests for FalconValueStateJNI.cpp — the 8 extern "C" Java_* entry points
 * that Flink's Java side calls into. Coverage focus: handle-cast happy path,
 * null-handle exception path, and (where applicable) jbyteArray-extraction
 * exception path via MockJniEnv::injectByteArrayFailureAtCall(N).
 *
 * The fixture builds a real RocksDB + a fresh FalconCache + MockJniEnv per
 * test; each test then drives the Java_* symbol directly. We pass the
 * cache pointer back as `falconHandle` (just like Java would), and feed
 * keys/values via mock byte arrays so GetByteArrayRegion in production
 * code reads real bytes.
 */

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "FalconCache.h"
#include "MockJniEnv.h"
#include "com_huawei_falcon_state_cache_FalconValueState.h"
#include "falcon_cache_fixture.h"

namespace {

using falcon_test::FalconCacheFixture;
using falcon_test::MockJniEnv;

// We can't use FalconCacheFixture directly because it owns the cache too.
// For these tests we want to drive the JNI path through `initFalconCache`
// + receive the handle back, then destroy via `destroyFalconCache`. So we
// inherit RocksDBFixture (DB only) and let the JNI entry points construct
// / destruct the cache themselves.
class FalconValueStateJniTest : public falcon_test::RocksDBFixture {
protected:
    MockJniEnv jni_;

    // Build a mock jbyteArray holding `bytes`.
    jbyteArray makeJBytes(const std::string& bytes) {
        jbyteArray arr = jni_.env()->NewByteArray(static_cast<jsize>(bytes.size()));
        if (!bytes.empty()) {
            jni_.env()->SetByteArrayRegion(arr, 0, static_cast<jsize>(bytes.size()),
                reinterpret_cast<const jbyte*>(bytes.data()));
        }
        return arr;
    }

    std::string arrayBytesToString(jbyteArray arr) {
        std::vector<jbyte> bytes = jni_.arrayBytes(arr);
        return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }
};

// ---------------------------------------------------------------------------
// initFalconCache + destroyFalconCache
// ---------------------------------------------------------------------------

TEST_F(FalconValueStateJniTest, InitFalconCache_ReturnsNonNullHandle) {
    jlong h = Java_com_huawei_falcon_state_cache_FalconValueState_initFalconCache(
        jni_.env(), /*obj=*/nullptr, /*cacheBypassThreshold=*/0.5);
    EXPECT_NE(h, 0);

    // Clean up.
    Java_com_huawei_falcon_state_cache_FalconValueState_destroyFalconCache(
        jni_.env(), nullptr, h);
}

TEST_F(FalconValueStateJniTest, DestroyFalconCache_NullHandle_NoCrash) {
    // delete on a nullptr is well-defined no-op. The implementation should
    // not crash on a 0 handle.
    Java_com_huawei_falcon_state_cache_FalconValueState_destroyFalconCache(
        jni_.env(), nullptr, /*falconHandle=*/0);
    // No assertion — just confirms we got here.
    EXPECT_FALSE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// get
// ---------------------------------------------------------------------------

TEST_F(FalconValueStateJniTest, Get_HappyPath_ReturnsValueFromRocksDB) {
    jlong cache_h = Java_com_huawei_falcon_state_cache_FalconValueState_initFalconCache(
        jni_.env(), nullptr, 0.5);
    auto* cache = reinterpret_cast<FalconCache*>(cache_h);
    cache->updateSizeLimit(jni_.env(), dbHandle(), cfHandle(), writeOptionsHandle(), 100);

    // Pre-populate RocksDB so get() finds it via cache miss → rocksdb_get path.
    rocksdb::Status s = db_->Put(write_options_, db_->DefaultColumnFamily(),
                                 rocksdb::Slice("k"), rocksdb::Slice("v"));
    ASSERT_TRUE(s.ok());

    jbyteArray jKey = makeJBytes("k");
    jbyteArray result = Java_com_huawei_falcon_state_cache_FalconValueState_get(
        jni_.env(), nullptr, cache_h, dbHandle(), cfHandle(), writeOptionsHandle(),
        jKey, /*offset=*/0, /*len=*/1);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(arrayBytesToString(result), "v");
    EXPECT_FALSE(jni_.hasPendingException());

    Java_com_huawei_falcon_state_cache_FalconValueState_destroyFalconCache(
        jni_.env(), nullptr, cache_h);
}

TEST_F(FalconValueStateJniTest, Get_NullFalconHandle_ThrowsException) {
    jbyteArray jKey = makeJBytes("k");
    jbyteArray result = Java_com_huawei_falcon_state_cache_FalconValueState_get(
        jni_.env(), nullptr, /*falconHandle=*/0, dbHandle(), cfHandle(),
        writeOptionsHandle(), jKey, 0, 1);
    EXPECT_EQ(result, nullptr);
    EXPECT_TRUE(jni_.hasPendingException());
}

TEST_F(FalconValueStateJniTest, Get_GetByteArrayRegionThrows_ReturnsNullCleanly) {
    jlong cache_h = Java_com_huawei_falcon_state_cache_FalconValueState_initFalconCache(
        jni_.env(), nullptr, 0.5);
    jbyteArray jKey = makeJBytes("k");

    // injectByteArrayFailureAtCall counts from "now" (resets internal counter),
    // so 1 = the very next byte-array call. The first thing Java_..._get does
    // is env->GetByteArrayRegion(jKey, ...) — that's the call we want to fail.
    jni_.injectByteArrayFailureAtCall(1);
    jbyteArray result = Java_com_huawei_falcon_state_cache_FalconValueState_get(
        jni_.env(), nullptr, cache_h, dbHandle(), cfHandle(), writeOptionsHandle(),
        jKey, 0, 1);
    EXPECT_EQ(result, nullptr);
    EXPECT_TRUE(jni_.hasPendingException());

    jni_.clearPendingException();
    Java_com_huawei_falcon_state_cache_FalconValueState_destroyFalconCache(
        jni_.env(), nullptr, cache_h);
}

// ---------------------------------------------------------------------------
// put
// ---------------------------------------------------------------------------

TEST_F(FalconValueStateJniTest, Put_HappyPath_StoredAndReadable) {
    jlong cache_h = Java_com_huawei_falcon_state_cache_FalconValueState_initFalconCache(
        jni_.env(), nullptr, 0.5);
    auto* cache = reinterpret_cast<FalconCache*>(cache_h);
    cache->updateSizeLimit(jni_.env(), dbHandle(), cfHandle(), writeOptionsHandle(), 100);

    jbyteArray jKey = makeJBytes("pk");
    jbyteArray jVal = makeJBytes("pv");
    Java_com_huawei_falcon_state_cache_FalconValueState_put(
        jni_.env(), nullptr, cache_h, dbHandle(), cfHandle(), writeOptionsHandle(),
        jKey, 0, 2, jVal, 0, 2);
    EXPECT_FALSE(jni_.hasPendingException());

    // Read back via cache.get to confirm it's there.
    jbyteArray jKey2 = makeJBytes("pk");
    jbyteArray result = Java_com_huawei_falcon_state_cache_FalconValueState_get(
        jni_.env(), nullptr, cache_h, dbHandle(), cfHandle(), writeOptionsHandle(),
        jKey2, 0, 2);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(arrayBytesToString(result), "pv");

    Java_com_huawei_falcon_state_cache_FalconValueState_destroyFalconCache(
        jni_.env(), nullptr, cache_h);
}

TEST_F(FalconValueStateJniTest, Put_NullFalconHandle_ThrowsException) {
    jbyteArray jKey = makeJBytes("k");
    jbyteArray jVal = makeJBytes("v");
    Java_com_huawei_falcon_state_cache_FalconValueState_put(
        jni_.env(), nullptr, /*falconHandle=*/0, dbHandle(), cfHandle(),
        writeOptionsHandle(), jKey, 0, 1, jVal, 0, 1);
    EXPECT_TRUE(jni_.hasPendingException());
}

TEST_F(FalconValueStateJniTest, Put_ValueGetByteArrayRegionThrows_DropsKeyBuffer) {
    jlong cache_h = Java_com_huawei_falcon_state_cache_FalconValueState_initFalconCache(
        jni_.env(), nullptr, 0.5);
    jbyteArray jKey = makeJBytes("kk");
    jbyteArray jVal = makeJBytes("vv");

    // injectByteArrayFailureAtCall counts from "now". Inside Java_..._put the
    // first byte-array call is GetByteArrayRegion(key); the second is
    // GetByteArrayRegion(value). We want the value extraction to fail so the
    // key buffer cleanup path runs.
    jni_.injectByteArrayFailureAtCall(2);
    Java_com_huawei_falcon_state_cache_FalconValueState_put(
        jni_.env(), nullptr, cache_h, dbHandle(), cfHandle(), writeOptionsHandle(),
        jKey, 0, 2, jVal, 0, 2);
    EXPECT_TRUE(jni_.hasPendingException());

    jni_.clearPendingException();
    Java_com_huawei_falcon_state_cache_FalconValueState_destroyFalconCache(
        jni_.env(), nullptr, cache_h);
}

// ---------------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------------

TEST_F(FalconValueStateJniTest, Delete_HappyPath_RemovesKey) {
    jlong cache_h = Java_com_huawei_falcon_state_cache_FalconValueState_initFalconCache(
        jni_.env(), nullptr, 0.5);
    auto* cache = reinterpret_cast<FalconCache*>(cache_h);
    cache->updateSizeLimit(jni_.env(), dbHandle(), cfHandle(), writeOptionsHandle(), 100);

    // Pre-populate RocksDB.
    rocksdb::Status s = db_->Put(write_options_, db_->DefaultColumnFamily(),
                                 rocksdb::Slice("dk"), rocksdb::Slice("dv"));
    ASSERT_TRUE(s.ok());

    jbyteArray jKey = makeJBytes("dk");
    Java_com_huawei_falcon_state_cache_FalconValueState_delete(
        jni_.env(), nullptr, cache_h, dbHandle(), cfHandle(), writeOptionsHandle(),
        jKey, 0, 2);
    EXPECT_FALSE(jni_.hasPendingException());

    // Confirm gone.
    std::string value;
    rocksdb::Status g = db_->Get(rocksdb::ReadOptions(), db_->DefaultColumnFamily(),
                                 rocksdb::Slice("dk"), &value);
    EXPECT_TRUE(g.IsNotFound());

    Java_com_huawei_falcon_state_cache_FalconValueState_destroyFalconCache(
        jni_.env(), nullptr, cache_h);
}

TEST_F(FalconValueStateJniTest, Delete_NullFalconHandle_ThrowsException) {
    jbyteArray jKey = makeJBytes("k");
    Java_com_huawei_falcon_state_cache_FalconValueState_delete(
        jni_.env(), nullptr, /*falconHandle=*/0, dbHandle(), cfHandle(),
        writeOptionsHandle(), jKey, 0, 1);
    EXPECT_TRUE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// getCacheSizeLimit / setCacheSizeLimit / flush
// ---------------------------------------------------------------------------

TEST_F(FalconValueStateJniTest, SizeLimit_GetThenSet_ReflectsLastSet) {
    jlong cache_h = Java_com_huawei_falcon_state_cache_FalconValueState_initFalconCache(
        jni_.env(), nullptr, 0.5);

    EXPECT_EQ(Java_com_huawei_falcon_state_cache_FalconValueState_getCacheSizeLimit(
                  jni_.env(), nullptr, cache_h),
              0);

    Java_com_huawei_falcon_state_cache_FalconValueState_setCacheSizeLimit(
        jni_.env(), nullptr, cache_h, dbHandle(), cfHandle(), writeOptionsHandle(),
        /*newSizeLimit=*/42);
    EXPECT_EQ(Java_com_huawei_falcon_state_cache_FalconValueState_getCacheSizeLimit(
                  jni_.env(), nullptr, cache_h),
              42);

    Java_com_huawei_falcon_state_cache_FalconValueState_destroyFalconCache(
        jni_.env(), nullptr, cache_h);
}

TEST_F(FalconValueStateJniTest, GetCacheSizeLimit_NullHandle_ThrowsAndReturnsZero) {
    jint r = Java_com_huawei_falcon_state_cache_FalconValueState_getCacheSizeLimit(
        jni_.env(), nullptr, /*falconHandle=*/0);
    EXPECT_EQ(r, 0);
    EXPECT_TRUE(jni_.hasPendingException());
}

TEST_F(FalconValueStateJniTest, SetCacheSizeLimit_NullHandle_Throws) {
    Java_com_huawei_falcon_state_cache_FalconValueState_setCacheSizeLimit(
        jni_.env(), nullptr, /*falconHandle=*/0, dbHandle(), cfHandle(),
        writeOptionsHandle(), 7);
    EXPECT_TRUE(jni_.hasPendingException());
}

TEST_F(FalconValueStateJniTest, Flush_HappyPath_PersistsAndClearsCache) {
    jlong cache_h = Java_com_huawei_falcon_state_cache_FalconValueState_initFalconCache(
        jni_.env(), nullptr, 0.5);
    auto* cache = reinterpret_cast<FalconCache*>(cache_h);
    cache->updateSizeLimit(jni_.env(), dbHandle(), cfHandle(), writeOptionsHandle(), 100);

    // Put one key via the JNI put entry, then flush.
    jbyteArray jKey = makeJBytes("fk");
    jbyteArray jVal = makeJBytes("fv");
    Java_com_huawei_falcon_state_cache_FalconValueState_put(
        jni_.env(), nullptr, cache_h, dbHandle(), cfHandle(), writeOptionsHandle(),
        jKey, 0, 2, jVal, 0, 2);
    Java_com_huawei_falcon_state_cache_FalconValueState_flush(
        jni_.env(), nullptr, cache_h, dbHandle(), cfHandle(), writeOptionsHandle());

    // Confirm RocksDB has it.
    std::string value;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), db_->DefaultColumnFamily(),
                                 rocksdb::Slice("fk"), &value);
    ASSERT_TRUE(s.ok());
    EXPECT_EQ(value, "fv");

    Java_com_huawei_falcon_state_cache_FalconValueState_destroyFalconCache(
        jni_.env(), nullptr, cache_h);
}

TEST_F(FalconValueStateJniTest, Flush_NullHandle_Throws) {
    Java_com_huawei_falcon_state_cache_FalconValueState_flush(
        jni_.env(), nullptr, /*falconHandle=*/0, dbHandle(), cfHandle(),
        writeOptionsHandle());
    EXPECT_TRUE(jni_.hasPendingException());
}

}  // namespace
