#include <gtest/gtest.h>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <algorithm>
#include <random>
#include <string>
#include <vector>

#include "FalconRocksDBHelper.h"
#include "MockJniEnv.h"
#include "rocksdb_fixture.h"

namespace {

using falcon_test::MockJniEnv;

// Re-export the shared fixture under the legacy test class name so the
// existing TEST_F(RocksDBHelperTest, ...) cases keep their identifiers.
class RocksDBHelperTest : public falcon_test::RocksDBFixture {};

// ---------------------------------------------------------------------------
// Baseline: get on a key that was never written returns nullptr, no exception.
// ---------------------------------------------------------------------------
TEST_F(RocksDBHelperTest, GetReturnsNullForMissingKey) {
    MockJniEnv jni;
    const std::string key = "missing";
    rocksdb::Slice key_slice(key.data(), key.size());

    jbyteArray result = rocksdb_get(jni.env(), dbHandle(), cfHandle(), key_slice);
    EXPECT_EQ(result, nullptr);
    EXPECT_FALSE(jni.hasPendingException());
}

// ---------------------------------------------------------------------------
// 1. PutThenGet_RoundTrip
//    rocksdb_put followed by rocksdb_get must return exact original bytes.
// ---------------------------------------------------------------------------
TEST_F(RocksDBHelperTest, PutThenGet_RoundTrip) {
    MockJniEnv jni;
    const std::string key = "rtrip_key";
    const std::string value = "rtrip_value";
    rocksdb::Slice key_slice(key.data(), key.size());
    rocksdb::Slice val_slice(value.data(), value.size());

    rocksdb_put(jni.env(), dbHandle(), cfHandle(), writeOptionsHandle(),
                key_slice, val_slice);
    ASSERT_FALSE(jni.hasPendingException()) << "put should not throw";

    jbyteArray result = rocksdb_get(jni.env(), dbHandle(), cfHandle(), key_slice);
    ASSERT_NE(result, nullptr) << "get after put must not return nullptr";
    EXPECT_FALSE(jni.hasPendingException());

    std::vector<jbyte> got = jni.arrayBytes(result);
    ASSERT_EQ(got.size(), value.size());
    EXPECT_TRUE(std::equal(got.begin(), got.end(),
                           reinterpret_cast<const jbyte*>(value.data())));
}

// ---------------------------------------------------------------------------
// 2. DeleteRemovesKey
//    put → delete → get returns nullptr, no exception.
// ---------------------------------------------------------------------------
TEST_F(RocksDBHelperTest, DeleteRemovesKey) {
    MockJniEnv jni;
    const std::string key = "del_key";
    const std::string value = "del_value";
    rocksdb::Slice key_slice(key.data(), key.size());
    rocksdb::Slice val_slice(value.data(), value.size());

    rocksdb_put(jni.env(), dbHandle(), cfHandle(), writeOptionsHandle(),
                key_slice, val_slice);
    ASSERT_FALSE(jni.hasPendingException());

    rocksdb_delete(jni.env(), dbHandle(), cfHandle(), writeOptionsHandle(),
                   key_slice);
    ASSERT_FALSE(jni.hasPendingException()) << "delete should not throw";

    jbyteArray result = rocksdb_get(jni.env(), dbHandle(), cfHandle(), key_slice);
    EXPECT_EQ(result, nullptr) << "key must be gone after delete";
    EXPECT_FALSE(jni.hasPendingException()) << "not-found is not an exception";
}

// ---------------------------------------------------------------------------
// 3. GetForExistingKey_ReturnsCorrectBytes
//    Bypass rocksdb_put and write directly via the underlying DB handle to
//    isolate rocksdb_get from rocksdb_put.
// ---------------------------------------------------------------------------
TEST_F(RocksDBHelperTest, GetForExistingKey_ReturnsCorrectBytes) {
    MockJniEnv jni;
    const std::string key = "direct_key";
    const std::string value = "direct_value";

    // Write directly through the RocksDB handle (not via the helper).
    rocksdb::Status s =
        db_->Put(write_options_, db_->DefaultColumnFamily(), key, value);
    ASSERT_TRUE(s.ok()) << "direct Put failed: " << s.ToString();

    rocksdb::Slice key_slice(key.data(), key.size());
    jbyteArray result = rocksdb_get(jni.env(), dbHandle(), cfHandle(), key_slice);
    ASSERT_NE(result, nullptr);
    EXPECT_FALSE(jni.hasPendingException());

    std::vector<jbyte> got = jni.arrayBytes(result);
    ASSERT_EQ(got.size(), value.size());
    EXPECT_TRUE(std::equal(got.begin(), got.end(),
                           reinterpret_cast<const jbyte*>(value.data())));
}

// ---------------------------------------------------------------------------
// 4. GetWithNullCfHandle_ThrowsFalconException
//    cfHandle == 0 must throw a Falcon exception (nullptr return + pending ex).
// ---------------------------------------------------------------------------
TEST_F(RocksDBHelperTest, GetWithNullCfHandle_ThrowsFalconException) {
    MockJniEnv jni;
    const std::string key = "key";
    rocksdb::Slice key_slice(key.data(), key.size());

    jbyteArray result =
        rocksdb_get(jni.env(), dbHandle(), /*cfHandle=*/0, key_slice);
    EXPECT_EQ(result, nullptr);
    EXPECT_TRUE(jni.hasPendingException())
        << "null cfHandle must set a pending exception";
}

// ---------------------------------------------------------------------------
// 5. GetWithNullDbHandle_ThrowsFalconException
// ---------------------------------------------------------------------------
TEST_F(RocksDBHelperTest, GetWithNullDbHandle_ThrowsFalconException) {
    MockJniEnv jni;
    const std::string key = "key";
    rocksdb::Slice key_slice(key.data(), key.size());

    jbyteArray result =
        rocksdb_get(jni.env(), /*rocksdbHandle=*/0, cfHandle(), key_slice);
    EXPECT_EQ(result, nullptr);
    EXPECT_TRUE(jni.hasPendingException())
        << "null dbHandle must set a pending exception";
}

// ---------------------------------------------------------------------------
// 6. PutWithNullCfHandle_ThrowsFalconException
// ---------------------------------------------------------------------------
TEST_F(RocksDBHelperTest, PutWithNullCfHandle_ThrowsFalconException) {
    MockJniEnv jni;
    const std::string key = "key";
    const std::string value = "value";
    rocksdb::Slice key_slice(key.data(), key.size());
    rocksdb::Slice val_slice(value.data(), value.size());

    rocksdb_put(jni.env(), dbHandle(), /*cfHandle=*/0, writeOptionsHandle(),
                key_slice, val_slice);
    EXPECT_TRUE(jni.hasPendingException())
        << "null cfHandle must set a pending exception on put";
}

// ---------------------------------------------------------------------------
// 7. DeleteWithNullCfHandle_ThrowsFalconException
// ---------------------------------------------------------------------------
TEST_F(RocksDBHelperTest, DeleteWithNullCfHandle_ThrowsFalconException) {
    MockJniEnv jni;
    const std::string key = "key";
    rocksdb::Slice key_slice(key.data(), key.size());

    rocksdb_delete(jni.env(), dbHandle(), /*cfHandle=*/0, writeOptionsHandle(),
                   key_slice);
    EXPECT_TRUE(jni.hasPendingException())
        << "null cfHandle must set a pending exception on delete";
}

// ---------------------------------------------------------------------------
// 7b/7c. Null writeOptionsHandle on put / delete must throw a Falcon exception.
//        Production code's three-way guard checks `write_options != nullptr`
//        as well — this fills the coverage gap the implementer flagged.
// ---------------------------------------------------------------------------
TEST_F(RocksDBHelperTest, PutWithNullWriteOptionsHandle_ThrowsFalconException) {
    MockJniEnv jni;
    const std::string key = "k";
    const std::string value = "v";
    rocksdb::Slice key_slice(key.data(), key.size());
    rocksdb::Slice val_slice(value.data(), value.size());

    rocksdb_put(jni.env(), dbHandle(), cfHandle(), /*writeOptionsHandle=*/0,
                key_slice, val_slice);
    EXPECT_TRUE(jni.hasPendingException())
        << "null writeOptionsHandle must set a pending exception on put";
}

TEST_F(RocksDBHelperTest, DeleteWithNullWriteOptionsHandle_ThrowsFalconException) {
    MockJniEnv jni;
    const std::string key = "k";
    rocksdb::Slice key_slice(key.data(), key.size());

    rocksdb_delete(jni.env(), dbHandle(), cfHandle(), /*writeOptionsHandle=*/0,
                   key_slice);
    EXPECT_TRUE(jni.hasPendingException())
        << "null writeOptionsHandle must set a pending exception on delete";
}

// ---------------------------------------------------------------------------
// 8. PutLargeValue_RoundTrip (renamed from old MAX_JARRAY_SIZE-claiming variant)
//
//    Real coverage of the 2 GiB MAX_JARRAY_SIZE branch in
//    JniUtil::createJavaByteArrayWithSizeCheck would require ~2 GiB of memory
//    or a custom mock; both are out of scope here. This test instead validates
//    that the 2 MiB common-large-value path round-trips cleanly: byte-exact
//    payload preservation, no truncation in copyBytes, no spurious exceptions.
//    See the test name — no claim about the size-overflow guard.
// ---------------------------------------------------------------------------
TEST_F(RocksDBHelperTest, PutLargeValue_RoundTrip) {
    MockJniEnv jni;
    const std::string key = "large_key";

    // Build a deterministic pseudo-random 2 MiB payload.
    constexpr size_t kValueSize = 2u * 1024u * 1024u;
    std::string value(kValueSize, '\0');
    std::mt19937 rng(42);
    std::uniform_int_distribution<unsigned int> dist(0, 255);
    for (char& c : value) {
        c = static_cast<char>(dist(rng));
    }

    rocksdb::Slice key_slice(key.data(), key.size());
    rocksdb::Slice val_slice(value.data(), value.size());

    rocksdb_put(jni.env(), dbHandle(), cfHandle(), writeOptionsHandle(),
                key_slice, val_slice);
    ASSERT_FALSE(jni.hasPendingException()) << "large put should not throw";

    jbyteArray result = rocksdb_get(jni.env(), dbHandle(), cfHandle(), key_slice);
    ASSERT_NE(result, nullptr);
    EXPECT_FALSE(jni.hasPendingException());

    std::vector<jbyte> got = jni.arrayBytes(result);
    ASSERT_EQ(got.size(), kValueSize) << "retrieved size must match";
    EXPECT_TRUE(std::equal(got.begin(), got.end(),
                           reinterpret_cast<const jbyte*>(value.data())))
        << "large value bytes must match exactly";
}

// ---------------------------------------------------------------------------
// 9. PutEmptyValue_RoundTrip
//    A zero-length value is semantically different from a missing key:
//    rocksdb_get must return a non-null jbyteArray of length 0.
// ---------------------------------------------------------------------------
TEST_F(RocksDBHelperTest, PutEmptyValue_RoundTrip) {
    MockJniEnv jni;
    const std::string key = "empty_val_key";
    const std::string value = "";  // zero-length
    rocksdb::Slice key_slice(key.data(), key.size());
    rocksdb::Slice val_slice(value.data(), value.size());

    rocksdb_put(jni.env(), dbHandle(), cfHandle(), writeOptionsHandle(),
                key_slice, val_slice);
    ASSERT_FALSE(jni.hasPendingException());

    jbyteArray result = rocksdb_get(jni.env(), dbHandle(), cfHandle(), key_slice);
    // An empty value stored in RocksDB is NOT the same as a missing key —
    // rocksdb_get must return a non-null, zero-length jbyteArray.
    ASSERT_NE(result, nullptr)
        << "empty value must return a jbyteArray, not nullptr";
    EXPECT_FALSE(jni.hasPendingException());

    std::vector<jbyte> got = jni.arrayBytes(result);
    EXPECT_EQ(got.size(), 0u) << "empty value must yield length-0 array";
}

// ---------------------------------------------------------------------------
// 10. OverwriteExistingKey
//     put k=v1, put k=v2, get returns v2 (not v1).
// ---------------------------------------------------------------------------
TEST_F(RocksDBHelperTest, OverwriteExistingKey) {
    MockJniEnv jni;
    const std::string key = "overwrite_key";
    const std::string v1 = "first_value";
    const std::string v2 = "second_value";
    rocksdb::Slice key_slice(key.data(), key.size());

    rocksdb_put(jni.env(), dbHandle(), cfHandle(), writeOptionsHandle(),
                key_slice, rocksdb::Slice(v1.data(), v1.size()));
    ASSERT_FALSE(jni.hasPendingException());

    rocksdb_put(jni.env(), dbHandle(), cfHandle(), writeOptionsHandle(),
                key_slice, rocksdb::Slice(v2.data(), v2.size()));
    ASSERT_FALSE(jni.hasPendingException());

    jbyteArray result = rocksdb_get(jni.env(), dbHandle(), cfHandle(), key_slice);
    ASSERT_NE(result, nullptr);
    EXPECT_FALSE(jni.hasPendingException());

    std::vector<jbyte> got = jni.arrayBytes(result);
    ASSERT_EQ(got.size(), v2.size());
    EXPECT_TRUE(std::equal(got.begin(), got.end(),
                           reinterpret_cast<const jbyte*>(v2.data())))
        << "overwritten key must return the second (latest) value";
}

}  // namespace
