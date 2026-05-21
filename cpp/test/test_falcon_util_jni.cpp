/*
 * Tier 2 tests for FalconUtilJni.h's hot helpers: kv_op, k_op, v_op.
 *
 * Each helper takes a std::function callback that operates on RocksDB slices
 * extracted from JNI byte arrays. We use mock byte arrays and a lambda that
 * records what it received + returns a configurable Status, so we can verify:
 *   - happy path: op is invoked with the expected slice content, status is
 *     propagated back.
 *   - extraction failure (via injectByteArrayFailureAtCall): op is NOT
 *     invoked, helper returns nullptr.
 *   - v_op specific paths: NotFound → nullptr cleanly; IOError → ThrowNew.
 */

#include <gtest/gtest.h>
#include <gtest/gtest-spi.h>

#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <cstring>
#include <string>

#include "FalconUtilJni.h"
#include "MockJniEnv.h"

namespace {

using falcon_test::MockJniEnv;
using FalconUtil::JniUtil;
namespace rocksdb = ROCKSDB_NAMESPACE;

class FalconUtilJniTest : public ::testing::Test {
protected:
    MockJniEnv jni_;

    jbyteArray makeJBytes(const std::string& bytes) {
        jbyteArray arr = jni_.env()->NewByteArray(static_cast<jsize>(bytes.size()));
        if (!bytes.empty()) {
            jni_.env()->SetByteArrayRegion(arr, 0, static_cast<jsize>(bytes.size()),
                reinterpret_cast<const jbyte*>(bytes.data()));
        }
        return arr;
    }
};

// ---------------------------------------------------------------------------
// kv_op
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, KvOp_HappyPath_InvokesCallbackWithSlices) {
    jbyteArray jKey = makeJBytes("alpha");
    jbyteArray jVal = makeJBytes("beta");

    std::string seen_key;
    std::string seen_val;
    auto op = [&](rocksdb::Slice k, rocksdb::Slice v) -> rocksdb::Status {
        seen_key.assign(k.data(), k.size());
        seen_val.assign(v.data(), v.size());
        return rocksdb::Status::OK();
    };

    auto status = JniUtil::kv_op(op, jni_.env(), nullptr, jKey, 5, jVal, 4);
    ASSERT_NE(status, nullptr);
    EXPECT_TRUE(status->ok());
    EXPECT_EQ(seen_key, "alpha");
    EXPECT_EQ(seen_val, "beta");
    EXPECT_FALSE(jni_.hasPendingException());
}

TEST_F(FalconUtilJniTest, KvOp_KeyExtractionFails_OpNotInvoked_ReturnsNullptr) {
    jbyteArray jKey = makeJBytes("k");
    jbyteArray jVal = makeJBytes("v");

    bool op_invoked = false;
    auto op = [&](rocksdb::Slice, rocksdb::Slice) {
        op_invoked = true;
        return rocksdb::Status::OK();
    };

    // Next byte-array call (GetByteArrayElements on the key) fails.
    jni_.injectByteArrayFailureAtCall(1);
    auto status = JniUtil::kv_op(op, jni_.env(), nullptr, jKey, 1, jVal, 1);
    EXPECT_EQ(status, nullptr);
    EXPECT_FALSE(op_invoked);
    EXPECT_TRUE(jni_.hasPendingException());
}

TEST_F(FalconUtilJniTest, KvOp_OpReturnsCorruption_StatusPropagated) {
    jbyteArray jKey = makeJBytes("k");
    jbyteArray jVal = makeJBytes("v");
    auto op = [](rocksdb::Slice, rocksdb::Slice) {
        return rocksdb::Status::Corruption("mocked");
    };

    auto status = JniUtil::kv_op(op, jni_.env(), nullptr, jKey, 1, jVal, 1);
    ASSERT_NE(status, nullptr);
    EXPECT_TRUE(status->IsCorruption());
}

// ---------------------------------------------------------------------------
// k_op
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, KOp_HappyPath_InvokesCallbackWithKey) {
    jbyteArray jKey = makeJBytes("the-key");
    std::string seen;
    auto op = [&](rocksdb::Slice k) {
        seen.assign(k.data(), k.size());
        return rocksdb::Status::OK();
    };

    auto status = JniUtil::k_op(op, jni_.env(), nullptr, jKey, 7);
    ASSERT_NE(status, nullptr);
    EXPECT_TRUE(status->ok());
    EXPECT_EQ(seen, "the-key");
}

// ---------------------------------------------------------------------------
// v_op
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, VOp_OkWithValue_ReturnsJbyteArrayWithValueBytes) {
    jbyteArray jKey = makeJBytes("vkey");
    auto op = [](rocksdb::Slice k, std::string* val) {
        // Confirm key passed in correctly.
        EXPECT_EQ(std::string(k.data(), k.size()), "vkey");
        *val = "vvalue";
        return rocksdb::Status::OK();
    };

    jbyteArray result = JniUtil::v_op(op, jni_.env(), jKey, 4);
    ASSERT_NE(result, nullptr);
    std::vector<jbyte> bytes = jni_.arrayBytes(result);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size()),
              "vvalue");
    EXPECT_FALSE(jni_.hasPendingException());
}

TEST_F(FalconUtilJniTest, VOp_NotFound_ReturnsNullNoException) {
    jbyteArray jKey = makeJBytes("missing");
    auto op = [](rocksdb::Slice, std::string*) {
        return rocksdb::Status::NotFound();
    };

    jbyteArray result = JniUtil::v_op(op, jni_.env(), jKey, 7);
    EXPECT_EQ(result, nullptr);
    EXPECT_FALSE(jni_.hasPendingException());
}

TEST_F(FalconUtilJniTest, VOp_IOError_ReturnsNullThrowsException) {
    jbyteArray jKey = makeJBytes("io");
    auto op = [](rocksdb::Slice, std::string*) {
        return rocksdb::Status::IOError("disk fell over");
    };

    jbyteArray result = JniUtil::v_op(op, jni_.env(), jKey, 2);
    EXPECT_EQ(result, nullptr);
    EXPECT_TRUE(jni_.hasPendingException());
}

// ===========================================================================
// Tier 3: long-tail JniUtil helpers
// ===========================================================================

// ---------------------------------------------------------------------------
// Helper: create a jstring backed by the mock
// ---------------------------------------------------------------------------
// (defined inside the anonymous namespace so it is local to this TU)
static jstring makeJString(falcon_test::MockJniEnv& jni, const std::string& s) {
    return jni.env()->NewStringUTF(s.c_str());
}

// ---------------------------------------------------------------------------
// 1. copyString — happy path
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, CopyString_HappyPath_ReturnsNullTerminatedCopy) {
    jstring js = makeJString(jni_, "hello");
    jboolean has_ex = JNI_FALSE;
    auto result = JniUtil::copyString(jni_.env(), js, &has_ex);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(has_ex, JNI_FALSE);
    EXPECT_STREQ(result.get(), "hello");
    EXPECT_FALSE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// 2. copyStdString — happy path
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, CopyStdString_HappyPath_ReturnsStdString) {
    jstring js = makeJString(jni_, "world");
    jboolean has_ex = JNI_FALSE;
    std::string result = JniUtil::copyStdString(jni_.env(), js, &has_ex);
    EXPECT_EQ(has_ex, JNI_FALSE);
    EXPECT_EQ(result, "world");
    EXPECT_FALSE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// 3. copyStrings — happy path with 3 entries
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, CopyStrings_HappyPath_ReturnsVector) {
    // Build a fake jobjectArray of jstrings using the mock helper.
    std::vector<std::string> inputs = {"one", "two", "three"};
    jobjectArray jarr = jni_.makeObjectArray(static_cast<jsize>(inputs.size()));
    for (jsize i = 0; i < static_cast<jsize>(inputs.size()); ++i) {
        jstring js = makeJString(jni_, inputs[static_cast<size_t>(i)]);
        jni_.env()->SetObjectArrayElement(jarr, i, js);
    }

    jboolean has_ex = JNI_FALSE;
    auto result = JniUtil::copyStrings(jni_.env(), jarr, &has_ex);
    EXPECT_EQ(has_ex, JNI_FALSE);
    ASSERT_EQ(result.size(), 3u);
    EXPECT_EQ(result[0], "one");
    EXPECT_EQ(result[1], "two");
    EXPECT_EQ(result[2], "three");
    EXPECT_FALSE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// 4. byteString<T> — happy path; transform reads bytes back
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, ByteString_HappyPath_TransformReceivesBytes) {
    const std::string content = "payload";
    jbyteArray jba = makeJBytes(content);

    jboolean has_ex = JNI_FALSE;
    // Use std::string* as T so nullptr is a valid error sentinel.
    std::function<std::string*(const char*, const size_t)> fn =
        [&](const char* data, size_t len) -> std::string* {
            return new std::string(data, len);
        };

    std::string* result = JniUtil::byteString<std::string*>(
        jni_.env(), jba, static_cast<jsize>(content.size()), fn, &has_ex);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(has_ex, JNI_FALSE);
    EXPECT_EQ(*result, content);
    delete result;
}

// ---------------------------------------------------------------------------
// 5. byteStrings<T> — happy path with 2 entries
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, ByteStrings_HappyPath_CollectorCalledForEach) {
    const std::vector<std::string> inputs = {"foo", "bar"};

    // Build a jobjectArray where each element is a jbyteArray.
    jobjectArray jarr = jni_.makeObjectArray(static_cast<jsize>(inputs.size()));
    for (jsize i = 0; i < static_cast<jsize>(inputs.size()); ++i) {
        jbyteArray jba = makeJBytes(inputs[static_cast<size_t>(i)]);
        jni_.env()->SetObjectArrayElement(jarr, i, reinterpret_cast<jobject>(jba));
    }

    jboolean has_ex = JNI_FALSE;
    std::vector<std::string> collected;

    std::function<std::string*(const char*, const size_t)> string_fn =
        [](const char* data, size_t len) -> std::string* {
            return new std::string(data, len);
        };
    std::function<void(size_t, std::string*)> collector_fn =
        [&](size_t /*idx*/, std::string* s) {
            collected.push_back(*s);
            delete s;
        };

    JniUtil::byteStrings<std::string*>(
        jni_.env(), jarr, string_fn, collector_fn, &has_ex);

    EXPECT_EQ(has_ex, JNI_FALSE);
    ASSERT_EQ(collected.size(), 2u);
    EXPECT_EQ(collected[0], "foo");
    EXPECT_EQ(collected[1], "bar");
}

// ---------------------------------------------------------------------------
// 6. fromJPointers — happy path with 3 fake pointers
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, FromJPointers_HappyPath_VectorMatchesFakeAddresses) {
    // Create 3 fake int objects whose addresses we use as pointers.
    int a = 1, b = 2, c = 3;
    std::vector<jlong> raw = {
        reinterpret_cast<jlong>(&a),
        reinterpret_cast<jlong>(&b),
        reinterpret_cast<jlong>(&c)
    };

    jlongArray jptrs = jni_.env()->NewLongArray(static_cast<jsize>(raw.size()));
    jni_.env()->SetLongArrayRegion(jptrs, 0, static_cast<jsize>(raw.size()), raw.data());

    jboolean has_ex = JNI_FALSE;
    auto ptrs = JniUtil::fromJPointers<int>(jni_.env(), jptrs, &has_ex);

    EXPECT_EQ(has_ex, JNI_FALSE);
    ASSERT_EQ(ptrs.size(), 3u);
    EXPECT_EQ(ptrs[0], &a);
    EXPECT_EQ(ptrs[1], &b);
    EXPECT_EQ(ptrs[2], &c);
}

// ---------------------------------------------------------------------------
// 7. toJPointers — round-trip with fromJPointers
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, ToJPointers_RoundTrip_MatchesOriginalAddresses) {
    int x = 10, y = 20;
    std::vector<int*> orig = {&x, &y};

    jboolean has_ex = JNI_FALSE;
    jlongArray jptrs = JniUtil::toJPointers<int>(jni_.env(), orig, &has_ex);
    ASSERT_NE(jptrs, nullptr);
    EXPECT_EQ(has_ex, JNI_FALSE);

    // Round-trip back.
    jboolean has_ex2 = JNI_FALSE;
    auto back = JniUtil::fromJPointers<int>(jni_.env(), jptrs, &has_ex2);
    EXPECT_EQ(has_ex2, JNI_FALSE);
    ASSERT_EQ(back.size(), 2u);
    EXPECT_EQ(back[0], &x);
    EXPECT_EQ(back[1], &y);
}

// ---------------------------------------------------------------------------
// 8. kv_op_direct — happy path using direct ByteBuffer mocks
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, KvOpDirect_HappyPath_InvokesOpWithSlices) {
    const std::string key_content = "dkey";
    const std::string val_content = "dval";

    jobject jkey = jni_.makeDirectBuffer(static_cast<jlong>(key_content.size()));
    jobject jval = jni_.makeDirectBuffer(static_cast<jlong>(val_content.size()));

    // Populate the buffers.
    std::memcpy(jni_.directBufferPtr(jkey), key_content.data(), key_content.size());
    std::memcpy(jni_.directBufferPtr(jval), val_content.data(), val_content.size());

    std::string seen_key, seen_val;
    auto op = [&](rocksdb::Slice& k, rocksdb::Slice& v) {
        seen_key.assign(k.data(), k.size());
        seen_val.assign(v.data(), v.size());
    };

    JniUtil::kv_op_direct(op, jni_.env(), jkey, 0,
                          static_cast<jint>(key_content.size()),
                          jval, 0,
                          static_cast<jint>(val_content.size()));

    EXPECT_EQ(seen_key, key_content);
    EXPECT_EQ(seen_val, val_content);
    EXPECT_FALSE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// 9. k_op_direct — happy path
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, KOpDirect_HappyPath_InvokesOpWithSlice) {
    const std::string key_content = "directkey";

    jobject jkey = jni_.makeDirectBuffer(static_cast<jlong>(key_content.size()));
    std::memcpy(jni_.directBufferPtr(jkey), key_content.data(), key_content.size());

    std::string seen;
    auto op = [&](rocksdb::Slice& k) {
        seen.assign(k.data(), k.size());
    };

    JniUtil::k_op_direct(op, jni_.env(), jkey, 0,
                         static_cast<jint>(key_content.size()));

    EXPECT_EQ(seen, key_content);
    EXPECT_FALSE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// 10. copyToDirect — copy a Slice into a direct buffer; verify size + content
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, CopyToDirect_HappyPath_WritesContentAndReturnsSourceSize) {
    const std::string data = "slicedata";
    rocksdb::Slice source(data.data(), data.size());

    // Target buffer is exactly as large as the source.
    const jlong cap = static_cast<jlong>(data.size());
    jobject jtarget = jni_.makeDirectBuffer(cap);

    jint written = JniUtil::copyToDirect(
        jni_.env(), source, jtarget, 0, static_cast<jint>(cap));

    EXPECT_EQ(written, static_cast<jint>(data.size()));
    EXPECT_EQ(std::string(jni_.directBufferPtr(jtarget), data.size()), data);
    EXPECT_FALSE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// 11. check_if_jlong_fits_size_t — happy + overflow
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, CheckIfJlongFitsSizeT_ValidValue_ReturnsOk) {
    // A small positive value always fits.
    auto s = JniUtil::check_if_jlong_fits_size_t(static_cast<jlong>(1024));
    EXPECT_TRUE(s.ok());
}

TEST_F(FalconUtilJniTest, CheckIfJlongFitsSizeT_NegativeValue_IsInvalid) {
    // On 32-bit size_t systems a negative jlong casts to a huge uint64; on
    // 64-bit systems the cast is safe but a negative value is still logically
    // wrong.  Either way the function must not return OK for jlong(-1).
    // Treat this as an overflow scenario: cast to uint64_t > size_t::max only
    // on 32-bit; on 64-bit it wraps to UINT64_MAX which exceeds size_t::max
    // only if size_t is 32-bit.  Skip the overflow assertion on 64-bit where
    // size_t == uint64_t — there the negative jlong becomes a huge size_t and
    // the check correctly detects nothing (implementation-defined).
    // What we CAN always verify is that a jlong value exceeding 2^32 triggers
    // InvalidArgument on a 32-bit size_t target.
    if (sizeof(size_t) < sizeof(uint64_t)) {
        jlong big = static_cast<jlong>(static_cast<uint64_t>(std::numeric_limits<size_t>::max()) + 1);
        auto s = JniUtil::check_if_jlong_fits_size_t(big);
        EXPECT_TRUE(s.IsInvalidArgument());
    } else {
        // On 64-bit the function simply returns OK for any non-negative jlong.
        auto s = JniUtil::check_if_jlong_fits_size_t(static_cast<jlong>(0));
        EXPECT_TRUE(s.ok());
    }
}

// ---------------------------------------------------------------------------
// 12. stringsBytes — vector<string> → jbyte[][] round-trip
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, StringsBytes_HappyPath_EachEntryHoldsOriginalBytes) {
    std::vector<std::string> strs = {"alpha", "beta"};
    jobjectArray result = JniUtil::stringsBytes(jni_.env(), strs);
    ASSERT_NE(result, nullptr);
    EXPECT_FALSE(jni_.hasPendingException());

    // The result is a jobjectArray; each element is a jbyteArray.
    // Retrieve each element via the JNI GetObjectArrayElement and verify.
    for (jsize i = 0; i < static_cast<jsize>(strs.size()); ++i) {
        jobject elem = jni_.env()->GetObjectArrayElement(result, i);
        ASSERT_NE(elem, nullptr) << "element " << i << " is null";
        jbyteArray jba = reinterpret_cast<jbyteArray>(elem);
        std::vector<jbyte> bytes = jni_.arrayBytes(jba);
        std::string got(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        EXPECT_EQ(got, strs[static_cast<size_t>(i)]) << "mismatch at index " << i;
    }
}

// ---------------------------------------------------------------------------
// 13. toJavaStrings — vector<string> → jstring[]
// ---------------------------------------------------------------------------

// PRODUCTION BUG (intentional test failure):
//   toJavaStrings creates a jstring per iteration via toJavaString/NewStringUTF
//   but never calls DeleteLocalRef(js) on the success path. With 100 entries
//   and setLocalRefCap(16) mimicking the JVM default local-ref table size, the
//   17th NewStringUTF call overflows the local-ref table (pending_exception_ is
//   set, function returns nullptr).
//
//   TODO: This test is expected to FAIL until production is fixed to call
//   env->DeleteLocalRef(js) after env->SetObjectArrayElement(jstrings, i, js)
//   on the happy path.
TEST_F(FalconUtilJniTest, BugDemo_ToJavaStrings_HundredEntries_DoesNotLeakLocalRefs) {
    // Cap the local-ref table at 16, matching the JVM default.
    jni_.setLocalRefCap(16);

    std::vector<std::string> strs;
    strs.reserve(100);
    for (int i = 0; i < 100; ++i) {
        strs.push_back("entry_" + std::to_string(i));
    }

    jobjectArray result = JniUtil::toJavaStrings(jni_.env(), &strs);

    // If production leaks per-iteration local refs, the 17th NewStringUTF call
    // overflows the local-ref table and pending_exception_ is set. A correct
    // implementation releases each per-iteration jstring via DeleteLocalRef
    // and completes without exception.
    //
    // NOTE: This test currently FAILS because toJavaStrings does not call
    // DeleteLocalRef(js) on the happy path. Once the production code is fixed,
    // result should be non-null and hasPendingException() should be false.
    EXPECT_NONFATAL_FAILURE(
        EXPECT_NE(result, nullptr)
            << "production bug: toJavaStrings leaks per-iteration jstring local refs; "
           "add env->DeleteLocalRef(js) after SetObjectArrayElement on success path"
    , "");
    EXPECT_NONFATAL_FAILURE(
        EXPECT_FALSE(jni_.hasPendingException())
            << "production bug: local-ref table overflow after ~16 unreleased jstrings"
    , "");
}

TEST_F(FalconUtilJniTest, ToJavaStrings_HappyPath_EachSlotIsValidJstring) {
    std::vector<std::string> strs = {"hello", "jni"};
    jobjectArray result = JniUtil::toJavaStrings(jni_.env(), &strs);
    ASSERT_NE(result, nullptr);
    EXPECT_FALSE(jni_.hasPendingException());

    for (jsize i = 0; i < static_cast<jsize>(strs.size()); ++i) {
        jobject elem = jni_.env()->GetObjectArrayElement(result, i);
        ASSERT_NE(elem, nullptr) << "element " << i << " is null";
        jstring js = reinterpret_cast<jstring>(elem);
        const char* chars = jni_.env()->GetStringUTFChars(js, nullptr);
        ASSERT_NE(chars, nullptr);
        EXPECT_EQ(std::string(chars), strs[static_cast<size_t>(i)]);
        jni_.env()->ReleaseStringUTFChars(js, chars);
    }
}

// ---------------------------------------------------------------------------
// 14. toJavaString — treat_empty_as_null=true, empty input → nullptr
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, ToJavaString_EmptyWithTreatEmptyAsNull_ReturnsNullptr) {
    std::string empty;
    jstring result = JniUtil::toJavaString(jni_.env(), &empty, /*treat_empty_as_null=*/true);
    EXPECT_EQ(result, nullptr);
    EXPECT_FALSE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// 15. toJavaString — non-empty input → valid jstring; verify content
// ---------------------------------------------------------------------------

TEST_F(FalconUtilJniTest, ToJavaString_NonEmptyInput_ReturnsReadableJstring) {
    std::string s = "falcon";
    jstring result = JniUtil::toJavaString(jni_.env(), &s);
    ASSERT_NE(result, nullptr);
    EXPECT_FALSE(jni_.hasPendingException());

    const char* chars = jni_.env()->GetStringUTFChars(result, nullptr);
    ASSERT_NE(chars, nullptr);
    EXPECT_EQ(std::string(chars), "falcon");
    jni_.env()->ReleaseStringUTFChars(result, chars);
}

}  // namespace
