/*
 * Tier 2 tests for FalconExceptionJni.h's hot paths:
 *   - JavaException::ThrowNew(env, msg) — pure-msg variant via FalconExceptionJni.
 *   - StatusJni byte round-trip: toJavaStatusCode / toJavaStatusSubCode and
 *     toCppStatus(jcode_value, jsub_code_value).
 *   - FalconExceptionJni::toCppStatus(env, jthrowable) — happy path with
 *     IsInstanceOf=true, and the early-return path with IsInstanceOf=false.
 */

#include <gtest/gtest.h>

#include <rocksdb/status.h>

#include <memory>

#include "FalconExceptionJni.h"
#include "MockJniEnv.h"

namespace {

using falcon_test::MockJniEnv;
using FalconException::FalconExceptionJni;
using FalconException::StatusJni;
namespace rocksdb = ROCKSDB_NAMESPACE;

class FalconExceptionJniTest : public ::testing::Test {
protected:
    MockJniEnv jni_;
};

// ---------------------------------------------------------------------------
// JavaException::ThrowNew(env, msg)
// ---------------------------------------------------------------------------

TEST_F(FalconExceptionJniTest, FalconExceptionThrowNew_MsgOnly_FlipsPendingException) {
    EXPECT_FALSE(jni_.hasPendingException());

    bool thrown = FalconExceptionJni::ThrowNew(jni_.env(), "boom");

    EXPECT_TRUE(thrown);
    EXPECT_TRUE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// StatusJni byte round-trip — pure C++, no JNI calls. We sample three
// representative codes (kOk, kNotFound, kIOError) plus the default branch.
// ---------------------------------------------------------------------------

TEST_F(FalconExceptionJniTest, StatusJniByteRoundTrip_Ok) {
    auto status = StatusJni::toCppStatus(static_cast<jbyte>(0x0), static_cast<jbyte>(0x0));
    ASSERT_NE(status, nullptr);
    EXPECT_TRUE(status->ok());
    EXPECT_EQ(StatusJni::toJavaStatusCode(rocksdb::Status::Code::kOk), 0x0);
}

TEST_F(FalconExceptionJniTest, StatusJniByteRoundTrip_NotFoundWithMutexTimeout) {
    auto status = StatusJni::toCppStatus(static_cast<jbyte>(0x1), static_cast<jbyte>(0x1));
    ASSERT_NE(status, nullptr);
    EXPECT_TRUE(status->IsNotFound());
    EXPECT_EQ(StatusJni::toJavaStatusCode(rocksdb::Status::Code::kNotFound), 0x1);
}

TEST_F(FalconExceptionJniTest, StatusJniByteRoundTrip_IOErrorWithNoSpace) {
    auto status = StatusJni::toCppStatus(static_cast<jbyte>(0x5), static_cast<jbyte>(0x4));
    ASSERT_NE(status, nullptr);
    EXPECT_TRUE(status->IsIOError());
    EXPECT_EQ(StatusJni::toJavaStatusCode(rocksdb::Status::Code::kIOError), 0x5);
}

TEST_F(FalconExceptionJniTest, StatusJniByteRoundTrip_DefaultByteReturnsNullptr) {
    // 0x7F is the documented "undefined" sentinel; toCppStatus must return
    // nullptr rather than fall through to a wrong status.
    auto status = StatusJni::toCppStatus(static_cast<jbyte>(0x7F), static_cast<jbyte>(0x0));
    EXPECT_EQ(status, nullptr);
}

// ---------------------------------------------------------------------------
// FalconExceptionJni::toCppStatus(env, jthrowable)
// ---------------------------------------------------------------------------

TEST_F(FalconExceptionJniTest, ToCppStatus_NotInstanceOfFalconException_ReturnsNull) {
    // Mock IsInstanceOf returns false → toCppStatus(env, jthrowable) returns
    // nullptr without further JNI work.
    jni_.setIsInstanceOfResult(JNI_FALSE);

    static int sentinel = 0;
    auto* fake_throwable = reinterpret_cast<jthrowable>(&sentinel);
    auto status = FalconExceptionJni::toCppStatus(jni_.env(), fake_throwable);
    EXPECT_EQ(status, nullptr);
}

TEST_F(FalconExceptionJniTest, ToCppStatus_InstanceOfFalconException_ReturnsOkStatus) {
    // Mock CallByteMethod returns 0 (kOk / kNone), so the chain
    // toCppStatus(env, jstatus) → toCppStatus(0, 0) → Status::OK() returns a
    // non-null OK status. This exercises the full JNI-walking path:
    //   IsInstanceOf → getStatusMethod → CallObjectMethod(jstatus) →
    //   getCodeMethod → CallObjectMethod(jcode) → getValueMethod →
    //   CallByteMethod (returns 0) → ... → toCppStatus(0, 0) = Status::OK().
    jni_.setIsInstanceOfResult(JNI_TRUE);

    static int sentinel = 0;
    auto* fake_throwable = reinterpret_cast<jthrowable>(&sentinel);
    auto status = FalconExceptionJni::toCppStatus(jni_.env(), fake_throwable);
    ASSERT_NE(status, nullptr);
    EXPECT_TRUE(status->ok());
    EXPECT_FALSE(jni_.hasPendingException());
}

// ===========================================================================
// Tier 3 — full enum-table coverage of StatusJni conversion logic
// ===========================================================================

// ---------------------------------------------------------------------------
// 1. StatusJni_AllCppToJava_CodeBytes
//    Table-driven: every rocksdb::Status::Code → expected Java byte.
// ---------------------------------------------------------------------------
TEST_F(FalconExceptionJniTest, StatusJni_AllCppToJava_CodeBytes) {
    struct Row {
        rocksdb::Status::Code code;
        jbyte expected;
    };
    const Row table[] = {
        {rocksdb::Status::Code::kOk,               static_cast<jbyte>(0x0)},
        {rocksdb::Status::Code::kNotFound,          static_cast<jbyte>(0x1)},
        {rocksdb::Status::Code::kCorruption,        static_cast<jbyte>(0x2)},
        {rocksdb::Status::Code::kNotSupported,      static_cast<jbyte>(0x3)},
        {rocksdb::Status::Code::kInvalidArgument,   static_cast<jbyte>(0x4)},
        {rocksdb::Status::Code::kIOError,           static_cast<jbyte>(0x5)},
        {rocksdb::Status::Code::kMergeInProgress,   static_cast<jbyte>(0x6)},
        {rocksdb::Status::Code::kIncomplete,        static_cast<jbyte>(0x7)},
        {rocksdb::Status::Code::kShutdownInProgress,static_cast<jbyte>(0x8)},
        {rocksdb::Status::Code::kTimedOut,          static_cast<jbyte>(0x9)},
        {rocksdb::Status::Code::kAborted,           static_cast<jbyte>(0xA)},
        {rocksdb::Status::Code::kBusy,              static_cast<jbyte>(0xB)},
        {rocksdb::Status::Code::kExpired,           static_cast<jbyte>(0xC)},
        {rocksdb::Status::Code::kTryAgain,          static_cast<jbyte>(0xD)},
        {rocksdb::Status::Code::kColumnFamilyDropped, static_cast<jbyte>(0xE)},
    };
    for (const auto& row : table) {
        EXPECT_EQ(StatusJni::toJavaStatusCode(row.code), row.expected)
            << "Unexpected byte for code " << static_cast<int>(row.code);
    }
}

// ---------------------------------------------------------------------------
// 2. StatusJni_AllCppToJava_SubCodeBytes
//    Table-driven: every rocksdb::Status::SubCode → expected Java byte.
// ---------------------------------------------------------------------------
TEST_F(FalconExceptionJniTest, StatusJni_AllCppToJava_SubCodeBytes) {
    struct Row {
        rocksdb::Status::SubCode sub;
        jbyte expected;
    };
    const Row table[] = {
        {rocksdb::Status::SubCode::kNone,        static_cast<jbyte>(0x0)},
        {rocksdb::Status::SubCode::kMutexTimeout,static_cast<jbyte>(0x1)},
        {rocksdb::Status::SubCode::kLockTimeout, static_cast<jbyte>(0x2)},
        {rocksdb::Status::SubCode::kLockLimit,   static_cast<jbyte>(0x3)},
        {rocksdb::Status::SubCode::kNoSpace,     static_cast<jbyte>(0x4)},
        {rocksdb::Status::SubCode::kDeadlock,    static_cast<jbyte>(0x5)},
        {rocksdb::Status::SubCode::kStaleFile,   static_cast<jbyte>(0x6)},
        {rocksdb::Status::SubCode::kMemoryLimit, static_cast<jbyte>(0x7)},
    };
    for (const auto& row : table) {
        EXPECT_EQ(StatusJni::toJavaStatusSubCode(row.sub), row.expected)
            << "Unexpected byte for subcode " << static_cast<int>(row.sub);
    }
}

// ---------------------------------------------------------------------------
// 3. StatusJni_RoundTripFiveSampleCodes
//    C++ → Java byte → C++: verify status identity for five representative codes.
// ---------------------------------------------------------------------------
TEST_F(FalconExceptionJniTest, StatusJni_RoundTripFiveSampleCodes) {
    // kOk
    {
        jbyte b = StatusJni::toJavaStatusCode(rocksdb::Status::Code::kOk);
        auto s = StatusJni::toCppStatus(b, static_cast<jbyte>(0x0));
        ASSERT_NE(s, nullptr);
        EXPECT_TRUE(s->ok());
    }
    // kCorruption
    {
        jbyte b = StatusJni::toJavaStatusCode(rocksdb::Status::Code::kCorruption);
        auto s = StatusJni::toCppStatus(b, static_cast<jbyte>(0x0));
        ASSERT_NE(s, nullptr);
        EXPECT_TRUE(s->IsCorruption());
    }
    // kNotSupported
    {
        jbyte b = StatusJni::toJavaStatusCode(rocksdb::Status::Code::kNotSupported);
        auto s = StatusJni::toCppStatus(b, static_cast<jbyte>(0x0));
        ASSERT_NE(s, nullptr);
        EXPECT_TRUE(s->IsNotSupported());
    }
    // kIOError
    {
        jbyte b = StatusJni::toJavaStatusCode(rocksdb::Status::Code::kIOError);
        auto s = StatusJni::toCppStatus(b, static_cast<jbyte>(0x0));
        ASSERT_NE(s, nullptr);
        EXPECT_TRUE(s->IsIOError());
    }
    // kColumnFamilyDropped
    {
        jbyte b = StatusJni::toJavaStatusCode(rocksdb::Status::Code::kColumnFamilyDropped);
        auto s = StatusJni::toCppStatus(b, static_cast<jbyte>(0x0));
        ASSERT_NE(s, nullptr);
        EXPECT_TRUE(s->IsColumnFamilyDropped());
    }
}

// ---------------------------------------------------------------------------
// 4. StatusJni_DefaultByteJavaToCpp_ReturnsNullptr
//    0x7F, 0x80 (= -128 as jbyte), and 0xFE (= -2) all hit the default branch
//    and must return nullptr rather than a wrong status.
// ---------------------------------------------------------------------------
TEST_F(FalconExceptionJniTest, StatusJni_DefaultByteJavaToCpp_ReturnsNullptr) {
    // 0x7F — documented undefined sentinel
    EXPECT_EQ(StatusJni::toCppStatus(static_cast<jbyte>(0x7F), static_cast<jbyte>(0x0)), nullptr);
    // 0x80 interpreted as jbyte = -128 → default branch
    EXPECT_EQ(StatusJni::toCppStatus(static_cast<jbyte>(static_cast<int8_t>(-128)),
                                     static_cast<jbyte>(0x0)), nullptr);
    // 0xFE interpreted as jbyte = -2 → default branch
    EXPECT_EQ(StatusJni::toCppStatus(static_cast<jbyte>(static_cast<int8_t>(-2)),
                                     static_cast<jbyte>(0x0)), nullptr);
}

// ---------------------------------------------------------------------------
// 5. StatusJni_OutOfRangeCppCode_ReturnsDefaultByte
//    An enum value with no explicit case in toJavaStatusCode → 0x7F.
//    kCompactionTooLarge (enum value 14, 0xE in unsigned) is present in the
//    RocksDB Status::Code enum but is NOT handled in toJavaStatusCode, so it
//    must map to the undefined sentinel 0x7F.
// ---------------------------------------------------------------------------
TEST_F(FalconExceptionJniTest, StatusJni_OutOfRangeCppCode_ReturnsDefaultByte) {
    EXPECT_EQ(StatusJni::toJavaStatusCode(rocksdb::Status::Code::kCompactionTooLarge),
              static_cast<jbyte>(0x7F));
}

// ---------------------------------------------------------------------------
// 6. StatusJni_SubCodeOutOfRange_ReturnsDefaultByte
//    kMaxSubCode is not a real subcode; it should hit the default branch → 0x7F.
// ---------------------------------------------------------------------------
TEST_F(FalconExceptionJniTest, StatusJni_SubCodeOutOfRange_ReturnsDefaultByte) {
    EXPECT_EQ(StatusJni::toJavaStatusSubCode(rocksdb::Status::SubCode::kMaxSubCode),
              static_cast<jbyte>(0x7F));
}

// ---------------------------------------------------------------------------
// 7. SubCodeJni_ToCppSubCode_AllCases
//    SubCodeJni::toCppSubCode maps each documented byte to the correct enum;
//    the default (0x7F) returns kNone.
// ---------------------------------------------------------------------------
TEST_F(FalconExceptionJniTest, SubCodeJni_ToCppSubCode_AllCases) {
    using SC = rocksdb::Status::SubCode;
    using FalconException::SubCodeJni;

    EXPECT_EQ(SubCodeJni::toCppSubCode(static_cast<jbyte>(0x0)), SC::kNone);
    EXPECT_EQ(SubCodeJni::toCppSubCode(static_cast<jbyte>(0x1)), SC::kMutexTimeout);
    EXPECT_EQ(SubCodeJni::toCppSubCode(static_cast<jbyte>(0x2)), SC::kLockTimeout);
    EXPECT_EQ(SubCodeJni::toCppSubCode(static_cast<jbyte>(0x3)), SC::kLockLimit);
    EXPECT_EQ(SubCodeJni::toCppSubCode(static_cast<jbyte>(0x4)), SC::kNoSpace);
    EXPECT_EQ(SubCodeJni::toCppSubCode(static_cast<jbyte>(0x5)), SC::kDeadlock);
    EXPECT_EQ(SubCodeJni::toCppSubCode(static_cast<jbyte>(0x6)), SC::kStaleFile);
    EXPECT_EQ(SubCodeJni::toCppSubCode(static_cast<jbyte>(0x7)), SC::kMemoryLimit);
    // default / out-of-range → kNone
    EXPECT_EQ(SubCodeJni::toCppSubCode(static_cast<jbyte>(0x7F)), SC::kNone);
}

// ---------------------------------------------------------------------------
// 8. StatusJni_ToCppStatus_NotFoundWithSubCode
//    toCppStatus(0x1, 0x4) → NotFound with kNoSpace subcode.
// ---------------------------------------------------------------------------
TEST_F(FalconExceptionJniTest, StatusJni_ToCppStatus_NotFoundWithSubCode) {
    auto status = StatusJni::toCppStatus(static_cast<jbyte>(0x1), static_cast<jbyte>(0x4));
    ASSERT_NE(status, nullptr);
    EXPECT_TRUE(status->IsNotFound());
    EXPECT_EQ(status->subcode(), rocksdb::Status::SubCode::kNoSpace);
}

// ---------------------------------------------------------------------------
// 9. StatusJni_ToCppStatus_TryAgain
//    toCppStatus(0xD, 0x0) → Status with TryAgain code.
// ---------------------------------------------------------------------------
TEST_F(FalconExceptionJniTest, StatusJni_ToCppStatus_TryAgain) {
    auto status = StatusJni::toCppStatus(static_cast<jbyte>(0xD), static_cast<jbyte>(0x0));
    ASSERT_NE(status, nullptr);
    EXPECT_TRUE(status->IsTryAgain());
}

// ---------------------------------------------------------------------------
// 10. StatusJni_ToCppStatus_BusyWithDeadlock
//     toCppStatus(0xB, 0x5) → Busy status with kDeadlock subcode.
// ---------------------------------------------------------------------------
TEST_F(FalconExceptionJniTest, StatusJni_ToCppStatus_BusyWithDeadlock) {
    auto status = StatusJni::toCppStatus(static_cast<jbyte>(0xB), static_cast<jbyte>(0x5));
    ASSERT_NE(status, nullptr);
    EXPECT_TRUE(status->IsBusy());
    EXPECT_EQ(status->subcode(), rocksdb::Status::SubCode::kDeadlock);
}

}  // namespace
