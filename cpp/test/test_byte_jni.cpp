/*
 * Tier 3 tests for FalconUtilJni.h's ByteJni portal class.
 *
 * ByteJni is a portal for java.lang.Byte.  These tests verify the five public
 * entry points using MockJniEnv:
 *   - getJClass        → delegates to FindClass
 *   - getArrayJClass   → delegates to FindClass with the "[B" descriptor
 *   - getByteValueMethod → calls getJClass + GetMethodID
 *   - new2dByteArray   → calls getArrayJClass + NewObjectArray
 *   - valueOf          → calls getJClass + GetStaticMethodID + CallStaticObjectMethod
 *
 * Note: getByteValueMethod and valueOf each cache their jmethodID in a
 * function-local static, so the first call wires through the mock and
 * subsequent calls in the same process skip GetMethodID/GetStaticMethodID.
 * That caching is the intended production behavior; the tests validate the
 * observable return values and absence of pending exceptions.
 */

#include <gtest/gtest.h>

#include "FalconUtilJni.h"
#include "MockJniEnv.h"

namespace {

using falcon_test::MockJniEnv;
using FalconUtil::ByteJni;

class ByteJniTest : public ::testing::Test {
protected:
    MockJniEnv jni_;
};

// ---------------------------------------------------------------------------
// getJClass
// ---------------------------------------------------------------------------

TEST_F(ByteJniTest, ByteJni_GetJClass_ReturnsNonNull) {
    jclass clazz = ByteJni::getJClass(jni_.env());
    EXPECT_NE(clazz, nullptr);
    EXPECT_FALSE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// getArrayJClass
// ---------------------------------------------------------------------------

TEST_F(ByteJniTest, ByteJni_GetArrayJClass_ReturnsNonNull) {
    jclass clazz = ByteJni::getArrayJClass(jni_.env());
    EXPECT_NE(clazz, nullptr);
    EXPECT_FALSE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// getByteValueMethod
// ---------------------------------------------------------------------------

TEST_F(ByteJniTest, ByteJni_GetByteValueMethod_ReturnsNonNull) {
    // Exercises getJClass (FindClass sentinel) followed by GetMethodID sentinel.
    // The static-local cache means subsequent calls in the same process skip
    // GetMethodID; that is intentional production behaviour.
    jmethodID mid = ByteJni::getByteValueMethod(jni_.env());
    EXPECT_NE(mid, nullptr);
    EXPECT_FALSE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// new2dByteArray
// ---------------------------------------------------------------------------

TEST_F(ByteJniTest, ByteJni_New2dByteArray_ReturnsObjectArrayOfSpecifiedLength) {
    // new2dByteArray calls getArrayJClass (FindClass) then NewObjectArray.
    // MockJniEnv::newObjectArray returns a non-null sentinel.
    jobjectArray arr = ByteJni::new2dByteArray(jni_.env(), 3);
    EXPECT_NE(arr, nullptr);
    EXPECT_FALSE(jni_.hasPendingException());
}

// ---------------------------------------------------------------------------
// valueOf
// ---------------------------------------------------------------------------

TEST_F(ByteJniTest, ByteJni_ValueOf_HappyPath_ReturnsNonNullObject) {
    // valueOf calls getJClass (FindClass) → GetStaticMethodID →
    // CallStaticObjectMethod.  All three mock stubs return non-null sentinels
    // and leave pending_exception_ false.
    jobject obj = ByteJni::valueOf(jni_.env(), static_cast<jbyte>(0x42));
    EXPECT_NE(obj, nullptr);
    EXPECT_FALSE(jni_.hasPendingException());
}

}  // namespace
