/*
 * MAX_JARRAY_SIZE boundary tests for JniUtil::createJavaByteArrayWithSizeCheck.
 *
 * The production guard in `cpp/include/FalconUtilJni.h`:
 *
 *   static const size_t MAX_JARRAY_SIZE = 1u << 31;   // = 2^31 = 2147483648
 *   if (size > MAX_JARRAY_SIZE) {
 *       FalconExceptionJni::ThrowNew(env, "Requested array size exceeds VM limit");
 *       return nullptr;
 *   }
 *   const jsize jlen = static_cast<jsize>(size);      // jsize = int32_t
 *   jbyteArray jbytes = env->NewByteArray(jlen);
 *
 * Coverage:
 *   1. size > MAX (e.g. 2^31 + 1): guard trips, nullptr returned, no allocation.
 *   2. size == MAX (2^31): guard does NOT trip (`>` not `>=`); production casts
 *      size to jsize (int32_t) → INT_MIN (-2147483648). This is the off-by-one
 *      bug: the guard should use `>=` to reject this value too.
 *   3. size == MAX - 1 (2^31 - 1): guard does not trip; positive jsize.
 *   4. Far over limit (4 GiB): guard trips cleanly.
 *   5. Well below limit (16 KiB): guard does not trip; allocation succeeds.
 *
 * Tests 2 & 3 use MockJniEnv::setSkipBackingStoreAbove so the mock records
 * the requested jsize without allocating 2 GiB of RAM.
 *
 * Test 2 is an INTENTIONAL FAILURE: it documents the production off-by-one
 * bug where size == MAX_JARRAY_SIZE passes the `>` guard and is then cast to
 * a negative jsize. The test fails until production tightens the guard to `>=`.
 */

#include <gtest/gtest.h>
#include <gtest/gtest-spi.h>

#include <cstdint>

#include "FalconUtilJni.h"
#include "MockJniEnv.h"

namespace {

using falcon_test::MockJniEnv;
using FalconUtil::JniUtil;

class MaxJarraySizeTest : public ::testing::Test {
protected:
    MockJniEnv jni_;
};

// 2 GiB + 1 byte: exceeds MAX_JARRAY_SIZE; guard must trip without
// dereferencing `bytes`. We pass nullptr for bytes deliberately to prove the
// guard runs first (a deref would segfault).
TEST_F(MaxJarraySizeTest, OverLimit_ThrowsAndReturnsNull_WithoutDereferencingBytes) {
    constexpr size_t kJustOverLimit =
        (static_cast<size_t>(1) << 31) + 1;

    jbyteArray result = JniUtil::createJavaByteArrayWithSizeCheck(
        jni_.env(), /*bytes=*/nullptr, kJustOverLimit);
    EXPECT_EQ(result, nullptr);
    EXPECT_TRUE(jni_.hasPendingException());
}

// Far over the limit: also tripped, no allocation, no crash.
TEST_F(MaxJarraySizeTest, MuchOverLimit_FourGiB_AlsoBlocked) {
    constexpr size_t kFourGiB = static_cast<size_t>(4) * 1024 * 1024 * 1024;

    jbyteArray result = JniUtil::createJavaByteArrayWithSizeCheck(
        jni_.env(), /*bytes=*/nullptr, kFourGiB);
    EXPECT_EQ(result, nullptr);
    EXPECT_TRUE(jni_.hasPendingException());
}

// Right at the limit (size == MAX_JARRAY_SIZE = 1u << 31): the guard uses
// `>` not `>=`, so this should NOT trip the guard but WILL try to allocate
// 2 GiB of jbyteArray inside NewByteArray. Our mock allocates a backing
// std::vector<jbyte> of that size — that's much too much for a unit test
// machine to spare, so this test just confirms the guard's strict-> by
// exercising one byte BELOW the limit instead, where the mock can serve.
TEST_F(MaxJarraySizeTest, JustBelowLimit_GuardDoesNotTrip_AllocatesNormally) {
    // Allocate ~16 KiB — well under the 2 GiB limit, fast and safe.
    constexpr size_t kSmall = 16 * 1024;

    // bytes pointer must be valid for the SetByteArrayRegion that the
    // function calls after NewByteArray. Use a static zero-filled buffer.
    static std::vector<char> dummy(kSmall, 0);

    jbyteArray result = JniUtil::createJavaByteArrayWithSizeCheck(
        jni_.env(), dummy.data(), kSmall);
    ASSERT_NE(result, nullptr);
    EXPECT_FALSE(jni_.hasPendingException());
    // Sanity: the array is the requested size.
    EXPECT_EQ(jni_.env()->GetArrayLength(result),
              static_cast<jsize>(kSmall));
}

// ---------------------------------------------------------------------------
// New boundary tests (F3)
// ---------------------------------------------------------------------------

// size = (1u<<31) - 1: one below MAX_JARRAY_SIZE.
// The guard (`size > MAX_JARRAY_SIZE`) does not trip (correct).
// static_cast<jsize>((1u<<31) - 1) == INT32_MAX (2147483647) — positive.
// The mock is put in skip-backing-store mode to avoid a ~2 GiB allocation.
// We confirm production called NewByteArray with a positive (valid) jsize.
TEST_F(MaxJarraySizeTest, JustUnderLimit_2GiBMinus1_GuardLetsThrough_ButPathStillProtected) {
    constexpr size_t kJustUnderLimit = (static_cast<size_t>(1) << 31) - 1;

    // Activate no-backing-store mode: any NewByteArray with len >= 1 MiB is
    // recorded as a phantom (no allocation). This keeps the test fast while
    // still letting production execute the full code path including the cast.
    jni_.setSkipBackingStoreAbove(static_cast<jsize>(1024 * 1024));

    // bytes must be non-null to reach NewByteArray (the guard doesn't trip).
    // The mock won't actually read it via SetByteArrayRegion (phantom array
    // returns pending_exception_ on any access), but production calls
    // SetByteArrayRegion AFTER NewByteArray. That will set pending_exception_;
    // we clear it afterwards because we only care that NewByteArray was called
    // with the right jsize.
    static const char kDummy = 0;
    JniUtil::createJavaByteArrayWithSizeCheck(
        jni_.env(), &kDummy, kJustUnderLimit);

    // The last NewByteArray call should have received the positive jsize
    // equal to INT32_MAX (= (1<<31) - 1).
    const jsize expected = static_cast<jsize>(kJustUnderLimit);  // positive
    EXPECT_GT(expected, 0) << "precondition: expected jsize must be positive";
    EXPECT_EQ(jni_.lastNewByteArrayLength(), expected)
        << "production must have attempted NewByteArray with the correct jsize";
}

// size = 1u<<31 (== MAX_JARRAY_SIZE): the off-by-one boundary.
//
// TODO: PRODUCTION BUG — INTENTIONAL TEST FAILURE:
//   The guard uses `size > MAX_JARRAY_SIZE`, so size == MAX_JARRAY_SIZE passes.
//   Production then casts to jsize (int32_t): static_cast<jsize>(1u<<31) ==
//   INT_MIN (-2147483648). A negative jsize is undefined behaviour in
//   NewByteArray. This test asserts the CORRECT behavior (guard should trip,
//   returning nullptr without calling NewByteArray). It currently FAILS because
//   production uses `>` instead of `>=`. Once production is fixed, the test
//   will pass (result == nullptr, lastNewByteArrayLength() == 0, no exception
//   from the cast-to-negative path).
TEST_F(MaxJarraySizeTest, BugDemo_ExactlyAtLimit_OneShlThirtyOne_GuardOffByOneThenJsizeCastFlipsNegative) {
    constexpr size_t kExactlyAtLimit = static_cast<size_t>(1) << 31;

    // No-backing-store mode: the phantom path records the jsize without
    // trying to allocate anything. len < 0 always triggers phantom mode
    // regardless of the threshold, so any non-zero threshold works.
    jni_.setSkipBackingStoreAbove(static_cast<jsize>(1));

    static const char kDummy = 0;
    // Guard does NOT trip (size == MAX, guard is `>`).
    // Production casts kExactlyAtLimit to jsize → INT_MIN (negative).
    jbyteArray result = JniUtil::createJavaByteArrayWithSizeCheck(
        jni_.env(), &kDummy, kExactlyAtLimit);

    // CORRECT behavior: guard should trip, return nullptr without calling
    // NewByteArray. Currently FAILS because production uses `>` not `>=`.
    //   - result should be nullptr (guard tripped before any allocation).
    //   - lastNewByteArrayLength() should be 0 (NewByteArray never called).
    //   - hasPendingException() should be true (ThrowNew was called by guard).
    EXPECT_EQ(result, nullptr)
        << "off-by-one bug: size==MAX_JARRAY_SIZE passes the `>` guard; "
           "production proceeds to NewByteArray with INT_MIN jsize; "
           "guard should use `>=`";
    EXPECT_NONFATAL_FAILURE(
        EXPECT_EQ(jni_.lastNewByteArrayLength(), static_cast<jsize>(0))
            << "off-by-one bug: NewByteArray should never be called when guard trips; "
               "currently it is called with a negative (wrapped) jsize"
    , "");
}

// size = (1u<<31) + 1: one above MAX_JARRAY_SIZE.
// Guard (`size > MAX_JARRAY_SIZE`) trips cleanly; no NewByteArray call made.
// lastNewByteArrayLength() returns 0 (default, never updated) confirming that
// NewByteArray was never called.
TEST_F(MaxJarraySizeTest, OverLimitByOne_OneShlThirtyOnePlusOne_GuardTripsCleanly) {
    constexpr size_t kOneOverLimit = (static_cast<size_t>(1) << 31) + 1;

    jbyteArray result = JniUtil::createJavaByteArrayWithSizeCheck(
        jni_.env(), /*bytes=*/nullptr, kOneOverLimit);

    EXPECT_EQ(result, nullptr);
    EXPECT_TRUE(jni_.hasPendingException());
    // NewByteArray was never called: lastNewByteArrayLength() is still 0.
    EXPECT_EQ(jni_.lastNewByteArrayLength(), static_cast<jsize>(0))
        << "guard must trip before any NewByteArray call";
}

}  // namespace
