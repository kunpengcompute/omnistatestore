#ifndef FALCON_TESTS_MOCK_JNI_ENV_H
#define FALCON_TESTS_MOCK_JNI_ENV_H

#include <jni.h>

#include <cstdarg>
#include <cstdint>
#include <cstring>
#include <string>
#include <memory>
#include <unordered_map>
#include <vector>

namespace falcon_test {

/**
 * Minimal in-process JNIEnv stand-in for unit tests.
 *
 * Only the JNI entry points actually used by the Falcon C++ side are wired up
 * (NewByteArray / SetByteArrayRegion / GetByteArrayRegion / GetArrayLength /
 *  GetByteArrayElements / ReleaseByteArrayElements / ExceptionCheck /
 *  ExceptionClear / ThrowNew / FindClass etc.). Anything else dispatches to a
 *  null function pointer and will crash the test, which is the intended signal
 *  that a new code path needs mock support.
 *
 * The mock owns the byte[] storage it hands out as `jbyteArray`, so tests can
 * inspect array contents after the SUT releases them.
 *
 * Extended knobs:
 *
 *   setSkipBackingStoreAbove(threshold):
 *     When threshold > 0 (or threshold < 0, indicating a negative jsize from a
 *     huge unsigned cast), NewByteArray records the handle + requested length
 *     WITHOUT allocating a backing std::vector. GetArrayLength returns the
 *     recorded length; GetByteArrayRegion / SetByteArrayRegion /
 *     GetByteArrayElements set pending_exception_ (no real storage).
 *     lastNewByteArrayLength() returns the jsize passed to the last
 *     NewByteArray call (whether backed or not).
 *     Default: 0 (always allocate — current behavior).
 *
 *   setLocalRefCap(cap):
 *     When cap > 0, live_local_refs_ exceeding cap on increment causes
 *     pending_exception_ to be set and the offending allocation function to
 *     return nullptr (mimics JVM local-ref table overflow). Default 0 =
 *     unlimited. DeleteLocalRef decrements the counter (lower-bounded at 0).
 *     currentLocalRefs() returns the current live count.
 */
class MockJniEnv {
public:
    MockJniEnv();
    ~MockJniEnv();

    MockJniEnv(const MockJniEnv&) = delete;
    MockJniEnv& operator=(const MockJniEnv&) = delete;

    // Hand to code under test.
    JNIEnv* env() { return &env_; }

    // Test inspection helpers.
    std::vector<jbyte> arrayBytes(jbyteArray array) const;
    bool hasPendingException() const { return pending_exception_; }
    void clearPendingException() { pending_exception_ = false; }

    // Exception-injection knob (1-indexed). After this many byte-array-related
    // JNI calls (NewByteArray, GetByteArrayRegion, SetByteArrayRegion,
    // GetByteArrayElements), the next call sets pending_exception_ and returns
    // a failure sentinel (nullptr for ones that return jbyteArray/jbyte*; void
    // ones just flip the flag). Fires once and self-disarms. Pass 0 to disarm.
    void injectByteArrayFailureAtCall(int call_number) {
        inject_byte_array_failure_at_ = call_number;
        byte_array_call_num_ = 0;
    }

    // Result-injection knob for IsInstanceOf — toggles the boolean returned.
    void setIsInstanceOfResult(jboolean result) { is_instance_of_result_ = result; }

    // -----------------------------------------------------------------------
    // F3: no-backing-store mode for huge NewByteArray calls.
    // -----------------------------------------------------------------------

    // When threshold != 0, NewByteArray calls where `len >= threshold` OR
    // `len < 0` (negative jsize from an unsigned cast overflow) record the
    // handle+length without allocating a backing vector. Access operations
    // (GetByteArrayRegion etc.) on such phantom arrays set pending_exception_.
    // Pass 0 (default) to always allocate (normal behavior).
    void setSkipBackingStoreAbove(jsize threshold) {
        skip_backing_store_above_ = threshold;
    }

    // Returns the jsize passed to the most recent NewByteArray call (or 0 if
    // NewByteArray has not been called yet). Works for both backed and phantom
    // arrays, so tests can confirm the production code attempted to allocate
    // the right (possibly negative) length.
    jsize lastNewByteArrayLength() const { return last_new_byte_array_len_; }

    // -----------------------------------------------------------------------
    // F4: local-ref tracking.
    // -----------------------------------------------------------------------

    // When cap > 0, incrementing live_local_refs_ beyond cap sets
    // pending_exception_ and makes the offending allocation return nullptr
    // (mimics JVM local-ref table overflow). Default 0 = unlimited.
    void setLocalRefCap(int cap) { local_ref_cap_ = cap; }

    // Returns the current count of live (unreleased) local refs.
    int currentLocalRefs() const { return live_local_refs_; }

    // -----------------------------------------------------------------------
    // Tier-3 test helpers: object arrays (jstring[] / byte[][])
    // -----------------------------------------------------------------------

    // Creates a fake jobjectArray of `len` nullptr-initialised slots.
    jobjectArray makeObjectArray(jsize len);

    // Reads the raw content of a fake long array (for test inspection).
    std::vector<jlong> longArrayValues(jlongArray arr) const;

    // -----------------------------------------------------------------------
    // Tier-3 test helpers: direct ByteBuffers
    // -----------------------------------------------------------------------

    // Allocates a direct buffer of `capacity` zero-initialised bytes and
    // returns an opaque jobject handle owned by the mock.
    jobject makeDirectBuffer(jlong capacity);

    // Returns the raw backing pointer for a previously created direct buffer.
    char* directBufferPtr(jobject buf) const;

    // Returns the capacity of a previously created direct buffer.
    jlong directBufferCapacity(jobject buf) const;

private:
    // Storage backing each fake jbyteArray. The handle we hand out is a
    // monotonically increasing integer cast to `jbyteArray`, NOT the address
    // of the heap holder — so addresses can be recycled by the allocator
    // without aliasing the active map entry.
    struct ByteArray {
        std::vector<jbyte> data;
    };

    // Phantom (no-backing-store) entry for huge/negative NewByteArray calls.
    // Stores only the recorded length; access operations set pending_exception_.
    struct PhantomArray {
        jsize recorded_len;
    };

    // Storage backing each fake jstring. Same monotonic-handle scheme as
    // ByteArray.
    struct StringObj {
        std::string utf;
    };

    // Storage backing each fake jobjectArray (used for jstring[] / byte[][]).
    struct ObjectArray {
        std::vector<jobject> elements;
    };

    // Storage backing each fake jlongArray.
    struct LongArray {
        std::vector<jlong> data;
    };

    // Storage backing each fake direct ByteBuffer.
    struct DirectBuffer {
        std::unique_ptr<char[]> mem;
        jlong capacity{0};
    };

    static MockJniEnv* fromEnv(JNIEnv* env);

    // JNI thunks --------------------------------------------------------
    static jbyteArray JNICALL newByteArray(JNIEnv* env, jsize len);
    static jsize JNICALL getArrayLength(JNIEnv* env, jarray array);
    static void JNICALL getByteArrayRegion(JNIEnv* env, jbyteArray array, jsize start, jsize len, jbyte* buf);
    static void JNICALL setByteArrayRegion(JNIEnv* env, jbyteArray array, jsize start, jsize len, const jbyte* buf);
    static jbyte* JNICALL getByteArrayElements(JNIEnv* env, jbyteArray array, jboolean* is_copy);
    static void JNICALL releaseByteArrayElements(JNIEnv* env, jbyteArray array, jbyte* elems, jint mode);
    static jboolean JNICALL exceptionCheck(JNIEnv* env);
    static void JNICALL exceptionClear(JNIEnv* env);
    static jclass JNICALL findClass(JNIEnv* env, const char* name);
    static jint JNICALL throwNew(JNIEnv* env, jclass clazz, const char* msg);

    // Extra stubs needed by FalconExceptionJni::ThrowNew(env, msg, status).
    // The full path calls GetMethodID / NewStringUTF / NewObject / Throw /
    // DeleteLocalRef; without these the test binary crashes on a null function
    // pointer the moment a null-handle test exercises the error path.
    static jmethodID JNICALL getMethodID(JNIEnv* env, jclass clazz, const char* name, const char* sig);
    static jstring JNICALL newStringUTF(JNIEnv* env, const char* utf);
    // env->NewObject(...) dispatches through NewObjectV in jni.h; wire that slot.
    static jobject JNICALL newObjectV(JNIEnv* env, jclass clazz, jmethodID mid, va_list args);
    static jint JNICALL throwObject(JNIEnv* env, jthrowable obj);
    static void JNICALL deleteLocalRef(JNIEnv* env, jobject obj);

    // Tier-2 additions for FalconUtilJni / FalconExceptionJni / JNI entries.
    static const char* JNICALL getStringUTFChars(JNIEnv* env, jstring str, jboolean* is_copy);
    static void JNICALL releaseStringUTFChars(JNIEnv* env, jstring str, const char* chars);
    static jsize JNICALL getStringUTFLength(JNIEnv* env, jstring str);
    static jboolean JNICALL isInstanceOf(JNIEnv* env, jobject obj, jclass clazz);
    static jmethodID JNICALL getStaticMethodID(JNIEnv* env, jclass clazz, const char* name, const char* sig);
    static jobject JNICALL callObjectMethodV(JNIEnv* env, jobject obj, jmethodID mid, va_list args);
    static jbyte JNICALL callByteMethodV(JNIEnv* env, jobject obj, jmethodID mid, va_list args);
    static jobject JNICALL callStaticObjectMethodV(JNIEnv* env, jclass clazz, jmethodID mid, va_list args);

    // Tier-3 additions: object arrays, long arrays, direct buffers.
    static jobjectArray JNICALL newObjectArray(JNIEnv* env, jsize len, jclass clazz, jobject init);
    static jobject JNICALL getObjectArrayElement(JNIEnv* env, jobjectArray arr, jsize idx);
    static void JNICALL setObjectArrayElement(JNIEnv* env, jobjectArray arr, jsize idx, jobject val);
    static jlongArray JNICALL newLongArray(JNIEnv* env, jsize len);
    static jlong* JNICALL getLongArrayElements(JNIEnv* env, jlongArray arr, jboolean* is_copy);
    static void JNICALL releaseLongArrayElements(JNIEnv* env, jlongArray arr, jlong* elems, jint mode);
    static void JNICALL setLongArrayRegion(JNIEnv* env, jlongArray arr, jsize start, jsize len, const jlong* buf);
    static void* JNICALL getDirectBufferAddress(JNIEnv* env, jobject buf);
    static jlong JNICALL getDirectBufferCapacity(JNIEnv* env, jobject buf);

    // Helper: returns true and sets pending_exception_ if the configured
    // injection point was reached on this call.
    bool maybeInjectByteArrayFailure();

    // Helper: increments the local-ref counter; if cap is set and exceeded,
    // sets pending_exception_ and returns false (caller should return nullptr).
    bool incrementLocalRef();

    JNINativeInterface_ functions_{};
    JNIEnv env_{};

    std::unordered_map<jbyteArray, std::unique_ptr<ByteArray>> arrays_{};
    std::unordered_map<jbyteArray, std::unique_ptr<PhantomArray>> phantom_arrays_{};
    std::unordered_map<jstring, std::unique_ptr<StringObj>> strings_{};
    std::unordered_map<jobjectArray, std::unique_ptr<ObjectArray>> object_arrays_{};
    std::unordered_map<jlongArray, std::unique_ptr<LongArray>> long_arrays_{};
    std::unordered_map<jobject, std::unique_ptr<DirectBuffer>> direct_buffers_{};
    std::uintptr_t next_handle_{1};  // 0 is reserved as "null handle"
    bool pending_exception_{false};

    // Injection state.
    int inject_byte_array_failure_at_{0};  // 0 = disarmed
    int byte_array_call_num_{0};
    jboolean is_instance_of_result_{JNI_TRUE};

    // F3: no-backing-store knob.
    jsize skip_backing_store_above_{0};    // 0 = always allocate
    jsize last_new_byte_array_len_{0};     // jsize of last NewByteArray call

    // F4: local-ref tracking.
    int live_local_refs_{0};
    int local_ref_cap_{0};                 // 0 = unlimited
};

}  // namespace falcon_test

#endif  // FALCON_TESTS_MOCK_JNI_ENV_H
