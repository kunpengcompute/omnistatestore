#include "MockJniEnv.h"
#include <string>
#include <cstdarg>
#include <cstdlib>
#include <cstring>

namespace falcon_test {

namespace {
// JNIEnv has no portable per-instance back-pointer slot (the `reserved*`
// fields live on the function table struct, not JNIEnv_ itself, and that's
// JDK-version dependent). Tests are sequential, so a thread_local pointer
// to the active mock is enough for the static thunks to find their owner.
thread_local MockJniEnv* current_mock_env = nullptr;
}

MockJniEnv::MockJniEnv() {
    // Wire only the JNI entry points the Falcon C++ code actually invokes.
    // Everything else stays nullptr — call sites that need new mock support
    // will null-deref loudly during a test run, which is the signal to extend
    // this mock rather than silently mis-stubbing.
    functions_.NewByteArray = &MockJniEnv::newByteArray;
    functions_.GetArrayLength = &MockJniEnv::getArrayLength;
    functions_.GetByteArrayRegion = &MockJniEnv::getByteArrayRegion;
    functions_.SetByteArrayRegion = &MockJniEnv::setByteArrayRegion;
    functions_.GetByteArrayElements = &MockJniEnv::getByteArrayElements;
    functions_.ReleaseByteArrayElements = &MockJniEnv::releaseByteArrayElements;
    functions_.ExceptionCheck = &MockJniEnv::exceptionCheck;
    functions_.ExceptionClear = &MockJniEnv::exceptionClear;
    functions_.FindClass = &MockJniEnv::findClass;
    functions_.ThrowNew = &MockJniEnv::throwNew;
    // Extra stubs for FalconExceptionJni::ThrowNew(env, msg, status).
    functions_.GetMethodID = &MockJniEnv::getMethodID;
    functions_.NewStringUTF = &MockJniEnv::newStringUTF;
    // env->NewObject(...) dispatches to NewObjectV in the JNI inline wrapper.
    functions_.NewObjectV = &MockJniEnv::newObjectV;
    functions_.Throw = &MockJniEnv::throwObject;
    functions_.DeleteLocalRef = &MockJniEnv::deleteLocalRef;
    // Tier-2 additions: string + Method-call slots used by JniUtil and
    // FalconExceptionJni::toCppStatus(env, jthrowable).
    functions_.GetStringUTFChars = &MockJniEnv::getStringUTFChars;
    functions_.ReleaseStringUTFChars = &MockJniEnv::releaseStringUTFChars;
    functions_.GetStringUTFLength = &MockJniEnv::getStringUTFLength;
    functions_.IsInstanceOf = &MockJniEnv::isInstanceOf;
    functions_.GetStaticMethodID = &MockJniEnv::getStaticMethodID;
    // CallObjectMethod / CallByteMethod / CallStaticObjectMethod inline-dispatch
    // through their *V variants (jni.h).
    functions_.CallObjectMethodV = &MockJniEnv::callObjectMethodV;
    functions_.CallByteMethodV = &MockJniEnv::callByteMethodV;
    functions_.CallStaticObjectMethodV = &MockJniEnv::callStaticObjectMethodV;
    // Tier-3 additions: object arrays, long arrays, direct buffers.
    functions_.NewObjectArray = &MockJniEnv::newObjectArray;
    functions_.GetObjectArrayElement = &MockJniEnv::getObjectArrayElement;
    functions_.SetObjectArrayElement = &MockJniEnv::setObjectArrayElement;
    functions_.NewLongArray = &MockJniEnv::newLongArray;
    functions_.GetLongArrayElements = &MockJniEnv::getLongArrayElements;
    functions_.ReleaseLongArrayElements = &MockJniEnv::releaseLongArrayElements;
    functions_.SetLongArrayRegion = &MockJniEnv::setLongArrayRegion;
    functions_.GetDirectBufferAddress = &MockJniEnv::getDirectBufferAddress;
    functions_.GetDirectBufferCapacity = &MockJniEnv::getDirectBufferCapacity;

    env_.functions = &functions_;
    current_mock_env = this;
}

bool MockJniEnv::maybeInjectByteArrayFailure() {
    ++byte_array_call_num_;
    if (inject_byte_array_failure_at_ > 0 &&
        byte_array_call_num_ == inject_byte_array_failure_at_) {
        pending_exception_ = true;
        inject_byte_array_failure_at_ = 0;  // self-disarm
        return true;
    }
    return false;
}

bool MockJniEnv::incrementLocalRef() {
    ++live_local_refs_;
    if (local_ref_cap_ > 0 && live_local_refs_ > local_ref_cap_) {
        pending_exception_ = true;
        --live_local_refs_;  // don't count the failed allocation
        return false;
    }
    return true;
}

MockJniEnv::~MockJniEnv() {
    if (current_mock_env == this) {
        current_mock_env = nullptr;
    }
}

MockJniEnv* MockJniEnv::fromEnv(JNIEnv* /*env*/) {
    return current_mock_env;
}

std::vector<jbyte> MockJniEnv::arrayBytes(jbyteArray array) const {
    auto it = arrays_.find(array);
    if (it == arrays_.end()) {
        return {};
    }
    return it->second->data;
}

// JNI thunks ----------------------------------------------------------------

jbyteArray MockJniEnv::newByteArray(JNIEnv* env, jsize len) {
    auto* self = fromEnv(env);
    if (self->maybeInjectByteArrayFailure()) {
        return nullptr;
    }
    // Record the raw jsize for every call so tests can inspect it.
    self->last_new_byte_array_len_ = len;

    // Hand out a monotonically increasing integer handle (cast to jbyteArray).
    // The actual storage lives in `arrays_` (or `phantom_arrays_`) keyed by
    // that handle, so allocator address recycling cannot alias an existing entry.
    auto handle = reinterpret_cast<jbyteArray>(self->next_handle_++);

    // No-backing-store mode: if the threshold is set and len is at or above it,
    // OR if len is negative (huge unsigned cast wrapping into int32_t), record
    // a phantom entry rather than allocating a real vector.
    bool use_phantom = false;
    if (self->skip_backing_store_above_ != 0) {
        // A negative len always means a huge unsigned value was cast to jsize.
        if (len < 0 || len >= self->skip_backing_store_above_) {
            use_phantom = true;
        }
    }

    if (use_phantom) {
        self->phantom_arrays_[handle] = std::make_unique<PhantomArray>(
            PhantomArray{len});
    } else {
        self->arrays_[handle] = std::make_unique<ByteArray>(
            ByteArray{std::vector<jbyte>(static_cast<size_t>(len), 0)});
    }
    return handle;
}

jsize MockJniEnv::getArrayLength(JNIEnv* env, jarray array) {
    auto* self = fromEnv(env);
    // Check backed byte arrays first.
    auto it = self->arrays_.find(reinterpret_cast<jbyteArray>(array));
    if (it != self->arrays_.end()) {
        return static_cast<jsize>(it->second->data.size());
    }
    // Check phantom (no-backing-store) byte arrays.
    auto pit = self->phantom_arrays_.find(reinterpret_cast<jbyteArray>(array));
    if (pit != self->phantom_arrays_.end()) {
        return pit->second->recorded_len;
    }
    // Check object arrays.
    auto oit = self->object_arrays_.find(reinterpret_cast<jobjectArray>(array));
    if (oit != self->object_arrays_.end()) {
        return static_cast<jsize>(oit->second->elements.size());
    }
    // Check long arrays.
    auto lit = self->long_arrays_.find(reinterpret_cast<jlongArray>(array));
    if (lit != self->long_arrays_.end()) {
        return static_cast<jsize>(lit->second->data.size());
    }
    return 0;
}

void MockJniEnv::getByteArrayRegion(JNIEnv* env, jbyteArray array, jsize start, jsize len, jbyte* buf) {
    auto* self = fromEnv(env);
    if (self->maybeInjectByteArrayFailure()) {
        return;
    }
    // Phantom arrays have no real storage; any access is an error.
    if (self->phantom_arrays_.count(array)) {
        self->pending_exception_ = true;
        return;
    }
    auto it = self->arrays_.find(array);
    if (it == self->arrays_.end()) {
        self->pending_exception_ = true;
        return;
    }
    if (start < 0 || len < 0 || static_cast<size_t>(start + len) > it->second->data.size()) {
        self->pending_exception_ = true;
        return;
    }
    std::memcpy(buf, it->second->data.data() + start, len);
}

void MockJniEnv::setByteArrayRegion(JNIEnv* env, jbyteArray array, jsize start, jsize len, const jbyte* buf) {
    auto* self = fromEnv(env);
    if (self->maybeInjectByteArrayFailure()) {
        return;
    }
    // Phantom arrays have no real storage; any write is an error.
    if (self->phantom_arrays_.count(array)) {
        self->pending_exception_ = true;
        return;
    }
    auto it = self->arrays_.find(array);
    if (it == self->arrays_.end()) {
        self->pending_exception_ = true;
        return;
    }
    if (start < 0 || len < 0 || static_cast<size_t>(start + len) > it->second->data.size()) {
        self->pending_exception_ = true;
        return;
    }
    std::memcpy(it->second->data.data() + start, buf, len);
}

jbyte* MockJniEnv::getByteArrayElements(JNIEnv* env, jbyteArray array, jboolean* is_copy) {
    auto* self = fromEnv(env);
    if (self->maybeInjectByteArrayFailure()) {
        return nullptr;
    }
    // Phantom arrays have no real storage; return nullptr (signals OOM to caller).
    if (self->phantom_arrays_.count(array)) {
        self->pending_exception_ = true;
        return nullptr;
    }
    auto it = self->arrays_.find(array);
    if (it == self->arrays_.end()) {
        self->pending_exception_ = true;
        return nullptr;
    }
    if (is_copy != nullptr) {
        *is_copy = JNI_FALSE;
    }
    return it->second->data.data();
}

void MockJniEnv::releaseByteArrayElements(JNIEnv* env, jbyteArray array, jbyte* /*elems*/, jint /*mode*/) {
    // Pinned access; nothing to free. Validate the array still exists so misuse
    // surfaces as a test failure.
    auto* self = fromEnv(env);
    if (self->arrays_.find(array) == self->arrays_.end()) {
        self->pending_exception_ = true;
    }
}

jboolean MockJniEnv::exceptionCheck(JNIEnv* env) {
    return fromEnv(env)->pending_exception_ ? JNI_TRUE : JNI_FALSE;
}

void MockJniEnv::exceptionClear(JNIEnv* env) {
    fromEnv(env)->pending_exception_ = false;
}

jclass MockJniEnv::findClass(JNIEnv* /*env*/, const char* /*name*/) {
    // Return a non-null sentinel; tests that exercise exception-throwing paths
    // don't introspect the class object.
    static int sentinel = 0;
    return reinterpret_cast<jclass>(&sentinel);
}

jint MockJniEnv::throwNew(JNIEnv* env, jclass /*clazz*/, const char* /*msg*/) {
    fromEnv(env)->pending_exception_ = true;
    return JNI_OK;
}

// Extra stubs required by FalconExceptionJni::ThrowNew(env, msg, status) -----

// Return a non-null sentinel method ID so callers don't abort on nullptr check.
jmethodID MockJniEnv::getMethodID(JNIEnv* /*env*/, jclass /*clazz*/,
                                   const char* /*name*/, const char* /*sig*/) {
    static int sentinel = 0;
    return reinterpret_cast<jmethodID>(&sentinel);
}

// Allocate a real backing string so subsequent GetStringUTFChars /
// GetStringUTFLength calls can read it back.
jstring MockJniEnv::newStringUTF(JNIEnv* env, const char* utf) {
    auto* self = fromEnv(env);
    if (!self->incrementLocalRef()) {
        return nullptr;  // local-ref table overflow
    }
    auto handle = reinterpret_cast<jstring>(self->next_handle_++);
    self->strings_[handle] = std::make_unique<StringObj>(
        StringObj{utf == nullptr ? std::string{} : std::string{utf}});
    return handle;
}

// Return a non-null sentinel object so callers don't abort on nullptr check.
// env->NewObject(...) dispatches here via the NewObjectV slot in jni.h.
jobject MockJniEnv::newObjectV(JNIEnv* env, jclass /*clazz*/,
                                jmethodID /*mid*/, va_list /*args*/) {
    auto* self = fromEnv(env);
    if (!self->incrementLocalRef()) {
        return nullptr;  // local-ref table overflow
    }
    static int sentinel = 0;
    return reinterpret_cast<jobject>(&sentinel);
}

// Mark the exception as pending (mirrors real JNI Throw semantics).
jint MockJniEnv::throwObject(JNIEnv* env, jthrowable /*obj*/) {
    fromEnv(env)->pending_exception_ = true;
    return JNI_OK;
}

// Decrement the live local-ref counter. The mock cannot distinguish which
// jobject is being released, so we just decrement (lower-bounded at 0).
// This faithfully models DeleteLocalRef for any local ref the production code
// obtained via NewStringUTF / NewObject / NewObjectArray.
void MockJniEnv::deleteLocalRef(JNIEnv* env, jobject /*obj*/) {
    auto* self = fromEnv(env);
    if (self->live_local_refs_ > 0) {
        --self->live_local_refs_;
    }
}

// String access -------------------------------------------------------------

const char* MockJniEnv::getStringUTFChars(JNIEnv* env, jstring str, jboolean* is_copy) {
    auto* self = fromEnv(env);
    auto it = self->strings_.find(str);
    if (it == self->strings_.end()) {
        self->pending_exception_ = true;
        return nullptr;
    }
    if (is_copy != nullptr) *is_copy = JNI_FALSE;
    return it->second->utf.c_str();
}

void MockJniEnv::releaseStringUTFChars(JNIEnv* /*env*/, jstring /*str*/, const char* /*chars*/) {
    // No-op: pinned access into the owned StringObj.
}

jsize MockJniEnv::getStringUTFLength(JNIEnv* env, jstring str) {
    auto* self = fromEnv(env);
    auto it = self->strings_.find(str);
    if (it == self->strings_.end()) {
        self->pending_exception_ = true;
        return 0;
    }
    return static_cast<jsize>(it->second->utf.size());
}

// Class / method introspection ---------------------------------------------

jboolean MockJniEnv::isInstanceOf(JNIEnv* env, jobject /*obj*/, jclass /*clazz*/) {
    return fromEnv(env)->is_instance_of_result_;
}

jmethodID MockJniEnv::getStaticMethodID(JNIEnv* /*env*/, jclass /*clazz*/,
                                         const char* /*name*/, const char* /*sig*/) {
    static int sentinel = 0;
    return reinterpret_cast<jmethodID>(&sentinel);
}

// Method calls --------------------------------------------------------------
// env->CallObjectMethod / CallByteMethod / CallStaticObjectMethod inline-
// dispatch through their *V variants in jni.h. Tests that need to drive
// FalconExceptionJni::toCppStatus only need these to return non-null
// sentinels (jobject) or 0 (jbyte → maps to Status::Code::kOk in
// StatusJni::toCppStatus).

jobject MockJniEnv::callObjectMethodV(JNIEnv* /*env*/, jobject /*obj*/,
                                       jmethodID /*mid*/, va_list /*args*/) {
    static int sentinel = 0;
    return reinterpret_cast<jobject>(&sentinel);
}

jbyte MockJniEnv::callByteMethodV(JNIEnv* /*env*/, jobject /*obj*/,
                                   jmethodID /*mid*/, va_list /*args*/) {
    return 0;  // 0x0 = Code::kOk, SubCode::kNone
}

jobject MockJniEnv::callStaticObjectMethodV(JNIEnv* /*env*/, jclass /*clazz*/,
                                             jmethodID /*mid*/, va_list /*args*/) {
    static int sentinel = 0;
    return reinterpret_cast<jobject>(&sentinel);
}

// Tier-3: object arrays -----------------------------------------------------

// Allocate a real backing object array with `len` nullptr-initialised slots.
jobjectArray MockJniEnv::newObjectArray(JNIEnv* env, jsize len,
                                         jclass /*clazz*/, jobject /*init*/) {
    auto* self = fromEnv(env);
    if (!self->incrementLocalRef()) {
        return nullptr;  // local-ref table overflow
    }
    auto handle = reinterpret_cast<jobjectArray>(self->next_handle_++);
    self->object_arrays_[handle] = std::make_unique<ObjectArray>(
        ObjectArray{std::vector<jobject>(static_cast<size_t>(len), nullptr)});
    return handle;
}

jobject MockJniEnv::getObjectArrayElement(JNIEnv* env, jobjectArray arr, jsize idx) {
    auto* self = fromEnv(env);
    auto it = self->object_arrays_.find(arr);
    if (it == self->object_arrays_.end() || idx < 0 ||
        static_cast<size_t>(idx) >= it->second->elements.size()) {
        self->pending_exception_ = true;
        return nullptr;
    }
    return it->second->elements[static_cast<size_t>(idx)];
}

void MockJniEnv::setObjectArrayElement(JNIEnv* env, jobjectArray arr, jsize idx, jobject val) {
    auto* self = fromEnv(env);
    auto it = self->object_arrays_.find(arr);
    if (it == self->object_arrays_.end() || idx < 0 ||
        static_cast<size_t>(idx) >= it->second->elements.size()) {
        self->pending_exception_ = true;
        return;
    }
    it->second->elements[static_cast<size_t>(idx)] = val;
}

// Test-side helper: build a jobjectArray from the test without going through
// the JNI layer.
jobjectArray MockJniEnv::makeObjectArray(jsize len) {
    auto handle = reinterpret_cast<jobjectArray>(next_handle_++);
    object_arrays_[handle] = std::make_unique<ObjectArray>(
        ObjectArray{std::vector<jobject>(static_cast<size_t>(len), nullptr)});
    return handle;
}

// Tier-3: long arrays -------------------------------------------------------

jlongArray MockJniEnv::newLongArray(JNIEnv* env, jsize len) {
    auto* self = fromEnv(env);
    auto handle = reinterpret_cast<jlongArray>(self->next_handle_++);
    self->long_arrays_[handle] = std::make_unique<LongArray>(
        LongArray{std::vector<jlong>(static_cast<size_t>(len), 0)});
    return handle;
}

jlong* MockJniEnv::getLongArrayElements(JNIEnv* env, jlongArray arr, jboolean* is_copy) {
    auto* self = fromEnv(env);
    auto it = self->long_arrays_.find(arr);
    if (it == self->long_arrays_.end()) {
        self->pending_exception_ = true;
        return nullptr;
    }
    if (is_copy != nullptr) *is_copy = JNI_FALSE;
    return it->second->data.data();
}

void MockJniEnv::releaseLongArrayElements(JNIEnv* env, jlongArray arr,
                                           jlong* /*elems*/, jint /*mode*/) {
    // Pinned access; nothing to free. Validate the array still exists.
    auto* self = fromEnv(env);
    if (self->long_arrays_.find(arr) == self->long_arrays_.end()) {
        self->pending_exception_ = true;
    }
}

void MockJniEnv::setLongArrayRegion(JNIEnv* env, jlongArray arr,
                                     jsize start, jsize len, const jlong* buf) {
    auto* self = fromEnv(env);
    auto it = self->long_arrays_.find(arr);
    if (it == self->long_arrays_.end()) {
        self->pending_exception_ = true;
        return;
    }
    if (start < 0 || len < 0 ||
        static_cast<size_t>(start + len) > it->second->data.size()) {
        self->pending_exception_ = true;
        return;
    }
    std::memcpy(it->second->data.data() + start, buf,
                static_cast<size_t>(len) * sizeof(jlong));
}

// Test-side helper: read raw long values back for inspection.
std::vector<jlong> MockJniEnv::longArrayValues(jlongArray arr) const {
    auto it = long_arrays_.find(arr);
    if (it == long_arrays_.end()) return {};
    return it->second->data;
}

// Tier-3: direct ByteBuffers ------------------------------------------------

// GetDirectBufferAddress — return the backing pointer for a direct buffer
// handle created via makeDirectBuffer().
void* MockJniEnv::getDirectBufferAddress(JNIEnv* env, jobject buf) {
    auto* self = fromEnv(env);
    auto it = self->direct_buffers_.find(buf);
    if (it == self->direct_buffers_.end()) {
        return nullptr;
    }
    return it->second->mem.get();
}

// GetDirectBufferCapacity — return the capacity of a previously created
// direct buffer, or -1 if the handle is unknown.
jlong MockJniEnv::getDirectBufferCapacity(JNIEnv* env, jobject buf) {
    auto* self = fromEnv(env);
    auto it = self->direct_buffers_.find(buf);
    if (it == self->direct_buffers_.end()) {
        return -1;
    }
    return it->second->capacity;
}

// Test-side helper: allocate a direct buffer of `capacity` zero-initialised
// bytes and return an opaque jobject handle owned by the mock.
jobject MockJniEnv::makeDirectBuffer(jlong capacity) {
    auto handle = reinterpret_cast<jobject>(next_handle_++);
    auto buf = std::make_unique<DirectBuffer>();
    buf->capacity = capacity;
    buf->mem = std::unique_ptr<char[]>(new char[static_cast<size_t>(capacity)]());
    direct_buffers_[handle] = std::move(buf);
    return handle;
}

char* MockJniEnv::directBufferPtr(jobject buf) const {
    auto it = direct_buffers_.find(buf);
    if (it == direct_buffers_.end()) return nullptr;
    return it->second->mem.get();
}

jlong MockJniEnv::directBufferCapacity(jobject buf) const {
    auto it = direct_buffers_.find(buf);
    if (it == direct_buffers_.end()) return -1;
    return it->second->capacity;
}

}  // namespace falcon_test
