/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistatestore is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

#include <jni.h>

#include "common/scope_guard.h"
#include "include/boost_state_table.h"

namespace ock {
namespace bss {
namespace {
class ThreadEnv {
public:
    explicit ThreadEnv(JavaVM *vm) : mVm(vm)
    {
        auto result = vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_6);
        if (result == JNI_EDETACHED) {
            mAttached = vm->AttachCurrentThreadAsDaemon(reinterpret_cast<void **>(&env), nullptr) == JNI_OK;
        }
    }
    ~ThreadEnv()
    {
        if (mAttached) {
            mVm->DetachCurrentThread();
        }
    }
    JNIEnv *env = nullptr;

private:
    JavaVM *mVm;
    bool mAttached = false;
};

JNIEnv *GetThreadEnv(JavaVM *vm)
{
    thread_local ThreadEnv thread(vm);
    return thread.env;
}

class JavaTtlFilter {
public:
    JavaTtlFilter(JNIEnv *env, jobject filter)
    {
        env->GetJavaVM(&mVm);
        mFilter = env->NewGlobalRef(filter);
        if (mFilter == nullptr) {
            return;
        }
        auto cls = env->GetObjectClass(filter);
        if (cls == nullptr) {
            return;
        }
        mClock = env->GetMethodID(cls, "currentTimestamp", "()J");
        if (mClock != nullptr) {
            mList = env->GetMethodID(cls, "filterList", "([BJJ)[B");
        }
        env->DeleteLocalRef(cls);
    }
    ~JavaTtlFilter()
    {
        ThreadEnv thread(mVm);
        if (thread.env != nullptr && mFilter != nullptr) {
            thread.env->DeleteGlobalRef(mFilter);
        }
    }
    bool Valid() const
    {
        return mFilter != nullptr && mClock != nullptr && mList != nullptr;
    }

    BResult Now(int64_t &now)
    {
        auto env = GetThreadEnv(mVm);
        if (env == nullptr) {
            return BSS_ERR;
        }
        now = env->CallLongMethod(mFilter, mClock);
        return CheckException(env);
    }

    BResult FilterList(Value &value, int64_t ttl, int64_t now, const Value::BufferAllocator &allocator)
    {
        auto env = GetThreadEnv(mVm);
        if (env == nullptr || value.ValueLen() > INT32_MAX) {
            return BSS_ERR;
        }
        if (env->PushLocalFrame(2) != JNI_OK) {
            CheckException(env);
            return BSS_ALLOC_FAIL;
        }
        SCOPE_EXIT({ env->PopLocalFrame(nullptr); });
        auto input = env->NewByteArray(static_cast<jsize>(value.ValueLen()));
        RETURN_NOT_OK(CheckException(env));
        RETURN_ALLOC_FAIL_AS_NULLPTR(input);
        env->SetByteArrayRegion(input, 0, static_cast<jsize>(value.ValueLen()),
                                reinterpret_cast<const jbyte *>(value.ValueData()));
        RETURN_NOT_OK(CheckException(env));
        auto output = static_cast<jbyteArray>(
            env->CallObjectMethod(mFilter, mList, input, static_cast<jlong>(ttl), static_cast<jlong>(now)));
        RETURN_NOT_OK(CheckException(env));
        if (env->IsSameObject(input, output)) {
            return BSS_OK;
        }
        if (output == nullptr && value.ValueType() != APPEND) {
            value.Init(DELETE, 0, nullptr, value.SeqId(), nullptr);
            return BSS_OK;
        }
        // An empty APPEND must not mask an older PUT outside the compaction inputs.
        uint32_t length = output == nullptr ? sizeof(uint32_t) : env->GetArrayLength(output);
        auto buffer = allocator(length);
        RETURN_ALLOC_FAIL_AS_NULLPTR(buffer);
        if (output == nullptr) {
            std::fill_n(buffer->Data(), length, 0);
        } else {
            env->GetByteArrayRegion(output, 0, length, reinterpret_cast<jbyte *>(buffer->Data()));
            RETURN_NOT_OK(CheckException(env));
        }
        value.Init(value.ValueType(), length, buffer->Data(), value.SeqId(), buffer);
        return BSS_OK;
    }

private:
    static BResult CheckException(JNIEnv *env)
    {
        if (!env->ExceptionCheck()) {
            return BSS_OK;
        }
        LOG_ERROR("Flink TTL callback failed; aborting compaction without publishing its output.");
        env->ExceptionDescribe();
        env->ExceptionClear();
        return BSS_ERR;
    }
    JavaVM *mVm = nullptr;
    jobject mFilter = nullptr;
    jmethodID mClock = nullptr;
    jmethodID mList = nullptr;
};
}  // namespace

ListTtlCompactionFilter CreateFlinkListCompactionFilter(JNIEnv *env, jobject filter, int64_t ttl)
{
    auto callback = std::make_shared<JavaTtlFilter>(env, filter);
    if (env->ExceptionCheck() || !callback->Valid()) {
        return nullptr;
    }
    return [callback, ttl](Value &value, const Value::BufferAllocator &allocator) -> BResult {
        if (value.ValueType() == DELETE) {
            return BSS_OK;
        }
        // List states are not stored as blob references.
        if (value.ValueType() != PUT && value.ValueType() != APPEND) {
            return BSS_INVALID_PARAM;
        }
        int64_t now = 0;
        RETURN_NOT_OK(callback->Now(now));
        return callback->FilterList(value, ttl, now, allocator);
    };
}
}  // namespace bss
}  // namespace ock

extern "C" JNIEXPORT jint JNICALL Java_com_huawei_ock_bss_table_KListTableImpl_registerTtlCompaction(
    JNIEnv *env, jclass, jlong handle, jlong ttl, jobject filter)
{
    using namespace ock::bss;
    auto table = reinterpret_cast<AbstractTable *>(handle);
    if (table == nullptr) {
        return BSS_INVALID_PARAM;
    }
    ListTtlCompactionFilter callback;
    if (filter != nullptr && ttl > 0) {
        callback = CreateFlinkListCompactionFilter(env, filter, ttl);
        if (!callback) {
            return BSS_ERR;
        }
    }
    table->RegisterListCompactionFilter(std::move(callback));
    return BSS_OK;
}
