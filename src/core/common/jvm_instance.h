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

#ifndef BOOST_SS_JVM_INSTANCE_H
#define BOOST_SS_JVM_INSTANCE_H

#include <jni.h>

#include "bss_log.h"
#include "include/bss_err.h"

namespace ock {
namespace bss {
class JVMInstance {
public:
    static BResult Init(JNIEnv *env)
    {
        if (mJvm != nullptr) {
            return BSS_OK;
        }
        env->GetJavaVM(&mJvm);
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            LOG_ERROR("Init JavaVM fail!");
            return BSS_ERR;
        }
        if (mJvm == nullptr) {
            LOG_ERROR("Init JavaVM fail, mJvm is nullptr!");
            return BSS_ERR;
        }
        return BSS_OK;
    }

    static JavaVM *GetInstance()
    {
        return mJvm;
    }

    static void Close()
    {
        if (mJvm != nullptr) {
            mJvm->DestroyJavaVM();
            mJvm = nullptr;
        }
    }

private:
    static JavaVM *mJvm;
};
}  // namespace bss
}  // namespace ock

#endif  // BOOST_SS_JVM_INSTANCE_H
