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

#include "executor_service.h"

#include <csignal>

namespace ock {
namespace bss {
const uint16_t MAX_THREAD_NUM = 100;

ExecutorServicePtr ExecutorService::Create(uint16_t threadNum, uint32_t queueCapacity)
{
    if (threadNum > MAX_THREAD_NUM) {
        LOG_ERROR("The num of executor service thread must less than " << MAX_THREAD_NUM);
        return nullptr;
    }

    return std::make_shared<ExecutorService>(threadNum, queueCapacity);
}

ExecutorService::ExecutorService(uint16_t threadNum, uint32_t queueCapacity)
    : mRunnableQueue(queueCapacity),
      mThreadNum(threadNum),
      mThreads(),
      mStarted(false),
      mStopped(false),
      mStartedThreadNum(0)
{
    mThreads.reserve(threadNum);
}

void ExecutorService::FreeThread()
{
    while (!mThreads.empty()) {
        delete (mThreads.back());
        mThreads.pop_back();
    }
}

ExecutorService::~ExecutorService()
{
    if (!mStopped) {
        Stop();
    }

    FreeThread();
    LOG_INFO("Delete executor service success, name:" << mThreadName.c_str());
}

bool ExecutorService::Start()
{
    if (mStarted) {
        return false;
    }

    for (uint16_t i = 0; i < mThreadNum; i++) {
        auto *thr = new (std::nothrow) std::thread(&ExecutorService::RunInThread, this);
        if (UNLIKELY(thr == nullptr)) {
            Stop();
            FreeThread();
            LOG_ERROR("Create executor service thread index:" << i << " thread total num:" << mThreadNum << "failed");
            return false;
        }

        mThreads.push_back(thr);
    }

    while (mStartedThreadNum < mThreadNum) {
        usleep(1);
    }

    mStarted = true;
    return true;
}

void ExecutorService::Stop()
{
    if (mStopped) {
        return;
    }

    mStopped = true;
    mRunnableQueue.Shutdown();
    for (auto &thr : mThreads) {
        if (thr != nullptr && thr->joinable()) {
            thr->join();
        }
    }
}

void ExecutorService::RunInThread()
{
    bool runFlag = true;
    uint16_t threadIndex = mStartedThreadNum++;

    pthread_setname_np(pthread_self(), mThreadName.empty() ? "executor" : mThreadName.c_str());
    LOG_INFO("Thread NO." << threadIndex << " is started for executor service <"
                          << (mThreadName.empty() ? "executor" : mThreadName.c_str()) << ">.");

    while (runFlag && !mStopped) {
        BResult hr = DoRunnable(runFlag);
        if (hr != BSS_OK) {
            LOG_ERROR("failed to run task, error code is " << hr);
            return;
        }
    }
    RunnablePtr task = nullptr;
    while (mRunnableQueue.QueueSize() != 0) {
        if (!mRunnableQueue.Dequeue(task)) {
            break;
        }
    }
}

bool ExecutorService::Execute(const RunnablePtr &runnable, bool flag)
{
    if (UNLIKELY(!mStarted || mStopped)) {
        return false;
    }
    RETURN_FALSE_AS_NULLPTR(runnable);
    return mRunnableQueue.Enqueue(runnable, flag);
}

BResult ExecutorService::DoRunnable(bool &flag)
{
    BResult hr = BSS_OK;
    HTRY
    {
        RunnablePtr task = nullptr;
        if (!mRunnableQueue.Dequeue(task)) {
            flag = false;
            return BSS_OK;
        }
        if (task != nullptr) {
            if (task->Type() == RunnableType::NORMAL) {
                task->Run();
            } else if (task->Type() == RunnableType::STOP) {
                flag = false;
            } else {
                HASSERT_LOG(false);
            }
        } else {
            LOG_ERROR("task is nullptr");
        }
    }
    HCATCH(hr);

    return hr;
}
}  // namespace bss
}  // namespace ock
