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

#ifndef BOOST_SS_ACCESS_RECORDER_H
#define BOOST_SS_ACCESS_RECORDER_H

#include <atomic>

namespace ock {
namespace bss {
class AccessRecorder {
public:
    inline void Record()
    {
        mAccessCount.fetch_add(1, std::memory_order_release);
    }

    inline uint64_t AccessCount()
    {
        return mAccessCount.load(std::memory_order_acquire);
    }

private:
    std::atomic<uint64_t> mAccessCount{ 0 };
};
using AccessRecorderRef = std::shared_ptr<AccessRecorder>;
}  // namespace bss
}  // namespace ock

#endif  // BOOST_SS_ACCESS_RECORDER_H
