/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "key_group_util.h"

#include <mutex>

#include "common/bss_log.h"

namespace ock {
namespace bss {

constexpr uint32_t KeyGroupUtil::VALID_HASH_BITS;
constexpr uint64_t KeyGroupUtil::HASH_SPACE;
uint32_t KeyGroupUtil::mMaxParallelism = 0;
uint32_t KeyGroupUtil::mGroupBits = 0;
uint32_t KeyGroupUtil::mQuotientBits = 0;
uint64_t KeyGroupUtil::mBase = 0;
uint64_t KeyGroupUtil::mExtra = 0;
uint64_t KeyGroupUtil::mLongSpan = 0;
KeyGroupUtil::SetKeyGroupFuncPtr KeyGroupUtil::mSetKeyGroupFunc = nullptr;
KeyGroupUtil::ComputeKeyGroupFuncPtr KeyGroupUtil::mComputeKeyGroupFunc = nullptr;

BResult KeyGroupUtil::Init(uint32_t maxParallelism)
{
    static std::mutex initMutex;
    std::lock_guard<std::mutex> lock(initMutex);
    if (UNLIKELY(maxParallelism == 0 || maxParallelism > MAX_PARALLELISM)) {
        LOG_ERROR("Invalid maxParallelism: " << maxParallelism << ", valid range: [1, " << MAX_PARALLELISM << "]");
        return BSS_INVALID_PARAM;
    }

    if (mMaxParallelism == maxParallelism) {
        return BSS_OK;
    }

    uint64_t base = HASH_SPACE / maxParallelism;
    uint64_t extra = HASH_SPACE % maxParallelism;
    uint32_t groupBits = 0;
    for (uint32_t value = maxParallelism; value > 1; value >>= 1) {
        ++groupBits;
    }

    bool powerOfTwo = (maxParallelism & (maxParallelism - 1)) == 0;
    mMaxParallelism = maxParallelism;
    mBase = base;
    mExtra = extra;
    mLongSpan = extra * (base + 1);
    mGroupBits = groupBits;
    mQuotientBits = VALID_HASH_BITS - groupBits;
    mSetKeyGroupFunc = powerOfTwo ? &KeyGroupUtil::SetPowerOfTwoKeyGroup : &KeyGroupUtil::SetGeneralKeyGroup;
    mComputeKeyGroupFunc = powerOfTwo ? &KeyGroupUtil::ComputePowerOfTwoKeyGroup :
                                        &KeyGroupUtil::ComputeGeneralKeyGroup;
    return BSS_OK;
}

}  // namespace bss
}  // namespace ock
