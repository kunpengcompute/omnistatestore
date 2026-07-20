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

#ifndef BOOST_SS_KEY_GROUP_UTIL_H
#define BOOST_SS_KEY_GROUP_UTIL_H

#include <cstdint>

#include "include/bss_err.h"

namespace ock {
namespace bss {

class KeyGroupUtil {
public:
    static BResult Init(uint32_t maxParallelism);

    static inline uint32_t ComputeKeyGroupForKeyHash(uint32_t orderHash)
    {
        return (*mComputeKeyGroupFunc)(orderHash);
    }

    static inline void SetKeyGroup(uint32_t &rawHash, uint32_t keyGroup)
    {
        (*mSetKeyGroupFunc)(rawHash, keyGroup);
    }

    static inline uint32_t ComputePQKeyGroupForKeyHash(uint32_t keyHash)
    {
        return mMaxParallelism < 129 ? ComputeOneByteKeyGroup(keyHash) : ComputeTwoBytesKeyGroup(keyHash);
    }

    static inline void SetPQKeyGroup(uint32_t &keyHashCode, uint32_t keyGroup)
    {
        if (mMaxParallelism < 129) {
            SetOneByteKeyGroup(keyHashCode, keyGroup);
        } else {
            SetTwoBytesKeyGroup(keyHashCode, keyGroup);
        }
    }

    static inline uint32_t ComputeOneByteKeyGroup(uint32_t keyHash)
    {
        return (keyHash & 0xFF000000) >> 24;
    }

    static inline uint32_t ComputeTwoBytesKeyGroup(uint32_t keyHash)
    {
        return (keyHash & 0xFFFF0000) >> 16;
    }

    static inline void SetOneByteKeyGroup(uint32_t &keyHashCode, uint32_t keyGroup)
    {
        keyHashCode = (keyHashCode & 0x00FFFFFF) | ((keyGroup & 0xFF) << 24);
    }

    static inline void SetTwoBytesKeyGroup(uint32_t &keyHashCode, uint32_t keyGroup)
    {
        keyHashCode = (keyHashCode & 0x0000FFFF) | ((keyGroup & 0xFFFF) << 16);
    }

private:
    static inline void SetPowerOfTwoKeyGroup(uint32_t &rawHash, uint32_t keyGroup)
    {
        rawHash = (keyGroup << mQuotientBits) | (rawHash >> mGroupBits);
    }

    static inline uint32_t ComputePowerOfTwoKeyGroup(uint32_t orderHash)
    {
        return orderHash >> mQuotientBits;
    }

    static inline void SetGeneralKeyGroup(uint32_t &rawHash, uint32_t keyGroup)
    {
        uint64_t quotient = rawHash / mMaxParallelism;
        uint64_t extraBefore = keyGroup < mExtra ? keyGroup : mExtra;
        uint64_t prefix = static_cast<uint64_t>(keyGroup) * mBase + extraBefore;
        rawHash = static_cast<uint32_t>(prefix + quotient);
    }

    static inline uint32_t ComputeGeneralKeyGroup(uint32_t orderHash)
    {
        uint64_t value = orderHash;
        if (value < mLongSpan) {
            return static_cast<uint32_t>(value / (mBase + 1));
        }
        return static_cast<uint32_t>(mExtra + (value - mLongSpan) / mBase);
    }

    static constexpr uint32_t VALID_HASH_BITS = 31;
    static constexpr uint64_t HASH_SPACE = 1ULL << VALID_HASH_BITS;

    using SetKeyGroupFuncPtr = void (*)(uint32_t &, uint32_t);
    using ComputeKeyGroupFuncPtr = uint32_t (*)(uint32_t);

    static uint32_t mMaxParallelism;
    static uint32_t mGroupBits;
    static uint32_t mQuotientBits;
    static uint64_t mBase;
    static uint64_t mExtra;
    static uint64_t mLongSpan;
    static SetKeyGroupFuncPtr mSetKeyGroupFunc;
    static ComputeKeyGroupFuncPtr mComputeKeyGroupFunc;
};
}  // namespace bss
}  // namespace ock

#endif  // BOOST_SS_KEY_GROUP_UTIL_H
