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

namespace ock {
namespace bss {

class KeyGroupUtil {
public:
    static inline void Init(uint32_t maxParallelism)
    {
        if (maxParallelism < 129) {
            mSetKeyGroupFunc = &KeyGroupUtil::SetOneByteKeyGroup;
            mComputeKeyGroupFunc = &KeyGroupUtil::ComputeOneByteKeyGroup;
        } else {
            mSetKeyGroupFunc = &KeyGroupUtil::SetTwoBytesKeyGroup;
            mComputeKeyGroupFunc = &KeyGroupUtil::ComputeTwoBytesKeyGroup;
        }
    }

    static inline uint32_t ComputeKeyGroupForKeyHash(uint32_t keyHash)
    {
        return (*mComputeKeyGroupFunc)(keyHash);
    }

    static inline void SetKeyGroup(uint32_t &keyHashCode, uint32_t keyGroup)
    {
        (*mSetKeyGroupFunc)(keyHashCode, keyGroup);
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
    using SetKeyGroupFuncPtr = void (*)(uint32_t &, uint32_t);
    using ComputeKeyGroupFuncPtr = uint32_t (*)(uint32_t);

    static SetKeyGroupFuncPtr mSetKeyGroupFunc;
    static ComputeKeyGroupFuncPtr mComputeKeyGroupFunc;
};
}  // namespace bss
}  // namespace ock

#endif  // BOOST_SS_KEY_GROUP_UTIL_H