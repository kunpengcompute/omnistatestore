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

#ifndef BOOST_SS_BSS_MATH_H
#define BOOST_SS_BSS_MATH_H

#include <cstdint>

#include "include/bss_types.h"

namespace ock {
namespace bss {

class BssMath {
public:
    static inline uint32_t RoundUpToPowerOfTwo(uint32_t n)
    {
        if (n == NO_0) {
            return 1;
        }
        n--;
        n |= n >> NO_1;
        n |= n >> NO_2;
        n |= n >> NO_4;
        n |= n >> NO_8;
        n |= n >> NO_16;
        // 对于 64 位整数，还需要加上 n |= n >> 32;
        n++;
        return n;
    }

    static inline uint32_t RoundDownToPowerOfTwo(uint32_t n)
    {
        if (n == 0) {
            return 0;
        }

        n |= n >> NO_1;
        n |= n >> NO_2;
        n |= n >> NO_4;
        n |= n >> NO_8;
        n |= n >> NO_16;
        // 对于 64 位整数，还需要加上 n |= n >> 32;
        n++;
        return n >> NO_1;
    }

    // 计算一个32位整数中二进制位为1的bit个数
    static inline uint32_t IntegerBitCount(uint32_t i)
    {
        // HD, Figure 5-2
        i = i - ((i >> NO_1) & 0x55555555);
        i = (i & 0x33333333) + ((i >> NO_2) & 0x33333333);
        i = (i + (i >> NO_4)) & 0x0f0f0f0f;
        i = i + (i >> NO_8);
        i = i + (i >> NO_16);
        return i & 0x3f;
    }

    static inline uint32_t NumberOfLeadingZeros(uint32_t i)
    {
        if (i == 0) {
            return NO_32;
        }
        uint32_t n = 1;
        if ((i >> NO_16) == 0) {
            n += NO_16;
            i <<= NO_16;
        }
        if ((i >> NO_24) == 0) {
            n += NO_8;
            i <<= NO_8;
        }
        if ((i >> NO_28) == 0) {
            n += NO_4;
            i <<= NO_4;
        }
        if ((i >> NO_30) == 0) {
            n += NO_2;
            i <<= NO_2;
        }
        n -= i >> NO_31;
        return n;
    }

    static uint32_t RotateRight(uint32_t i, uint32_t distance)
    {
        distance %= NO_32;
        return distance == 0 ? i : (i >> distance) | (i << (NO_32 - distance));
    }

    template <typename T> inline static int32_t IntegerCompare(T x, T y)
    {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }
};

}  // namespace bss
}  // namespace ock

#endif  // BOOST_SS_BSS_MATH_H
