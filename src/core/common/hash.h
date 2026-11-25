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

#ifndef BOOST_SS_HASH_H
#define BOOST_SS_HASH_H

#include "bss_def.h"

namespace ock {
namespace bss {
class HashCode {
public:
    inline static uint32_t Hash(const uint8_t *data, uint32_t dataLen)
    {
        uint32_t hash = NO_1;
        for (uint32_t i = 0; i < dataLen; i++) {
            hash = NO_31 * hash + data[i];
        }
        return hash;
    }
};
}  // namespace bss
}  // namespace ock

#endif  // BOOST_SS_HASH_H
