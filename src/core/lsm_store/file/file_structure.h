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

#ifndef BOOST_SS_FILE_STRUCTURE_H
#define BOOST_SS_FILE_STRUCTURE_H

#include <cstdint>

namespace ock {
namespace bss {
struct MetaIndexStructure {
    uint32_t offset;
    uint32_t capacity;
};

struct FooterStructure {
    uint64_t magicNum;
    uint32_t metaIndexBlockHandleOffset;
    uint32_t metaIndexBlockHandleSize;
};

}  // namespace bss
}  // namespace ock
#endif  // BOOST_SS_FILE_STRUCTURE_H