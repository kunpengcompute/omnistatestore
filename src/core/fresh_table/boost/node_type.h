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
#ifndef BOOST_SS_NODETYPE_H
#define BOOST_SS_NODETYPE_H

#include <cstdint>

namespace ock {
namespace bss {
enum class NodeType : uint8_t {
    SERIALIZED = 0,
    HASHMAP = 1,
    COMPOSITE = 2,
    NONE = 4,
};
}
}  // namespace ock
#endif  // BOOST_SS_NODETYPE_H