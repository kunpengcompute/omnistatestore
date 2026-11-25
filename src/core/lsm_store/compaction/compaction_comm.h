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

#ifndef BSS_DEV_COMPACTION_COMM_H
#define BSS_DEV_COMPACTION_COMM_H

namespace ock {
namespace bss {
enum class Reason {
    KEY_GROUP_RESCALED,
    ORDER_RANGE_REDUNDANT,
    LEVEL0_NUM_TRIGGERED,
    LEVEL_SIZE_TRIGGERED,
    NO_NEED_COMPACTION,
};
}  // namespace bss
}  // namespace ock
#endif  // BSS_DEV_COMPACTION_COMM_H
