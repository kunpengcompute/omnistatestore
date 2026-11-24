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

#ifndef BOOST_SS_SNAPSHOT_STATUS_H
#define BOOST_SS_SNAPSHOT_STATUS_H

namespace ock {
namespace bss {
enum class SnapshotStatus { INIT = 0, RUNNING = 1, FINISH = 2 };

enum class SnapshotStatusType { EVICT = 0, COMPACTION = 1 };

}  // namespace bss
}  // namespace ock
#endif  // BOOST_SS_SNAPSHOT_STATUS_H