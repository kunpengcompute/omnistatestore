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

#ifndef BOOST_SS_LAZY_DOWN_TYPE_H
#define BOOST_SS_LAZY_DOWN_TYPE_H

namespace ock {
namespace bss {
enum class LazyRestoreType { OFF, ON, ONLY_FAIL_OVER, TRUE, FALSE };

enum class LazyDownloadType { OFF, ON_RESTORE, ON_READ };

}  // namespace bss
}  // namespace ock
#endif  // BOOST_SS_LAZY_DOWN_TYPE_H