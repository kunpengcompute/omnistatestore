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

#include "primary_address.h"

namespace ock {
namespace bss {
PersistentAddressRef PERSISTENT_ADDRESS_NONE = std::make_shared<PersistentAddress>(UINT64_MAX, UINT32_MAX, "", false);

}  // namespace bss
}  // namespace ock