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

#ifndef BSS_DEV_FILE_NAME_H
#define BSS_DEV_FILE_NAME_H

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>

#include "common/path.h"

namespace ock {
namespace bss {
class FileName {
public:
    inline static std::string CreateFileName(const std::string &prefix)
    {
        static std::string sst = ".sst";
        return prefix + sst;
    }
};

}  // namespace bss
}  // namespace ock
#endif  // BSS_DEV_FILE_NAME_H