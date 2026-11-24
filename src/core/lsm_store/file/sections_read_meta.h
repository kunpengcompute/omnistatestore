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

#ifndef BOOST_SS_SECTIONS_READ_META_H
#define BOOST_SS_SECTIONS_READ_META_H

#include "version/version.h"

namespace ock {
namespace bss {
struct SectionsReadMeta {
    VersionPtr mCurrent;
    Key key;
    std::vector<FileMetaDataRef> mFilesForKey;
};
using SectionsReadMetaRef = std::shared_ptr<SectionsReadMeta>;

}  // namespace bss
}  // namespace ock
#endif  // BOOST_SS_SECTIONS_READ_META_H