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

#include "evict_manager.h"

namespace ock {
namespace bss {
BResult EvictManager::TryEvict(bool isSync, bool force, uint32_t minSize)
{
    return mFullSortEvictor->TryEvict(isSync, force, minSize);
}

void EvictManager::ForceEvict()
{
    mFullSortEvictor->ForceEvict();
}

BResult EvictManager::Initialize(const ConfigRef &config, const BucketGroupManagerRef &bucketGroupManager,
                                 const AccessRecorderRef &accessRecord)
{
    mSliceEvictManager = std::make_shared<SliceEvictManager>();
    mFullSortEvictor = std::make_shared<FullSortEvictor>();
    return mFullSortEvictor->Initialize(config->GetEvictMinSize(), bucketGroupManager, mSliceEvictManager,
                                        accessRecord);
}

void EvictManager::Exit()
{
    if (mFullSortEvictor != nullptr) {
        mFullSortEvictor->Exit();
        mFullSortEvictor = nullptr;  // 置空避免循环引用.
    }
}
}  // namespace bss
}  // namespace ock