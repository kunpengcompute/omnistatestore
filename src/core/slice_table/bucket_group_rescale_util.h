/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef BOOST_SS_BUCKET_GROUP_RESCALE_UTIL_H
#define BOOST_SS_BUCKET_GROUP_RESCALE_UTIL_H

#include "slice_table/bucket_group_manager.h"
#include "slice_table/bucket_group_range.h"

namespace ock {
namespace bss {

class BucketGroupRescaleUtil {
public:
    /**
     * 处理slice bucket的缩放场景
     *
     * @param [in]oldSegmentGroup 恢复的segmentGroup
     * @param [in]newSegmentGroup 当前配置的SegmentGroup
     * @param [in]bucketGroupManager bucketGroupManager
     * @param [out]rescaleRelation 缩放关系
     */
    static BResult Rescale(const SliceBucketGroupRangeGroupRef &oldSegmentGroup,
                           const SliceBucketGroupRangeGroupRef &newSegmentGroup,
                           const BucketGroupManagerRef &bucketGroupManager,
                           std::unordered_map<uint32_t, std::vector<uint32_t>> &rescaleRelation)
    {
        uint32_t oldGroupPoint = 0;
        uint32_t newGroupPoint = 0;
        std::vector<BucketGroupRangeRef> oldSegmentRanges = oldSegmentGroup->mSliceSegments;
        std::vector<BucketGroupRangeRef> newSegmentRanges = newSegmentGroup->mSliceSegments;

        while (oldGroupPoint < oldSegmentRanges.size() && newGroupPoint < newSegmentRanges.size()) {
            auto curOldRange = oldSegmentRanges[oldGroupPoint];
            auto curNewRange = newSegmentRanges[newGroupPoint];
            int32_t cmp = curOldRange->OverlapCompare(curNewRange);
            LOG_DEBUG("SliceTable handle rescale, cmpResult:" << cmp << ", oldGroupPoint:" << oldGroupPoint
                                                              << ", newGroupPoint" << newGroupPoint);
            if (UNLIKELY(cmp == ERROR_CASE_DIV_BY_ZERO)) {
                LOG_ERROR("Failed to compare overlap.");
                return BSS_ERR;
            }
            if (cmp == OVERLAP_COMPARE_RESULT_CASE_4) {
                oldGroupPoint++;
                continue;
            }
            if (cmp == OVERLAP_COMPARE_RESULT_CASE_2 || cmp == OVERLAP_COMPARE_RESULT_CASE_0) {
                MergeBucketGroup(oldSegmentGroup, curOldRange, newSegmentGroup, curNewRange, bucketGroupManager);
                std::vector<uint32_t> curGroupRescaleRelation;
                auto iter = rescaleRelation.find(curNewRange->GetBucketGroupId());
                if (iter != rescaleRelation.end()) {
                    iter->second.push_back(curOldRange->GetBucketGroupId());
                } else {
                    curGroupRescaleRelation = std::vector<uint32_t>();
                    curGroupRescaleRelation.push_back(curOldRange->GetBucketGroupId());
                    rescaleRelation.emplace(curNewRange->GetBucketGroupId(), curGroupRescaleRelation);
                }
                oldGroupPoint++;
                continue;
            }
            if (cmp == OVERLAP_COMPARE_RESULT_CASE_1) {
                MergeBucketGroup(oldSegmentGroup, curOldRange, newSegmentGroup, curNewRange, bucketGroupManager);
                auto iter = rescaleRelation.find(curNewRange->GetBucketGroupId());
                if (iter != rescaleRelation.end()) {
                    iter->second.push_back(curOldRange->GetBucketGroupId());
                } else {
                    std::vector<uint32_t> curGroupRescaleRelation = std::vector<uint32_t>();
                    curGroupRescaleRelation.push_back(curOldRange->GetBucketGroupId());
                    rescaleRelation.emplace(curNewRange->GetBucketGroupId(), curGroupRescaleRelation);
                }
                newGroupPoint++;
                continue;
            }
            // case: cmp == OVERLAP_COMPARE_RESULT_CASE_3
            newGroupPoint++;
        }
        return BSS_OK;
    }

private:
    static BResult AddLogicalSLiceChainIntoMappingTable(SliceBucketIndexRef &sliceBucketIndex, uint32_t indexSlot,
                                                        LogicalSliceChainRef &logicalSliceChain)
    {
        auto logicChainedSlice = sliceBucketIndex->GetLogicChainedSlice(indexSlot);
        if (UNLIKELY(logicChainedSlice == nullptr)) {
            LOG_ERROR("LogicChainedSlice is null, indexSlot: " << indexSlot);
            return BSS_ERR;
        }
        if (logicChainedSlice->IsNone()) {
            sliceBucketIndex->SetLogicChainedSlice(indexSlot, std::make_shared<CompositeLogicalSliceChain>());
        }
        LogicalSliceChainRef nowChain = sliceBucketIndex->GetLogicChainedSlice(indexSlot);
        if (UNLIKELY(nowChain == nullptr)) {
            LOG_ERROR("NowChain is null, indexSlot: " << indexSlot);
            return BSS_ERR;
        }
        std::dynamic_pointer_cast<CompositeLogicalSliceChain>(nowChain)->AddLogicalSliceChain(logicalSliceChain);
        return BSS_OK;
    }

    static void MergeBucketGroup(SliceBucketGroupRangeGroupRef oldSegmentGroup, BucketGroupRangeRef oldRange,
                                 SliceBucketGroupRangeGroupRef newSegmentGroup, BucketGroupRangeRef newRange,
                                 BucketGroupManagerRef bucketGroupManager)
    {
        // 缩容
        if (oldSegmentGroup->mTotalBucket >= newSegmentGroup->mTotalBucket && newSegmentGroup->mTotalBucket != 0) {
            uint32_t shrink = oldSegmentGroup->mTotalBucket / newSegmentGroup->mTotalBucket;
            for (auto i = oldRange->GetStartBucket(); i <= oldRange->GetEndBucket() && shrink != 0; i++) {
                uint32_t mappingIndex = i / shrink;
                LogicalSliceChainRef oldLogicalSliceChain = oldSegmentGroup->mSliceBucketIndex->GetLogicChainedSlice(i);
                if (oldLogicalSliceChain->IsNone()) {
                    continue;
                }

                if (mappingIndex < newRange->GetStartBucket() || mappingIndex > newRange->GetEndBucket()) {
                    continue;
                }

                std::vector<FilePageRef> filePages;
                oldLogicalSliceChain->GetFilePages(filePages);
                if (!filePages.empty()) {
                    LsmStoreRef lsmStore = bucketGroupManager->GetLsmStoreByBucketIndex(mappingIndex);
                    if (lsmStore == nullptr) {
                        return;
                    }
                    oldLogicalSliceChain->RestoreFilePage(lsmStore);
                }
                RETURN_AS_NOT_OK_NO_LOG(AddLogicalSLiceChainIntoMappingTable(newSegmentGroup->mSliceBucketIndex,
                                                                             mappingIndex, oldLogicalSliceChain));
            }
            return;
        }

        // 扩容
        if (oldSegmentGroup->mTotalBucket == 0) {
            LOG_ERROR("Total bucket is 0.");
            return;
        }

        uint32_t expand = newSegmentGroup->mTotalBucket / oldSegmentGroup->mTotalBucket;
        for (auto i = oldRange->GetStartBucket(); i <= oldRange->GetEndBucket(); i++) {
            LogicalSliceChainRef oldLogicalSliceChain = oldSegmentGroup->mSliceBucketIndex->GetLogicChainedSlice(i);
            if (oldLogicalSliceChain->IsNone()) {
                continue;
            }

            uint32_t mappingStartIndex = i * expand;
            uint32_t mappingEndIndex = (i + 1) * expand - 1;
            for (uint32_t j = std::max(mappingStartIndex, newRange->GetStartBucket());
                 j <= std::min(mappingEndIndex, newRange->GetEndBucket()); j++) {
                LogicalSliceChainRef logicalSliceChain = oldLogicalSliceChain->DeepCopy();
                std::vector<FilePageRef> filePages;
                logicalSliceChain->GetFilePages(filePages);
                if (!filePages.empty()) {
                    LsmStoreRef lsmStore = bucketGroupManager->GetLsmStoreByBucketIndex(j);
                    if (lsmStore == nullptr) {
                        return;
                    }
                    logicalSliceChain->RestoreFilePage(lsmStore);
                }
                RETURN_AS_NOT_OK_NO_LOG(
                    AddLogicalSLiceChainIntoMappingTable(newSegmentGroup->mSliceBucketIndex, j, logicalSliceChain));
            }
        }
    }
};

}  // namespace bss
}  // namespace ock
#endif  // BOOST_SS_BUCKET_GROUP_RESCALE_UTIL_H