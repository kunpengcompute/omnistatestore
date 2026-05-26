/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef BOOST_SS_SLICE_TABLE_RESTORE_OPERATION_H
#define BOOST_SS_SLICE_TABLE_RESTORE_OPERATION_H

#include <unordered_map>
#include <vector>

#include "include/config.h"
#include "bucket_group_manager.h"
#include "snapshot_restore_utils.h"
#include "transform/fresh_transformer.h"

namespace ock {
namespace bss {
class SliceTableRestoreOperation {
public:
    SliceTableRestoreOperation() = default;

    SliceTableRestoreOperation(const ConfigRef &config, const SliceTableManagerRef &sliceTable)
        : mConfig(config),
          mSliceTable(sliceTable),
          mSliceBucketIndex(sliceTable->GetSliceBucketIndex()),
          mCurTotalBucketNum(sliceTable->GetSliceBucketIndex()->GetIndexCapacity()),
          mBucketGroupManager(sliceTable->GetBucketGroupManager()),
          mMemManager(sliceTable->GetMemManager())
    {
        mSliceCompactionPolicy = std::make_shared<SliceCompactionPolicy>();
        mSliceCompactionPolicy->Init(config, mSliceBucketIndex);
    }

    BResult RestoreSliceBucketIndex(std::vector<RestoredDbMetaRef> &restoredDbMetas,
                                    std::vector<SliceTableRestoreMetaRef> &restoreMetaList);

    inline BResult RestoreFileStoreOperator(const std::vector<SliceTableRestoreMetaRef> &metaList,
                                            std::unordered_map<std::string, std::string> &lazyPathMapping,
                                            std::unordered_map<std::string, uint32_t> &restorePathFileIdMap,
                                            bool isLazyDownload)
    {
        return mBucketGroupManager->RestoreFileStore(metaList, lazyPathMapping, restorePathFileIdMap, isLazyDownload);
    }

    BResult RestoreSliceBucketIndex(const FileInputViewRef &reader, uint32_t snapshotVersion,
                                    SliceBucketGroupRangeGroupRef &oldSliceSegmentGroup, uint64_t snapshotId);

    BResult RestoreBlobStore(std::vector<SliceTableRestoreMetaRef> &metaList,
                             std::unordered_map<std::string, uint32_t> &restorePathFileIdMap);

    void TryEvictCompositeLogicalSliceChain(uint64_t chainMemory);

    BResult LoadSlicesIntoSliceTable(uint32_t snapshotVersion, bool isFailOver);
    BResult LoadSlicesIntoLogicalSliceChain(const LogicalSliceChainRef &sliceChain, uint32_t snapshotVersion);
    BResult TryLoadCompositeLogicalSliceChain(const LogicalSliceChainRef &sliceChain, uint32_t snapshotVersion);

    void SyncUpdateChain(const LogicalSliceChainRef &oldLogicalSliceChain,
                         const LogicalSliceChainRef &newLogicalSliceChain, uint32_t sliceIndexSlot);

    BResult ReplaceCompositeLogicalSlice(LogicalSliceChainRef &logicalSliceChain, uint32_t sliceIndexSlot,
                                         const DataSliceRef &compactedDataSLice,
                                         std::vector<SliceAddressRef> &invalidSliceAddressList);
    BResult DoCompactCompositeSlice(const std::vector<DataSliceRef> &canCompactSliceListReversed,
                                    DataSliceRef &compactedDataSlice, bool forceFilter, uint32_t bucketIndex);
    BResult DoCompositeCompaction(const SliceIndexContextRef &sliceIndexContext);
    BResult DirectReplaceCompositeSlice(const SliceIndexContextRef &sliceIndexContext);
    BResult CompactCompositeLogicalSliceChain(uint32_t sliceIndexSlot);
    BucketGroupRef GetBucketGroup() const
    {
        return mBucketGroupManager->GetBucketGroupVector()[0];
    }

private:
    ConfigRef mConfig = nullptr;
    SliceTableManagerRef mSliceTable = nullptr;
    SliceBucketIndexRef mSliceBucketIndex = nullptr;
    uint32_t mCurTotalBucketNum = 0;
    BucketGroupManagerRef mBucketGroupManager = nullptr;
    MemManagerRef mMemManager = nullptr;
    SliceCompactionPolicyRef mSliceCompactionPolicy = nullptr;
};

}  // namespace bss
}  // namespace ock
#endif  // BOOST_SS_SLICE_TABLE_RESTORE_OPERATION_H