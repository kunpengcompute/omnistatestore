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

#include "slice_table_snapshot.h"

#include "flushing_bucket_group.h"
#include "snapshot_sync_task.h"

namespace ock {
namespace bss {
const std::shared_ptr<SliceTableSnapshot::LogicalSliceChainSnapshotMeta> SliceTableSnapshot::mEmptySliceMeta = nullptr;

BResult SliceTableSnapshot::Initialize(const SliceBucketIndexRef &sliceBucketIndex,
                                       const BucketGroupManagerRef &bucketGroupManager, const MemManagerRef &memManager,
                                       bool isSavepoint, uint64_t snapshotId)
{
    if (UNLIKELY(sliceBucketIndex == nullptr || bucketGroupManager == nullptr)) {
        LOG_ERROR("Slice bucketIndex or bucketGroupManager is nullptr");
        return BSS_ERR;
    }
    mBucketGroupManager = bucketGroupManager;
    mSliceBucketIndex = sliceBucketIndex;
    mTotalBucketNum = sliceBucketIndex->GetIndexCapacity();
    mMemManager = memManager;
    mIsSavepoint = isSavepoint;
    mSnapshotId = snapshotId;
    return BSS_OK;
}

BResult SliceTableSnapshot::GenSliceTableIndexSnapshot()
{
    {
        std::lock_guard<std::mutex> lk(mChainSnapMutex);
        mSliceChainSnapshotArray.reserve(mTotalBucketNum);
    }
    uint32_t totalSlice = 0;
    // 遍历当前每个bucket的sliceChain.
    for (uint32_t idx = 0; idx < mTotalBucketNum; idx++) {
        mSliceBucketIndex->LockWrite(idx);
        LogicalSliceChainRef curLogicSliceChain = mSliceBucketIndex->GetLogicChainedSlice(idx);
        if (UNLIKELY(curLogicSliceChain == nullptr)) {
            LOG_ERROR("Current logic slice chain is nullptr.");
            mSliceBucketIndex->Unlock(idx);
            return BSS_ERR;
        }

        // case 1: 若sliceChain为空, 则插入EmptySliceChain和emptyMeta.
        if (curLogicSliceChain->IsNone()) {
            std::lock_guard<std::mutex> lock(mChainSnapMutex);
            mSliceChainSnapshotArray.emplace_back(LogicalSliceChain::mEmptySliceChain, mEmptySliceMeta);
            mSliceBucketIndex->Unlock(idx);
            continue;
        }

        // case 2: 若SliceChain不为空但未挂载dataSlice数据, 再判断是否有挂载fileStore.
        if (curLogicSliceChain->GetSliceChainTailIndex() < 0) {
            SnapshotNoDataSlicesLogicalSliceChain(idx, curLogicSliceChain->HasFilePage());
            mSliceBucketIndex->Unlock(idx);
            continue;
        }

        // case 3: 当前SliceChain有dataSlice数据.
        int32_t tailIndex = curLogicSliceChain->GetSliceChainTailIndex();
        int32_t headIndex = -1;
        bool hasEvictedSlice = false;
        for (int32_t j = 0; j <= tailIndex; j++) {  // 从前往后找到第一个状态为非evicted的,
                                                    // 状态为Flushing的slice也打入到快照中.
            SliceAddressRef curSliceAddress = curLogicSliceChain->GetSliceAddress(j);
            if (UNLIKELY(curSliceAddress == nullptr)) {
                mSliceBucketIndex->Unlock(idx);
                return BSS_INVALID_PARAM;
            }
            if (!curSliceAddress->IsEvicted()) {
                headIndex = j;
                break;
            }
            hasEvictedSlice = true;
        }

        std::vector<FilePageRef> filePages;
        curLogicSliceChain->GetFilePages(filePages);
        bool hasFilePage = hasEvictedSlice || !filePages.empty();
        if (headIndex == -1) {  // 表示当前SliceChain上的数据已经处于evicted, 按照case2处理.
            SnapshotNoDataSlicesLogicalSliceChain(idx, hasFilePage);
            mSliceBucketIndex->Unlock(idx);
            continue;
        }
        auto chainSnapshotMeta = std::make_shared<LogicalSliceChainSnapshotMeta>(headIndex, tailIndex, hasFilePage);
        CopySliceChainParams params{ {}, {}, true, true };  // 此处需要深拷贝sliceAddress.
        LogicalSliceChainRef snapshotSliceChain = nullptr;
        auto ret = CopyLogicSliceChain(curLogicSliceChain, chainSnapshotMeta, params, snapshotSliceChain);
        mSliceBucketIndex->Unlock(idx);
        if (UNLIKELY(ret != BSS_OK)) {
            LOG_ERROR("Copy logic slice chain failed, ret:" << ret);
            return BSS_ERR;
        }
        totalSlice += (tailIndex - headIndex + 1);
        std::lock_guard<std::mutex> lock(mChainSnapMutex);
        mSliceChainSnapshotArray.emplace_back(snapshotSliceChain, chainSnapshotMeta);
        LOG_DEBUG("Insert slice chain, bucketIndex:" << idx << ", snapshotId:" << mSnapshotId
                                                     << ", sliceSize:" << snapshotSliceChain->GetSliceSize()
                                                     << ", headIndex:" << headIndex << ", tailIndex:" << tailIndex);
    }
    LOG_INFO("Generate slice table snapshot success, totalSlice:" << totalSlice << ", snapshotId:" << mSnapshotId);
    return BSS_OK;
}

void SliceTableSnapshot::SnapshotNoDataSlicesLogicalSliceChain(uint32_t slot, bool hasFilePage)
{
    if (!hasFilePage) {  // 不含有filePage则插入EmptySliceChain和emptyMeta.
        std::lock_guard<std::mutex> lock(mChainSnapMutex);
        mSliceChainSnapshotArray.emplace_back(LogicalSliceChain::mEmptySliceChain, mEmptySliceMeta);
        LOG_DEBUG("Insert empty slice chain, bucketIndex:" << slot << ", snapshotId:" << mSnapshotId);
    } else {  // 表示sliceChain上的数据已经被flush到fileStore, 需要插入含有一个filePage的sliceChain和meta.
        auto chainSnapshotMeta = std::make_shared<LogicalSliceChainSnapshotMeta>(-1, -1, true);
        LogicalSliceChainRef sliceChain = std::make_shared<LogicalSliceChainImpl>();
        sliceChain->SetFilePages(std::vector<FilePageRef>(NO_1));  // 默认只有一个filePage.
        std::lock_guard<std::mutex> lock(mChainSnapMutex);
        mSliceChainSnapshotArray.emplace_back(sliceChain, chainSnapshotMeta);
        LOG_DEBUG("Insert slice chain, has fileStore, bucketIndex:" << slot << ", snapshotId:" << mSnapshotId);
    }
}

BResult SliceTableSnapshot::CopyLogicSliceChain(const LogicalSliceChainRef &sliceChainBeforeCopy,
                                                const LogicalSliceChainSnapshotMetaRef &chainMeta,
                                                CopySliceChainParams &params, LogicalSliceChainRef &copiedChain)
{
    copiedChain = std::make_shared<LogicalSliceChainImpl>();
    RETURN_NOT_OK(copiedChain->Initialize(sliceChainBeforeCopy, chainMeta->mSliceChainHeadIndex,
                                          chainMeta->mSliceChainTailIndex, params.copiedDataSliceReference,
                                          params.deepCopySliceAddress, chainMeta->mHasFilePage));
    if (sliceChainBeforeCopy->HasFilePage()) {
        auto bucketGroup = GetBucketGroup();
        RETURN_ERROR_AS_NULLPTR(bucketGroup);
        copiedChain->InsertFilePageIfEmpty(std::make_shared<FilePage>(bucketGroup->GetLsmStore()));
    }
    return BSS_OK;
}

SnapshotMetaRef SliceTableSnapshot::SnapshotMetaFunc(uint64_t snapshotId, const FileOutputViewRef &localOutputView)
{
    std::lock_guard<std::mutex> lk(mResourceMutex);
    if (mIsReleased.load()) {
        return nullptr;
    }

    // 文件中的元数据内容: KV分离标记+bucket数量+空sliceChain标记+非空sliceChain信息+bucketGroup信息.
    localOutputView->WriteUint8(NO_0);  // 当前版本仅支持KV不分离模式
    localOutputView->WriteUint32(mTotalBucketNum);

    uint64_t totalSize = 0;
    std::lock_guard<std::mutex> lock(mChainSnapMutex);
    for (const auto &logicalSliceChain : mSliceChainSnapshotArray) {
        if (UNLIKELY(logicalSliceChain.first == nullptr)) {
            LOG_ERROR("Logical slice chain should not be nullptr.");
            return nullptr;
        }
        if (logicalSliceChain.first == LogicalSliceChain::mEmptySliceChain) {
            // 该字段表示是否为emptyChain, 1表示空链, 0表示非空链.
            localOutputView->WriteUint8(NO_1);
        } else {
            localOutputView->WriteUint8(NO_0);
            logicalSliceChain.first->SnapshotMeta(snapshotId, localOutputView, mSnapshotMeta);
            totalSize += logicalSliceChain.first->GetSliceSize();
        }
    }
    LOG_DEBUG("SliceTable write slice chain success, checkpointId:" << snapshotId << ", sliceTotalSize:" << totalSize);

    mBucketGroupManager->SnapshotMeta(localOutputView);
    return mSnapshotMeta;
}

SliceKVIteratorPtr SliceTableSnapshot::Iterator()
{
    std::vector<std::vector<DataSliceRef>> dataSlices;
    uint32_t curSlot = 0;
    std::lock_guard<std::mutex> lk(mChainSnapMutex);
    RETURN_NULLPTR_AS_FALSE(mSliceChainSnapshotArray.size() != mTotalBucketNum, "Wrong sliceChain snapshot array size");
    while (curSlot < mTotalBucketNum) {
        auto chainMeta = mSliceChainSnapshotArray[curSlot].second;
        if (chainMeta == nullptr || chainMeta->mSliceChainTailIndex < 0 ||
            mSliceChainSnapshotArray[curSlot].first == LogicalSliceChain::mEmptySliceChain) {
            curSlot++;
            continue;
        }
        auto chainBeforeCopy = mSliceBucketIndex->GetLogicChainedSlice(curSlot);
        if (chainMeta->IsLocked()) {
            WaitCopyOnWriteComplete(curSlot);
            RETURN_NULLPTR_AS_NULLPTR(mSliceChainSnapshotArray[curSlot].first);
        }

        auto snapShotChain = mSliceChainSnapshotArray[curSlot].first;
        if (snapShotChain == nullptr) {
            // 有chainMeta但没有chainBeforeCopy是不正常的.
            LOG_ERROR("LogicalPageChain is empty before savepoint copy logicalPageChain.");
            return nullptr;
        }
        auto curChain = mSliceChainSnapshotArray[curSlot].first;
        RETURN_NULLPTR_AS_NULLPTR(curChain);
        int32_t curIndex = curChain->GetSliceChainTailIndex();
        std::vector<DataSliceRef> dataSliceList;
        while (curIndex >= 0) {
            auto sliceAddress = mSliceChainSnapshotArray[curSlot].first->GetSliceAddress(curIndex);
            if (sliceAddress == nullptr || sliceAddress->GetDataSlice() == nullptr) {
                LOG_ERROR("sliceAddress or dataSlice is null when sliceTable savepoint iterator LogicalSliceChain.");
                return nullptr;
            }
            auto dataSlice = sliceAddress->GetDataSlice();
            dataSliceList.emplace(dataSliceList.begin(), dataSlice);
            curIndex--;
        }
        curSlot++;
        dataSlices.emplace_back(dataSliceList);
    }
    Ref<FlushingBucketGroupIterator> dataSliceVectorIterator = MakeRef<FlushingBucketGroupIterator>();
    RETURN_NULLPTR_AS_NULLPTR(dataSliceVectorIterator);
    dataSliceVectorIterator->Initialize(dataSlices);
    auto sliceKVIterator = std::make_shared<SliceKVIterator>(dataSliceVectorIterator, mMemManager);
    sliceKVIterator->SetSavepointFlag(true);
    return sliceKVIterator;
}

void SliceTableSnapshot::ReleaseResource()
{
    std::lock_guard<std::mutex> lk(mResourceMutex);
    if (mIsReleased.load()) {
        return;
    }
    mIsReleased.store(true);
    mSliceChainSnapshotArray.clear();
}

void SliceTableSnapshot::SliceTableSnapshotSliceFlushIterator::Advance()
{
    // 遍历SliceTableSnapshot中的每个SliceChain.
    while (static_cast<uint32_t>(++mChainArrayCursor) < mSliceTableSnapshot->mTotalBucketNum) {
        // 1. 获取sliceChain的snapshotMeta.
        SliceTableSnapshot::LogicalSliceChainSnapshotMetaRef meta = nullptr;
        bool isEmptyChain = false;
        GetSnapshotMeta(meta, isEmptyChain);
        if (meta == mEmptySliceMeta || meta->mSliceChainHeadIndex < 0 || isEmptyChain) {
            continue;
        }

        // 2. 获取SnapshotSliceChain.
        LogicalSliceChainRef sliceChainSnapshot = nullptr;
        GetSnapshotSliceChain(sliceChainSnapshot);
        if (UNLIKELY(sliceChainSnapshot == nullptr || sliceChainSnapshot->IsNone())) {
            LOG_ERROR("Logical slice chain snapshot is empty, index:" << mChainArrayCursor);
            continue;
        }

        // 3. 构建迭代器.
        mCurrentChain = sliceChainSnapshot->SliceIterator();
        return;
    }
}

void SliceTableSnapshot::SliceTableSnapshotSliceFlushIterator::GetSnapshotMeta(
    SliceTableSnapshot::LogicalSliceChainSnapshotMetaRef &meta, bool &isEmptyChain)
{
    std::lock_guard<std::mutex> lk(mSliceTableSnapshot->mChainSnapMutex);
    isEmptyChain = mSliceTableSnapshot->mSliceChainSnapshotArray[mChainArrayCursor].first ==
                   LogicalSliceChain::mEmptySliceChain;
    meta = mSliceTableSnapshot->mSliceChainSnapshotArray[mChainArrayCursor].second;
}

void SliceTableSnapshot::SliceTableSnapshotSliceFlushIterator::GetSnapshotSliceChain(LogicalSliceChainRef &sliceChain)
{
    std::lock_guard<std::mutex> lk(mSliceTableSnapshot->mChainSnapMutex);
    sliceChain = mSliceTableSnapshot->mSliceChainSnapshotArray[mChainArrayCursor].first;
}

}  // namespace bss
}  // namespace ock