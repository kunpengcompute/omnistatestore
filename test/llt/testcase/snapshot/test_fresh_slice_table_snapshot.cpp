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

#include "test_fresh_slice_table_snapshot.h"

#include <cstring>
#include <fstream>
#include <map>
#include <vector>

#include "fresh_table/fresh_restore_memory.h"
#include "snapshot/snapshot_compression_utils.h"
#include "snapshot/snapshot_restore_utils.h"

using namespace ock::bss;

BoostStateDBImpl *TestFreshSliceTableSnapshot::mBoostStateDB = nullptr;
ConfigRef TestFreshSliceTableSnapshot::mConfig = nullptr;
MemManagerRef TestFreshSliceTableSnapshot::mMemManager = nullptr;
uint64_t TestFreshSliceTableSnapshot::mCheckpointId = NO_1;
std::vector<QueryKey> TestFreshSliceTableSnapshot::originKeyList{};
std::vector<Value> TestFreshSliceTableSnapshot::originValueList{};

TEST(SnapshotCompressionUtilsTest, TryCompressIntoFallsBackForNone)
{
    std::vector<uint8_t> input(IO_SIZE_4K, 7);
    std::vector<uint8_t> output(input.size(), 0);
    CompressAlgo storedAlgo = CompressAlgo::LZ4;
    uint32_t storedLength = 0;

    ASSERT_EQ(SnapshotCompressionUtils::TryCompressInto(CompressAlgo::NONE, input.data(), input.size(), output.data(),
                                                        output.size(), storedAlgo, storedLength),
              BSS_OK);

    ASSERT_EQ(storedAlgo, CompressAlgo::NONE);
    ASSERT_EQ(storedLength, input.size());
    ASSERT_EQ(memcmp(input.data(), output.data(), input.size()), 0);
}

TEST(SnapshotCompressionUtilsTest, TryCompressIntoUsesLz4WhenBeneficial)
{
    std::vector<uint8_t> input(IO_SIZE_16K, 0);
    for (uint32_t i = 0; i < input.size(); ++i) {
        input[i] = static_cast<uint8_t>(i % NO_4);
    }
    std::vector<uint8_t> compressed(input.size(), 0);
    std::vector<uint8_t> restored(input.size(), 0);
    CompressAlgo storedAlgo = CompressAlgo::NONE;
    uint32_t storedLength = 0;

    ASSERT_EQ(SnapshotCompressionUtils::TryCompressInto(CompressAlgo::LZ4, input.data(), input.size(),
                                                        compressed.data(), compressed.size(), storedAlgo,
                                                        storedLength),
              BSS_OK);

    ASSERT_EQ(storedAlgo, CompressAlgo::LZ4);
    ASSERT_LT(storedLength, input.size());
    ASSERT_EQ(SnapshotCompressionUtils::Decompress(storedAlgo, compressed.data(), storedLength, restored.data(),
                                                   restored.size()),
              BSS_OK);
    ASSERT_EQ(memcmp(input.data(), restored.data(), input.size()), 0);
}

TEST(SnapshotCompressionUtilsTest, TryCompressIntoEmptyInputUsesNoCompression)
{
    std::vector<uint8_t> placeholder(NO_1, 0);
    CompressAlgo storedAlgo = CompressAlgo::LZ4;
    uint32_t storedLength = NO_1;

    ASSERT_EQ(SnapshotCompressionUtils::TryCompressInto(CompressAlgo::LZ4, placeholder.data(), NO_0,
                                                        placeholder.data(), NO_0, storedAlgo, storedLength),
              BSS_OK);
    ASSERT_EQ(storedAlgo, CompressAlgo::NONE);
    ASSERT_EQ(storedLength, NO_0);
}

TEST(SnapshotCompressionUtilsTest, DecompressRejectsInvalidLz4Payload)
{
    std::vector<uint8_t> invalidCompressed(IO_SIZE_1K, 3);
    std::vector<uint8_t> restored(IO_SIZE_4K, 0);

    ASSERT_NE(SnapshotCompressionUtils::Decompress(CompressAlgo::LZ4, invalidCompressed.data(),
                                                   invalidCompressed.size(), restored.data(), restored.size()),
              BSS_OK);
}

TEST_F(TestFreshSliceTableSnapshot, TestFreshRestoreCompressedScratchUsesTemporaryHeap)
{
    uint64_t restoreUsed = mMemManager->GetMemoryUseSize(MemoryType::RESTORE);
    uint64_t snapshotUsed = mMemManager->GetMemoryUseSize(MemoryType::SNAPSHOT);
    MemorySegmentRef scratchSegment = nullptr;

    ASSERT_EQ(AllocateFreshRestoreCompressedSegment(IO_SIZE_1K, scratchSegment), BSS_OK);
    ASSERT_TRUE(scratchSegment != nullptr);
    ASSERT_EQ(mMemManager->GetMemoryUseSize(MemoryType::RESTORE), restoreUsed);
    ASSERT_EQ(mMemManager->GetMemoryUseSize(MemoryType::SNAPSHOT), snapshotUsed);

    scratchSegment = nullptr;
    ASSERT_EQ(mMemManager->GetMemoryUseSize(MemoryType::RESTORE), restoreUsed);
    ASSERT_EQ(mMemManager->GetMemoryUseSize(MemoryType::SNAPSHOT), snapshotUsed);
}

TEST_F(TestFreshSliceTableSnapshot, TestFreshTableCheckpointWritesCompressedBytes)
{
    mConfig->SetFreshTableSnapshotCompressionPolicy("lz4");
    std::vector<std::vector<uint8_t>> keyStorage;
    std::vector<std::vector<uint8_t>> valueStorage;
    keyStorage.reserve(NO_1000);
    valueStorage.reserve(NO_1000);
    uint16_t stateId = VALUE << NO_13;

    for (uint32_t index = 0; index < NO_1000; ++index) {
        keyStorage.emplace_back(NO_32, 0);
        valueStorage.emplace_back(NO_1024, 1);
        auto &keyData = keyStorage.back();
        keyData[0] = static_cast<uint8_t>(index & NO_U8_255);
        keyData[1] = static_cast<uint8_t>((index >> NO_8) & NO_U8_255);
        uint32_t hashCode = test::HashForTest(keyData.data(), keyData.size());
        BinaryData priKey(keyData.data(), keyData.size());
        QueryKey queryKey(stateId, hashCode, priKey);

        Value putVal;
        putVal.Init(ValueType::PUT, valueStorage.back().size(), valueStorage.back().data(), index);
        ASSERT_EQ(mBoostStateDB->GetFreshTable()->Put(queryKey, putVal), BSS_OK);
    }

    uint32_t rawLength = mBoostStateDB->GetFreshTable()->GetActiveSegment()->GetMemorySegment()->GetCurPos();
    ASSERT_GT(rawLength, NO_0);
    ASSERT_NE(TestFreshSliceTableSnapshot::mBoostStateDB->CreateSyncCheckpoint("/tmp/" + std::to_string(mCheckpointId),
                                                                               mCheckpointId),
              nullptr);
    ASSERT_EQ(mBoostStateDB->CreateAsyncCheckpoint(mCheckpointId, false), BSS_OK);

    std::string freshTableFile = "/tmp/" + std::to_string(mCheckpointId) + "/fresh_table.dat";
    struct stat fileStat {};
    ASSERT_EQ(stat(freshTableFile.c_str(), &fileStat), 0);
    ASSERT_LT(static_cast<uint64_t>(fileStat.st_size), rawLength);
}

TEST_F(TestFreshSliceTableSnapshot, TestFreshTableRestoreReadsCompressedCheckpointByMeta)
{
    mConfig->SetFreshTableSnapshotCompressionPolicy("lz4");
    std::vector<std::vector<uint8_t>> keyStorage;
    std::vector<std::vector<uint8_t>> valueStorage;
    std::vector<QueryKey> keys;
    keyStorage.reserve(NO_1000);
    valueStorage.reserve(NO_1000);
    keys.reserve(NO_1000);
    uint16_t stateId = VALUE << NO_13;

    for (uint32_t index = 0; index < NO_1000; ++index) {
        keyStorage.emplace_back(NO_32, 0);
        valueStorage.emplace_back(NO_1024, 2);
        auto &keyData = keyStorage.back();
        keyData[0] = static_cast<uint8_t>(index & NO_U8_255);
        keyData[1] = static_cast<uint8_t>((index >> NO_8) & NO_U8_255);
        uint32_t hashCode = test::HashForTest(keyData.data(), keyData.size());
        BinaryData priKey(keyData.data(), keyData.size());
        keys.emplace_back(stateId, hashCode, priKey);

        Value putVal;
        putVal.Init(ValueType::PUT, valueStorage.back().size(), valueStorage.back().data(), index);
        ASSERT_EQ(mBoostStateDB->GetFreshTable()->Put(keys.back(), putVal), BSS_OK);
    }

    ASSERT_NE(TestFreshSliceTableSnapshot::mBoostStateDB->CreateSyncCheckpoint("/tmp/" + std::to_string(mCheckpointId),
                                                                               mCheckpointId),
              nullptr);
    ASSERT_EQ(mBoostStateDB->CreateAsyncCheckpoint(mCheckpointId, false), BSS_OK);

    if (mBoostStateDB != nullptr) {
        mBoostStateDB->Close();
        delete mBoostStateDB;
        mBoostStateDB = nullptr;
    }

    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_15, NO_16);
    config->mMemorySegmentSize = IO_SIZE_64M;
    config->SetEvictMinSize(IO_SIZE_2G);
    config->SetSliceStandardSizePerBucket(IO_SIZE_1M);
    config->SetLocalPath("/tmp/" + std::to_string(mCheckpointId) + "/sst");
    config->SetFreshTableSnapshotCompressionPolicy("none");
    mConfig = config;
    mMemManager = std::make_shared<MemManager>(AllocatorType::DIRECT);
    mMemManager->Initialize(config);
    mBoostStateDB = (BoostStateDBImpl *)BoostStateDBFactory::Create();
    ASSERT_EQ(mBoostStateDB->Open(config), BSS_OK);
    mBoostStateDB->GetSliceTable()->SetMemHighMark(IO_SIZE_2G);

    std::string restorePath = "/tmp/" + std::to_string(mCheckpointId);
    std::unordered_map<std::string, std::string> pathMap;
    std::vector<std::string> restorePaths;
    restorePaths.emplace_back(restorePath);
    ASSERT_EQ(mBoostStateDB->Restore(restorePaths, pathMap, false, true), BSS_OK);

    for (const auto &key : keys) {
        Value value{};
        mBoostStateDB->GetFreshTable()->Get(key, value);
        ASSERT_TRUE(!value.IsNull());
        ASSERT_EQ(value.ValueLen(), NO_1024);
        ASSERT_EQ(value.ValueData()[0], 2);
    }
}

TEST_F(TestFreshSliceTableSnapshot, TestFreshTableRestoreRejectsZeroRawLengthWithStoredPayload)
{
    std::vector<uint8_t> keyData(NO_32, 0);
    std::vector<uint8_t> valueData(NO_1024, 3);
    uint16_t stateId = VALUE << NO_13;
    uint32_t hashCode = test::HashForTest(keyData.data(), keyData.size());
    BinaryData priKey(keyData.data(), keyData.size());
    QueryKey queryKey(stateId, hashCode, priKey);
    Value putVal;
    putVal.Init(ValueType::PUT, valueData.size(), valueData.data(), NO_1);
    ASSERT_EQ(mBoostStateDB->GetFreshTable()->Put(queryKey, putVal), BSS_OK);

    std::string checkpointPath = "/tmp/" + std::to_string(mCheckpointId);
    ASSERT_NE(mBoostStateDB->CreateSyncCheckpoint(checkpointPath, mCheckpointId), nullptr);
    ASSERT_EQ(mBoostStateDB->CreateAsyncCheckpoint(mCheckpointId, false), BSS_OK);

    std::string metadataPath = checkpointPath + "/metadata";
    uint64_t rawLengthOffset = 0;
    bool foundFreshTableMeta = false;
    {
        auto restoredDbMeta = SnapshotRestoreUtils::ReadDbMeta(std::make_shared<Path>(metadataPath));
        ASSERT_NE(restoredDbMeta, nullptr);
        auto metaInputView = restoredDbMeta->GetSnapshotMetaInputView();
        for (const auto &opInfo : restoredDbMeta->GetRestoredSnapshotOperatorInfos()) {
            if (opInfo->GetSnapshotOperatorInfo()->GetSnapshotOperatorType() != SnapshotOperatorType::FRESH_TABLE) {
                continue;
            }
            uint64_t freshMetaOffset = opInfo->GetSnapshotOperatorMetaOffset();
            metaInputView->Seek(freshMetaOffset);
            std::string address;
            ASSERT_EQ(metaInputView->ReadUTF(address), BSS_OK);
            rawLengthOffset = freshMetaOffset + sizeof(uint64_t) + address.size();
            foundFreshTableMeta = true;
            break;
        }
    }
    ASSERT_TRUE(foundFreshTableMeta);

    std::fstream metadata(metadataPath, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(metadata.is_open());
    metadata.seekp(static_cast<std::streamoff>(rawLengthOffset));
    uint32_t zeroRawLength = 0;
    metadata.write(reinterpret_cast<const char *>(&zeroRawLength), sizeof(zeroRawLength));
    metadata.close();
    ASSERT_FALSE(metadata.fail());

    mBoostStateDB->Close();
    delete mBoostStateDB;
    mBoostStateDB = nullptr;

    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_15, NO_16);
    config->mMemorySegmentSize = IO_SIZE_64M;
    config->SetEvictMinSize(IO_SIZE_2G);
    config->SetSliceStandardSizePerBucket(IO_SIZE_1M);
    config->SetLocalPath(checkpointPath + "/sst");
    mConfig = config;
    mMemManager = std::make_shared<MemManager>(AllocatorType::DIRECT);
    mMemManager->Initialize(config);
    mBoostStateDB = (BoostStateDBImpl *)BoostStateDBFactory::Create();
    ASSERT_EQ(mBoostStateDB->Open(config), BSS_OK);
    mBoostStateDB->GetSliceTable()->SetMemHighMark(IO_SIZE_2G);

    std::unordered_map<std::string, std::string> pathMap;
    std::vector<std::string> restorePaths{ checkpointPath };
    ASSERT_NE(mBoostStateDB->Restore(restorePaths, pathMap, false, true), BSS_OK);
}

void TestFreshSliceTableSnapshot::SetUp()
{
    std::string restorePath = "/tmp/" + std::to_string(mCheckpointId);
    // 删除目录下的所有文件
    DeleteDirectoryContents(restorePath);
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_15, NO_16);
    mConfig = config;
    mConfig->mMemorySegmentSize = IO_SIZE_64M;
    mConfig->SetEvictMinSize(IO_SIZE_2G);
    mConfig->SetSliceStandardSizePerBucket(IO_SIZE_1M);
    mConfig->SetLocalPath("/tmp/" + std::to_string(mCheckpointId) + "/sst");
    mMemManager = std::make_shared<MemManager>(AllocatorType::DIRECT);
    mMemManager->Initialize(config);

    mBoostStateDB = (BoostStateDBImpl *)BoostStateDBFactory::Create();

    BResult result = mBoostStateDB->Open(config);

    // 设置水位2G，保证不向lsm淘汰
    mBoostStateDB->GetSliceTable()->SetMemHighMark(IO_SIZE_2G);

    ASSERT_TRUE(result == BSS_OK);
    // 初始化OK
    ASSERT_TRUE(mBoostStateDB != nullptr);
}

void TestFreshSliceTableSnapshot::TearDown()
{
    if (mBoostStateDB != nullptr) {
        mBoostStateDB->Close();
        delete mBoostStateDB;
        mBoostStateDB = nullptr;
    }
    std::string restorePath = "/tmp/" + std::to_string(mCheckpointId);
    // 删除目录下的所有文件
    DeleteDirectoryContents(restorePath);
}

void TestFreshSliceTableSnapshot::SetUpTestCase()
{
}

void TestFreshSliceTableSnapshot::TearDownTestCase()
{
}

TEST_F(TestFreshSliceTableSnapshot, TestSnapshotFuncInFreshTable)
{
    // 写10000条数据
    TestFreshSliceTableSnapshot::PutDataToFreshSliceTable(originKeyList, originValueList, NO_10000);

    for (auto &key : originKeyList) {
        Value value{};
        mBoostStateDB->GetFreshTable()->Get(key, value);
        ASSERT_TRUE(!value.IsNull());
    }
    // 测试同步snapshot流程
    ASSERT_NE(TestFreshSliceTableSnapshot::mBoostStateDB->CreateSyncCheckpoint("/tmp/" + std::to_string(mCheckpointId),
                                                                               mCheckpointId),
              nullptr);
    // 测试异步snapshot流程
    ASSERT_EQ(mBoostStateDB->CreateAsyncCheckpoint(mCheckpointId, false), BSS_OK);

    // 以下是恢复流程
    // 先删除DB
    if (mBoostStateDB != nullptr) {
        mBoostStateDB->Close();
        delete mBoostStateDB;
        mBoostStateDB = nullptr;
    }
    // 重新创建DB
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_15, NO_16);
    mConfig = config;
    mConfig->mMemorySegmentSize = IO_SIZE_64M;
    mConfig->SetEvictMinSize(IO_SIZE_2G);
    mConfig->SetLocalPath("/tmp/" + std::to_string(mCheckpointId) + "/sst");
    mMemManager = std::make_shared<MemManager>(AllocatorType::DIRECT);
    mMemManager->Initialize(config);

    mBoostStateDB = (BoostStateDBImpl *)BoostStateDBFactory::Create();

    BResult result = mBoostStateDB->Open(config);
    ASSERT_TRUE(result == BSS_OK);
    // 设置水位2G，保证不向lsm淘汰
    mBoostStateDB->GetSliceTable()->SetMemHighMark(IO_SIZE_2G);
    // 恢复
    std::string restorePath = "/tmp/" + std::to_string(mCheckpointId);
    std::unordered_map<std::string, std::string> pathMap;
    std::vector<std::string> restorePaths;
    restorePaths.emplace_back(restorePath);
    ASSERT_EQ(mBoostStateDB->Restore(restorePaths, pathMap, false, true), BSS_OK);

    // 判断恢复后能找到写入的key，保证数据正确
    for (auto &key : originKeyList) {
        Value value{};
        mBoostStateDB->GetFreshTable()->Get(key, value);
        ASSERT_TRUE(!value.IsNull());
    }
}

TEST_F(TestFreshSliceTableSnapshot, TestSnapshotFuncInSliceTable)
{
    originKeyList.clear();
    originValueList.clear();
    // 写10000条数据
    TestFreshSliceTableSnapshot::PutDataToFreshSliceTable(originKeyList, originValueList, NO_10000);
    // 强刷进slice table中
    BResult ret = mBoostStateDB->GetFreshTable()->TriggerSegmentFlush();
    ASSERT_EQ(ret, BSS_OK);
    while (!mBoostStateDB->GetFreshTable()->IsSnapshotQueueEmpty()) {
        sleep(NO_1);
    }
    // 数据写入完成后查询，必须能在slice中找到数据
    TestAllKeysFindInSliceAndLsm();
    // 测试同步snapshot流程
    ASSERT_NE(TestFreshSliceTableSnapshot::mBoostStateDB->CreateSyncCheckpoint("/tmp/" + std::to_string(mCheckpointId),
                                                                               mCheckpointId),
              nullptr);
    // 测试异步snapshot流程
    ASSERT_EQ(mBoostStateDB->CreateAsyncCheckpoint(mCheckpointId, false), BSS_OK);

    // 以下是恢复流程
    // 先删除DB
    if (mBoostStateDB != nullptr) {
        mBoostStateDB->Close();
        delete mBoostStateDB;
        mBoostStateDB = nullptr;
    }
    // 重新创建DB
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_15, NO_16);
    mConfig = config;
    mConfig->SetSliceStandardSizePerBucket(IO_SIZE_1M);
    mConfig->mMemorySegmentSize = IO_SIZE_16K;
    mConfig->SetEvictMinSize(IO_SIZE_2G);
    mConfig->SetLocalPath("/tmp/" + std::to_string(mCheckpointId) + "/sst");
    mMemManager = std::make_shared<MemManager>(AllocatorType::DIRECT);
    mMemManager->Initialize(config);

    mBoostStateDB = (BoostStateDBImpl *)BoostStateDBFactory::Create();

    BResult result = mBoostStateDB->Open(config);
    ASSERT_TRUE(result == BSS_OK);
    // 设置水位2G，保证不向lsm淘汰
    mBoostStateDB->GetSliceTable()->SetMemHighMark(IO_SIZE_2G);
    // 恢复
    std::string restorePath = "/tmp/" + std::to_string(mCheckpointId);
    std::unordered_map<std::string, std::string> pathMap;
    std::vector<std::string> restorePaths;
    restorePaths.emplace_back(restorePath);
    ASSERT_EQ(mBoostStateDB->Restore(restorePaths, pathMap, false, true), BSS_OK);

    // 判断恢复后能在slice中找到写入的key，保证数据正确
    TestAllKeysFindInSliceAndLsm();
}

TEST_F(TestFreshSliceTableSnapshot, TestSnapshotFuncInFileStore)
{
    originKeyList.clear();
    originValueList.clear();
    // 设置最小淘汰大小为0，保证一直向lsm淘汰
    mBoostStateDB->GetSliceTable()->GetFullSortEvictor()->SetVictMinSize(1);
    // 设置水位0G，保证一直向lsm淘汰
    mBoostStateDB->GetSliceTable()->SetMemHighMark(NO_0);
    // 写100000条数据
    TestFreshSliceTableSnapshot::PutDataToFreshSliceTable(originKeyList, originValueList, NO_100000);
    // 直接刷到slice table中
    BResult ret = mBoostStateDB->GetFreshTable()->TriggerSegmentFlush();
    ASSERT_EQ(ret, BSS_OK);
    while (!mBoostStateDB->GetFreshTable()->IsSnapshotQueueEmpty()) {
        sleep(NO_1);
    }
    // 保证slice中的数据已经全部淘汰到lsm中
    while (!IsAllSliceEvicted()) {
        mBoostStateDB->GetSliceTable()->TryCurrentDBEvict(0, true, true);
    }
    // 数据写入完成后查询，必须能在lsm
    // store中找到数据,因为水位线设置为0.并且fresh中向下刷了，所以正常所有的数据都会在lsm store中找到。
    TestAllKeysFindInSliceAndLsm();
    // 测试同步snapshot流程
    ASSERT_NE(TestFreshSliceTableSnapshot::mBoostStateDB->CreateSyncCheckpoint("/tmp/" + std::to_string(mCheckpointId),
                                                                               mCheckpointId),
              nullptr);
    // 测试异步snapshot流程
    ASSERT_EQ(mBoostStateDB->CreateAsyncCheckpoint(mCheckpointId, false), BSS_OK);

    // 以下是恢复流程
    // 先删除DB
    if (mBoostStateDB != nullptr) {
        mBoostStateDB->Close();
        delete mBoostStateDB;
        mBoostStateDB = nullptr;
    }
    // 重新创建DB
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_15, NO_16);
    mConfig = config;
    mConfig->mTotalMemHighMarkRatio = 0;
    mConfig->SetEvictMinSize(NO_1);
    mConfig->SetSliceStandardSizePerBucket(IO_SIZE_1M);
    mConfig->mMemorySegmentSize = IO_SIZE_16K;
    mConfig->SetLocalPath("/tmp/" + std::to_string(mCheckpointId) + "/sst");
    mMemManager = std::make_shared<MemManager>(AllocatorType::DIRECT);
    mMemManager->Initialize(config);

    mBoostStateDB = (BoostStateDBImpl *)BoostStateDBFactory::Create();

    BResult result = mBoostStateDB->Open(config);
    ASSERT_TRUE(result == BSS_OK);
    // 设置水位0G，保证一直向lsm淘汰
    mBoostStateDB->GetSliceTable()->SetMemHighMark(NO_0);
    // 恢复
    std::string restorePath = "/tmp/" + std::to_string(mCheckpointId);
    std::unordered_map<std::string, std::string> pathMap;
    std::vector<std::string> restorePaths;
    restorePaths.emplace_back(restorePath);
    ASSERT_EQ(mBoostStateDB->Restore(restorePaths, pathMap, false, true), BSS_OK);

    // 判断恢复后能在slice中找到写入的key，保证数据正确
    TestAllKeysFindInSliceAndLsm();
    // 确保恢复后能够正常写入数据，不影响正常IO流程
    TestFreshSliceTableSnapshot::PutDataToFreshSliceTable(originKeyList, originValueList, NO_100);
    // 直接刷到slice table中
    ret = mBoostStateDB->GetFreshTable()->TriggerSegmentFlush();
    ASSERT_EQ(ret, BSS_OK);
    while (!mBoostStateDB->GetFreshTable()->IsSnapshotQueueEmpty()) {
        sleep(NO_1);
    }
    // 保证slice中的数据已经全部淘汰到lsm中
    while (!IsAllSliceEvicted()) {
        mBoostStateDB->GetSliceTable()->TryCurrentDBEvict(0, true, true);
    }
}
