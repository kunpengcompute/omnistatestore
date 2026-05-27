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

#include <map>

#include "test_fresh_slice_table_snapshot.h"

using namespace ock::bss;

BoostStateDBImpl *TestFreshSliceTableSnapshot::mBoostStateDB = nullptr;
ConfigRef TestFreshSliceTableSnapshot::mConfig = nullptr;
MemManagerRef TestFreshSliceTableSnapshot::mMemManager = nullptr;
uint64_t TestFreshSliceTableSnapshot::mCheckpointId = NO_1;
std::vector<QueryKey> TestFreshSliceTableSnapshot::originKeyList{};
std::vector<Value> TestFreshSliceTableSnapshot::originValueList{};

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