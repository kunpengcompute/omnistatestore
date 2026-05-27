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

#include <iostream>
#include <vector>

#include "include/bss_types.h"
#include "include/config.h"
#include "binary/query_binary.h"
#include "test_file.h"

using namespace ock::bss;

const std::vector<std::string> compressionLevelPolicyForTestFile = {"lz4", "lz4", "lz4"};
const std::string lsmStoreCompressionPolicyForTestFile = "lz4";

std::vector<std::pair<SliceKey, Value>> TestFile::GenerateKeyValue(uint64_t size, int32_t seed)
{
    uint64_t currentSize = 0;
    std::vector<std::pair<SliceKey, Value>> result;
    std::set<uint32_t> prKeyHash;
    while (currentSize < size) {
        // state id;
        uint16_t stateId = VALUE << NO_13;

        SliceKey sglKey = mGenerator->GenerateSglKey(NO_10, stateId);
        Value value = mGenerator->GenerateValue(NO_1024, mSeqGenerator->Next());
        result.emplace_back(std::pair<SliceKey, Value>(sglKey, value));
        currentSize++;
    }
    return result;
}

std::vector<std::pair<SliceKey, Value>> TestFile::GenerateKeyMapValue(uint64_t size, std::vector<uint32_t> &priKeyHash,
    KeyVectorValueMap &prefixEntries, int32_t seed)
{
    uint64_t currentSize = 0;
    std::vector<std::pair<SliceKey, Value>> result;
    while (currentSize < size) {
        // dual key.
        SliceKey dualKey = mGenerator->GenerateDualKey(NO_10, NO_20);

        // prepare value.
        Value value = mGenerator->GenerateValue(NO_1024, mSeqGenerator->Next());
        result.emplace_back(std::pair<SliceKey, Value>(dualKey, value));
        currentSize++;
    }

    return result;
}

std::vector<std::pair<SliceKey, Value>> TestFile::GenerateListKeyValue(uint64_t size, ValueType valueType, int32_t seed)
{
    uint64_t currentSize = 0;
    std::set<uint32_t> prKeyHash;
    std::vector<std::pair<SliceKey, Value>> result;
    while (currentSize < size) {
        // state id;
        uint16_t stateId = LIST << NO_13;

        SliceKey sglKey = mGenerator->GenerateSglKey(NO_10, stateId);
        // 2、生成value
        Value value = mGenerator->GenerateValue(NO_1024, mSeqGenerator->Next());
        result.emplace_back(sglKey, value);
        currentSize++;
    }
    return result;
}

Value TestFile::GenerateDeleteValue()
{
    Value value;
    value.Init(DELETE, 0, nullptr, mSeqGenerator->Next(), nullptr);
    return value;
}

// 单个文件kv读写
TEST_F(TestFile, test_write_read_single_file_return_ok)
{
    InitEnv(std::make_shared<Config>(NO_0, NO_15, NO_16));

    std::vector<std::pair<SliceKey, Value>> queryEntries = GenerateKeyValue(IO_SIZE_16K);
    std::vector<std::pair<SliceKey, Value>> putEntries;
    std::vector<uint32_t> existPriHashCode;

    for (auto &entry : queryEntries) {
        uint32_t hash = entry.first.KeyHashCode();
        // 生成的重复hash跳过，避免对比数据出错
        if (std::find(existPriHashCode.begin(), existPriHashCode.end(), hash) != existPriHashCode.end()) {
            continue;
        }
        existPriHashCode.emplace_back(hash);
        putEntries.emplace_back(entry);
    }

    FlushLevel0Table(putEntries);
    ASSERT_EQ(mMetric->GetFileFlush(), 1);
    // check get value result.
    for (auto &entry : queryEntries) {
        GetAndCheckValue(entry.first, entry.second);
    }

    // check prefix iterator result.
    for (auto &entry : queryEntries) {
        GetAndCheckPrefixIterator(entry.first, entry.second);
    }
}

// 多个文件读写，触发compaction
TEST_F(TestFile, test_compact_2_files_return_ok)
{
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_15, NO_16);
    config->SetLsmStoreCompactionSwitch(1);
    config->SetLsmStoreCompressionPolicy(lsmStoreCompressionPolicyForTestFile);
    config->SetCompressionLevelPolicy(compressionLevelPolicyForTestFile);
    config->SetFileStoreL0NumTrigger(NO_2);
    InitEnv(config);

    std::set<uint32_t> priKeyHash;
    std::vector<std::pair<SliceKey, Value>> queryEntries;
    uint32_t fileCount = NO_2;
    for (uint32_t i = 0; i < fileCount; i++) {
        std::vector<std::pair<SliceKey, Value>> putPair;
        std::vector<std::pair<SliceKey, Value>> pair = GenerateKeyValue(2048, 10 * i + 1);
        for (auto &item : pair) {
            uint32_t hash = item.first.KeyHashCode();
            // 生成的重复hash跳过，避免对比数据出错
            if (std::find(priKeyHash.begin(), priKeyHash.end(), hash) != priKeyHash.end()) {
                continue;
            }
            priKeyHash.emplace(hash);
            putPair.emplace_back(item);
            queryEntries.emplace_back(item);
        }
        FlushLevel0Table(queryEntries);
    }

    while (!mLsmStore->CheckCompactionCompleted()) {
        sleep(1);
    }

    // check get value result.
    for (auto &entry : queryEntries) {
        GetAndCheckValue(entry.first, entry.second);
    }

    // check prefix iterator result.
    for (auto &entry : queryEntries) {
        GetAndCheckPrefixIterator(entry.first, entry.second);
    }
}

// 多个文件读写，触发compaction
TEST_F(TestFile, test_compact_3_files_return_ok)
{
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_15, NO_16);
    config->SetLsmStoreCompactionSwitch(1);
    config->SetLsmStoreCompressionPolicy(lsmStoreCompressionPolicyForTestFile);
    config->SetCompressionLevelPolicy(compressionLevelPolicyForTestFile);
    config->SetFileStoreL0NumTrigger(NO_2);
    InitEnv(config);

    std::vector<std::pair<SliceKey, Value>> queryEntries;
    KeyVectorValueMap prefixEntries;
    uint32_t fileCount = NO_3;
    std::vector<uint32_t> priKeyHash;
    for (uint32_t i = 0; i < fileCount; i++) {
        std::vector<std::pair<SliceKey, Value>> putEntries;
        std::vector<std::pair<SliceKey, Value>> pair = GenerateKeyMapValue(NO_1024, priKeyHash, prefixEntries);
        for (auto &item : pair) {
            putEntries.emplace_back(item);
            queryEntries.emplace_back(item);
        }
        FlushLevel0Table(putEntries);
    }

    while (!mLsmStore->CheckCompactionCompleted()) {
        sleep(1);
    }

    // check get value result.
    for (auto &entry : queryEntries) {
        GetAndCheckValue(entry.first, entry.second);
    }

    // check prefix iterator result.
    for (auto &entry : prefixEntries) {
        GetAndCheckPrefixIterator(entry.first, entry.second);
    }
}

// 多个文件读写，包括delete数据触发compaction
TEST_F(TestFile, test_value_state_with_deleted_value_return_ok)
{
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_15, NO_16);
    config->SetLsmStoreCompactionSwitch(1);
    config->SetLsmStoreCompressionPolicy(lsmStoreCompressionPolicyForTestFile);
    config->SetCompressionLevelPolicy(compressionLevelPolicyForTestFile);
    config->SetFileStoreL0NumTrigger(NO_2);
    InitEnv(config);

    std::vector<std::pair<SliceKey, Value>> queryEntries;
    std::vector<SliceKey> allDeletedKeys;
    KeyVectorValueMap prefixEntries;

    // 存放firstKey-values 用于前缀查找
    uint32_t fileCount = NO_3;
    std::vector<SliceKey> deletedKeys;
    std::map<uint32_t, uint32_t> deleteHashCodes;

    for (uint32_t i = 0; i < fileCount; i++) {
        std::vector<std::pair<SliceKey, Value>> putEntries;
        std::vector<uint32_t> priKeyHash;
        std::vector<std::pair<SliceKey, Value>> newEntries =
            GenerateKeyMapValue(NO_1024, priKeyHash, prefixEntries, i * NO_10000);
        // 写入上个文件选择需要删除的key
        for (auto &deletedKey : deletedKeys) {
            Value deletedValue = GenerateDeleteValue();
            putEntries.emplace_back(deletedKey, deletedValue);
        }
        deletedKeys.clear();
        uint32_t deleteFlag = 0;
        for (auto &entry : newEntries) {
            putEntries.emplace_back(entry);
            queryEntries.emplace_back(entry);
            // 每10个key选一个删除，并且不是最后一个文件，避免选择了无法删除
            if (++deleteFlag % NO_10 == 0 && i != (fileCount - 1)) {
                auto it = deleteHashCodes.find(entry.first.KeyHashCode());
                if (it == deleteHashCodes.end()) {
                    deletedKeys.emplace_back(entry.first);
                    allDeletedKeys.emplace_back(entry.first);
                    deleteHashCodes.emplace(entry.first.KeyHashCode(), entry.first.KeyHashCode());
                }
            }
        }

        FlushLevel0Table(putEntries);
    }

    // 等待compaction结束
    while (!mLsmStore->CheckCompactionCompleted()) {
        sleep(1);
    }

    for (auto &entry : queryEntries) {
        GetAndCheckValue(entry.first, entry.second, allDeletedKeys);
    }

    for (auto &entry : prefixEntries) {
        GetAndCheckPrefixIterator(entry.first, entry.second, allDeletedKeys);
    }
}

TEST_F(TestFile, test_list_state_return_ok)
{
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_15, NO_16);
    config->SetLsmStoreCompactionSwitch(1);
    config->SetLsmStoreCompressionPolicy(lsmStoreCompressionPolicyForTestFile);
    config->SetCompressionLevelPolicy(compressionLevelPolicyForTestFile);
    config->SetFileStoreL0NumTrigger(NO_2);
    config->SetTotalDBSize(IO_SIZE_2G);
    InitEnv(config);

    uint32_t fileCount = NO_3;
    std::vector<std::pair<SliceKey, Value>> queryEntries;
    std::set<uint32_t> priKeyHash;
    for (uint32_t i = 0; i < fileCount; i++) {
        std::vector<std::pair<SliceKey, Value>> putEntries;
        std::vector<std::pair<SliceKey, Value>> newEntries = GenerateListKeyValue(NO_1024, PUT, i * 10000);
        for (auto &entry : newEntries) {
            uint32_t hash = entry.first.KeyHashCode();
            // 生成的重复hash跳过，避免对比数据出错
            if (std::find(priKeyHash.begin(), priKeyHash.end(), hash) != priKeyHash.end()) {
                continue;
            }
            priKeyHash.emplace(hash);
            putEntries.emplace_back(entry);
            queryEntries.emplace_back(entry);
        }
        FlushLevel0Table(putEntries);
    }

    while (!mLsmStore->CheckCompactionCompleted()) {
        sleep(1);
    }

    for (auto &entry : queryEntries) {
        GetAndCheckValue(entry.first, entry.second);
    }

    for (auto &item : queryEntries) {
        GetAndCheckPrefixIterator(item.first, item.second);
    }
}

// list、value 覆盖写并查询
TEST_F(TestFile, test_value_and_list_state_with_covered_value_return_ok)
{
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_15, NO_16);
    config->SetLsmStoreCompactionSwitch(1);
    config->SetLsmStoreCompressionPolicy(lsmStoreCompressionPolicyForTestFile);
    config->SetCompressionLevelPolicy(compressionLevelPolicyForTestFile);
    InitEnv(config);

    uint32_t fileCount = NO_3;
    SliceKVMap queryEntries;
    std::vector<SliceKey> coverKeys;
    std::set<uint32_t> priKeyHash;
    for (uint32_t i = 0; i < fileCount; i++) {
        std::vector<std::pair<SliceKey, Value>> putEntries;
        // 写入上一个文件选择覆盖的key，重新生成value，queryPair更新为最新的value用于查询时对比
        for (auto &key : coverKeys) {
            Value value = mGenerator->GenerateValue(NO_1024, mSeqGenerator->Next());
            putEntries.emplace_back(key, value);
            queryEntries[key] = value;
        }
        coverKeys.clear();
        uint32_t count = 0;

        std::vector<std::pair<SliceKey, Value>> listPair = GenerateListKeyValue(NO_1024, PUT);
        std::vector<std::pair<SliceKey, Value>> valuePair = GenerateKeyValue(NO_512);
        valuePair.insert(valuePair.end(), listPair.begin(), listPair.end());
        for (auto &item : valuePair) {
            uint32_t hash = item.first.KeyHashCode();
            // 生成的重复hash跳过，避免对比数据出错
            if (std::find(priKeyHash.begin(), priKeyHash.end(), hash) != priKeyHash.end()) {
                continue;
            }
            priKeyHash.emplace(hash);
            count++;
            putEntries.emplace_back(item);
            queryEntries.emplace(item.first, item.second);
            // 每10个key覆盖写一个
            if (count % NO_10 == 0 && (fileCount - 1) != i) {
                coverKeys.emplace_back(item.first);
            }
        }
        FlushLevel0Table(putEntries);
    }

    while (!mLsmStore->CheckCompactionCompleted()) {
        sleep(1);
    }

    for (auto &entry : queryEntries) {
        GetAndCheckValue(entry.first, entry.second);
    }

    for (auto &item : queryEntries) {
        GetAndCheckPrefixIterator(item.first, item.second);
    }
}

TEST_F(TestFile, test_write_level4_return_ok)
{
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_15, NO_16);
    config->SetLsmStoreCompactionSwitch(1);
    config->SetLsmStoreCompressionPolicy(lsmStoreCompressionPolicyForTestFile);
    config->SetCompressionLevelPolicy(compressionLevelPolicyForTestFile);
    // 每个文件最大值
    config->SetFileStoreFileBaseSize(NO_8 * NO_1024);
    // 除0层，每层触发compaction文件大小
    config->SetFileStoreMaxBaseLevelBytes(NO_3 * NO_1024);
    // 1层以上，每层文件可容纳大小按此倍数递增
    config->SetFileStoreMultiple(NO_2);
    InitEnv(config);

    uint32_t fileCount = NO_10;

    SliceKVMap queryEntries;
    std::vector<uint32_t> priKeyHash;
    for (uint32_t i = 0; i < fileCount; i++) {
        std::vector<std::pair<SliceKey, Value>> putEntries;
        std::vector<std::pair<SliceKey, Value>> newEntries = GenerateKeyValue(NO_512);
        for (auto &entry : newEntries) {
            uint32_t hash = entry.first.KeyHashCode();
            if (std::find(priKeyHash.begin(), priKeyHash.end(), hash) != priKeyHash.end()) {
                continue;
            }
            priKeyHash.emplace_back(hash);
            putEntries.emplace_back(entry);
            queryEntries.emplace(entry.first, entry.second);
        }
        FlushLevel0Table(putEntries);
    }

    while (!mLsmStore->CheckCompactionCompleted()) {
        sleep(1);
    }

    for (auto &entry : queryEntries) {
        GetAndCheckValue(entry.first, entry.second);
    }

    for (auto &entry : queryEntries) {
        GetAndCheckPrefixIterator(entry.first, entry.second);
    }
}

TEST_F(TestFile, test_value_and_list_state_with_deleted_value_return_ok)
{
    ConfigRef config = std::make_shared<Config>();
    config->Init(NO_0, NO_15, NO_16);
    config->SetLsmStoreCompactionSwitch(1);
    config->SetLsmStoreCompressionPolicy(lsmStoreCompressionPolicyForTestFile);
    config->SetCompressionLevelPolicy(compressionLevelPolicyForTestFile);
    config->SetFileStoreL0NumTrigger(NO_2);
    InitEnv(config);

    uint32_t fileCount = NO_3;

    SliceKVMap queryEntries;
    std::vector<SliceKey> allDeletedKeys;
    std::vector<SliceKey> tempDeletedKeys;
    std::set<uint32_t> priKeyHash;
    for (uint32_t i = 0; i < fileCount; i++) {
        std::vector<std::pair<SliceKey, Value>> putEntries;
        // 写入上一个文件选择覆盖的key，重新生成value，queryPair更新为最新的value用于查询时对比
        for (auto &deletedKey : tempDeletedKeys) {
            Value deletedValue = GenerateDeleteValue();
            putEntries.emplace_back(deletedKey, deletedValue);
        }
        tempDeletedKeys.clear();
        uint32_t count = 0;

        std::vector<std::pair<SliceKey, Value>> listPair = GenerateListKeyValue(NO_32, PUT);
        std::vector<std::pair<SliceKey, Value>> valuePair = GenerateKeyValue(NO_32);
        valuePair.insert(valuePair.end(), listPair.begin(), listPair.end());
        for (auto &item : valuePair) {
            uint32_t hash = item.first.KeyHashCode();
            // 生成的重复hash跳过，避免对比数据出错
            if (priKeyHash.find(hash) != priKeyHash.end()) {
                continue;
            }
            priKeyHash.emplace(hash);
            count++;
            putEntries.emplace_back(item);
            queryEntries.emplace(item.first, item.second);
            // 每10个key覆盖写一个
            if (count % NO_10 == 0 && (fileCount - 1) != i) {
                tempDeletedKeys.emplace_back(item.first);
                allDeletedKeys.emplace_back(item.first);
            }
        }
        FlushLevel0Table(putEntries);
    }

    while (!mLsmStore->CheckCompactionCompleted()) {
        sleep(1);
    }

    for (auto &entry : queryEntries) {
        GetAndCheckValue(entry.first, entry.second, allDeletedKeys);
    }

    for (auto &entry : queryEntries) {
        GetAndCheckPrefixIterator(entry.first, entry.second, allDeletedKeys);
    }
}

TEST_F(TestFile, test_build_filter_block_return_ok)
{
    std::random_device rd;
    std::mt19937 gen(rd());

    uint32_t byteBegin = NO_1;
    uint32_t byteEnd = UINT32_MAX;
    std::uniform_int_distribution<uint32_t> byteDist(byteBegin, byteEnd);
    uint32_t count = 10000;
    std::set<uint32_t> vector;
    std::vector<uint32_t> testVector;
    for (uint32_t j = 0; j < count; j++) {
        auto hash = byteDist(gen);
        vector.emplace(hash);
        testVector.emplace_back(hash + NO_MAX_INT32_VALUE_2G);
    }
    uint32_t match;
    std::vector<uint8_t> array = BuildFilterBlock(vector);

    for (uint32_t j = 0; j < testVector.size(); j++) {
        bool ret = KeyMatch(array, testVector[j]);
        if (ret) {
            match++;
        }
    }
    printf("test count:%zu,match :%u \n", testVector.size(), match);
}

std::vector<uint8_t> TestFile::BuildFilterBlock(std::set<uint32_t> hashVec)
{
    uint32_t bits = static_cast<double>(-(hashVec.size() * log(0.01) / log(2) / log(2)));
    uint32_t k = ceil(bits / hashVec.size() * log(2));
    if (bits < NO_64) {
        bits = NO_64;
    }
    const uint32_t bytes = (bits + NO_7) / NO_8;
    bits = bytes * NO_8;
    std::vector<uint8_t> array(bytes + 1);
    array[bytes] = k;
    for (auto hash : hashVec) {
        uint32_t delta = (hash >> NO_17) | (hash << NO_15);
        for (uint32_t j = 0; j < k; j++) {
            uint32_t bitPosition = static_cast<uint32_t>((static_cast<uint64_t>(hash) % bits));
            uint32_t i = bitPosition / NO_8;
            array[i] = static_cast<uint8_t>(array[i] | (1 << (bitPosition % NO_8)));
            hash += delta;
        }
    }
    return array;
}

bool TestFile::KeyMatch(std::vector<uint8_t> array, uint32_t hash)
{
    uint32_t len = array.size();
    uint32_t bits = (len - 1) * NO_8;
    uint32_t k = array[len - 1];
    if (k > NO_30) {
        return true;
    }
    uint32_t delta = hash >> NO_17 | hash << NO_15;
    for (uint32_t i = 0; i < k; i++) {
        auto bitPosition = static_cast<uint32_t>((static_cast<uint64_t>(hash) % bits));
        uint8_t value = array[bitPosition / NO_8];
        if ((value & (1 << (bitPosition % NO_8))) == 0) {
            return false;
        }
        hash += delta;
    }
    return true;
}
