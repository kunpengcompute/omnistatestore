/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <set>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "common/util/key_group_util.h"
#include "gtest/gtest.h"

namespace ock {
namespace bss {

class KeyGroupUtilTest : public testing::Test {
};

static uint32_t EncodeKV(uint32_t rawHash, uint32_t maxParallelism)
{
    if (maxParallelism == 0) {
        return rawHash;
    }
    uint32_t keyGroup = rawHash % maxParallelism;
    uint32_t orderHash = rawHash;
    KeyGroupUtil::SetKeyGroup(orderHash, keyGroup);
    return orderHash;
}

TEST_F(KeyGroupUtilTest, KvEncodingRoundTripsRepresentativeParallelism)
{
    const std::vector<uint32_t> maxParallelisms = { 1, 2, 3, 127, 128, 129, 30000, 32767, 32768 };
    const std::vector<uint32_t> hashes = { 0, 1, 2, 127, 128, 129, 0x12345678U, static_cast<uint32_t>(INT32_MAX) };
    for (uint32_t maxParallelism : maxParallelisms) {
        ASSERT_EQ(KeyGroupUtil::Init(maxParallelism), BSS_OK);
        for (uint32_t rawHash : hashes) {
            uint32_t orderHash = EncodeKV(rawHash, maxParallelism);
            ASSERT_LE(orderHash, static_cast<uint32_t>(INT32_MAX));
            ASSERT_EQ(KeyGroupUtil::ComputeKeyGroupForKeyHash(orderHash), rawHash % maxParallelism);
        }
    }
}

TEST_F(KeyGroupUtilTest, KvOrderIsLexicographicByGroupThenRawHash)
{
    for (uint32_t maxParallelism : { 3U, 128U, 129U, 32768U }) {
        ASSERT_EQ(KeyGroupUtil::Init(maxParallelism), BSS_OK);
        std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> encoded;
        for (uint32_t rawHash = 0; rawHash < 200000; rawHash += 37) {
            encoded.emplace_back(EncodeKV(rawHash, maxParallelism), rawHash % maxParallelism, rawHash);
        }
        std::sort(encoded.begin(), encoded.end());
        for (size_t i = 1; i < encoded.size(); ++i) {
            auto previous = std::make_pair(std::get<1>(encoded[i - 1]), std::get<2>(encoded[i - 1]));
            auto current = std::make_pair(std::get<1>(encoded[i]), std::get<2>(encoded[i]));
            ASSERT_TRUE(previous <= current);
        }
    }
}

TEST_F(KeyGroupUtilTest, PowerOfTwoLayoutMatchesExpectedBits)
{
    uint32_t raw128 = (0x123456U << 7U) | 0x55U;
    ASSERT_EQ(KeyGroupUtil::Init(128), BSS_OK);
    ASSERT_EQ(EncodeKV(raw128, 128), (0x55U << 24U) | 0x123456U);

    uint32_t raw32768 = (0xABCDU << 15U) | 0x2345U;
    ASSERT_EQ(KeyGroupUtil::Init(32768), BSS_OK);
    ASSERT_EQ(EncodeKV(raw32768, 32768), (0x2345U << 16U) | 0xABCDU);
}

TEST_F(KeyGroupUtilTest, ConcurrentInitWithSameParallelismIsStable)
{
    constexpr uint32_t threadCount = 16;
    constexpr uint32_t maxParallelism = 32768;
    ASSERT_EQ(KeyGroupUtil::Init(128), BSS_OK);

    std::atomic<uint32_t> readyCount{ 0 };
    std::atomic<bool> start{ false };
    std::vector<BResult> results(threadCount, BSS_ERR);
    std::vector<std::thread> threads;
    threads.reserve(threadCount);
    for (uint32_t i = 0; i < threadCount; ++i) {
        threads.emplace_back([i, &readyCount, &start, &results]() {
            readyCount.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            results[i] = KeyGroupUtil::Init(maxParallelism);
        });
    }
    while (readyCount.load(std::memory_order_acquire) != threadCount) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto &thread : threads) {
        thread.join();
    }

    for (BResult result : results) {
        EXPECT_EQ(result, BSS_OK);
    }
    uint32_t rawHash = (0xABCDU << 15U) | 0x2345U;
    EXPECT_EQ(EncodeKV(rawHash, maxParallelism), (0x2345U << 16U) | 0xABCDU);
}

TEST_F(KeyGroupUtilTest, FixedGroupUsesAllAvailableFreshBuckets)
{
    constexpr uint32_t maxParallelism = 32768;
    constexpr uint32_t keyGroup = 7;
    ASSERT_EQ(KeyGroupUtil::Init(maxParallelism), BSS_OK);
    for (uint32_t bucketCount : { 8U, 64U, 1024U, 32768U, 65536U }) {
        std::set<uint32_t> buckets;
        for (uint32_t quotient = 0; quotient < 65536; ++quotient) {
            uint32_t rawHash = quotient * maxParallelism + keyGroup;
            buckets.insert(EncodeKV(rawHash, maxParallelism) & (bucketCount - 1));
        }
        ASSERT_EQ(buckets.size(), bucketCount);
    }
}

TEST_F(KeyGroupUtilTest, PqEncodingRemainsByteCompatible)
{
    uint32_t oneByteHash = 0x12345678U;
    ASSERT_EQ(KeyGroupUtil::Init(128), BSS_OK);
    KeyGroupUtil::SetPQKeyGroup(oneByteHash, 0x5AU);
    ASSERT_EQ(oneByteHash, 0x5A345678U);
    ASSERT_EQ(KeyGroupUtil::ComputePQKeyGroupForKeyHash(oneByteHash), 0x5AU);

    uint32_t twoByteHash = 0x12345678U;
    ASSERT_EQ(KeyGroupUtil::Init(32768), BSS_OK);
    KeyGroupUtil::SetPQKeyGroup(twoByteHash, 0x2345U);
    ASSERT_EQ(twoByteHash, 0x23455678U);
    ASSERT_EQ(KeyGroupUtil::ComputePQKeyGroupForKeyHash(twoByteHash), 0x2345U);
}

TEST_F(KeyGroupUtilTest, InitRejectsInvalidParallelismWithoutChangingValidState)
{
    ASSERT_EQ(KeyGroupUtil::Init(128), BSS_OK);

    uint32_t rawHash = 0x12345678U;
    uint32_t keyGroup = rawHash % 128;
    uint32_t orderHash = rawHash;
    KeyGroupUtil::SetKeyGroup(orderHash, keyGroup);
    ASSERT_EQ(KeyGroupUtil::ComputeKeyGroupForKeyHash(orderHash), keyGroup);

    EXPECT_EQ(KeyGroupUtil::Init(0), BSS_INVALID_PARAM);
    EXPECT_EQ(KeyGroupUtil::Init(32769), BSS_INVALID_PARAM);

    orderHash = rawHash;
    KeyGroupUtil::SetKeyGroup(orderHash, keyGroup);
    EXPECT_EQ(KeyGroupUtil::ComputeKeyGroupForKeyHash(orderHash), keyGroup);
}

}  // namespace bss
}  // namespace ock
