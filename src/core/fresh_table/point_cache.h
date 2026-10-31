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

#ifndef BOOST_SS_POINT_CACHE_H
#define BOOST_SS_POINT_CACHE_H

#include <array>
#include <cstdint>

#include "include/binary_data.h"
#include "include/state_type.h"

namespace ock {
namespace bss {
struct PointCacheKey {
    uint16_t stateId;
    uint32_t hashCode;
    const uint8_t *priKey;
    uint32_t priKeyLen;
    const uint8_t *secKey;
    uint32_t secKeyLen;
};

// One cache per DB. Like table reads/writes, all index and queue operations run on the task thread.
// The synchronous Flink State adapters deserialize/copy and release each native result before returning to State code.
// Returned buffers keep their allocation alive, but their contents are not snapshots across cache operations.
class PointCache {
public:
    static constexpr uint32_t CACHE_BUDGET = 2 * 1024 * 1024;
    static constexpr uint32_t QUEUE_COUNT = 3;
    static constexpr uint32_t MAX_KV_SIZE = 140;

    PointCache() = default;
    PointCache(const PointCache &) = delete;
    PointCache &operator=(const PointCache &) = delete;

    // Allocation failure disables the cache; the DB can continue using its normal storage path.
    bool Initialize();
    bool Get(const PointCacheKey &key, BinaryData &value);
    // Write probes neither promote entries nor acquire returned-buffer references.
    bool Contains(const PointCacheKey &key);
    // Probe before writing: an existing entry or a free slot in its size queue can receive the successful write.
    bool PrepareWrite(const PointCacheKey &key, uint32_t valueLen);
    // Admit values read from storage, evicting the queue head when needed.
    void Put(const PointCacheKey &key, const BinaryData &value);
    // Complete a successful storage write. New entries use spare slots only; existing entries can be updated.
    void Write(const PointCacheKey &key, const BinaryData &value);
    void Erase(const PointCacheKey &key);
    void Clear();

    // Native TTL is applied below this cache. Such states must bypass it entirely.
    void SetStateEnabled(uint16_t stateId, bool enabled);
    inline bool IsStateEnabled(uint16_t stateId) const
    {
        uint32_t type = stateId >> 13;
        return mInitialized && (type == VALUE || type == MAP || type == SUB_VALUE || type == SUB_MAP) &&
               (mDisabledStates[stateId >> 6] & (1ULL << (stateId & 63))) == 0;
    }

    uint32_t GetSize(uint32_t queue) const;
    uint32_t GetCapacity(uint32_t queue) const;
    uint32_t GetAllocatedBytes() const;

private:
    static constexpr uint32_t PAGE_SIZE = 4096;
    static constexpr uint32_t PAGE_COUNT = 480;
    static constexpr uint32_t BUCKET_COUNT = 32768;
    static constexpr uint32_t SLOT_BITS = 7;
    static constexpr uint32_t SLOT_MASK = (1U << SLOT_BITS) - 1;

    struct Entry {
        uint32_t hashCode;
        uint16_t stateId;
        uint16_t prev;
        uint16_t next;
        uint16_t hashNext;
        uint8_t priKeyLen;
        uint8_t secKeyLen;
        uint8_t valueLen;
        uint8_t queue;
    };

    struct Page : public Buffer {
        uint8_t *Data() override
        {
            return data;
        }

        uint32_t Length() override
        {
            return sizeof(data);
        }

        uint16_t entrySize = 0;
        uint16_t queue = 0;
        uint16_t entryCount = 0;
        alignas(Entry) uint8_t data[PAGE_SIZE - sizeof(Buffer) - 8];
    };

    struct Queue {
        uint16_t head = 0;
        uint16_t tail = 0;
        uint16_t free = 0;
        uint16_t size = 0;
        uint16_t capacity = 0;
    };

    struct Miss {
        uint32_t hashCode = 0;
        uint16_t stateId = 0;
        uint8_t priKeyLen = 0;
        uint8_t secKeyLen = 0;
        uint8_t data[MAX_KV_SIZE]{};
        bool valid = false;
    };

    bool CanCache(const PointCacheKey &key) const;
    Entry *Find(const PointCacheKey &key, uint16_t &id);
    uint16_t *FindHashLink(uint16_t id);
    Entry &GetEntry(uint16_t id);
    void Unlink(Entry &entry);
    void PushBack(uint16_t id, Entry &entry);
    void Remove(uint16_t id, uint16_t *hashLink);
    void Remember(uint16_t id, Entry &entry);
    void Store(const PointCacheKey &key, const BinaryData &value, bool write);
    bool UpdateEntry(uint16_t id, Entry &entry, const BinaryData &value, uint32_t queue);
    void RememberMiss(const PointCacheKey &key);
    bool IsKnownMiss(const PointCacheKey &key) const;

    static uint32_t Bucket(uint32_t hashCode);
    static uint32_t QueueIndex(uint32_t size);
    static uint8_t *Payload(Entry &entry);
    static bool Matches(const PointCacheKey &key, Entry &entry);

    std::array<uint16_t, BUCKET_COUNT> mBuckets{};
    std::array<Ref<Page>, PAGE_COUNT> mPages{};
    std::array<Queue, QUEUE_COUNT> mQueues{};
    std::array<uint64_t, 1024> mDisabledStates{};
    Miss mLastMiss;
    Entry *mLastEntry = nullptr;
    uint16_t mLastId = 0;
    bool mInitialized = false;
};
}  // namespace bss
}  // namespace ock

#endif  // BOOST_SS_POINT_CACHE_H
