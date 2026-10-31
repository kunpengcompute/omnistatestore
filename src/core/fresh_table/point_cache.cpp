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

#include "point_cache.h"

#include <cstring>
#include <new>
#include <utility>

namespace ock {
namespace bss {
constexpr uint32_t PointCache::CACHE_BUDGET;
constexpr uint32_t PointCache::QUEUE_COUNT;
constexpr uint32_t PointCache::MAX_KV_SIZE;

namespace {
constexpr uint32_t QUEUE_PAGES[] = { 336, 96, 48 };
constexpr uint32_t QUEUE_KV_BYTES[] = { 35, 70, 140 };

inline bool EqualBytes(const uint8_t *first, const uint8_t *second, uint32_t length)
{
    if (length >= sizeof(uint64_t) && length <= 2 * sizeof(uint64_t)) {
        uint64_t firstHead, secondHead, firstTail, secondTail;
        memcpy(&firstHead, first, sizeof(firstHead));
        memcpy(&secondHead, second, sizeof(secondHead));
        memcpy(&firstTail, first + length - sizeof(firstTail), sizeof(firstTail));
        memcpy(&secondTail, second + length - sizeof(secondTail), sizeof(secondTail));
        return firstHead == secondHead && firstTail == secondTail;
    }
    if (length == 1) {
        return *first == *second;
    }
    return length == 0 || memcmp(first, second, length) == 0;
}

inline void CopyBytes(uint8_t *destination, const uint8_t *source, uint32_t length)
{
    if (length == sizeof(uint64_t)) {
        uint64_t number;
        memcpy(&number, source, sizeof(number));
        memcpy(destination, &number, sizeof(number));
    } else if (length > sizeof(uint64_t) && length <= 2 * sizeof(uint64_t)) {
        uint64_t head, tail;
        memcpy(&head, source, sizeof(head));
        memcpy(&tail, source + length - sizeof(tail), sizeof(tail));
        memcpy(destination, &head, sizeof(head));
        memcpy(destination + length - sizeof(tail), &tail, sizeof(tail));
    } else if (length == 1) {
        *destination = *source;
    } else if (length != 0) {
        memmove(destination, source, length);
    }
}
}  // namespace

bool PointCache::Initialize()
{
    static_assert(sizeof(Entry) == 16, "Point cache entry metadata must stay compact");
    static_assert(sizeof(Page) == PAGE_SIZE, "Point cache pages must include their buffer metadata");
    static_assert(sizeof(PointCache) + PAGE_COUNT * (PAGE_SIZE + 32) <= CACHE_BUDGET,
                  "Include index, queues, page references and allocation headroom in the 2 MiB budget");
    static_assert(PAGE_COUNT < (1U << (16 - SLOT_BITS)), "Page handles must fit in 16 bits");
    static_assert(QUEUE_PAGES[0] + QUEUE_PAGES[1] + QUEUE_PAGES[2] == PAGE_COUNT, "All pages need a size queue");
    static_assert((PAGE_SIZE - sizeof(Buffer) - 8) / ((sizeof(Entry) + QUEUE_KV_BYTES[0] + 3) & ~3U) <= SLOT_MASK,
                  "Slot handles must fit in the low seven bits");
    if (mInitialized) {
        return true;
    }
    uint32_t pageIndex = 0;
    for (uint32_t queue = 0; queue < QUEUE_COUNT; ++queue) {
        uint32_t entrySize = (sizeof(Entry) + QUEUE_KV_BYTES[queue] + alignof(Entry) - 1) & ~(alignof(Entry) - 1);
        for (uint32_t i = 0; i < QUEUE_PAGES[queue]; ++i) {
            Ref<Page> page = new (std::nothrow) Page();
            if (page == nullptr) {
                for (auto &allocated : mPages) {
                    allocated = nullptr;
                }
                return false;
            }
            page->entrySize = entrySize;
            page->queue = queue;
            page->entryCount = sizeof(page->data) / entrySize;
            mPages[pageIndex++] = std::move(page);
        }
    }
    mInitialized = true;
    Clear();
    return true;
}

uint32_t PointCache::Bucket(uint32_t hashCode)
{
    return (hashCode ^ (hashCode >> 16)) & (BUCKET_COUNT - 1);
}

uint32_t PointCache::QueueIndex(uint32_t size)
{
    return size <= QUEUE_KV_BYTES[0] ? 0 : (size <= QUEUE_KV_BYTES[1] ? 1 : 2);
}

uint8_t *PointCache::Payload(Entry &entry)
{
    return reinterpret_cast<uint8_t *>(&entry) + sizeof(Entry);
}

PointCache::Entry &PointCache::GetEntry(uint16_t id)
{
    auto &page = *mPages[id >> SLOT_BITS];
    return *reinterpret_cast<Entry *>(page.data + ((id & SLOT_MASK) - 1) * page.entrySize);
}

bool PointCache::CanCache(const PointCacheKey &key) const
{
    return IsStateEnabled(key.stateId) && key.priKeyLen <= MAX_KV_SIZE &&
           key.secKeyLen <= MAX_KV_SIZE - key.priKeyLen && (key.priKeyLen == 0 || key.priKey != nullptr) &&
           (key.secKeyLen == 0 || key.secKey != nullptr);
}

bool PointCache::Matches(const PointCacheKey &key, Entry &entry)
{
    return entry.hashCode == key.hashCode && entry.stateId == key.stateId && entry.priKeyLen == key.priKeyLen &&
           entry.secKeyLen == key.secKeyLen && EqualBytes(Payload(entry), key.priKey, key.priKeyLen) &&
           EqualBytes(Payload(entry) + key.priKeyLen, key.secKey, key.secKeyLen);
}

PointCache::Entry *PointCache::Find(const PointCacheKey &key, uint16_t &id)
{
    id = mBuckets[Bucket(key.hashCode)];
    while (id != 0) {
        auto &entry = GetEntry(id);
        if (Matches(key, entry)) {
            return &entry;
        }
        id = entry.hashNext;
    }
    return nullptr;
}

void PointCache::Remember(uint16_t id, Entry &entry)
{
    mLastId = id;
    mLastEntry = &entry;
}

void PointCache::RememberMiss(const PointCacheKey &key)
{
    mLastMiss.hashCode = key.hashCode;
    mLastMiss.stateId = key.stateId;
    mLastMiss.priKeyLen = key.priKeyLen;
    mLastMiss.secKeyLen = key.secKeyLen;
    CopyBytes(mLastMiss.data, key.priKey, key.priKeyLen);
    CopyBytes(mLastMiss.data + key.priKeyLen, key.secKey, key.secKeyLen);
    mLastMiss.valid = true;
}

bool PointCache::IsKnownMiss(const PointCacheKey &key) const
{
    return mLastMiss.valid && mLastMiss.hashCode == key.hashCode && mLastMiss.stateId == key.stateId &&
           mLastMiss.priKeyLen == key.priKeyLen && mLastMiss.secKeyLen == key.secKeyLen &&
           EqualBytes(mLastMiss.data, key.priKey, key.priKeyLen) &&
           EqualBytes(mLastMiss.data + key.priKeyLen, key.secKey, key.secKeyLen);
}

bool PointCache::Contains(const PointCacheKey &key)
{
    mLastMiss.valid = false;
    if (!CanCache(key)) {
        return false;
    }
    uint16_t id = mLastId;
    Entry *entry = mLastEntry;
    if (entry == nullptr || !Matches(key, *entry)) {
        entry = Find(key, id);
    }
    if (entry == nullptr) {
        return false;
    }
    Remember(id, *entry);
    return true;
}

bool PointCache::PrepareWrite(const PointCacheKey &key, uint32_t valueLen)
{
    if (Contains(key)) {
        return true;
    }
    if (!CanCache(key) || valueLen > MAX_KV_SIZE - key.priKeyLen - key.secKeyLen) {
        return false;
    }
    auto &queue = mQueues[QueueIndex(key.priKeyLen + key.secKeyLen + valueLen)];
    if (queue.size == queue.capacity) {
        return false;
    }
    RememberMiss(key);
    return true;
}

bool PointCache::UpdateEntry(uint16_t id, Entry &entry, const BinaryData &value, uint32_t queue)
{
    if (entry.queue != queue) {
        return false;
    }
    CopyBytes(Payload(entry) + entry.priKeyLen + entry.secKeyLen, value.Data(), value.Length());
    entry.valueLen = value.Length();
    if (mQueues[queue].tail != id) {
        Unlink(entry);
        PushBack(id, entry);
    }
    Remember(id, entry);
    return true;
}

uint16_t *PointCache::FindHashLink(uint16_t id)
{
    uint16_t *link = &mBuckets[Bucket(GetEntry(id).hashCode)];
    while (*link != id) {
        link = &GetEntry(*link).hashNext;
    }
    return link;
}

void PointCache::Unlink(Entry &entry)
{
    auto &queue = mQueues[entry.queue];
    if (entry.prev == 0) {
        queue.head = entry.next;
    } else {
        GetEntry(entry.prev).next = entry.next;
    }
    if (entry.next == 0) {
        queue.tail = entry.prev;
    } else {
        GetEntry(entry.next).prev = entry.prev;
    }
}

void PointCache::PushBack(uint16_t id, Entry &entry)
{
    auto &queue = mQueues[entry.queue];
    entry.prev = queue.tail;
    entry.next = 0;
    if (queue.tail == 0) {
        queue.head = id;
    } else {
        GetEntry(queue.tail).next = id;
    }
    queue.tail = id;
}

void PointCache::Remove(uint16_t id, uint16_t *hashLink)
{
    if (id == mLastId) {
        mLastEntry = nullptr;
    }
    auto &entry = GetEntry(id);
    *hashLink = entry.hashNext;
    Unlink(entry);
    auto &queue = mQueues[entry.queue];
    entry.next = queue.free;
    queue.free = id;
    --queue.size;
}

bool PointCache::Get(const PointCacheKey &key, BinaryData &value)
{
    if (!CanCache(key)) {
        return false;
    }
    uint16_t id;
    auto *found = Find(key, id);
    mLastEntry = found;
    if (found == nullptr) {
        RememberMiss(key);
        return false;
    }
    mLastMiss.valid = false;
    auto &entry = *found;
    if (mQueues[entry.queue].tail != id) {
        Unlink(entry);
        PushBack(id, entry);
    }
    Remember(id, entry);
    value.Init(Payload(entry) + entry.priKeyLen + entry.secKeyLen, entry.valueLen, mPages[id >> SLOT_BITS].Get());
    return true;
}

void PointCache::Put(const PointCacheKey &key, const BinaryData &value)
{
    Store(key, value, false);
}

void PointCache::Write(const PointCacheKey &key, const BinaryData &value)
{
    Store(key, value, true);
}

void PointCache::Store(const PointCacheKey &key, const BinaryData &value, bool write)
{
    if (!CanCache(key)) {
        return;
    }
    uint32_t keySize = key.priKeyLen + key.secKeyLen;
    bool knownMiss = IsKnownMiss(key);
    mLastMiss.valid = false;
    uint16_t id = mLastId;
    auto *found = knownMiss ? nullptr : mLastEntry;
    if (!knownMiss && (found == nullptr || !Matches(key, *found))) {
        found = Find(key, id);
    }
    if (value.Length() > MAX_KV_SIZE - keySize || (value.Length() != 0 && value.Data() == nullptr)) {
        if (found != nullptr) {
            Remove(id, FindHashLink(id));
        }
        return;
    }
    uint32_t queueIndex = QueueIndex(keySize + value.Length());
    auto &queue = mQueues[queueIndex];
    if (write && found == nullptr && queue.size == queue.capacity) {
        return;
    }
    if (found != nullptr) {
        auto &entry = *found;
        if (UpdateEntry(id, entry, value, queueIndex)) {
            return;
        }
        Remove(id, FindHashLink(id));
    }
    if (queue.free == 0) {
        // Reuse exactly the FIFO head. No search or global ranking on the admission path.
        Remove(queue.head, FindHashLink(queue.head));
    }
    id = queue.free;
    auto &entry = GetEntry(id);
    queue.free = entry.next;
    entry.hashCode = key.hashCode;
    entry.stateId = key.stateId;
    entry.priKeyLen = key.priKeyLen;
    entry.secKeyLen = key.secKeyLen;
    entry.valueLen = value.Length();
    entry.queue = queueIndex;
    CopyBytes(Payload(entry), key.priKey, key.priKeyLen);
    CopyBytes(Payload(entry) + key.priKeyLen, key.secKey, key.secKeyLen);
    CopyBytes(Payload(entry) + keySize, value.Data(), value.Length());
    uint32_t bucket = Bucket(key.hashCode);
    entry.hashNext = mBuckets[bucket];
    mBuckets[bucket] = id;
    PushBack(id, entry);
    ++queue.size;
    Remember(id, entry);
}

void PointCache::Erase(const PointCacheKey &key)
{
    mLastMiss.valid = false;
    if (!CanCache(key)) {
        return;
    }
    uint16_t id;
    if (Find(key, id) != nullptr) {
        Remove(id, FindHashLink(id));
    }
}

void PointCache::Clear()
{
    mLastEntry = nullptr;
    mLastId = 0;
    mLastMiss.valid = false;
    mBuckets.fill(0);
    mQueues.fill(Queue{});
    if (!mInitialized) {
        return;
    }
    for (uint32_t pageIndex = PAGE_COUNT; pageIndex-- > 0;) {
        auto &page = *mPages[pageIndex];
        auto &queue = mQueues[page.queue];
        uint32_t slots = page.entryCount;
        for (uint32_t slot = slots; slot-- > 0;) {
            uint16_t id = (pageIndex << SLOT_BITS) | (slot + 1);
            auto *entry = new (page.data + slot * page.entrySize) Entry{};
            entry->next = queue.free;
            entry->queue = page.queue;
            queue.free = id;
            ++queue.capacity;
        }
    }
}

void PointCache::SetStateEnabled(uint16_t stateId, bool enabled)
{
    uint64_t &word = mDisabledStates[stateId >> 6];
    uint64_t mask = 1ULL << (stateId & 63);
    bool wasEnabled = (word & mask) == 0;
    if (enabled != wasEnabled) {
        word = enabled ? word & ~mask : word | mask;
        Clear();
    }
}

uint32_t PointCache::GetSize(uint32_t queue) const
{
    return queue < QUEUE_COUNT ? mQueues[queue].size : 0;
}

uint32_t PointCache::GetCapacity(uint32_t queue) const
{
    return queue < QUEUE_COUNT ? mQueues[queue].capacity : 0;
}

uint32_t PointCache::GetAllocatedBytes() const
{
    return sizeof(*this) + (mInitialized ? PAGE_COUNT * sizeof(Page) : 0);
}
}  // namespace bss
}  // namespace ock
