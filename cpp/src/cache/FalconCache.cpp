/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#include "FalconCache.h"
#include "FalconRocksDBHelper.h"

const int CACHE_SIZE_UPPER_LIMIT_RATIO = 2;
const int BYPASS_CHECK_PERIOD = 20000;

inline void copyDataTojVal(void* dst, const void* src, size_t len) {
    uint8_t* jArray = static_cast<uint8_t*>(dst);
    const uint8_t* sliceData = static_cast<const uint8_t*>(src);

    while (((uintptr_t)jArray & 15) && len > 0) {
        *jArray++ = *sliceData++;
        len--;
    }

    while (len >= 64) {
        uint8x16_t v0 = vld1q_u8(sliceData);
        uint8x16_t v1 = vld1q_u8(sliceData + 16);
        uint8x16_t v2 = vld1q_u8(sliceData + 32);
        uint8x16_t v3 = vld1q_u8(sliceData + 48);

        vst1q_u8(jArray, v0);
        vst1q_u8(jArray + 16, v1);
        vst1q_u8(jArray + 32, v2);
        vst1q_u8(jArray + 48, v3);

        sliceData += 64;
        jArray += 64;
        len -= 64;
    }

    while (len >= 16) {
        vst1q_u8(jArray, vld1q_u8(sliceData));
        sliceData += 16;
        jArray += 16;
        len -= 16;
    }

    memcpy(jArray, sliceData, len);
}

jbyteArray FalconCache::get(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                            ROCKSDB_NAMESPACE::Slice key_slice)
{
    accessCnt++;
    auto state_pos = cache.find(key_slice);
    if (state_pos != cache.end()) { // falcon cache hit, get value from cache
        hitCnt++;

        // value in cache will not be null all the time
        ROCKSDB_NAMESPACE::Slice val_slice = state_pos->second;

        //jbyteArray jVal = FalconUtil::JniUtil::copyBytes(env, value_slice.ToString());
        jsize len = static_cast<jsize>(val_slice.size());
        jbyteArray jVal = env->NewByteArray(len);
        if (jVal == nullptr) {
            return nullptr;
        }

        if (len >= 64) {
            jboolean is_copy;
            jbyte* array_elems = env->GetByteArrayElements(jVal, &is_copy);
            copyDataTojVal(array_elems, val_slice.data(), len);
            env->ReleaseByteArrayElements(jVal, array_elems, 0);
        } else {
            env->SetByteArrayRegion(jVal, 0, len, reinterpret_cast<const jbyte*>(val_slice.data()));
        }

        // if falcon cache hit, directly delete key_slice
        delete[] key_slice.data_;
        key_slice.data_ = nullptr;  // defensive programming

        return jVal;
    } else { // falcon cache miss, get from rocksdb and insert into falcon cache
        jbyteArray jVal = rocksdb_get(env, rocksdbHandle, cfHandle, key_slice);

        if (jVal == nullptr) {
            // falcon cache miss, but value in rocksdb is null, do not insert it into cache, and delete key_slice
            delete[] key_slice.data_;
            key_slice.data_ = nullptr;
        } else {
            // transfer jVal into Slice type, and insert into falcon cache
            jsize jValLen = env->GetArrayLength(jVal);
            jbyte* value = new jbyte[jValLen];
            env->GetByteArrayRegion(jVal, 0, jValLen, value);
            if (env->ExceptionCheck()) {  // exception thrown: ArrayIndexOutOfBoundsException
                delete[] value;
                delete[] key_slice.data_;
                value = nullptr;
                key_slice.data_ = nullptr;  // defensive programming
                return nullptr;
            }

            // insert key_slice and value_slice into cache
            ROCKSDB_NAMESPACE::Slice value_slice(reinterpret_cast<char*>(value), jValLen);
            cache.emplace(key_slice, value_slice);

            // key_slice and value_slice will be deleted in removeEldestState() when cache elimination fires
            if (cache.size() > cacheSizeLimit) {
                removeEldestState(env, rocksdbHandle, cfHandle, writeOptionsHandle);
            }
        }

        return jVal;
    }
}

void FalconCache::put(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                      ROCKSDB_NAMESPACE::Slice key_slice, ROCKSDB_NAMESPACE::Slice value_slice)
{
    accessCnt++;
    auto state_pos = cache.find(key_slice);
    if (state_pos != cache.end()) { // falcon cache hit, update key_slice and value_slice inside falcon cache
        hitCnt++;

        delete[] state_pos->first.data_;
        delete[] state_pos->second.data_;
        state_pos->second.data_ = nullptr;  // key.data_ can not be set to null
        cache.erase(state_pos);
        cache.emplace(key_slice, value_slice);
    } else { // falcon cache hit, insert key_slice and value_slice into cache
        cache.emplace(key_slice, value_slice);
        // key_slice and value_slice will be deleted in removeEldestState() when cache elimination fires
        if (cache.size() > cacheSizeLimit) {
            removeEldestState(env, rocksdbHandle, cfHandle, writeOptionsHandle);
        }
    }
}

void FalconCache::remove(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                         ROCKSDB_NAMESPACE::Slice key_slice)
{
    accessCnt++;
    auto state_pos = cache.find(key_slice);
    if (state_pos != cache.end()) { // falcon cache hit, remove key_slice and value_slice from cache
        hitCnt++;

        delete[] state_pos->first.data_;
        delete[] state_pos->second.data_;
        state_pos->second.data_ = nullptr;  // key.data_ can not be set to null
        cache.erase(state_pos);
    }
    // remove key_slice and value_slice from rocksdb
    rocksdb_delete(env, rocksdbHandle, cfHandle, writeOptionsHandle, key_slice);
    delete[] key_slice.data();
    key_slice.data_ = nullptr;
}

void FalconCache::flush(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle)
{
    for (auto &[key_slice, val_slice] : cache) {
        rocksdb_put(env, rocksdbHandle, cfHandle, writeOptionsHandle, key_slice, val_slice);
    }
}

void FalconCache::clearAll()
{
    for (auto &[key_slice, value_slice] : cache) {
        delete[] key_slice.data_;
        delete[] value_slice.data_;
        value_slice.data_ = nullptr;  // key.data_ can not be set to null
    }
    cache.clear();
    hitCnt = 0;
    accessCnt = 0;
}

void FalconCache::removeEldestState(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle)
{
    flush(env, rocksdbHandle, cfHandle, writeOptionsHandle);
    clearAll();
}

int FalconCache::getSizeLimit() const
{
    return cacheSizeLimit;
}

void FalconCache::updateSizeLimit(JNIEnv *env, jlong rocksdbHandle, jlong cfHandle, jlong writeOptionsHandle,
                                  jint newSizeLimit)
{
    cacheSizeLimit = newSizeLimit;
    if (cache.size() > cacheSizeLimit) {
        flush(env, rocksdbHandle, cfHandle, writeOptionsHandle);
        clearAll();
    }
}

bool FalconCache::bypassCache() const
{
    // perform hit ratio check every 2w times (avoid too much calculating), then decide whether to bypass cache
    if (accessCnt % BYPASS_CHECK_PERIOD == 0) {
        double hitRatio = static_cast<double>(hitCnt) / static_cast<double>(accessCnt);
        if (hitRatio < hitThreshold) {
            FALCON_LOG("cache hit ratio is less than bypass threshold, bypass cache. (" << hitCnt << "/" << accessCnt <<
                       " = " << hitRatio << " < " << hitThreshold << ")");
            return true;
        } else {
            return false;
        }
    }
    return false;
}