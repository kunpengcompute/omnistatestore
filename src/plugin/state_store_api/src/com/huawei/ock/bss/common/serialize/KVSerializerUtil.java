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

package com.huawei.ock.bss.common.serialize;

import com.huawei.ock.bss.common.memory.DirectBuffer;
import com.huawei.ock.bss.common.memory.DirectDataInputDeserializer;
import com.huawei.ock.bss.common.memory.DirectDataOutputSerializer;
import com.huawei.ock.bss.table.result.EntryResult;
import com.huawei.ock.bss.table.result.StateListResult;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.MemoryUtils;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 序列化工具类
 *
 * @since BeiMing 25.0.T1
 */
public class KVSerializerUtil {
    private static final int LIST_BATCH_NUM = 100;

    @SuppressWarnings("restriction")
    private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

    @SuppressWarnings("restriction")
    private static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    /**
     * serKey
     *
     * @param key           key
     * @param keySerializer keySerializer
     * @return ByteBuffer
     * @throws IOException IOException
     */
    public static <K> DirectDataOutputSerializer serKey(K key, TypeSerializer<K> keySerializer) throws IOException {
        DirectDataOutputSerializer outputView = ThreadLocalOutputViewPool.current(true).getOutputView();
        keySerializer.serialize(key, outputView);
        return outputView;
    }

    /**
     * desKey
     *
     * @param entryResult   entryResult
     * @param keySerializer keySerializer
     * @param inputView     inputView
     * @return K
     * @throws IOException IOException
     */
    public static <K> K desKey(EntryResult entryResult, TypeSerializer<K> keySerializer,
        DirectDataInputDeserializer inputView) throws IOException {
        inputView.setBuffer(entryResult.getKeyAddr(), entryResult.getKeyLen());
        return keySerializer.deserialize(inputView);
    }

    /**
     * desKey
     *
     * @param keyAddr       keyAddr
     * @param keyLen        keyLen
     * @param keySerializer keySerializer
     * @param inputView     inputView
     * @return K
     * @throws IOException IOException
     */
    public static <K> K desKey(long keyAddr, int keyLen, TypeSerializer<K> keySerializer,
        DirectDataInputDeserializer inputView) throws IOException {
        inputView.setBuffer(keyAddr, keyLen);
        return keySerializer.deserialize(inputView);
    }

    /**
     * serSubKey
     *
     * @param key              key
     * @param subKeySerializer subKeySerializer
     * @return ByteBuffer
     * @throws IOException IOException
     */
    public static <K> DirectDataOutputSerializer serSubKey(K key, TypeSerializer<K> subKeySerializer)
        throws IOException {
        DirectDataOutputSerializer outputView = ThreadLocalOutputViewPool.current(false).getOutputView();
        subKeySerializer.serialize(key, outputView);
        return outputView;
    }

    /**
     * serValue
     *
     * @param value           value
     * @param valueSerializer valueSerializer
     * @return ByteBuffer
     * @throws IOException IOException
     */
    public static <V> DirectDataOutputSerializer serValue(V value, TypeSerializer<V> valueSerializer)
        throws IOException {
        DirectDataOutputSerializer outputView = ThreadLocalOutputViewPool.current(false).getOutputView();
        valueSerializer.serialize(value, outputView);
        return outputView;
    }

    /**
     * desValue
     *
     * @param buffer          buffer
     * @param valueSerializer valueSerializer
     * @param inputView       inputView
     * @return V
     * @throws IOException IOException
     */
    public static <V> V desValue(DirectBuffer buffer, TypeSerializer<V> valueSerializer,
        DirectDataInputDeserializer inputView) throws IOException {
        inputView.setBuffer(buffer);
        return valueSerializer.deserialize(inputView);
    }

    /**
     * desEntry
     *
     * @param entryResult     entryResult
     * @param keySerializer   keySerializer
     * @param valueSerializer valueSerializer
     * @param inputView       inputView
     * @return java.util.Map.Entry<K, V>
     * @throws IOException IOException
     */
    public static <K, V> Map.Entry<K, V> desEntry(EntryResult entryResult, TypeSerializer<K> keySerializer,
        TypeSerializer<V> valueSerializer, DirectDataInputDeserializer inputView) throws IOException {
        inputView.setBuffer(entryResult.getKeyAddr(), entryResult.getKeyLen());
        K key = keySerializer.deserialize(inputView);
        inputView.setBuffer(entryResult.getValueAddr(), entryResult.getValueLen());
        V value = valueSerializer.deserialize(inputView);
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    /**
     * desTuple3
     *
     * @param entryResult      entryResult
     * @param keySerializer    keySerializer
     * @param subKeySerializer subKeySerializer
     * @param valueSerializer  valueSerializer
     * @param inputView        inputView
     * @return Tuple3<K, MK, V>
     * @throws IOException IOException
     */
    public static <K, MK, V> Tuple3<K, MK, V> desTuple3(EntryResult entryResult, TypeSerializer<K> keySerializer,
        TypeSerializer<MK> subKeySerializer, TypeSerializer<V> valueSerializer, DirectDataInputDeserializer inputView)
        throws IOException {
        inputView.setBuffer(entryResult.getKeyAddr(), entryResult.getKeyLen());
        K key = keySerializer.deserialize(inputView);
        inputView.setBuffer(entryResult.getValueAddr(), entryResult.getValueLen());
        V value = valueSerializer.deserialize(inputView);
        inputView.setBuffer(entryResult.getSubKeyAddr(), entryResult.getSubKeyLen());
        MK subKey = subKeySerializer.deserialize(inputView);
        return new Tuple3<>(key, subKey, value);
    }

    /**
     * serList(序列化list，格式为元素个数和依次序列化的元素)
     *
     * @param elements          elements
     * @param elementSerializer elementSerializer
     * @return DirectBuffer
     * @throws IOException IOException
     */
    public static <E> DirectBuffer serList(List<? extends E> elements, TypeSerializer<E> elementSerializer)
        throws IOException {
        DirectDataOutputSerializer outputView = ThreadLocalOutputViewPool.current(false).getOutputView();
        outputView.writeInt(elements.size());

        for (E element : elements) {
            elementSerializer.serialize(element, outputView);
        }

        return outputView.wrapDirectData();
    }

    /**
     * serListByBatch
     *
     * @param elements          elements
     * @param elementSerializer elementSerializer
     * @return java.util.List<com.huawei.ock.bss.common.memory.DirectBuffer>
     * @throws IOException IOException
     */
    public static <E> List<DirectBuffer> serListByBatch(List<? extends E> elements, TypeSerializer<E> elementSerializer)
        throws IOException {
        List<DirectBuffer> buffers = new ArrayList<>();
        int batchCount = elements.size() / LIST_BATCH_NUM + 1;
        for (int i = 0; i < batchCount; i++) {
            int fromIndex = i * LIST_BATCH_NUM;
            int toIndex = Math.min((i + 1) * LIST_BATCH_NUM, elements.size());
            List<? extends E> subList = elements.subList(fromIndex, toIndex);
            DirectBuffer buffer = serList(subList, elementSerializer);
            buffers.add(buffer);
        }
        return buffers;
    }

    /**
     * desList(反序列化list数据，data可能是由多个cpp层的list数据拼接到一起)
     *
     * @param listCount         listCount
     * @param elementSerializer elementSerializer
     * @param inputView         inputView
     * @return java.util.List<? extends E>
     * @throws IOException IOException
     */
    public static <E> List<? extends E> desList(StateListResult listCount,
        TypeSerializer<E> elementSerializer, DirectDataInputDeserializer inputView) throws IOException {
        List<E> list = new ArrayList<>();
        desListInto(listCount, elementSerializer, inputView, list);
        return list;
    }

    /**
     * Append decoded elements to an existing list in buffer order.
     *
     * @param listCount         buffers to decode
     * @param elementSerializer element serializer
     * @param inputView         input view
     * @param destination       list receiving the decoded elements
     * @param <E>               element type
     * @throws IOException if an element cannot be decoded
     */
    public static <E> void desListInto(StateListResult listCount, TypeSerializer<E> elementSerializer,
        DirectDataInputDeserializer inputView, List<E> destination) throws IOException {
        int size = listCount.getSize();
        long[] addresses = listCount.getAddresses();
        int[] lengths = listCount.getLengths();
        for (int i = 0; i < size; i++) {
            inputView.setBuffer(addresses[i], lengths[i]);
            while (inputView.available() > 0) {
                int subListSize = inputView.readInt();
                for (int j = 0; j < subListSize; j++) {
                    destination.add(elementSerializer.deserialize(inputView));
                }
            }
        }
    }

    /**
     * 获取ByteBuffer中当前有效数据范围的拷贝
     *
     * @param buffer buffer
     * @return byte[]
     */
    public static byte[] getCopyOfBuffer(DirectBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        byte[] copy = new byte[buffer.length()];
        UNSAFE.copyMemory(null, buffer.data(), copy, BYTE_ARRAY_BASE_OFFSET, buffer.length());
        return copy;
    }

    /**
     * 获取ByteBuffer中当前有效数据范围的拷贝
     *
     * @param data   buffer
     * @param length length
     * @return byte[]
     */
    public static byte[] getCopyOfBuffer(long data, int length) {
        if (length == 0) {
            return null;
        }
        byte[] copy = new byte[length];
        UNSAFE.copyMemory(null, data, copy, BYTE_ARRAY_BASE_OFFSET, length);
        return copy;
    }

    /**
     * 将byte数组中的每个byte转换为无符号数
     *
     * @param bytes bytes
     * @return int[]
     */
    public static int[] toUnsignedArray(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        int[] intArr = new int[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            intArr[i] = Byte.toUnsignedInt(bytes[i]);
        }

        return intArr;
    }
}
