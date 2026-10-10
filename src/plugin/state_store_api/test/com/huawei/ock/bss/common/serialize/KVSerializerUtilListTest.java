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
import com.huawei.ock.bss.table.result.StateListResult;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.IntPrimitiveArraySerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemoryUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * List decoding tests using owned off-heap buffers without JNI.
 */
public class KVSerializerUtilListTest {
    private final List<Long> allocations = new ArrayList<>();

    @After
    public void tearDown() {
        for (long address : allocations) {
            MemoryUtils.UNSAFE.freeMemory(address);
        }
    }

    @Test
    public void testMultipleBuffersAndEncodedListsKeepOrder() throws IOException {
        DirectBuffer first = buffer(IntSerializer.INSTANCE, Arrays.asList(1, 2), Collections.emptyList(),
            Collections.singletonList(3));
        DirectBuffer empty = nativeBuffer(new DataOutputSerializer(8));
        DirectBuffer last = buffer(IntSerializer.INSTANCE, Arrays.asList(4, 5));

        List<? extends Integer> decoded = KVSerializerUtil.desList(result(first, empty, last), IntSerializer.INSTANCE,
            new DirectDataInputDeserializer());

        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5), decoded);
    }

    @Test
    public void testAppendToExistingListAcrossCalls() throws IOException {
        List<Integer> destination = new LinkedList<>(Collections.singletonList(9));
        DirectDataInputDeserializer input = new DirectDataInputDeserializer();
        KVSerializerUtil.desListInto(result(buffer(IntSerializer.INSTANCE, Arrays.asList(1, 2))),
            IntSerializer.INSTANCE, input, destination);
        KVSerializerUtil.desListInto(result(buffer(IntSerializer.INSTANCE, Collections.singletonList(3))),
            IntSerializer.INSTANCE, input, destination);

        Assert.assertEquals(Arrays.asList(9, 1, 2, 3), destination);
    }

    @Test
    public void testVariableLengthStringsNullAndDuplicates() throws IOException {
        char[] chars = new char[70000];
        Arrays.fill(chars, '\u754c');
        String longString = new String(chars);
        List<String> expected = Arrays.asList("same", null, "\u4e2d\u6587", longString, "same");

        Assert.assertEquals(expected, KVSerializerUtil.desList(result(buffer(StringSerializer.INSTANCE, expected)),
            StringSerializer.INSTANCE, new DirectDataInputDeserializer()));
    }

    @Test
    public void testMutableElementsAreIndependent() throws IOException {
        int[] value = {1, 2};
        List<? extends int[]> decoded = KVSerializerUtil.desList(
            result(buffer(IntPrimitiveArraySerializer.INSTANCE, Arrays.asList(value, value))),
            IntPrimitiveArraySerializer.INSTANCE, new DirectDataInputDeserializer());

        int[] first = decoded.get(0);
        int[] second = decoded.get(1);
        Assert.assertArrayEquals(value, first);
        Assert.assertArrayEquals(value, second);
        Assert.assertNotSame(first, second);
        first[0] = 99;
        Assert.assertEquals(1, second[0]);
    }

    @Test
    public void testWrapperReturnsMutableList() throws IOException {
        List<? extends Integer> decoded = KVSerializerUtil.desList(
            result(buffer(IntSerializer.INSTANCE, Arrays.asList(1, 2))), IntSerializer.INSTANCE,
            new DirectDataInputDeserializer());

        decoded.remove(0);
        decoded.clear();
        Assert.assertTrue(decoded.isEmpty());
    }

    @Test
    public void testEmptyResultDoesNotRequireMetadataArrays() throws IOException {
        Assert.assertTrue(KVSerializerUtil.desList(new StateListResult(), IntSerializer.INSTANCE,
            new DirectDataInputDeserializer()).isEmpty());
    }

    @Test
    public void testHugeCountWithMissingPayloadDoesNotPreallocate() throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(8);
        output.writeInt(Integer.MAX_VALUE);
        List<Integer> destination = new ArrayList<>();
        try {
            KVSerializerUtil.desListInto(result(nativeBuffer(output)), IntSerializer.INSTANCE,
                new DirectDataInputDeserializer(), destination);
            Assert.fail("Expected truncated payload to fail decoding");
        } catch (EOFException expected) {
            Assert.assertTrue(destination.isEmpty());
        }
    }

    @Test(expected = EOFException.class)
    public void testTruncatedCountFails() throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(8);
        output.writeByte(1);
        KVSerializerUtil.desList(result(nativeBuffer(output)), IntSerializer.INSTANCE,
            new DirectDataInputDeserializer());
    }

    @SafeVarargs
    private final <E> DirectBuffer buffer(TypeSerializer<E> serializer, List<E>... lists) throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(32);
        for (List<E> list : lists) {
            output.writeInt(list.size());
            for (E element : list) {
                serializer.serialize(element, output);
            }
        }
        return nativeBuffer(output);
    }

    private DirectBuffer nativeBuffer(DataOutputSerializer output) {
        byte[] bytes = output.getCopyOfBuffer();
        long address = MemoryUtils.UNSAFE.allocateMemory(Math.max(1, bytes.length));
        allocations.add(address);
        MemoryUtils.UNSAFE.copyMemory(bytes, MemoryUtils.UNSAFE.arrayBaseOffset(byte[].class), null, address,
            bytes.length);
        return new DirectBuffer(address, bytes.length);
    }

    private StateListResult result(DirectBuffer... buffers) {
        StateListResult result = new StateListResult(0, buffers.length);
        long[] addresses = new long[buffers.length];
        int[] lengths = new int[buffers.length];
        for (int i = 0; i < buffers.length; i++) {
            addresses[i] = buffers[i].data();
            lengths[i] = buffers[i].length();
        }
        result.setAddresses(addresses);
        result.setLengths(lengths);
        return result;
    }
}
