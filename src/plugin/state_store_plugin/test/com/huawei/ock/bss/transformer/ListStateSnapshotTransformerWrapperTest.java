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

package com.huawei.ock.bss.transformer;

import com.huawei.ock.bss.common.serialize.KVSerializerUtil;
import com.huawei.ock.bss.common.serialize.ThreadLocalOutputViewPool;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

/**
 * ListStateSnapshotTransformerWrapperTest
 *
 * @since BeiMing 25.0.T1
 */
public class ListStateSnapshotTransformerWrapperTest {
    private ListStateSnapshotTransformerWrapper<String> transformerWrapper;

    private final TypeSerializer<String> elementSerializer = new StringSerializer();

    @Before
    public void setUp() {
        ThreadLocalOutputViewPool.current(true);
        transformerWrapper = new ListStateSnapshotTransformerWrapper<>(new TestSnapshotTransformer(),
            elementSerializer);
    }

    private static class TestSnapshotTransformer implements StateSnapshotTransformer<String> {
        @Nullable
        @Override
        public String filterOrTransform(@Nullable String s) {
            return s + "-t";
        }
    }

    @After
    public void tearDown() {
    }

    @Test
    public void test_filterOrTransform() throws IOException {
        List<String> testList = new ArrayList<>();
        testList.add("str1");
        testList.add("str2");
        testList.add("str3");
        List<String> destList = new ArrayList<>();
        destList.add("str1-t");
        destList.add("str2-t");
        destList.add("str3-t");
        byte[] serializedData = KVSerializerUtil.getCopyOfBuffer(KVSerializerUtil.serList(testList, elementSerializer));
        byte[] transformedData = transformerWrapper.filterOrTransform(serializedData);
        Assert.assertNotNull(transformedData);
        List<String> transformedList = toList(transformedData, elementSerializer, new DataInputDeserializer());
        Assert.assertEquals(destList, transformedList);
        Assert.assertArrayEquals(KVSerializerUtil.getCopyOfBuffer(KVSerializerUtil.serList(destList, elementSerializer)),
            transformedData);
    }

    @Test
    public void test_filterOrTransform_multi_list() throws IOException {
        List<String> testList1 = new ArrayList<>();
        testList1.add("str1");
        List<String> testList2 = new ArrayList<>();
        testList2.add("str2");
        testList2.add("str3");
        List<String> destList = new ArrayList<>();
        destList.add("str1-t");
        destList.add("str2-t");
        destList.add("str3-t");
        byte[] serializedData1 = KVSerializerUtil.getCopyOfBuffer(
            KVSerializerUtil.serList(testList1, elementSerializer));
        byte[] serializedData2 = KVSerializerUtil.getCopyOfBuffer(
            KVSerializerUtil.serList(testList2, elementSerializer));
        byte[] serializedDatas = new byte[serializedData1.length + serializedData2.length];
        System.arraycopy(serializedData1, 0, serializedDatas, 0, serializedData1.length);
        System.arraycopy(serializedData2, 0, serializedDatas, serializedData1.length, serializedData2.length);
        byte[] transformedData = transformerWrapper.filterOrTransform(serializedDatas);
        Assert.assertNotNull(transformedData);
        List<String> transformedList = toList(transformedData, elementSerializer, new DataInputDeserializer());
        Assert.assertEquals(destList, transformedList);
    }

    @Test
    public void testNullValue() {
        Assert.assertNull(transformerWrapper.filterOrTransform(null));
    }

    @Test
    public void testEmptyList() throws IOException {
        Assert.assertNull(transformerWrapper.filterOrTransform(serializeLists(Collections.emptyList())));
    }

    @Test
    public void testFilterAndTransformAcrossLists() throws IOException {
        ListStateSnapshotTransformerWrapper<String> wrapper = new ListStateSnapshotTransformerWrapper<>(
            value -> value.startsWith("drop") ? null : value + "-t", elementSerializer);
        byte[] transformed = wrapper.filterOrTransform(serializeLists(
            Arrays.asList("drop-first", "keep"), Collections.emptyList(), Arrays.asList("drop-last", "long-keep")));
        List<String> expected = Arrays.asList("keep-t", "long-keep-t");

        Assert.assertEquals(expected, toList(transformed, elementSerializer, new DataInputDeserializer()));
        Assert.assertArrayEquals(serializeLists(expected), transformed);
    }

    @Test
    public void testStopAtFirstElement() throws IOException {
        assertStopOnFirstIncluded(Arrays.asList("keep-first", "drop-tail", "drop-later"), 1,
            Arrays.asList("keep-first", "drop-tail"), Collections.singletonList("drop-later"));
    }

    @Test
    public void testStopAfterPrefixInFirstList() throws IOException {
        assertStopOnFirstIncluded(Arrays.asList("keep", "drop-tail", "keep-later"), 2,
            Arrays.asList("drop-prefix", "keep", "drop-tail"), Collections.singletonList("keep-later"));
    }

    @Test
    public void testStopAtStartOfLaterList() throws IOException {
        assertStopOnFirstIncluded(Arrays.asList("keep", "drop-tail", "drop-later"), 2,
            Collections.singletonList("drop-prefix"), Collections.emptyList(),
            Arrays.asList("keep", "drop-tail"), Collections.singletonList("drop-later"));
    }

    @Test
    public void testStopAfterPrefixInLaterList() throws IOException {
        assertStopOnFirstIncluded(Arrays.asList("keep", "drop-tail", "drop-later"), 3,
            Collections.singletonList("drop-first"), Collections.emptyList(),
            Arrays.asList("drop-prefix", "keep", "drop-tail"), Collections.singletonList("drop-later"));
    }

    @Test
    public void testStopWithAllElementsExcluded() throws IOException {
        assertStopOnFirstIncluded(null, 2, Collections.singletonList("drop-first"), Collections.emptyList(),
            Collections.singletonList("drop-last"));
    }

    @SafeVarargs
    private final void assertStopOnFirstIncluded(List<String> expected, int expectedCalls, List<String>... lists)
        throws IOException {
        StopOnFirstIncludedTransformer transformer = new StopOnFirstIncludedTransformer();
        ListStateSnapshotTransformerWrapper<String> wrapper = new ListStateSnapshotTransformerWrapper<>(
            transformer, elementSerializer);
        byte[] transformed = wrapper.filterOrTransform(serializeLists(lists));

        Assert.assertEquals(expectedCalls, transformer.calls);
        if (expected == null) {
            Assert.assertNull(transformed);
        } else {
            Assert.assertNotNull(transformed);
            Assert.assertEquals(expected, toList(transformed, elementSerializer, new DataInputDeserializer()));
        }
    }

    @SafeVarargs
    private final byte[] serializeLists(List<String>... lists) throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(16);
        for (List<String> list : lists) {
            output.write(KVSerializerUtil.getCopyOfBuffer(KVSerializerUtil.serList(list, elementSerializer)));
        }
        return output.getCopyOfBuffer();
    }

    private static class StopOnFirstIncludedTransformer
        implements StateSnapshotTransformer.CollectionStateSnapshotTransformer<String> {
        private int calls;

        @Override
        public TransformStrategy getFilterStrategy() {
            return TransformStrategy.STOP_ON_FIRST_INCLUDED;
        }

        @Override
        public String filterOrTransform(String value) {
            calls++;
            return value.startsWith("keep") ? value : null;
        }
    }

    private static <E> List<E> toList(byte[] data, TypeSerializer<E> elementSerializer,
        DataInputDeserializer inputView) throws IOException {
        inputView.setBuffer(data);
        List<E> list = new ArrayList<>();

        while (inputView.available() != 0) {
            list.addAll(toSingleList(elementSerializer, inputView));
        }

        return list;
    }

    private static <E> List<E> toSingleList(TypeSerializer<E> elementSerializer,
        DataInputDeserializer inputView) throws IOException {
        int size = inputView.readInt();
        List<E> list = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            list.add(elementSerializer.deserialize(inputView));
        }

        return list;
    }
}
