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

package com.huawei.ock.bss.common.serialize;

import com.huawei.ock.bss.common.memory.DirectDataInputDeserializer;
import com.huawei.ock.bss.common.memory.DirectDataOutputSerializer;
import com.huawei.ock.bss.table.description.KListTableDescription;
import com.huawei.ock.bss.table.result.StateListResult;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class FlinkTtlListCompactionFilterTest {
    private final TtlStateFactory.TtlSerializer<String> serializer =
        new TtlStateFactory.TtlSerializer<>(LongSerializer.INSTANCE, StringSerializer.INSTANCE);

    private FlinkTtlListCompactionFilter filter() {
        return FlinkTtlListCompactionFilter.create(new KListTableDescription<>("ttl", 128,
            new TableSerializer<>(IntSerializer.INSTANCE, serializer)), () -> 1100L);
    }

    @SafeVarargs
    private final byte[] chunk(TtlValue<String>... values) throws IOException {
        return serialize(Arrays.asList(values), serializer);
    }

    private <E> byte[] serialize(List<E> values, TypeSerializer<E> elementSerializer) throws IOException {
        ThreadLocalOutputViewPool.current(true);
        return KVSerializerUtil.getCopyOfBuffer(KVSerializerUtil.serList(values, elementSerializer));
    }

    private <E> List<? extends E> deserialize(byte[] bytes, TypeSerializer<E> elementSerializer) throws IOException {
        DirectDataOutputSerializer output = ThreadLocalOutputViewPool.current(true).getOutputView();
        output.write(bytes);
        StateListResult result = new StateListResult(0, 1);
        result.setAddresses(new long[] {output.data()});
        result.setLengths(new int[] {output.length()});
        return KVSerializerUtil.desList(result, elementSerializer, new DirectDataInputDeserializer());
    }

    @Test
    public void mergedChunksRetainOnlyLiveElementsWithOriginalTimestamps() throws Exception {
        byte[] older = chunk(new TtlValue<>("expired", 900), new TtlValue<>("live-long-value", 1050));
        byte[] newer = chunk(new TtlValue<>("also expired", 1000), new TtlValue<>("x", 1090));
        byte[] merged = Arrays.copyOf(older, older.length + newer.length);
        System.arraycopy(newer, 0, merged, older.length, newer.length);
        byte[] result = filter().filterList(merged, 100, 1100);
        assertArrayEquals(chunk(new TtlValue<>("live-long-value", 1050), new TtlValue<>("x", 1090)), result);
        List<? extends TtlValue<String>> values = deserialize(result, serializer);
        assertEquals(2, values.size());
        assertEquals("live-long-value", values.get(0).getUserValue());
        assertEquals(1050, values.get(0).getLastAccessTimestamp());
        assertEquals("x", values.get(1).getUserValue());
        assertEquals(1090, values.get(1).getLastAccessTimestamp());
        assertSame(result, filter().filterList(result, 100, 1100));
        assertNull(filter().filterList(result, 100, 1190));
    }

    @Test
    public void shortTtlElementsDoNotRequireLengthTrailer() throws Exception {
        TtlStateFactory.TtlSerializer<Byte> byteSerializer =
            new TtlStateFactory.TtlSerializer<>(LongSerializer.INSTANCE, ByteSerializer.INSTANCE);
        FlinkTtlListCompactionFilter callback = FlinkTtlListCompactionFilter.create(
            new KListTableDescription<>("ttl-byte", 128,
                new TableSerializer<>(IntSerializer.INSTANCE, byteSerializer)), () -> 1100L);
        byte[] bytes = serialize(Arrays.asList(new TtlValue<>((byte) 1, 900), new TtlValue<>((byte) 2, 1050)),
            byteSerializer);
        byte[] result = callback.filterList(bytes, 100, 1100);

        assertArrayEquals(serialize(Collections.singletonList(new TtlValue<>((byte) 2, 1050)), byteSerializer), result);
        assertEquals(Byte.valueOf((byte) 2), deserialize(result, byteSerializer).get(0).getUserValue());
        assertSame(result, callback.filterList(result, 100, 1100));
    }

    @Test
    public void restoredBytesExpireAtOriginalDeadline() throws Exception {
        byte[] savepointRestoredValue = chunk(new TtlValue<>("restored", 1000));
        assertSame(savepointRestoredValue, filter().filterList(savepointRestoredValue, 100, 1099));
        assertNull(filter().filterList(savepointRestoredValue, 100, 1100));
        assertNull(filter().filterList(savepointRestoredValue, 100, 100000));
        assertEquals(1100, filter().currentTimestamp());
    }

    @Test
    public void nonTtlSerializerIsNotInterpretedAsTimestamp() {
        assertNull(FlinkTtlListCompactionFilter.create(new KListTableDescription<>("plain", 128,
            new TableSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE)), () -> 1100L));
    }

    @Test(expected = IOException.class)
    public void truncatedElementFailsInsteadOfDroppingState() throws Exception {
        byte[] malformed = chunk(new TtlValue<>("expired", 0));
        filter().filterList(Arrays.copyOf(malformed, malformed.length - 1), 100, 1100);
    }

    @Test(expected = IOException.class)
    public void invalidElementCountFailsInsteadOfDroppingState() throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(Integer.BYTES);
        output.writeInt(Integer.MAX_VALUE);
        filter().filterList(output.getCopyOfBuffer(), 100, 1100);
    }

    @Test
    public void callbackRestoresCompactionThreadClassLoader() throws Exception {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            FlinkTtlListCompactionFilter callback = filter();
            ClassLoader workerLoader = new ClassLoader(previous) { };
            Thread.currentThread().setContextClassLoader(workerLoader);
            callback.filterList(chunk(new TtlValue<>("expired", 0)), 100, 1100);
            assertSame(workerLoader, Thread.currentThread().getContextClassLoader());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }
}
