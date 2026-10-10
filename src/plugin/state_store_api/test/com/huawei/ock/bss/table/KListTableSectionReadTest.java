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

package com.huawei.ock.bss.table;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;

import com.huawei.ock.bss.common.BoostStateDB;
import com.huawei.ock.bss.common.conf.BoostConfig;
import com.huawei.ock.bss.common.memory.DirectBuffer;
import com.huawei.ock.bss.common.serialize.KeyPairSerializer;
import com.huawei.ock.bss.common.serialize.TableSerializer;
import com.huawei.ock.bss.table.description.KListTableDescription;
import com.huawei.ock.bss.table.description.NsKListTableDescription;
import com.huawei.ock.bss.table.namespace.NsKListTableImpl;
import com.huawei.ock.bss.table.result.StateListResult;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemoryUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Decode real buffers while simulating JNI batch replacement and buffer invalidation.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AbstractTable.class, KListTableImpl.class})
public class KListTableSectionReadTest {
    private final List<Long> allocations = new ArrayList<>();

    private BoostStateDB db;

    private KListTableImpl<String, String> table;

    @Before
    public void setUp() {
        PowerMockito.mockStatic(KListTableImpl.class);
        PowerMockito.mockStatic(AbstractTable.class);
        PowerMockito.when(AbstractTable.open(anyLong(), Mockito.nullable(String.class), any())).thenReturn(1L);
        db = Mockito.mock(BoostStateDB.class);
        Mockito.when(db.getConfig()).thenReturn(Mockito.mock(BoostConfig.class));
        Mockito.when(db.getNativeHandle()).thenReturn(1L);
        TableSerializer<String, String> serializer = new TableSerializer<>(StringSerializer.INSTANCE,
            StringSerializer.INSTANCE);
        table = new KListTableImpl<>(db, new KListTableDescription<>("section-test", 128, serializer));
    }

    @After
    public void tearDown() {
        for (long address : allocations) {
            MemoryUtils.UNSAFE.freeMemory(address);
        }
    }

    @Test
    public void testSingleBatchKeepsBufferAndSubListOrder() throws Exception {
        mockReadBatches(batch(buffer(Arrays.asList("a", "b"), Collections.singletonList("c")),
            buffer(Arrays.asList("d", "e"))));

        assertMutableResult(Arrays.asList("a", "b", "c", "d", "e"), table.get("key"));
    }

    @Test
    public void testMultipleBatchesReverseBatchOrderOnly() throws Exception {
        mockReadBatches(batch(buffer(Collections.singletonList("e"), Collections.singletonList("f")),
                buffer(Arrays.asList("g", "h"))),
            batch(buffer(Arrays.asList("c", "d"))),
            batch(buffer(Collections.singletonList("a")), buffer(Collections.singletonList("b"))));

        assertMutableResult(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h"), table.get("key"));
    }

    @Test
    public void testEmptyFirstMiddleAndLastBatches() throws Exception {
        mockReadBatches(batch(buffer(Collections.emptyList())), batch(buffer(Arrays.asList("c", "d"))), batch(),
            batch(buffer(Arrays.asList("a", "b"))), batch(buffer(Collections.emptyList())));

        assertMutableResult(Arrays.asList("a", "b", "c", "d"), table.get("key"));
    }

    @Test
    public void testOnlyOneNonemptyBatchWithEmptySections() throws Exception {
        mockReadBatches(batch(buffer(Arrays.asList("a", "b", "c"))), batch(),
            batch(buffer(Collections.emptyList())), batch());

        assertMutableResult(Arrays.asList("a", "b", "c"), table.get("key"));
    }

    @Test
    public void testAllEncodedBatchesEmpty() throws Exception {
        mockReadBatches(batch(buffer(Collections.emptyList())), batch(), batch(buffer(Collections.emptyList())));

        assertMutableResult(Collections.emptyList(), table.get("key"));
    }

    @Test
    public void testDuplicatesAndNullKeepTheirPositions() throws Exception {
        mockReadBatches(batch(buffer(Arrays.asList("tail", "same", null))),
            batch(buffer(Arrays.asList("same", "middle"))), batch(buffer(Arrays.asList("head", "same"))));

        assertMutableResult(Arrays.asList("head", "same", "same", "middle", "tail", "same", null), table.get("key"));
    }

    @Test
    public void testManyUnevenBatchesGrowResultWithoutChangingOrder() throws Exception {
        StateListResult[] batches = new StateListResult[40];
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < batches.length; i++) {
            List<String> values = new ArrayList<>();
            for (int j = 0; j < i % 11; j++) {
                values.add(i + ":" + j);
            }
            batches[i] = batch(buffer(values));
            expected.addAll(0, values);
        }
        mockReadBatches(batches);

        assertMutableResult(expected, table.get("key"));
    }

    @Test
    public void testResultsFromDifferentGetsAreIndependent() throws Exception {
        mockReadBatches(batch(buffer(Arrays.asList("a", "b"))));
        List<String> first = table.get("key");
        mockReadBatches(batch(buffer(Collections.singletonList("d"))), batch(buffer(Collections.singletonList("c"))));
        List<String> second = table.get("key");

        Assert.assertNotSame(first, second);
        Assert.assertEquals(Arrays.asList("a", "b"), first);
        Assert.assertEquals(Arrays.asList("c", "d"), second);
        first.clear();
        Assert.assertEquals(Arrays.asList("c", "d"), second);
    }

    @Test
    public void testNamespaceTableUsesSameSectionAssembly() throws Exception {
        KeyPairSerializer<String, String> keySerializer = new KeyPairSerializer<>(StringSerializer.INSTANCE,
            StringSerializer.INSTANCE);
        NsKListTableImpl<String, String, String> namespaceTable = new NsKListTableImpl<>(db,
            new NsKListTableDescription<>("namespace-section-test", 128,
                new TableSerializer<>(keySerializer, StringSerializer.INSTANCE)));
        mockReadBatches(batch(buffer(Arrays.asList("c", "d"))), batch(buffer(Arrays.asList("a", "b"))));

        assertMutableResult(Arrays.asList("a", "b", "c", "d"),
            namespaceTable.get(namespaceTable.getKeyPair("key", "namespace")));
    }

    private void assertMutableResult(List<String> expected, List<String> actual) {
        Assert.assertEquals(expected, actual);
        actual.add("added");
        Assert.assertTrue(actual.remove("added"));
        Assert.assertEquals(expected, actual);
        if (!actual.isEmpty()) {
            actual.remove(0);
            Assert.assertEquals(expected.subList(1, expected.size()), actual);
        }
    }

    private void mockReadBatches(StateListResult... batches) throws Exception {
        AtomicInteger index = new AtomicInteger();
        StateListResult[] carrier = new StateListResult[1];
        PowerMockito.doAnswer(invocation -> {
            carrier[0] = invocation.getArgument(4);
            copyBatch(carrier[0], batches[0], batches.length > 1);
            return null;
        }).when(KListTableImpl.class);
        KListTableImpl.get(anyLong(), anyInt(), anyLong(), anyInt(), any(StateListResult.class));
        PowerMockito.doAnswer(invocation -> {
            StateListResult current = invocation.getArgument(1);
            Assert.assertSame(carrier[0], current);
            // A later JNI call invalidates all addresses returned by the preceding call.
            for (int i = 0; i < current.getSize(); i++) {
                MemoryUtils.UNSAFE.setMemory(current.getAddresses()[i], current.getLengths()[i], (byte) 0x7f);
            }
            int next = index.incrementAndGet();
            Assert.assertTrue("Unexpected extra section read", next < batches.length);
            copyBatch(current, batches[next], next + 1 < batches.length);
            return current;
        }).when(KListTableImpl.class, "doNativeSectionRead", anyLong(), any(StateListResult.class));
    }

    private void copyBatch(StateListResult target, StateListResult source, boolean more) {
        target.setResId(0);
        target.setSize(source.getSize());
        target.setAddresses(source.getAddresses());
        target.setLengths(source.getLengths());
        Whitebox.setInternalState(target, "readSectionId", more ? 1 : 0);
    }

    @SafeVarargs
    private final DirectBuffer buffer(List<String>... lists) throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(32);
        for (List<String> list : lists) {
            output.writeInt(list.size());
            for (String element : list) {
                StringSerializer.INSTANCE.serialize(element, output);
            }
        }
        byte[] bytes = output.getCopyOfBuffer();
        long address = MemoryUtils.UNSAFE.allocateMemory(Math.max(1, bytes.length));
        allocations.add(address);
        MemoryUtils.UNSAFE.copyMemory(bytes, MemoryUtils.UNSAFE.arrayBaseOffset(byte[].class), null, address,
            bytes.length);
        return new DirectBuffer(address, bytes.length);
    }

    private StateListResult batch(DirectBuffer... buffers) {
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
