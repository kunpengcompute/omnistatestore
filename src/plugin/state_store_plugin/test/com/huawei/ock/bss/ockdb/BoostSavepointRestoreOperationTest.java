/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of the Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

package com.huawei.ock.bss.ockdb;

import com.huawei.ock.bss.EmbeddedOckStateBackend;
import com.huawei.ock.bss.OckDBKeyedStateBackend;
import com.huawei.ock.bss.common.BoostStateDB;
import com.huawei.ock.bss.resource.ResourceContainer;
import com.huawei.ock.bss.table.KVTableImpl;
import com.huawei.ock.bss.table.KListTableImpl;
import com.huawei.ock.bss.table.KMapTableImpl;
import com.huawei.ock.bss.table.KeyPair;
import com.huawei.ock.bss.table.api.Table;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.ListDelimitedSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.mockito.Mockito;
import java.util.Map;
import java.util.List;
import java.util.Arrays;
import com.huawei.ock.bss.restore.BoostRestoreOperation;
import com.huawei.ock.bss.restore.BoostSavepointRestoreOperation;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.SavepointKeyedStateHandle;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

/**
 * Savepoint restore tests.
 */
public class BoostSavepointRestoreOperationTest {
    @Test
    public void shouldUseTotalKeyGroupsPrefixForSavepointRestore() throws Exception {
        TestBuilder builder = new TestBuilder();

        BoostRestoreOperation<String> restoreOperation = builder.createSavepointRestoreOperation();

        Assert.assertTrue(restoreOperation instanceof BoostSavepointRestoreOperation);
        Assert.assertEquals(2,
            ((Integer) Whitebox.getInternalState(restoreOperation, "keyGroupPrefixBytes")).intValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void savepointValuePayloadPreservesOriginalTimestamp() throws Exception {
        BoostRestoreOperation<String> operation = new TestBuilder().createSavepointRestoreOperation();
        Map<String, Table> tables = Whitebox.getInternalState(operation, "tables");
        KVTableImpl<String, TtlValue<String>> table = Mockito.mock(KVTableImpl.class);
        tables.put("state", table);
        TtlStateFactory.TtlSerializer<String> serializer = ttlSerializer();
        TtlValue<String> value = new TtlValue<>("old", 123);
        DataOutputSerializer bytes = new DataOutputSerializer(32);
        serializer.serialize(value, bytes);
        DataInputDeserializer input = Whitebox.getInternalState(operation, "valueDeserializer");
        byte[] valueBytes = bytes.getCopyOfBuffer();
        input.setBuffer(valueBytes);
        Whitebox.invokeMethod(operation, "restoreKeyedAndNSKeyedState", "state", "key",
            VoidNamespace.INSTANCE, new byte[0], new byte[0], "key".hashCode(), 0, 17, serializer, valueBytes);
        Mockito.verify(table).restore(Mockito.eq("key"), Mockito.argThat(v -> sameTtlValue(value, v)),
            Mockito.eq(123L));
        Mockito.verifyNoMoreInteractions(table);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void savepointListPayloadRestoresOnceInOriginalOrder() throws Exception {
        BoostRestoreOperation<String> operation = new TestBuilder().createSavepointRestoreOperation();
        Map<String, Table> tables = Whitebox.getInternalState(operation, "tables");
        KListTableImpl<KeyPair<String, String>, TtlValue<String>> table = Mockito.mock(KListTableImpl.class);
        tables.put("state", table);
        TtlStateFactory.TtlSerializer<String> serializer = ttlSerializer();
        List<TtlValue<String>> values = Arrays.asList(new TtlValue<>("new", 456), new TtlValue<>("old", 123));
        byte[] bytes = new ListDelimitedSerializer().serializeList(values, serializer);
        Whitebox.invokeMethod(operation, "restoreListState", "state", "key", "namespace",
            new byte[0], new byte[0], "key".hashCode(), "namespace".hashCode(), 17, bytes,
            new ListSerializer<>(serializer));
        Mockito.verify(table).restore(Mockito.eq(new KeyPair<>("key", "namespace")),
            Mockito.argThat(v -> v.size() == 2 && sameTtlValue(values.get(0), v.get(0))
                && sameTtlValue(values.get(1), v.get(1))), Mockito.eq(456L));
        Mockito.verifyNoMoreInteractions(table);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void savepointMapPayloadPreservesEntryTimestamp() throws Exception {
        BoostRestoreOperation<String> operation = new TestBuilder().createSavepointRestoreOperation();
        Map<String, Table> tables = Whitebox.getInternalState(operation, "tables");
        KMapTableImpl<String, String, TtlValue<String>> table = Mockito.mock(KMapTableImpl.class);
        tables.put("state", table);
        TtlStateFactory.TtlSerializer<String> serializer = ttlSerializer();
        TtlValue<String> value = new TtlValue<>("old", 123);
        DataOutputSerializer bytes = new DataOutputSerializer(32);
        bytes.writeBoolean(false);
        serializer.serialize(value, bytes);
        DataInputDeserializer input = Whitebox.getInternalState(operation, "valueDeserializer");
        byte[] valueBytes = bytes.getCopyOfBuffer();
        input.setBuffer(valueBytes);
        bytes.clear();
        StringSerializer.INSTANCE.serialize("mapKey", bytes);
        byte[] canonicalKey = bytes.getCopyOfBuffer();
        DataInputDeserializer keyInput = Whitebox.getInternalState(operation, "keyDeserializer");
        keyInput.setBuffer(canonicalKey);
        Whitebox.invokeMethod(operation, "restoreMapState", "state", "key", VoidNamespace.INSTANCE,
            new byte[0], new byte[0], "key".hashCode(), 0, 17, canonicalKey, valueBytes,
            new MapSerializer<>(StringSerializer.INSTANCE, serializer));
        Mockito.verify(table).restore(Mockito.eq("key"), Mockito.eq("mapKey"),
            Mockito.argThat(v -> sameTtlValue(value, v)), Mockito.eq(123L));
        Mockito.verifyNoMoreInteractions(table);
    }

    private static boolean sameTtlValue(TtlValue<String> expected, TtlValue<String> actual) {
        return actual != null && expected.getLastAccessTimestamp() == actual.getLastAccessTimestamp()
            && expected.getUserValue().equals(actual.getUserValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void nonTtlSavepointUsesSerializedRestorePath() throws Exception {
        BoostRestoreOperation<String> operation = new TestBuilder().createSavepointRestoreOperation();
        Map<String, Table> tables = Whitebox.getInternalState(operation, "tables");
        KVTableImpl<String, String> table = Mockito.mock(KVTableImpl.class);
        tables.put("state", table);
        byte[] keyBytes = new byte[]{1, 2, 3};
        DataOutputSerializer bytes = new DataOutputSerializer(32);
        StringSerializer.INSTANCE.serialize("value", bytes);
        byte[] valueBytes = bytes.getCopyOfBuffer();
        Whitebox.invokeMethod(operation, "restoreKeyedAndNSKeyedState", "state", "key",
            VoidNamespace.INSTANCE, keyBytes, new byte[0], 101, 0, 202,
            StringSerializer.INSTANCE, valueBytes);
        Mockito.verify(table).restoreSerialized(Mockito.same(keyBytes), Mockito.eq(202), Mockito.eq(101),
            Mockito.same(valueBytes));
        Mockito.verifyNoMoreInteractions(table);
    }

    private static TtlStateFactory.TtlSerializer<String> ttlSerializer() {
        return new TtlStateFactory.TtlSerializer<>(LongSerializer.INSTANCE, StringSerializer.INSTANCE);
    }

    private static final class TestBuilder extends AbstractOckDBKeyedStateBackendBuilder<String> {
        private TestBuilder() {
            super(1024, new KeyGroupRange(40, 48), userCodeClassLoader(), new File("test"),
                PowerMockito.mock(LocalRecoveryConfig.class), PowerMockito.mock(TaskKvStateRegistry.class), "test",
                new ExecutionConfig(), StringSerializer.INSTANCE, TtlTimeProvider.DEFAULT, latencyTrackingConfig(),
                new CloseableRegistry(), PowerMockito.mock(StreamCompressionDecorator.class), savepointHandle(), false,
                PowerMockito.mock(ResourceContainer.class), EmbeddedOckStateBackend.PriorityQueueStateType.HEAP,
                new Configuration());
        }

        private static UserCodeClassLoader userCodeClassLoader() {
            UserCodeClassLoader userCodeClassLoader = PowerMockito.mock(UserCodeClassLoader.class);
            PowerMockito.when(userCodeClassLoader.asClassLoader())
                .thenReturn(BoostSavepointRestoreOperationTest.class.getClassLoader());
            return userCodeClassLoader;
        }

        private static LatencyTrackingStateConfig latencyTrackingConfig() {
            LatencyTrackingStateConfig latencyTrackingConfig = PowerMockito.mock(LatencyTrackingStateConfig.class);
            PowerMockito.when(latencyTrackingConfig.getMetricGroup()).thenReturn(PowerMockito.mock(MetricGroup.class));
            return latencyTrackingConfig;
        }

        private static Collection<KeyedStateHandle> savepointHandle() {
            return Collections.<KeyedStateHandle>singleton(PowerMockito.mock(SavepointKeyedStateHandle.class));
        }

        private BoostRestoreOperation<String> createSavepointRestoreOperation() throws Exception {
            return initRestoreOperation("test", PowerMockito.mock(BoostStateDB.class), 2, new HashMap<>(),
                new HashMap<>(), PowerMockito.mock(PriorityQueueSetFactory.class), new HashMap<>(), new HashMap<>(),
                new HashMap<>());
        }

        @Override
        public OckDBKeyedStateBackend<String> build() {
            return null;
        }
    }
}
