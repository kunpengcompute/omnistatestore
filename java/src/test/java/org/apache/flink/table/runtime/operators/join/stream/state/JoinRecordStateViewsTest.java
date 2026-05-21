/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join.stream.state;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.huawei.falcon.state.merge.MergeableState;
import java.lang.reflect.Constructor;
import java.util.*;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.UserFacingMapState;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.junit.Test;

public class JoinRecordStateViewsTest {

    private static final InternalTypeInfo<RowData> RECORD_TYPE = InternalTypeInfo.ofFields(
        new IntType(),
        new IntType()
    );
    private static final InternalTypeInfo<RowData> UNIQUE_KEY_TYPE = InternalTypeInfo.ofFields(new IntType());
    private static final KeySelector<RowData, RowData> UNIQUE_KEY_SELECTOR = record ->
        GenericRowData.of(record.getInt(0));

    @Test
    public void createJoinKeyContainsUniqueKeyStoresSingleRecord() throws Exception {
        ValueStateDescriptor<RowData>[] enabledDescriptor = valueDescriptorHolder();
        JoinRecordStateView enabledView = JoinRecordStateViews.create(
            valueStateContext(new TestingValueState<RowData>(), enabledDescriptor),
            "left-records",
            JoinInputSideSpec.withUniqueKeyContainedByJoinKey(UNIQUE_KEY_TYPE, UNIQUE_KEY_SELECTOR),
            RECORD_TYPE,
            1000L,
            false
        );

        assertDescriptorTtl(enabledDescriptor[0], true);
        assertTrue(toList(enabledView.getRecords()).isEmpty());

        RowData record = row(1, 10);
        enabledView.addRecord(record);
        assertEquals(Arrays.asList(record), toList(enabledView.getRecords()));

        enabledView.retractRecord(record);
        assertTrue(toList(enabledView.getRecords()).isEmpty());

        ValueStateDescriptor<RowData>[] disabledDescriptor = valueDescriptorHolder();
        JoinRecordStateViews.create(
            valueStateContext(new TestingValueState<RowData>(), disabledDescriptor),
            "left-records",
            JoinInputSideSpec.withUniqueKeyContainedByJoinKey(UNIQUE_KEY_TYPE, UNIQUE_KEY_SELECTOR),
            RECORD_TYPE,
            0L,
            false
        );

        assertDescriptorTtl(disabledDescriptor[0], false);
    }

    @Test
    public void createInputSideHasUniqueKeyUsesMapState() throws Exception {
        MapStateDescriptor<RowData, RowData>[] enabledDescriptor = mapDescriptorHolder();
        JoinRecordStateViews.create(
            mapStateContext(new TestingMapState<RowData, RowData>(), enabledDescriptor),
            "unique-records",
            JoinInputSideSpec.withUniqueKey(UNIQUE_KEY_TYPE, UNIQUE_KEY_SELECTOR),
            RECORD_TYPE,
            1000L,
            false
        );

        assertDescriptorTtl(enabledDescriptor[0], true);

        MapStateDescriptor<RowData, RowData>[] disabledDescriptor = mapDescriptorHolder();
        JoinRecordStateView disabledView = JoinRecordStateViews.create(
            mapStateContext(new TestingMapState<RowData, RowData>(), disabledDescriptor),
            "unique-records",
            JoinInputSideSpec.withUniqueKey(UNIQUE_KEY_TYPE, UNIQUE_KEY_SELECTOR),
            RECORD_TYPE,
            0L,
            false
        );

        assertDescriptorTtl(disabledDescriptor[0], false);

        RowData recordA = row(1, 10);
        RowData replacementA = row(1, 11);
        RowData recordB = row(2, 20);

        disabledView.addRecord(recordA);
        disabledView.addRecord(replacementA);
        disabledView.addRecord(recordB);
        assertEquals(Arrays.asList(replacementA, recordB), toList(disabledView.getRecords()));

        disabledView.retractRecord(replacementA);
        assertEquals(Arrays.asList(recordB), toList(disabledView.getRecords()));
    }

    @Test
    public void createInputSideWithoutUniqueKeyCountsDuplicates() throws Exception {
        MapStateDescriptor<RowData, Integer>[] enabledDescriptor = mapDescriptorHolder();
        JoinRecordStateView enabledView = JoinRecordStateViews.create(
            mapStateContext(new TestingMapState<RowData, Integer>(), enabledDescriptor),
            "plain-records",
            JoinInputSideSpec.withoutUniqueKey(),
            RECORD_TYPE,
            1000L,
            false
        );

        assertDescriptorTtl(enabledDescriptor[0], true);

        RowData recordA = row(1, 10);
        RowData recordB = row(2, 20);

        enabledView.addRecord(recordA);
        enabledView.addRecord(recordA);
        enabledView.addRecord(recordB);

        Iterable<RowData> iterable = enabledView.getRecords();
        Iterator<RowData> iterator = iterable.iterator();
        assertSame(iterator, iterable.iterator());
        assertTrue(iterator.hasNext());
        assertEquals(recordA, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(recordA, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(recordB, iterator.next());
        assertFalse(iterator.hasNext());

        enabledView.retractRecord(recordA);
        assertEquals(Arrays.asList(recordA, recordB), toList(enabledView.getRecords()));

        enabledView.retractRecord(recordA);
        enabledView.retractRecord(recordA);
        assertEquals(Arrays.asList(recordB), toList(enabledView.getRecords()));

        MapStateDescriptor<RowData, Integer>[] disabledDescriptor = mapDescriptorHolder();
        JoinRecordStateViews.create(
            mapStateContext(new TestingMapState<RowData, Integer>(), disabledDescriptor),
            "plain-records",
            JoinInputSideSpec.withoutUniqueKey(),
            RECORD_TYPE,
            0L,
            false
        );

        assertDescriptorTtl(disabledDescriptor[0], false);
    }

    @Test
    public void createInputSideWithoutUniqueKeyUsesMergeWhenAvailable() throws Exception {
        MergeableTestingMapState<RowData> originalState = new MergeableTestingMapState<>();
        MapState<RowData, Long> userFacingState = newUserFacingMapState(originalState);
        MapStateDescriptor<RowData, Long>[] enabledDescriptor = mapDescriptorHolder();
        JoinRecordStateView enabledView = JoinRecordStateViews.create(
            mapStateContext(userFacingState, enabledDescriptor),
            "merge-records",
            JoinInputSideSpec.withoutUniqueKey(),
            RECORD_TYPE,
            1000L,
            true
        );

        assertDescriptorTtl(enabledDescriptor[0], true);

        RowData recordA = row(1, 10);
        RowData recordB = row(2, 20);

        enabledView.addRecord(recordA);
        enabledView.addRecord(recordA);
        enabledView.addRecord(recordB);
        assertEquals(3, originalState.getMergeCount());

        Iterable<RowData> iterable = enabledView.getRecords();
        Iterator<RowData> iterator = iterable.iterator();
        assertSame(iterator, iterable.iterator());
        assertTrue(iterator.hasNext());
        assertEquals(recordA, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(recordA, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(recordB, iterator.next());
        assertFalse(iterator.hasNext());

        enabledView.retractRecord(recordA);
        assertEquals(Arrays.asList(recordA, recordB), toList(enabledView.getRecords()));

        enabledView.retractRecord(recordA);
        enabledView.retractRecord(recordA);
        assertEquals(Arrays.asList(recordB), toList(enabledView.getRecords()));
    }

    @Test
    public void createInputSideWithoutUniqueKeyFallsBackWithoutMergeableState() throws Exception {
        TestingMapState<RowData, Long> originalState = new TestingMapState<>();
        MapState<RowData, Long> userFacingState = newUserFacingMapState(originalState);
        MapStateDescriptor<RowData, Long>[] disabledDescriptor = mapDescriptorHolder();
        JoinRecordStateView disabledView = JoinRecordStateViews.create(
            mapStateContext(userFacingState, disabledDescriptor),
            "merge-records",
            JoinInputSideSpec.withoutUniqueKey(),
            RECORD_TYPE,
            0L,
            true
        );

        assertDescriptorTtl(disabledDescriptor[0], false);

        RowData recordA = row(1, 10);
        RowData recordB = row(2, 20);

        disabledView.addRecord(recordA);
        disabledView.addRecord(recordA);
        disabledView.addRecord(recordB);
        assertEquals(Arrays.asList(recordA, recordA, recordB), toList(disabledView.getRecords()));

        disabledView.retractRecord(recordA);
        assertEquals(Arrays.asList(recordA, recordB), toList(disabledView.getRecords()));

        disabledView.retractRecord(recordA);
        disabledView.retractRecord(recordA);
        assertEquals(Arrays.asList(recordB), toList(disabledView.getRecords()));
    }

    private static RowData row(int id, int value) {
        return GenericRowData.of(id, value);
    }

    private static List<RowData> toList(Iterable<RowData> records) {
        List<RowData> results = new ArrayList<>();
        for (RowData record : records) {
            results.add(record);
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    private static ValueStateDescriptor<RowData>[] valueDescriptorHolder() {
        return (ValueStateDescriptor<RowData>[]) new ValueStateDescriptor<?>[1];
    }

    @SuppressWarnings("unchecked")
    private static <V> MapStateDescriptor<RowData, V>[] mapDescriptorHolder() {
        return (MapStateDescriptor<RowData, V>[]) new MapStateDescriptor<?, ?>[1];
    }

    @SuppressWarnings("unchecked")
    private static RuntimeContext valueStateContext(
        ValueState<RowData> state,
        ValueStateDescriptor<RowData>[] descriptorHolder
    ) throws Exception {
        RuntimeContext context = mock(RuntimeContext.class);
        when(context.getState(any(ValueStateDescriptor.class))).thenAnswer(invocation -> {
            descriptorHolder[0] = (ValueStateDescriptor<RowData>) invocation.getArgument(0);
            return state;
        });
        return context;
    }

    @SuppressWarnings("unchecked")
    private static <V> RuntimeContext mapStateContext(
        MapState<RowData, V> state,
        MapStateDescriptor<RowData, V>[] descriptorHolder
    ) throws Exception {
        RuntimeContext context = mock(RuntimeContext.class);
        when(context.getMapState(any(MapStateDescriptor.class))).thenAnswer(invocation -> {
            descriptorHolder[0] = (MapStateDescriptor<RowData, V>) invocation.getArgument(0);
            return state;
        });
        return context;
    }

    @SuppressWarnings("unchecked")
    // NOTE: Reflection-based access to the private UserFacingMapState constructor is fragile.
    // If UserFacingMapState changes its constructor signature or visibility, this will break.
    private static <K, V> UserFacingMapState<K, V> newUserFacingMapState(MapState<K, V> originalState)
        throws Exception {
        Constructor<UserFacingMapState> constructor = UserFacingMapState.class.getDeclaredConstructor(MapState.class);
        constructor.setAccessible(true);
        return constructor.newInstance(originalState);
    }

    private static void assertDescriptorTtl(StateDescriptor<?, ?> descriptor, boolean enabled) {
        assertEquals(enabled, descriptor.getTtlConfig().isEnabled());
    }

    private static final class TestingValueState<T> implements ValueState<T> {

        private T value;

        @Override
        public T value() {
            return value;
        }

        @Override
        public void update(T value) {
            this.value = value;
        }

        @Override
        public void clear() {
            value = null;
        }
    }

    private static class TestingMapState<K, V> implements MapState<K, V> {

        private final LinkedHashMap<K, V> values = new LinkedHashMap<>();

        @Override
        public V get(K key) {
            return values.get(key);
        }

        @Override
        public void put(K key, V value) {
            values.put(key, value);
        }

        @Override
        public void putAll(Map<K, V> map) {
            values.putAll(map);
        }

        @Override
        public void remove(K key) {
            values.remove(key);
        }

        @Override
        public boolean contains(K key) {
            return values.containsKey(key);
        }

        @Override
        public Iterable<Map.Entry<K, V>> entries() {
            return values.entrySet();
        }

        @Override
        public Iterable<K> keys() {
            return values.keySet();
        }

        @Override
        public Iterable<V> values() {
            return values.values();
        }

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return values.entrySet().iterator();
        }

        @Override
        public boolean isEmpty() {
            return values.isEmpty();
        }

        @Override
        public void clear() {
            values.clear();
        }
    }

    private static final class MergeableTestingMapState<K>
        extends TestingMapState<K, Long>
        implements MergeableState<K, Long>
    {

        private int mergeCount;

        @Override
        public void merge(K key, Long operand) throws Exception {
            Long value = get(key);
            put(key, value == null ? operand : value + operand);
            mergeCount++;
        }

        private int getMergeCount() {
            return mergeCount;
        }
    }
}
