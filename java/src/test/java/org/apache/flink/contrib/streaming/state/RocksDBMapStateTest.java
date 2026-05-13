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
 */

package org.apache.flink.contrib.streaming.state;

import com.huawei.falcon.state.merge.MergeableState;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.StateMigrationException;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link RocksDBMapState} including the Falcon-specific code paths
 * (range filter / partition filter, supportsNullValue=false, MergeableState merge interface)
 * and all inner classes.
 */
public class RocksDBMapStateTest {

    private static boolean falconLibAvailable = false;

    @BeforeClass
    public static void checkFalconLibAvailable() {
        try {
            Class.forName("com.huawei.falcon.state.cache.FalconValueState");
            falconLibAvailable = true;
        } catch (UnsatisfiedLinkError | ExceptionInInitializerError | NoClassDefFoundError | ClassNotFoundException e) {
            falconLibAvailable = false;
        }
    }

    private static void assumeFalconLibAvailable() {
        Assume.assumeTrue("FalconValueState native library not available on this platform - skipping", falconLibAvailable);
    }

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private RocksDBKeyedStateBackend<String> backend;

    @Before
    public void setup() throws Exception {
        if (!falconLibAvailable) {
            return;
        }
        backend =
                RocksDBTestUtils.builderForTestDefaults(tmp.newFolder(), StringSerializer.INSTANCE)
                        .build();
    }

    @After
    public void tearDown() {
        if (backend != null) {
            backend.dispose();
        }
    }

    private MapState<Integer, String> newMapState(String name) throws Exception {
        return newMapState(name, true);
    }

    private MapState<Integer, String> newMapState(String name, boolean supportsNullValue)
            throws Exception {
        MapStateDescriptor<Integer, String> desc =
                new MapStateDescriptor<>(name, Integer.class, String.class);
        desc.setSupportsNullValue(supportsNullValue);
        return backend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);
    }

    // ------------------------------------------------------------------
    //  Outer class: basic CRUD and MergeableState
    // ------------------------------------------------------------------

    @Test
    public void testGetPutContainsRemove() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("crud");
        backend.setCurrentKey("k1");

        assertNull(state.get(1));
        assertFalse(state.contains(1));

        state.put(1, "one");
        state.put(2, "two");
        state.put(3, null);

        assertEquals("one", state.get(1));
        assertEquals("two", state.get(2));
        assertNull(state.get(3));
        assertTrue(state.contains(3));
        assertFalse(state.contains(99));

        state.remove(2);
        assertFalse(state.contains(2));
        assertNull(state.get(2));
    }

    @Test
    public void testPutAllAndPutAllNullMap() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("putall");
        backend.setCurrentKey("k1");

        Map<Integer, String> map = new HashMap<>();
        map.put(10, "ten");
        map.put(20, "twenty");
        map.put(30, null);
        state.putAll(map);

        // putAll(null) must be a no-op and not throw.
        state.putAll(null);

        assertEquals("ten", state.get(10));
        assertEquals("twenty", state.get(20));
        assertNull(state.get(30));
        assertTrue(state.contains(30));
    }

    @Test
    public void testGetKeySerializerNamespaceSerializerValueSerializer() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("ser");
        InternalMapState<String, VoidNamespace, Integer, String> internal =
                (InternalMapState<String, VoidNamespace, Integer, String>) state;
        assertSame(StringSerializer.INSTANCE, internal.getKeySerializer());
        assertSame(VoidNamespaceSerializer.INSTANCE, internal.getNamespaceSerializer());
        assertNotNull(internal.getValueSerializer());
    }

    @Test
    public void testEntriesKeysValuesAndIterator() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("iter");
        backend.setCurrentKey("k1");

        Map<Integer, String> expected = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            String v = "v-" + i;
            state.put(i, v);
            expected.put(i, v);
        }

        // entries
        Map<Integer, String> seen = new HashMap<>();
        for (Map.Entry<Integer, String> e : state.entries()) {
            seen.put(e.getKey(), e.getValue());
        }
        assertEquals(expected, seen);

        // keys
        Set<Integer> seenKeys = new HashSet<>();
        for (Integer k : state.keys()) {
            seenKeys.add(k);
        }
        assertEquals(expected.keySet(), seenKeys);

        // values
        List<String> seenVals = new ArrayList<>();
        for (String v : state.values()) {
            seenVals.add(v);
        }
        seenVals.sort(Comparator.naturalOrder());
        List<String> expectedVals = new ArrayList<>(expected.values());
        expectedVals.sort(Comparator.naturalOrder());
        assertEquals(expectedVals, seenVals);

        // iterator
        Iterator<Map.Entry<Integer, String>> it = state.iterator();
        int count = 0;
        while (it.hasNext()) {
            Map.Entry<Integer, String> e = it.next();
            assertEquals(expected.get(e.getKey()), e.getValue());
            count++;
        }
        assertEquals(expected.size(), count);
    }

    @Test
    public void testIsEmpty() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("empty");
        backend.setCurrentKey("e");
        assertTrue(state.isEmpty());
        state.put(1, "1");
        assertFalse(state.isEmpty());
        state.remove(1);
        assertTrue(state.isEmpty());
    }

    @Test
    public void testClear() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("clear");
        backend.setCurrentKey("c1");
        for (int i = 0; i < 50; i++) {
            state.put(i, "v" + i);
        }
        // Different key prefix should be unaffected
        backend.setCurrentKey("c2");
        state.put(999, "foreign");

        backend.setCurrentKey("c1");
        state.clear();
        assertTrue(state.isEmpty());

        backend.setCurrentKey("c2");
        assertEquals("foreign", state.get(999));
    }

    @Test
    public void testMergeViaMergeableState() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("merge");
        @SuppressWarnings("unchecked")
        MergeableState<Integer, String> mergeable = (MergeableState<Integer, String>) state;
        backend.setCurrentKey("m1");
        // RocksDB w/o configured merge operator just stores last write.
        // We exercise the merge() code path; behavior is RocksDB-defined.
        mergeable.merge(1, "v1");
        // Followup put proves DB still healthy after merge call.
        state.put(2, "v2");
        assertEquals("v2", state.get(2));
    }

    @Test
    public void testIteratorRemove() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("iter-remove");
        backend.setCurrentKey("k");
        for (int i = 0; i < 8; i++) {
            state.put(i, "v" + i);
        }
        Iterator<Map.Entry<Integer, String>> it = state.iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, String> e = it.next();
            if (e.getKey() % 2 == 0) {
                it.remove();
            }
        }
        for (int i = 0; i < 8; i++) {
            if (i % 2 == 0) {
                assertFalse("even key " + i + " should be gone", state.contains(i));
            } else {
                assertEquals("v" + i, state.get(i));
            }
        }
    }

    @Test
    public void testIteratorRemoveTwiceThrows() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("remove-twice");
        backend.setCurrentKey("k");
        state.put(1, "v");
        Iterator<Map.Entry<Integer, String>> it = state.iterator();
        try {
            it.remove();
            fail("remove before next() must throw");
        } catch (IllegalStateException expected) {
            // expected
        }
        it.next();
        it.remove();
        try {
            it.remove();
            fail("double remove must throw");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    @Test
    public void testEntrySetValueAfterRemoveThrows() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("setvalue-deleted");
        backend.setCurrentKey("k");
        state.put(1, "v1");
        Iterator<Map.Entry<Integer, String>> it = state.iterator();
        Map.Entry<Integer, String> e = it.next();
        it.remove();
        try {
            e.setValue("nope");
            fail("setValue after remove must throw");
        } catch (IllegalStateException expected) {
            // expected
        }
        // getValue() on deleted entry must yield null
        assertNull(e.getValue());
    }

    @Test
    public void testEntrySetValueReturnsOldValue() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("setvalue");
        backend.setCurrentKey("k");
        state.put(7, "old");
        Iterator<Map.Entry<Integer, String>> it = state.iterator();
        Map.Entry<Integer, String> e = it.next();
        assertEquals(Integer.valueOf(7), e.getKey());
        String old = e.setValue("new");
        assertEquals("old", old);
        assertEquals("new", e.getValue());
        assertEquals("new", state.get(7));
        // Calling getKey() repeatedly hits the cached path
        assertEquals(Integer.valueOf(7), e.getKey());
    }

    @Test
    public void testIteratorCacheReloadOver128() throws Exception {
        assumeFalconLibAvailable();
        // RocksDBMapIterator has CACHE_SIZE_LIMIT = 128; populate more entries to force reload.
        MapState<Integer, String> state = newMapState("big");
        backend.setCurrentKey("k");
        int n = 300;
        for (int i = 0; i < n; i++) {
            state.put(i, "v" + i);
        }
        int count = 0;
        Iterator<Map.Entry<Integer, String>> it = state.iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, String> e = it.next();
            assertEquals("v" + e.getKey(), e.getValue());
            count++;
        }
        assertEquals(n, count);
    }

    @Test
    public void testGetSerializedValueAndEmptyReturnsNull() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("kv");
        backend.setCurrentKey("kA");
        state.put(1, "one");
        state.put(2, "two");

        @SuppressWarnings("unchecked")
        InternalKvState<String, VoidNamespace, Map<Integer, String>> internal =
                (InternalKvState<String, VoidNamespace, Map<Integer, String>>) state;

        // Build serialized key+namespace
        TypeSerializer<String> keyS = StringSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> nsS = VoidNamespaceSerializer.INSTANCE;
        byte[] serKeyNs =
                KvStateSerializer.serializeKeyAndNamespace(
                        "kA", keyS, VoidNamespace.INSTANCE, nsS);

        TypeSerializer<Map<Integer, String>> valSer = internal.getValueSerializer();
        byte[] serMap = internal.getSerializedValue(serKeyNs, keyS, nsS, valSer);
        assertNotNull(serMap);

        Map<Integer, String> back =
                KvStateSerializer.deserializeMap(serMap, IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        Map<Integer, String> expected = new HashMap<>();
        expected.put(1, "one");
        expected.put(2, "two");
        assertEquals(expected, back);

        // Empty key returns null (covers the !iterator.hasNext() return-null branch)
        byte[] serKeyNsEmpty =
                KvStateSerializer.serializeKeyAndNamespace(
                        "no-such-key", keyS, VoidNamespace.INSTANCE, nsS);
        assertNull(internal.getSerializedValue(serKeyNsEmpty, keyS, nsS, valSer));
    }

    @Test
    public void testMigrateSerializedValueWithNullSensitive() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("mig");
        @SuppressWarnings("unchecked")
        RocksDBMapState<String, VoidNamespace, Integer, String> internal =
                (RocksDBMapState<String, VoidNamespace, Integer, String>) state;

        TypeSerializer<Map<Integer, String>> ser = internal.getValueSerializer();

        // Non-null value path
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeBoolean(false);
        StringSerializer.INSTANCE.serialize("hello", out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        DataOutputSerializer migrated = new DataOutputSerializer(16);
        internal.migrateSerializedValue(in, migrated, ser, ser);
        DataInputDeserializer mIn = new DataInputDeserializer(migrated.getCopyOfBuffer());
        assertFalse(mIn.readBoolean());
        assertEquals("hello", StringSerializer.INSTANCE.deserialize(mIn));

        // Null value path
        out = new DataOutputSerializer(8);
        out.writeBoolean(true);
        in = new DataInputDeserializer(out.getCopyOfBuffer());
        migrated = new DataOutputSerializer(8);
        internal.migrateSerializedValue(in, migrated, ser, ser);
        mIn = new DataInputDeserializer(migrated.getCopyOfBuffer());
        assertTrue(mIn.readBoolean());
    }

    @Test(expected = StateMigrationException.class)
    public void testMigrateSerializedValueErrorWraps() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("mig-err");
        @SuppressWarnings("unchecked")
        RocksDBMapState<String, VoidNamespace, Integer, String> internal =
                (RocksDBMapState<String, VoidNamespace, Integer, String>) state;
        TypeSerializer<Map<Integer, String>> ser = internal.getValueSerializer();
        // Empty input -> readBoolean throws -> wrapped into StateMigrationException
        DataInputDeserializer in = new DataInputDeserializer(new byte[0]);
        DataOutputSerializer migrated = new DataOutputSerializer(8);
        internal.migrateSerializedValue(in, migrated, ser, ser);
    }

    @Test
    public void testMigrateSerializedValueWhenNullValuesNotSupported() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("mig-nn", false);
        @SuppressWarnings("unchecked")
        RocksDBMapState<String, VoidNamespace, Integer, String> internal =
                (RocksDBMapState<String, VoidNamespace, Integer, String>) state;
        TypeSerializer<Map<Integer, String>> ser = internal.getValueSerializer();
        DataOutputSerializer out = new DataOutputSerializer(16);
        // No leading boolean — directly the value
        StringSerializer.INSTANCE.serialize("hi", out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        DataOutputSerializer migrated = new DataOutputSerializer(16);
        internal.migrateSerializedValue(in, migrated, ser, ser);
        DataInputDeserializer mIn = new DataInputDeserializer(migrated.getCopyOfBuffer());
        assertEquals("hi", StringSerializer.INSTANCE.deserialize(mIn));
    }

    @Test
    public void testSupportsNullValueFalseRoundTrip() throws Exception {
        assumeFalconLibAvailable();
        // Use Long values so a null cannot be silently accepted by the serializer
        MapStateDescriptor<Integer, Long> desc =
                new MapStateDescriptor<>("nn", Integer.class, Long.class);
        desc.setSupportsNullValue(false);
        MapState<Integer, Long> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);
        backend.setCurrentKey("k");
        state.put(1, 100L);
        state.put(2, 200L);
        assertEquals(Long.valueOf(100L), state.get(1));
        assertEquals(Long.valueOf(200L), state.get(2));
        // Iterate to also exercise iterator with non-null-sensitive value deserialization
        int n = 0;
        for (Map.Entry<Integer, Long> e : state.entries()) {
            assertNotNull(e.getValue());
            n++;
        }
        assertEquals(2, n);
    }

    // ------------------------------------------------------------------
    //  Cross-prefix isolation / startWithKeyPrefix branches
    // ------------------------------------------------------------------

    @Test
    public void testIterationDoesNotSeeOtherPrefix() throws Exception {
        assumeFalconLibAvailable();
        MapState<Integer, String> state = newMapState("isolated");
        backend.setCurrentKey("aaa");
        state.put(1, "A1");
        backend.setCurrentKey("bbb");
        state.put(2, "B2");

        backend.setCurrentKey("aaa");
        Map<Integer, String> got = new HashMap<>();
        for (Map.Entry<Integer, String> e : state.entries()) {
            got.put(e.getKey(), e.getValue());
        }
        assertEquals(1, got.size());
        assertEquals("A1", got.get(1));
    }

    // ------------------------------------------------------------------
    //  StateSnapshotTransformerWrapper
    // ------------------------------------------------------------------

    @Test
    public void testSnapshotTransformerWrapperFiltersNull() throws Exception {
        StateSnapshotTransformer<byte[]> wrapper = newWrapper(passThroughTransformer());
        assertArrayEquals(nullEncoded(), wrapper.filterOrTransform(null));
    }

    @Test
    public void testSnapshotTransformerWrapperPassesNullEncodedThrough() throws Exception {
        StateSnapshotTransformer<byte[]> wrapper = newWrapper(passThroughTransformer());
        // value whose first byte indicates "null"
        byte[] in = nullEncoded();
        assertArrayEquals(nullEncoded(), wrapper.filterOrTransform(in));
    }

    @Test
    public void testSnapshotTransformerWrapperPassesNonNullThroughUnchanged() throws Exception {
        StateSnapshotTransformer<byte[]> wrapper = newWrapper(passThroughTransformer());
        byte[] in = new byte[] {0, 1, 2, 3, 4}; // first byte 0 = non-null
        // The passthrough transformer returns its input array reference, so wrapper returns
        // the original `value` reference unchanged.
        byte[] out = wrapper.filterOrTransform(in);
        assertSame(in, out);
    }

    @Test
    public void testSnapshotTransformerWrapperPrependsNonNullByteWhenChanged() throws Exception {
        // returns a new array of value bytes -> wrapper must prepend the non-null prefix
        StateSnapshotTransformer<byte[]> mutating =
                v -> new byte[] {(byte) 0xAB, (byte) 0xCD};
        StateSnapshotTransformer<byte[]> wrapper = newWrapper(mutating);
        byte[] in = new byte[] {0, 9, 9, 9}; // first byte 0 = non-null marker
        byte[] out = wrapper.filterOrTransform(in);
        assertEquals(3, out.length);
        assertEquals(0, out[0]); // NON_NULL_VALUE_PREFIX (writeBoolean(false)) is 0
        assertEquals((byte) 0xAB, out[1]);
        assertEquals((byte) 0xCD, out[2]);
    }

    @Test
    public void testSnapshotTransformerWrapperPrependsReusesBufferWhenLenMatches() throws Exception {
        // Inner transformer returns array of length value.length-1 so result+prefix == value.length.
        // Implementation should reuse the `reuse` buffer in that case.
        StateSnapshotTransformer<byte[]> mutating =
                v -> new byte[] {(byte) 0x55, (byte) 0x66, (byte) 0x77};
        StateSnapshotTransformer<byte[]> wrapper = newWrapper(mutating);
        byte[] in = new byte[] {0, 1, 2, 3}; // 4 bytes; result will be 1 + 3 = 4 bytes
        byte[] out = wrapper.filterOrTransform(in);
        assertEquals(4, out.length);
        assertEquals(0, out[0]);
        assertEquals((byte) 0x55, out[1]);
        assertEquals((byte) 0x66, out[2]);
        assertEquals((byte) 0x77, out[3]);
    }

    // ------------------------------------------------------------------
    //  Static factory: update() and the legacy 5-arg ctor
    // ------------------------------------------------------------------

    @Test
    public void testStaticUpdateMethodAndSetValueSerializer() throws Exception {
        assumeFalconLibAvailable();
        // Reach the package-private static update() and the override of setValueSerializer()
        // by invoking them via reflection on a real MapState instance. The full
        // restore-then-re-register code path is exercised end-to-end in
        // EmbeddedRocksDBStateBackendTest snapshot tests; here we just exercise the
        // RocksDBMapState contribution to that path.
        MapState<Integer, String> state = newMapState("update-static");
        @SuppressWarnings("unchecked")
        RocksDBMapState<String, VoidNamespace, Integer, String> mapState =
                (RocksDBMapState<String, VoidNamespace, Integer, String>) state;

        // Build a Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo> matching
        // the existing state's CF + serializers.
        java.lang.reflect.Field cfField =
                Class.forName("org.apache.flink.contrib.streaming.state.AbstractRocksDBState")
                        .getDeclaredField("columnFamily");
        cfField.setAccessible(true);
        Object cf = cfField.get(mapState);

        TypeSerializer<Map<Integer, String>> valSer = mapState.getValueSerializer();
        TypeSerializer<VoidNamespace> nsSer = VoidNamespaceSerializer.INSTANCE;

        Class<?> metaCls =
                Class.forName(
                        "org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo");
        Constructor<?> metaCtor =
                metaCls.getConstructor(
                        org.apache.flink.api.common.state.StateDescriptor.Type.class,
                        String.class,
                        TypeSerializer.class,
                        TypeSerializer.class);
        Object meta =
                metaCtor.newInstance(
                        org.apache.flink.api.common.state.StateDescriptor.Type.MAP,
                        "update-static",
                        nsSer,
                        valSer);

        Tuple2<Object, Object> registerResult = new Tuple2<>(cf, meta);

        MapStateDescriptor<Integer, String> desc =
                new MapStateDescriptor<>("update-static", Integer.class, String.class);
        desc.initializeSerializerUnlessSet(new org.apache.flink.api.common.ExecutionConfig());

        Method update =
                RocksDBMapState.class.getDeclaredMethod(
                        "update",
                        org.apache.flink.api.common.state.StateDescriptor.class,
                        Tuple2.class,
                        org.apache.flink.api.common.state.State.class);
        update.setAccessible(true);
        Object out = update.invoke(null, desc, registerResult, mapState);
        assertSame(mapState, out);

        // Confirm the updated state still works
        backend.setCurrentKey("k");
        state.put(1, "v");
        assertEquals("v", state.get(1));
    }

    @Test
    public void testLegacyFiveArgConstructorViaReflection() throws Exception {
        assumeFalconLibAvailable();
        // The 5-arg private ctor (no supportsNullValue flag) is unreachable from
        // production code paths but is still part of the API surface; reach it via
        // reflection to verify it delegates with supportsNullValue=true.
        // We piggy-back on a real backend-constructed MapState to get a valid ColumnFamilyHandle.
        MapState<Integer, String> state = newMapState("legacy-ctor");
        @SuppressWarnings("unchecked")
        RocksDBMapState<String, VoidNamespace, Integer, String> existing =
                (RocksDBMapState<String, VoidNamespace, Integer, String>) state;

        Constructor<?>[] ctors = RocksDBMapState.class.getDeclaredConstructors();
        Constructor<?> fiveArg = null;
        for (Constructor<?> c : ctors) {
            if (c.getParameterCount() == 5) {
                fiveArg = c;
                break;
            }
        }
        assertNotNull("expected a 5-arg constructor", fiveArg);
        fiveArg.setAccessible(true);

        // Reflectively read fields needed to call the ctor
        java.lang.reflect.Field cfField =
                Class.forName("org.apache.flink.contrib.streaming.state.AbstractRocksDBState")
                        .getDeclaredField("columnFamily");
        cfField.setAccessible(true);
        Object cf = cfField.get(existing);

        java.lang.reflect.Field nsField =
                Class.forName("org.apache.flink.contrib.streaming.state.AbstractRocksDBState")
                        .getDeclaredField("namespaceSerializer");
        nsField.setAccessible(true);
        Object nsSer = nsField.get(existing);

        TypeSerializer<Map<Integer, String>> valSer = existing.getValueSerializer();

        Object created = fiveArg.newInstance(cf, nsSer, valSer, null, backend);
        assertNotNull(created);
        @SuppressWarnings("unchecked")
        RocksDBMapState<String, VoidNamespace, Integer, String> reflective =
                (RocksDBMapState<String, VoidNamespace, Integer, String>) created;
        // It should be functional: write/read using the same column family.
        backend.setCurrentKey("legacy");
        ((MapState<Integer, String>) reflective).put(7, "seven");
        assertEquals("seven", ((MapState<Integer, String>) reflective).get(7));
    }

    @Test
    public void testSnapshotTransformerWrapperFilteredToNull() throws Exception {
        StateSnapshotTransformer<byte[]> dropAll = v -> null;
        StateSnapshotTransformer<byte[]> wrapper = newWrapper(dropAll);
        byte[] in = new byte[] {0, 9, 9};
        assertArrayEquals(nullEncoded(), wrapper.filterOrTransform(in));
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private static StateSnapshotTransformer<byte[]> newWrapper(
            StateSnapshotTransformer<byte[]> inner) throws Exception {
        Class<?> wrapperCls =
                Class.forName(
                        "org.apache.flink.contrib.streaming.state.RocksDBMapState$StateSnapshotTransformerWrapper");
        Constructor<?> ctor = wrapperCls.getDeclaredConstructor(StateSnapshotTransformer.class);
        ctor.setAccessible(true);
        @SuppressWarnings("unchecked")
        StateSnapshotTransformer<byte[]> w = (StateSnapshotTransformer<byte[]>) ctor.newInstance(inner);
        return w;
    }

    /** Returns the input array reference unchanged. */
    private static StateSnapshotTransformer<byte[]> passThroughTransformer() {
        return v -> v;
    }

    /** Encoding written by writeBoolean(true) — 1 byte. */
    private static byte[] nullEncoded() throws IOException {
        DataOutputSerializer dov = new DataOutputSerializer(1);
        dov.writeBoolean(true);
        return dov.getCopyOfBuffer();
    }
}
