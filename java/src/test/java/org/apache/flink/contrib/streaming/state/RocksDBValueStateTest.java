package org.apache.flink.contrib.streaming.state;

import com.huawei.falcon.state.cache.FalconException;
import com.huawei.falcon.state.cache.FalconValueState;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RocksDBValueState}.
 *
 * <p>Coverage strategy:
 *
 * <ul>
 *   <li>The non-Falcon branch (when {@code falcon.isFalconCacheOpen() == false}) is already
 *       exercised end-to-end by {@code EmbeddedRocksDBStateBackendTest} since the test-resource
 *       stub {@code libfalcon.so} returns {@code getCacheSizeLimit() == 0}.
 *   <li>This file covers (a) the Falcon branch by reflectively replacing the {@code falcon} field
 *       on a real {@code RocksDBValueState} instance with a Mockito mock that returns {@code true}
 *       from {@code isFalconCacheOpen()}, and (b) auxiliary methods such as the serializer
 *       getters, {@code getSerializedValue} and the static {@code update} factory.
 * </ul>
 */
public class RocksDBValueStateTest {

    /** True if FalconValueState native library loaded successfully. */
    private static boolean falconLibAvailable = false;

    @BeforeClass
    public static void checkFalconLib() {
        try {
            Class.forName("com.huawei.falcon.state.cache.FalconValueState");
            falconLibAvailable = true;
        } catch (UnsatisfiedLinkError | ExceptionInInitializerError | NoClassDefFoundError | ClassNotFoundException e) {
            falconLibAvailable = false;
        }
    }

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private RocksDBKeyedStateBackend<String> backend;

    /** Skip all tests if FalconValueState native library is not available on this platform. */
    private static void assumeFalconLibAvailable() {
        Assume.assumeTrue("FalconValueState native library not available on this platform - skipping", falconLibAvailable);
    }

    @Before
    public void setUp() throws Exception {
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

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private RocksDBValueState<String, VoidNamespace, Integer> newValueState(String name)
            throws Exception {
        ValueStateDescriptor<Integer> desc =
                new ValueStateDescriptor<>(name, IntSerializer.INSTANCE);
        ValueState<Integer> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);
        return (RocksDBValueState<String, VoidNamespace, Integer>) state;
    }

    @SuppressWarnings("unchecked")
    private RocksDBValueState<String, VoidNamespace, String> newStringValueState(String name)
            throws Exception {
        ValueStateDescriptor<String> desc =
                new ValueStateDescriptor<>(name, StringSerializer.INSTANCE);
        ValueState<String> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);
        return (RocksDBValueState<String, VoidNamespace, String>) state;
    }

    private static FalconValueState swapFalconWithMock(RocksDBValueState<?, ?, ?> state)
            throws Exception {
        FalconValueState mock = mock(FalconValueState.class);
        Field f = RocksDBValueState.class.getDeclaredField("falcon");
        f.setAccessible(true);
        // Close the real falcon first to release native handle.
        FalconValueState real = (FalconValueState) f.get(state);
        if (real != null) {
            real.close();
        }
        f.set(state, mock);
        return mock;
    }

    // ------------------------------------------------------------------
    //  Non-Falcon branch (the falcon mock returns isFalconCacheOpen() == false)
    // ------------------------------------------------------------------

    @Test
    public void testValueAndUpdateNonFalconBranch() throws Exception {
        assumeFalconLibAvailable();
        RocksDBValueState<String, VoidNamespace, Integer> vs = newValueState("vs-non-falcon");
        FalconValueState fmock = swapFalconWithMock(vs);
        when(fmock.isFalconCacheOpen()).thenReturn(false);

        backend.setCurrentKey("k");
        // Default value (descriptor default is null for plain ValueStateDescriptor)
        assertNull(vs.value());

        vs.update(7);
        assertEquals(Integer.valueOf(7), vs.value());

        vs.update(null); // -> clear()
        assertNull(vs.value());

        // Falcon mock should never have been used for data-plane operations.
        verify(fmock, never()).get(any(), any(), any());
        verify(fmock, never()).put(any(), any(), any(), any());
        verify(fmock, never()).delete(any(), any(), any());
    }

    @Test
    public void testClearNonFalconBranch() throws Exception {
        assumeFalconLibAvailable();
        RocksDBValueState<String, VoidNamespace, Integer> vs = newValueState("vs-clear-nf");
        FalconValueState fmock = swapFalconWithMock(vs);
        when(fmock.isFalconCacheOpen()).thenReturn(false);

        backend.setCurrentKey("k");
        vs.update(123);
        assertEquals(Integer.valueOf(123), vs.value());
        vs.clear();
        assertNull(vs.value());
    }

    // ------------------------------------------------------------------
    //  Falcon branch — driven via the mocked FalconValueState
    // ------------------------------------------------------------------

    @Test
    public void testValueDelegatesToFalconWhenCacheOpen() throws Exception {
        assumeFalconLibAvailable();
        RocksDBValueState<String, VoidNamespace, Integer> vs = newValueState("vs-fc-value");
        FalconValueState fmock = swapFalconWithMock(vs);
        when(fmock.isFalconCacheOpen()).thenReturn(true);

        // Pre-encode the int 42 the same way IntSerializer would
        byte[] encoded = serializeInt(42);
        when(fmock.get(any(), any(), any())).thenReturn(encoded);

        backend.setCurrentKey("k");
        Integer result = vs.value();
        assertEquals(Integer.valueOf(42), result);

        verify(fmock, times(1)).get(any(), any(), any());
    }

    @Test
    public void testValueReturnsDefaultWhenFalconReturnsNull() throws Exception {
        assumeFalconLibAvailable();
        // Use a state with a non-null default so the default-branch is observable.
        ValueStateDescriptor<Integer> desc =
                new ValueStateDescriptor<>("vs-fc-default", IntSerializer.INSTANCE, 99);
        @SuppressWarnings("unchecked")
        RocksDBValueState<String, VoidNamespace, Integer> vs =
                (RocksDBValueState<String, VoidNamespace, Integer>)
                        backend.getPartitionedState(
                                VoidNamespace.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                desc);
        FalconValueState fmock = swapFalconWithMock(vs);
        when(fmock.isFalconCacheOpen()).thenReturn(true);
        when(fmock.get(any(), any(), any())).thenReturn(null);

        backend.setCurrentKey("k");
        assertEquals(Integer.valueOf(99), vs.value());
    }

    @Test
    public void testUpdateDelegatesToFalconWhenCacheOpen() throws Exception {
        assumeFalconLibAvailable();
        RocksDBValueState<String, VoidNamespace, Integer> vs = newValueState("vs-fc-update");
        FalconValueState fmock = swapFalconWithMock(vs);
        when(fmock.isFalconCacheOpen()).thenReturn(true);

        backend.setCurrentKey("k");
        vs.update(13);

        ArgumentCaptor<byte[]> keyCap = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<byte[]> valCap = ArgumentCaptor.forClass(byte[].class);
        verify(fmock, times(1))
                .put(any(), any(), keyCap.capture(), valCap.capture());

        // Verify the value bytes are exactly the IntSerializer encoding of 13.
        assertArrayEquals(serializeInt(13), valCap.getValue());
        // Key must be non-empty (it includes key-group + key + namespace).
        assertNotNull(keyCap.getValue());
        assertTrue(keyCap.getValue().length > 0);
    }

    @Test
    public void testClearDelegatesToFalconWhenCacheOpen() throws Exception {
        assumeFalconLibAvailable();
        RocksDBValueState<String, VoidNamespace, Integer> vs = newValueState("vs-fc-clear");
        FalconValueState fmock = swapFalconWithMock(vs);
        when(fmock.isFalconCacheOpen()).thenReturn(true);

        backend.setCurrentKey("k");
        vs.clear();
        verify(fmock, times(1)).delete(any(), any(), any());
    }

    @Test
    public void testUpdateWithNullValueClearsViaFalcon() throws Exception {
        assumeFalconLibAvailable();
        RocksDBValueState<String, VoidNamespace, Integer> vs = newValueState("vs-fc-update-null");
        FalconValueState fmock = swapFalconWithMock(vs);
        when(fmock.isFalconCacheOpen()).thenReturn(true);

        backend.setCurrentKey("k");
        vs.update(null);
        // update(null) routes through clear(), which under the falcon branch calls delete().
        verify(fmock, atLeastOnce()).delete(any(), any(), any());
        verify(fmock, never()).put(any(), any(), any(), any());
    }

    // ------------------------------------------------------------------
    //  Exception wrapping
    // ------------------------------------------------------------------

    @Test
    public void testValueWrapsFalconException() throws Exception {
        assumeFalconLibAvailable();
        RocksDBValueState<String, VoidNamespace, Integer> vs = newValueState("vs-fc-ex-value");
        FalconValueState fmock = swapFalconWithMock(vs);
        when(fmock.isFalconCacheOpen()).thenReturn(true);
        when(fmock.get(any(), any(), any())).thenThrow(new FalconException("boom"));

        backend.setCurrentKey("k");
        try {
            vs.value();
            fail("expected FlinkRuntimeException");
        } catch (RuntimeException expected) {
            assertTrue(expected.getCause() instanceof FalconException);
        }
    }

    @Test
    public void testUpdateWrapsFalconException() throws Exception {
        assumeFalconLibAvailable();
        RocksDBValueState<String, VoidNamespace, Integer> vs = newValueState("vs-fc-ex-update");
        FalconValueState fmock = swapFalconWithMock(vs);
        when(fmock.isFalconCacheOpen()).thenReturn(true);
        doThrow(new FalconException("boom"))
                .when(fmock)
                .put(any(), any(), any(), any());

        backend.setCurrentKey("k");
        try {
            vs.update(1);
            fail("expected FlinkRuntimeException");
        } catch (RuntimeException expected) {
            assertNotNull(expected.getCause());
        }
    }

    @Test
    public void testClearWrapsFalconException() throws Exception {
        assumeFalconLibAvailable();
        RocksDBValueState<String, VoidNamespace, Integer> vs = newValueState("vs-fc-ex-clear");
        FalconValueState fmock = swapFalconWithMock(vs);
        when(fmock.isFalconCacheOpen()).thenReturn(true);
        doThrow(new FalconException("boom"))
                .when(fmock)
                .delete(any(), any(), any());

        backend.setCurrentKey("k");
        try {
            vs.clear();
            fail("expected FlinkRuntimeException");
        } catch (RuntimeException expected) {
            assertTrue(expected.getCause() instanceof FalconException);
        }
    }

    // ------------------------------------------------------------------
    //  Serializer accessors
    // ------------------------------------------------------------------

    @Test
    public void testGetSerializers() throws Exception {
        assumeFalconLibAvailable();
        RocksDBValueState<String, VoidNamespace, Integer> vs = newValueState("vs-ser");
        assertSame(StringSerializer.INSTANCE, vs.getKeySerializer());
        assertSame(VoidNamespaceSerializer.INSTANCE, vs.getNamespaceSerializer());
        TypeSerializer<Integer> valSer = vs.getValueSerializer();
        assertNotNull(valSer);
        // The descriptor was built from IntSerializer.INSTANCE
        assertEquals(IntSerializer.INSTANCE.createInstance(), valSer.createInstance());
    }

    // ------------------------------------------------------------------
    //  getSerializedValue — both branches
    // ------------------------------------------------------------------

    @Test
    public void testGetSerializedValueNonFalconBranch() throws Exception {
        assumeFalconLibAvailable();
        RocksDBValueState<String, VoidNamespace, String> vs =
                newStringValueState("vs-getser-nf");
        FalconValueState fmock = swapFalconWithMock(vs);
        when(fmock.isFalconCacheOpen()).thenReturn(false);

        backend.setCurrentKey("alpha");
        ((ValueState<String>) vs).update("hello");

        @SuppressWarnings("unchecked")
        InternalKvState<String, VoidNamespace, String> internal =
                (InternalKvState<String, VoidNamespace, String>) vs;

        byte[] keyAndNs =
                KvStateSerializer.serializeKeyAndNamespace(
                        "alpha",
                        StringSerializer.INSTANCE,
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE);
        byte[] raw =
                internal.getSerializedValue(
                        keyAndNs,
                        StringSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        assertNotNull(raw);
    }

    @Test
    public void testGetSerializedValueFalconBranch() throws Exception {
        assumeFalconLibAvailable();
        RocksDBValueState<String, VoidNamespace, String> vs =
                newStringValueState("vs-getser-fc");
        FalconValueState fmock = swapFalconWithMock(vs);
        when(fmock.isFalconCacheOpen()).thenReturn(true);
        byte[] payload = new byte[] {1, 2, 3, 4};
        when(fmock.get(any(), any(), any())).thenReturn(payload);

        @SuppressWarnings("unchecked")
        InternalKvState<String, VoidNamespace, String> internal =
                (InternalKvState<String, VoidNamespace, String>) vs;

        byte[] keyAndNs =
                KvStateSerializer.serializeKeyAndNamespace(
                        "alpha",
                        StringSerializer.INSTANCE,
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE);
        byte[] raw =
                internal.getSerializedValue(
                        keyAndNs,
                        StringSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        assertSame(payload, raw);
        verify(fmock, times(1)).get(any(), any(), any());
    }

    // ------------------------------------------------------------------
    //  Static factory: update()
    // ------------------------------------------------------------------

    @Test
    public void testStaticUpdateFactory() throws Exception {
        assumeFalconLibAvailable();
        RocksDBValueState<String, VoidNamespace, Integer> vs = newValueState("vs-static-update");

        // Build a Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo> matching
        // the existing state.
        Field cfField =
                Class.forName("org.apache.flink.contrib.streaming.state.AbstractRocksDBState")
                        .getDeclaredField("columnFamily");
        cfField.setAccessible(true);
        Object cf = cfField.get(vs);

        Class<?> metaCls =
                Class.forName(
                        "org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo");
        java.lang.reflect.Constructor<?> metaCtor =
                metaCls.getConstructor(
                        org.apache.flink.api.common.state.StateDescriptor.Type.class,
                        String.class,
                        TypeSerializer.class,
                        TypeSerializer.class);
        Object meta =
                metaCtor.newInstance(
                        org.apache.flink.api.common.state.StateDescriptor.Type.VALUE,
                        "vs-static-update",
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        org.apache.flink.api.java.tuple.Tuple2<Object, Object> registerResult =
                new org.apache.flink.api.java.tuple.Tuple2<>(cf, meta);

        ValueStateDescriptor<Integer> desc =
                new ValueStateDescriptor<>("vs-static-update", IntSerializer.INSTANCE, 17);
        desc.initializeSerializerUnlessSet(new org.apache.flink.api.common.ExecutionConfig());

        java.lang.reflect.Method update =
                RocksDBValueState.class.getDeclaredMethod(
                        "update",
                        org.apache.flink.api.common.state.StateDescriptor.class,
                        org.apache.flink.api.java.tuple.Tuple2.class,
                        org.apache.flink.api.common.state.State.class);
        update.setAccessible(true);
        Object out = update.invoke(null, desc, registerResult, vs);
        assertSame(vs, out);
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    /** Reproduce IntSerializer's wire format (4 BE bytes). */
    private static byte[] serializeInt(int value) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(4);
        out.write((value >>> 24) & 0xff);
        out.write((value >>> 16) & 0xff);
        out.write((value >>> 8) & 0xff);
        out.write(value & 0xff);
        return out.toByteArray();
    }

    // Reference the unused field to silence warnings if the compiler complains about the import.
    @SuppressWarnings("unused")
    private static final AtomicReference<RocksDBException> __ignore__ = new AtomicReference<>();
}
