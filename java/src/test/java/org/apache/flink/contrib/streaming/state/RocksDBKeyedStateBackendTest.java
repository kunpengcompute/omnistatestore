package org.apache.flink.contrib.streaming.state;

import com.huawei.falcon.state.cache.FalconException;
import com.huawei.falcon.state.cache.FalconValueState;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.util.StateMigrationException;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.WriteOptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pure-mock unit tests for the Falcon-specific surface of
 * {@link RocksDBKeyedStateBackend}: {@code flushCacheBeforeCheckpoint},
 * {@code InitFalconCache}, and {@code CheckMergeOperatorNameChange}, plus the
 * {@code MERGE_OPERATOR_NAME} constant.
 *
 * <p>These tests intentionally do <em>not</em> open a real RocksDB instance.
 * Constructing a backend goes through native code paths (DB open, column-family
 * handles, native metric monitor) which can hang on test teardown when an
 * intentionally-broken backend (e.g. with a mocked falcon throwing on
 * {@code updateCacheSizeLimit}) is disposed. Instead we mock the whole class,
 * inject the four fields the methods touch, and {@code doCallRealMethod()} on
 * the three methods under test.
 */
public class RocksDBKeyedStateBackendTest {

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

    // -------- reflection helpers --------

    private static Field findField(Class<?> cls, String name) throws NoSuchFieldException {
        Class<?> c = cls;
        while (c != null) {
            try {
                return c.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = findField(target.getClass(), name);
        f.setAccessible(true);
        try {
            // Allow setting `final` fields on Java 8.
            Field modifiers = Field.class.getDeclaredField("modifiers");
            modifiers.setAccessible(true);
            modifiers.setInt(f, f.getModifiers() & ~Modifier.FINAL);
        } catch (Exception ignored) {
            // Newer JDKs disallow this; reflection-set without it works for non-final fields.
        }
        f.set(target, value);
    }

    /**
     * Builds a mocked RocksDBKeyedStateBackend with the four fields the methods under test
     * touch (falconCache, writeOptions, enableMerge, enableFalconCache) plus an
     * ExecutionConfig that returns a configurable subTaskFalconSize.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static RocksDBKeyedStateBackend<String> stubBackend(
            boolean enableMerge,
            boolean enableFalconCache,
            int subTaskFalconSize,
            HashMap<ColumnFamilyHandle, State> falconCache) throws Exception {
        RocksDBKeyedStateBackend backend = mock(RocksDBKeyedStateBackend.class);
        setField(backend, "falconCache", falconCache);
        setField(backend, "writeOptions", mock(WriteOptions.class));
        setField(backend, "enableMerge", enableMerge);
        setField(backend, "enableFalconCache", enableFalconCache);

        // Use a real ExecutionConfig with the Falcon-specific setter rather than a Mockito
        // stub: the production method uses `super.getSubTaskFalconSize()` (invokespecial),
        // and stubs on a mocked ExecutionConfig don't always apply through that dispatch.
        ExecutionConfig ec = new ExecutionConfig();
        ec.setSubTaskFalconSize(subTaskFalconSize);
        // executionConfig lives on the AbstractKeyedStateBackend superclass.
        setField(backend, "executionConfig", ec);

        // mockito-inline instruments method dispatch (incl. invokespecial paths into super
        // classes). To make `super.getSubTaskFalconSize()` from the production method actually
        // read the executionConfig we just injected, force the inherited method to call the
        // real implementation rather than mockito's default stub.
        doCallRealMethod().when(backend).getSubTaskFalconSize();
        return backend;
    }

    private static RocksDBValueState<?, ?, ?> mockedValueStateWith(FalconValueState falcon) throws Exception {
        RocksDBValueState<?, ?, ?> vs = mock(RocksDBValueState.class);
        setField(vs, "falcon", falcon);
        return vs;
    }

    // -------- flushCacheBeforeCheckpoint --------

    @Test
    public void testFlushCacheBeforeCheckpointEmptyCache() throws Exception {
        assumeFalconLibAvailable();
        RocksDBKeyedStateBackend<String> backend = stubBackend(false, false, 0, new HashMap<>());
        doCallRealMethod().when(backend).flushCacheBeforeCheckpoint();

        backend.flushCacheBeforeCheckpoint(); // no-op, no exception
    }

    @Test
    public void testFlushCacheBeforeCheckpointInvokesFlushPerEntry() throws Exception {
        assumeFalconLibAvailable();
        FalconValueState falcon1 = mock(FalconValueState.class);
        FalconValueState falcon2 = mock(FalconValueState.class);
        ColumnFamilyHandle cf1 = mock(ColumnFamilyHandle.class);
        ColumnFamilyHandle cf2 = mock(ColumnFamilyHandle.class);
        HashMap<ColumnFamilyHandle, State> cache = new HashMap<>();
        cache.put(cf1, mockedValueStateWith(falcon1));
        cache.put(cf2, mockedValueStateWith(falcon2));

        RocksDBKeyedStateBackend<String> backend = stubBackend(false, true, 100, cache);
        doCallRealMethod().when(backend).flushCacheBeforeCheckpoint();

        backend.flushCacheBeforeCheckpoint();

        Field woField = findField(backend.getClass(), "writeOptions");
        woField.setAccessible(true);
        WriteOptions wo = (WriteOptions) woField.get(backend);
        verify(falcon1, times(1)).flushWhenCheckpoint(eq(cf1), eq(wo));
        verify(falcon2, times(1)).flushWhenCheckpoint(eq(cf2), eq(wo));
    }

    @Test
    public void testFlushCacheBeforeCheckpointSkipsNonRocksDBValueState() throws Exception {
        assumeFalconLibAvailable();
        // The method only acts on entries whose value is RocksDBValueState.
        State nonRocksValueState = mock(State.class); // not a RocksDBValueState
        ColumnFamilyHandle cf = mock(ColumnFamilyHandle.class);
        HashMap<ColumnFamilyHandle, State> cache = new HashMap<>();
        cache.put(cf, nonRocksValueState);

        RocksDBKeyedStateBackend<String> backend = stubBackend(false, true, 100, cache);
        doCallRealMethod().when(backend).flushCacheBeforeCheckpoint();

        backend.flushCacheBeforeCheckpoint(); // no exception, no flush call
    }

    @Test
    public void testFlushCacheBeforeCheckpointWrapsFalconException() throws Exception {
        assumeFalconLibAvailable();
        FalconValueState falcon = mock(FalconValueState.class);
        doThrow(new FalconException("boom")).when(falcon).flushWhenCheckpoint(any(), any());

        ColumnFamilyHandle cf = mock(ColumnFamilyHandle.class);
        HashMap<ColumnFamilyHandle, State> cache = new HashMap<>();
        cache.put(cf, mockedValueStateWith(falcon));

        RocksDBKeyedStateBackend<String> backend = stubBackend(false, true, 100, cache);
        doCallRealMethod().when(backend).flushCacheBeforeCheckpoint();

        try {
            backend.flushCacheBeforeCheckpoint();
            fail("expected RuntimeException wrapping FalconException");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof FalconException);
            assertTrue(e.getMessage().contains("flush falcon cache"));
        }
    }

    // -------- CheckMergeOperatorNameChange --------

    private static RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> meta(String mergeOpName) {
        RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> m =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.VALUE,
                        "s",
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        if (mergeOpName != null) {
            m.setMergeOperatorName(mergeOpName);
        }
        return m;
    }

    private static ValueStateDescriptor<Integer> desc(String mergeOpName) {
        ValueStateDescriptor<Integer> d = new ValueStateDescriptor<>("s", IntSerializer.INSTANCE);
        if (mergeOpName != null) {
            d.setMergeOperatorName(mergeOpName);
        }
        return d;
    }

    @Test
    public void testCheckMergeOperatorNameChangeDisabled() throws Exception {
        assumeFalconLibAvailable();
        // enableMerge == false: even mismatched names must not throw.
        RocksDBKeyedStateBackend<String> backend = stubBackend(false, false, 0, new HashMap<>());
        doCallRealMethod().when(backend).CheckMergeOperatorNameChange(any(), any());

        backend.CheckMergeOperatorNameChange(meta("op-a"), desc("op-b"));
    }

    @Test
    public void testCheckMergeOperatorNameChangeEnabledSameName() throws Exception {
        assumeFalconLibAvailable();
        RocksDBKeyedStateBackend<String> backend = stubBackend(true, false, 0, new HashMap<>());
        doCallRealMethod().when(backend).CheckMergeOperatorNameChange(any(), any());

        backend.CheckMergeOperatorNameChange(meta("uint64add"), desc("uint64add"));
    }

    @Test
    public void testCheckMergeOperatorNameChangeEnabledDifferentNameThrows() throws Exception {
        assumeFalconLibAvailable();
        RocksDBKeyedStateBackend<String> backend = stubBackend(true, false, 0, new HashMap<>());
        doCallRealMethod().when(backend).CheckMergeOperatorNameChange(any(), any());

        try {
            backend.CheckMergeOperatorNameChange(meta("uint64add"), desc("stringappendtest"));
            fail("expected StateMigrationException");
        } catch (StateMigrationException expected) {
            assertTrue(expected.getMessage().contains("Merge operator"));
        }
    }

    @Test
    public void testCheckMergeOperatorNameChangeBothNullDefaultsMatch() throws Exception {
        assumeFalconLibAvailable();
        // null + null both default to MERGE_OPERATOR_NAME -> equal, no throw.
        RocksDBKeyedStateBackend<String> backend = stubBackend(true, false, 0, new HashMap<>());
        doCallRealMethod().when(backend).CheckMergeOperatorNameChange(any(), any());

        backend.CheckMergeOperatorNameChange(meta(null), desc(null));
    }

    @Test
    public void testCheckMergeOperatorNameChangeOldNullNewDefault() throws Exception {
        assumeFalconLibAvailable();
        // old=null -> defaults to MERGE_OPERATOR_NAME ("stringappendtest");
        // new explicit "stringappendtest" -> equal, no throw.
        RocksDBKeyedStateBackend<String> backend = stubBackend(true, false, 0, new HashMap<>());
        doCallRealMethod().when(backend).CheckMergeOperatorNameChange(any(), any());

        backend.CheckMergeOperatorNameChange(meta(null), desc(RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME));
    }

    @Test
    public void testCheckMergeOperatorNameChangeOldNullNewDifferentThrows() throws Exception {
        assumeFalconLibAvailable();
        // old=null defaults to MERGE_OPERATOR_NAME; new "uint64add" -> different -> throw.
        RocksDBKeyedStateBackend<String> backend = stubBackend(true, false, 0, new HashMap<>());
        doCallRealMethod().when(backend).CheckMergeOperatorNameChange(any(), any());

        try {
            backend.CheckMergeOperatorNameChange(meta(null), desc("uint64add"));
            fail("expected StateMigrationException");
        } catch (StateMigrationException expected) {
            // ok
        }
    }

    // -------- InitFalconCache --------

    /**
     * Builds the third argument {@code Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo>}
     * the production method receives.
     */
    private static Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer>>
            registerResult(ColumnFamilyHandle cf, String stateName) {
        RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> mi =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.VALUE,
                        stateName,
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);
        return Tuple2.of(cf, mi);
    }

    /**
     * Invokes {@code InitFalconCache} reflectively to sidestep javac's inability to
     * infer the four type parameters from the supplied arguments.
     */
    private static void invokeInitFalconCache(
            RocksDBKeyedStateBackend<?> backend,
            StateDescriptor<?, ?> stateDesc,
            State createdState,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer>> registerResult)
            throws Exception {
        java.lang.reflect.Method m = null;
        for (java.lang.reflect.Method candidate : RocksDBKeyedStateBackend.class.getDeclaredMethods()) {
            if (candidate.getName().equals("InitFalconCache")) {
                m = candidate;
                break;
            }
        }
        if (m == null) {
            throw new AssertionError("InitFalconCache method not found");
        }
        m.setAccessible(true);
        try {
            m.invoke(backend, stateDesc, createdState, registerResult);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw e;
        }
    }

    private static MockedStatic<GlobalConfiguration> stubGlobalConfig(boolean sqlTtlEnabled) {
        Configuration conf = new Configuration();
        if (sqlTtlEnabled) {
            conf.setString("table.exec.state.ttl", "1 min");
        }
        MockedStatic<GlobalConfiguration> mocked = mockStatic(GlobalConfiguration.class);
        mocked.when(GlobalConfiguration::loadConfiguration).thenReturn(conf);
        return mocked;
    }

    @Test
    public void testInitFalconCacheSkippedWhenFalconCacheDisabled() throws Exception {
        assumeFalconLibAvailable();
        HashMap<ColumnFamilyHandle, State> cache = new HashMap<>();
        RocksDBKeyedStateBackend<String> backend = stubBackend(false, false, 100, cache);
        doCallRealMethod().when(backend).InitFalconCache(any(), any(), any());

        ColumnFamilyHandle cf = mock(ColumnFamilyHandle.class);
        FalconValueState falcon = mock(FalconValueState.class);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(false)) {
            invokeInitFalconCache(backend, 
                    desc(null),
                    mockedValueStateWith(falcon),
                    registerResult(cf, "s"));
        }

        assertTrue("falconCache must remain empty when feature disabled", cache.isEmpty());
        verify(falcon, times(0)).updateCacheSizeLimit(any(), any(), any(Integer.class));
    }

    @Test
    public void testInitFalconCacheSkippedWhenStateNotRocksDBValueState() throws Exception {
        assumeFalconLibAvailable();
        HashMap<ColumnFamilyHandle, State> cache = new HashMap<>();
        RocksDBKeyedStateBackend<String> backend = stubBackend(false, true, 100, cache);
        doCallRealMethod().when(backend).InitFalconCache(any(), any(), any());

        ColumnFamilyHandle cf = mock(ColumnFamilyHandle.class);
        State otherState = mock(State.class); // not a RocksDBValueState

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(false)) {
            invokeInitFalconCache(backend, desc(null), otherState, registerResult(cf, "s"));
        }

        assertTrue(cache.isEmpty());
    }

    @Test
    public void testInitFalconCacheSkippedWhenUdfTtlEnabled() throws Exception {
        assumeFalconLibAvailable();
        HashMap<ColumnFamilyHandle, State> cache = new HashMap<>();
        RocksDBKeyedStateBackend<String> backend = stubBackend(false, true, 100, cache);
        doCallRealMethod().when(backend).InitFalconCache(any(), any(), any());

        ColumnFamilyHandle cf = mock(ColumnFamilyHandle.class);
        FalconValueState falcon = mock(FalconValueState.class);

        ValueStateDescriptor<Integer> ttlDesc = desc(null);
        ttlDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(1)).build());

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(false)) {
            invokeInitFalconCache(backend, 
                    ttlDesc,
                    mockedValueStateWith(falcon),
                    registerResult(cf, "s"));
        }

        assertTrue(cache.isEmpty());
    }

    @Test
    public void testInitFalconCacheSkippedWhenSqlTtlEnabled() throws Exception {
        assumeFalconLibAvailable();
        HashMap<ColumnFamilyHandle, State> cache = new HashMap<>();
        RocksDBKeyedStateBackend<String> backend = stubBackend(false, true, 100, cache);
        doCallRealMethod().when(backend).InitFalconCache(any(), any(), any());

        ColumnFamilyHandle cf = mock(ColumnFamilyHandle.class);
        FalconValueState falcon = mock(FalconValueState.class);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(true /* sqlTtl */)) {
            invokeInitFalconCache(backend, 
                    desc(null),
                    mockedValueStateWith(falcon),
                    registerResult(cf, "s"));
        }

        assertTrue(cache.isEmpty());
    }

    @Test
    public void testInitFalconCachePopulatesAndUpdatesSize() throws Exception {
        assumeFalconLibAvailable();
        HashMap<ColumnFamilyHandle, State> cache = new HashMap<>();
        // subTaskFalconSize=100, expect newCacheSize = 100 / 1 = 100.
        RocksDBKeyedStateBackend<String> backend = stubBackend(false, true, 100, cache);
        doCallRealMethod().when(backend).InitFalconCache(any(), any(), any());

        ColumnFamilyHandle cf = mock(ColumnFamilyHandle.class);
        FalconValueState falcon = mock(FalconValueState.class);
        RocksDBValueState<?, ?, ?> vs = mockedValueStateWith(falcon);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(false)) {
            invokeInitFalconCache(backend, desc(null), vs, registerResult(cf, "s"));
        }

        assertEquals(1, cache.size());
        assertEquals(vs, cache.get(cf));
        verify(falcon, times(1))
                .updateCacheSizeLimit(eq(cf), any(), eq(100));
    }

    @Test
    public void testInitFalconCacheSecondStateRedistributesSize() throws Exception {
        assumeFalconLibAvailable();
        // First entry already in cache with its own falcon; second insertion should
        // recalculate newCacheSize = 100/2 = 50 and call updateCacheSizeLimit on both.
        HashMap<ColumnFamilyHandle, State> cache = new HashMap<>();
        FalconValueState existingFalcon = mock(FalconValueState.class);
        ColumnFamilyHandle existingCf = mock(ColumnFamilyHandle.class);
        cache.put(existingCf, mockedValueStateWith(existingFalcon));

        RocksDBKeyedStateBackend<String> backend = stubBackend(false, true, 100, cache);
        doCallRealMethod().when(backend).InitFalconCache(any(), any(), any());

        ColumnFamilyHandle newCf = mock(ColumnFamilyHandle.class);
        FalconValueState newFalcon = mock(FalconValueState.class);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(false)) {
            invokeInitFalconCache(backend, 
                    desc(null),
                    mockedValueStateWith(newFalcon),
                    registerResult(newCf, "s2"));
        }

        assertEquals(2, cache.size());
        verify(existingFalcon, times(1)).updateCacheSizeLimit(eq(existingCf), any(), eq(50));
        verify(newFalcon, times(1)).updateCacheSizeLimit(eq(newCf), any(), eq(50));
    }

    @Test
    public void testInitFalconCacheWrapsUpdateCacheSizeLimitException() throws Exception {
        assumeFalconLibAvailable();
        HashMap<ColumnFamilyHandle, State> cache = new HashMap<>();
        RocksDBKeyedStateBackend<String> backend = stubBackend(false, true, 100, cache);
        doCallRealMethod().when(backend).InitFalconCache(any(), any(), any());

        ColumnFamilyHandle cf = mock(ColumnFamilyHandle.class);
        FalconValueState falcon = mock(FalconValueState.class);
        doThrow(new FalconException("size-error"))
                .when(falcon).updateCacheSizeLimit(any(), any(), any(Integer.class));

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(false)) {
            invokeInitFalconCache(backend, 
                    desc(null),
                    mockedValueStateWith(falcon),
                    registerResult(cf, "s"));
            fail("expected RuntimeException");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof FalconException);
            assertTrue(e.getMessage().contains("falcon cache size"));
        }
    }

    // -------- Constants --------

    @Test
    public void testMergeOperatorNameConstant() {
        assumeFalconLibAvailable();
        assertEquals("stringappendtest", RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME);
    }

    @Test
    public void testRocksDbKvStateInfoFields() {
        assumeFalconLibAvailable();
        ColumnFamilyHandle cf = mock(ColumnFamilyHandle.class);
        RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> mi =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.VALUE,
                        "x",
                        VoidNamespaceSerializer.INSTANCE,
                        IntSerializer.INSTANCE);

        RocksDBKeyedStateBackend.RocksDbKvStateInfo info =
                new RocksDBKeyedStateBackend.RocksDbKvStateInfo(cf, mi);

        assertEquals(cf, info.columnFamilyHandle);
        assertEquals(mi, info.metaInfo);
        assertNotNull(info.toString());
    }
}
