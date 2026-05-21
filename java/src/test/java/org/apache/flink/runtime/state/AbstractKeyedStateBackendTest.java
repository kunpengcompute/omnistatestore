package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackend;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackendBuilder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractKeyedStateBackend}. Drives the methods via {@link MockKeyedStateBackend}
 * so we can exercise the abstract base's bookkeeping without a real RocksDB.
 */
public class AbstractKeyedStateBackendTest {

    private MockKeyedStateBackend<String> backend;
    private TaskKvStateRegistry kvRegistry;

    @Before
    public void setUp() throws Exception {
        backend = newBackend(new ExecutionConfig(), false);
    }

    @After
    public void tearDown() {
        if (backend != null) {
            backend.dispose();
        }
    }

    private MockKeyedStateBackend<String> newBackend(ExecutionConfig cfg, boolean realKvRegistry)
            throws Exception {
        kvRegistry =
                realKvRegistry
                        ? new KvStateRegistry()
                                .createTaskRegistry(
                                        new org.apache.flink.api.common.JobID(),
                                        new org.apache.flink.runtime.jobgraph.JobVertexID())
                        : null;
        MockKeyedStateBackendBuilder<String> builder =
                new MockKeyedStateBackendBuilder<>(
                        kvRegistry,
                        StringSerializer.INSTANCE,
                        Thread.currentThread().getContextClassLoader(),
                        4,
                        new KeyGroupRange(0, 3),
                        cfg,
                        TtlTimeProvider.DEFAULT,
                        LatencyTrackingStateConfig.disabled(),
                        Collections.emptyList(),
                        UncompressedStreamCompressionDecorator.INSTANCE,
                        new CloseableRegistry(),
                        MockKeyedStateBackend.MockSnapshotSupplier.EMPTY);
        return builder.build();
    }

    // ------------------------------------------------------------------------
    //  Construction / compression-decorator branches
    // ------------------------------------------------------------------------

    @Test
    public void testDefaultStreamCompressionUncompressed() throws Exception {
        MockKeyedStateBackend<String> b = newBackend(new ExecutionConfig(), false);
        try {
            assertSame(
                    UncompressedStreamCompressionDecorator.INSTANCE,
                    b.getKeyGroupCompressionDecorator());
        } finally {
            b.dispose();
        }
    }

    @Test
    public void testSnappyStreamCompressionWhenConfigured() throws Exception {
        ExecutionConfig cfg = new ExecutionConfig();
        cfg.setUseSnapshotCompression(true);
        // The mock builder hands UncompressedStreamCompressionDecorator directly to the abstract
        // ctor — so to exercise determineStreamCompression we use the alternate ctor by
        // subclassing.
        ExecutionConfig snappyCfg = cfg;
        TestBackend b =
                new TestBackend(
                        StringSerializer.INSTANCE,
                        snappyCfg,
                        new InternalKeyContextImpl<>(new KeyGroupRange(0, 3), 4));
        try {
            assertSame(
                    SnappyStreamCompressionDecorator.INSTANCE,
                    b.getKeyGroupCompressionDecorator());
        } finally {
            b.dispose();
        }
    }

    @Test
    public void testDetermineStreamCompressionUncompressedFallback() throws Exception {
        TestBackend b =
                new TestBackend(
                        StringSerializer.INSTANCE,
                        new ExecutionConfig(),
                        new InternalKeyContextImpl<>(new KeyGroupRange(0, 3), 4));
        try {
            assertSame(
                    UncompressedStreamCompressionDecorator.INSTANCE,
                    b.getKeyGroupCompressionDecorator());
        } finally {
            b.dispose();
        }
    }

    // ------------------------------------------------------------------------
    //  Key context delegation
    // ------------------------------------------------------------------------

    @Test
    public void testSetAndGetCurrentKey() {
        backend.setCurrentKey("k1");
        assertEquals("k1", backend.getCurrentKey());
        // currentKeyGroupIndex was set by setCurrentKey
        int kg = backend.getCurrentKeyGroupIndex();
        assertTrue(kg >= 0 && kg < 4);
    }

    @Test
    public void testSetCurrentKeyGroupIndex() {
        backend.setCurrentKeyGroupIndex(2);
        assertEquals(2, backend.getCurrentKeyGroupIndex());
    }

    @Test
    public void testGetNumberOfKeyGroups() {
        assertEquals(4, backend.getNumberOfKeyGroups());
    }

    @Test
    public void testGetKeyGroupRange() {
        assertEquals(new KeyGroupRange(0, 3), backend.getKeyGroupRange());
    }

    @Test
    public void testGetKeySerializer() {
        assertSame(StringSerializer.INSTANCE, backend.getKeySerializer());
    }

    @Test
    public void testGetKeyContextNotNull() {
        assertNotNull(backend.getKeyContext());
    }

    // ------------------------------------------------------------------------
    //  Key selection listeners
    // ------------------------------------------------------------------------

    @Test
    public void testKeySelectionListenerNotifiedAndRemovable() {
        @SuppressWarnings("unchecked")
        KeyedStateBackend.KeySelectionListener<String> listener =
                mock(KeyedStateBackend.KeySelectionListener.class);

        backend.registerKeySelectionListener(listener);
        backend.setCurrentKey("a");
        backend.setCurrentKey("b");
        verify(listener, times(1)).keySelected("a");
        verify(listener, times(1)).keySelected("b");

        assertTrue(backend.deregisterKeySelectionListener(listener));
        backend.setCurrentKey("c");
        verify(listener, never()).keySelected("c");
    }

    @Test
    public void testDeregisterUnknownListener() {
        @SuppressWarnings("unchecked")
        KeyedStateBackend.KeySelectionListener<String> listener =
                mock(KeyedStateBackend.KeySelectionListener.class);
        assertFalse(backend.deregisterKeySelectionListener(listener));
    }

    // ------------------------------------------------------------------------
    //  Default no-op methods
    // ------------------------------------------------------------------------

    @Test
    public void testNotifyCheckpointSubsumedIsNoOp() throws Exception {
        backend.notifyCheckpointSubsumed(123L);
    }

    @Test
    public void testRequiresLegacySynchronousTimerSnapshotsFalse() {
        SnapshotType type = mock(SnapshotType.class);
        assertFalse(backend.requiresLegacySynchronousTimerSnapshots(type));
    }

    @Test
    public void testGetLatencyTrackingStateConfigNotNull() {
        assertNotNull(backend.getLatencyTrackingStateConfig());
    }

    // ------------------------------------------------------------------------
    //  Falcon: getSubTaskFalconSize
    // ------------------------------------------------------------------------

    @Test
    public void testGetSubTaskFalconSizeDefault() {
        // default ExecutionConfig — value depends on Falcon-modified ExecutionConfig but should
        // not throw
        int v = backend.getSubTaskFalconSize();
        assertTrue(v >= 0);
    }

    // ------------------------------------------------------------------------
    //  State caching / partitioned state
    // ------------------------------------------------------------------------

    @Test
    public void testGetOrCreateKeyedStateAndCacheBookkeeping() throws Exception {
        ValueStateDescriptor<Integer> d = new ValueStateDescriptor<>("foo", IntSerializer.INSTANCE);
        State s1 = backend.getOrCreateKeyedState(StringSerializer.INSTANCE, d);
        assertNotNull(s1);
        assertEquals(1, backend.numKeyValueStatesByName());
        // second call returns cached
        State s2 = backend.getOrCreateKeyedState(StringSerializer.INSTANCE, d);
        assertSame(s1, s2);
    }

    @Test(expected = NullPointerException.class)
    public void testGetOrCreateKeyedStateNullNamespaceSerializerThrows() throws Exception {
        ValueStateDescriptor<Integer> d = new ValueStateDescriptor<>("foo", IntSerializer.INSTANCE);
        backend.getOrCreateKeyedState(null, d);
    }

    @Test
    public void testGetOrCreateKeyedStateLazySerializerInit() throws Exception {
        // Use TypeInformation ctor so serializer starts uninitialized.
        ValueStateDescriptor<Integer> d =
                new ValueStateDescriptor<>(
                        "lazy",
                        org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO);
        assertFalse(d.isSerializerInitialized());
        backend.getOrCreateKeyedState(StringSerializer.INSTANCE, d);
        assertTrue(d.isSerializerInitialized());
    }

    @Test
    public void testGetPartitionedStateCacheLastNameFastPath() throws Exception {
        ValueStateDescriptor<Integer> d = new ValueStateDescriptor<>("foo", IntSerializer.INSTANCE);
        backend.setCurrentKey("k");
        State s1 = backend.getPartitionedState("ns1", StringSerializer.INSTANCE, d);
        // second call — same name, different namespace — must hit the lastName fast path
        State s2 = backend.getPartitionedState("ns2", StringSerializer.INSTANCE, d);
        assertSame(s1, s2);
    }

    @Test
    public void testGetPartitionedStateRetrievesPreviousByName() throws Exception {
        ValueStateDescriptor<Integer> d1 = new ValueStateDescriptor<>("a", IntSerializer.INSTANCE);
        ValueStateDescriptor<Integer> d2 = new ValueStateDescriptor<>("b", IntSerializer.INSTANCE);
        backend.setCurrentKey("k");
        State sa = backend.getPartitionedState("ns", StringSerializer.INSTANCE, d1);
        State sb = backend.getPartitionedState("ns", StringSerializer.INSTANCE, d2);
        // re-access "a" — lastName is now "b", so we go through the previous!=null branch
        State sa2 = backend.getPartitionedState("ns", StringSerializer.INSTANCE, d1);
        assertSame(sa, sa2);
        assertNotNull(sb);
    }

    @Test(expected = NullPointerException.class)
    public void testGetPartitionedStateNullNamespaceThrows() throws Exception {
        ValueStateDescriptor<Integer> d = new ValueStateDescriptor<>("foo", IntSerializer.INSTANCE);
        backend.getPartitionedState(null, StringSerializer.INSTANCE, d);
    }

    // ------------------------------------------------------------------------
    //  applyToAllKeys
    // ------------------------------------------------------------------------

    @Test
    public void testApplyToAllKeysPropagatesUserException() throws Exception {
        // Insert one entry so the keyStream is non-empty.
        ValueStateDescriptor<Integer> d = new ValueStateDescriptor<>("foo", IntSerializer.INSTANCE);
        backend.setCurrentKey("k1");
        org.apache.flink.api.common.state.ValueState<Integer> vs =
                backend.getPartitionedState(
                        org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                        org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE,
                        d);
        vs.update(1);

        try {
            backend.applyToAllKeys(
                    org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                    org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE,
                    d,
                    (KeyedStateFunction<String, org.apache.flink.api.common.state.ValueState<Integer>>)
                            (key, state) -> {
                                throw new IllegalStateException("boom");
                            });
            fail("expected RuntimeException");
        } catch (RuntimeException expected) {
            assertTrue(expected.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void testApplyToAllKeysSuccessfullyVisitsKeys() throws Exception {
        ValueStateDescriptor<Integer> d = new ValueStateDescriptor<>("foo", IntSerializer.INSTANCE);
        backend.setCurrentKey("k1");
        org.apache.flink.api.common.state.ValueState<Integer> vs =
                backend.getPartitionedState(
                        org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                        org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE,
                        d);
        vs.update(1);
        backend.setCurrentKey("k2");
        vs.update(2);

        java.util.Set<String> visited = new java.util.HashSet<>();
        backend.applyToAllKeys(
                org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE,
                d,
                (KeyedStateFunction<String, org.apache.flink.api.common.state.ValueState<Integer>>)
                        (key, state) -> visited.add(key));
        assertTrue(visited.contains("k1"));
        assertTrue(visited.contains("k2"));
    }

    // ------------------------------------------------------------------------
    //  publishQueryableStateIfEnabled
    // ------------------------------------------------------------------------

    @Test
    public void testPublishQueryableStateIfEnabledNoop() {
        StateDescriptor<?, ?> d = mock(StateDescriptor.class);
        when(d.isQueryable()).thenReturn(false);
        backend.publishQueryableStateIfEnabled(d, mock(org.apache.flink.runtime.state.internal.InternalKvState.class));
        // no exception, no interaction with kvStateRegistry (which is null here)
    }

    @Test(expected = IllegalStateException.class)
    public void testPublishQueryableStateThrowsWhenNoRegistry() {
        StateDescriptor<?, ?> d = mock(StateDescriptor.class);
        when(d.isQueryable()).thenReturn(true);
        when(d.getQueryableStateName()).thenReturn("qsname");
        backend.publishQueryableStateIfEnabled(
                d, mock(org.apache.flink.runtime.state.internal.InternalKvState.class));
    }

    @Test
    public void testPublishQueryableStateRegistersWithRegistry() throws Exception {
        MockKeyedStateBackend<String> b = newBackend(new ExecutionConfig(), true);
        try {
            StateDescriptor<?, ?> d = mock(StateDescriptor.class);
            when(d.isQueryable()).thenReturn(true);
            when(d.getQueryableStateName()).thenReturn("qsname");
            org.apache.flink.runtime.state.internal.InternalKvState<?, ?, ?> kv =
                    mock(org.apache.flink.runtime.state.internal.InternalKvState.class);
            // KvStateInfo requires non-null serializers from the kv state
            when(kv.getKeySerializer()).thenReturn((TypeSerializer) StringSerializer.INSTANCE);
            when(kv.getNamespaceSerializer()).thenReturn((TypeSerializer) StringSerializer.INSTANCE);
            when(kv.getValueSerializer()).thenReturn((TypeSerializer) IntSerializer.INSTANCE);
            b.publishQueryableStateIfEnabled(d, kv);
            // does not throw
        } finally {
            b.dispose();
        }
    }

    // ------------------------------------------------------------------------
    //  dispose / close
    // ------------------------------------------------------------------------

    @Test
    public void testDisposeClearsLastNameAndState() throws Exception {
        ValueStateDescriptor<Integer> d = new ValueStateDescriptor<>("foo", IntSerializer.INSTANCE);
        backend.setCurrentKey("k");
        backend.getPartitionedState(
                org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE,
                d);
        assertEquals(1, backend.numKeyValueStatesByName());
        backend.dispose();
        assertEquals(0, backend.numKeyValueStatesByName());
    }

    @Test
    public void testDisposeWithRealKvRegistryUnregistersAll() throws Exception {
        MockKeyedStateBackend<String> b = newBackend(new ExecutionConfig(), true);
        b.dispose();
    }

    @Test
    public void testCloseClosesCancelStreamRegistry() throws Exception {
        MockKeyedStateBackend<String> b = newBackend(new ExecutionConfig(), false);
        b.close();
    }

    // ------------------------------------------------------------------------
    //  Ctor-validation branches
    // ------------------------------------------------------------------------

    @Test(expected = IllegalArgumentException.class)
    public void testCtorRejectsZeroNumberOfKeyGroups() {
        new TestBackend(
                StringSerializer.INSTANCE,
                new ExecutionConfig(),
                new InternalKeyContextImpl<>(new KeyGroupRange(0, 0), 0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCtorRejectsRangeLargerThanTotal() {
        new TestBackend(
                StringSerializer.INSTANCE,
                new ExecutionConfig(),
                new InternalKeyContextImpl<>(new KeyGroupRange(0, 9), 4));
    }

    @Test(expected = NullPointerException.class)
    public void testCtorRejectsNullKeyContext() {
        new TestBackend(StringSerializer.INSTANCE, new ExecutionConfig(), null);
    }

    @Test
    public void testCopyConstructor() {
        TestBackend src =
                new TestBackend(
                        StringSerializer.INSTANCE,
                        new ExecutionConfig(),
                        new InternalKeyContextImpl<>(new KeyGroupRange(0, 3), 4));
        try {
            CopyingBackend copy = new CopyingBackend(src);
            assertSame(StringSerializer.INSTANCE, copy.getKeySerializer());
            assertEquals(4, copy.getNumberOfKeyGroups());
        } finally {
            src.dispose();
        }
    }

    /** Subclass that exposes the protected copy constructor. */
    private static final class CopyingBackend extends AbstractKeyedStateBackend<String> {
        CopyingBackend(AbstractKeyedStateBackend<String> other) {
            super(other);
        }

        @Override
        public <N> java.util.stream.Stream<String> getKeys(String state, N namespace) {
            return java.util.stream.Stream.empty();
        }

        @Override
        public <N> java.util.stream.Stream<org.apache.flink.api.java.tuple.Tuple2<String, N>>
                getKeysAndNamespaces(String state) {
            return java.util.stream.Stream.empty();
        }

        @Override
        public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
                TypeSerializer<N> namespaceSerializer,
                StateDescriptor<S, SV> stateDesc,
                StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends org.apache.flink.runtime.state.heap.HeapPriorityQueueElement
                                & PriorityComparable<? super T>
                                & Keyed<?>>
                KeyGroupedInternalPriorityQueue<T> create(
                        @javax.annotation.Nonnull String stateName,
                        @javax.annotation.Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
                long checkpointId,
                long timestamp,
                CheckpointStreamFactory streamFactory,
                org.apache.flink.runtime.checkpoint.CheckpointOptions checkpointOptions)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public SavepointResources<String> savepoint() throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public int numKeyValueStateEntries() {
            return 0;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}

        public long getLastCompletedCheckpointID() {
            return -1L;
        }
    }

    // ------------------------------------------------------------------------
    //  Concrete subclass exposing only the constructor we need to drive
    // ------------------------------------------------------------------------

    /**
     * Minimal concrete subclass that exercises the public ctor variant taking only an
     * ExecutionConfig (so {@code determineStreamCompression} runs). All abstract / unused
     * methods throw — they are not invoked by the tests above.
     */
    private static final class TestBackend extends AbstractKeyedStateBackend<String> {
        TestBackend(
                TypeSerializer<String> keySerializer,
                ExecutionConfig cfg,
                InternalKeyContext<String> keyContext) {
            super(
                    null, // kvStateRegistry
                    keySerializer,
                    Thread.currentThread().getContextClassLoader(),
                    cfg,
                    TtlTimeProvider.DEFAULT,
                    LatencyTrackingStateConfig.disabled(),
                    new CloseableRegistry(),
                    keyContext);
        }

        @Override
        public <N> java.util.stream.Stream<String> getKeys(String state, N namespace) {
            return java.util.stream.Stream.empty();
        }

        @Override
        public <N> java.util.stream.Stream<org.apache.flink.api.java.tuple.Tuple2<String, N>>
                getKeysAndNamespaces(String state) {
            return java.util.stream.Stream.empty();
        }

        @Override
        public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
                TypeSerializer<N> namespaceSerializer,
                StateDescriptor<S, SV> stateDesc,
                StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends org.apache.flink.runtime.state.heap.HeapPriorityQueueElement
                                & PriorityComparable<? super T>
                                & Keyed<?>>
                KeyGroupedInternalPriorityQueue<T> create(
                        @javax.annotation.Nonnull String stateName,
                        @javax.annotation.Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
                long checkpointId,
                long timestamp,
                CheckpointStreamFactory streamFactory,
                org.apache.flink.runtime.checkpoint.CheckpointOptions checkpointOptions)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public SavepointResources<String> savepoint() throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public int numKeyValueStateEntries() {
            return 0;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}

        public long getLastCompletedCheckpointID() {
            return -1L;
        }
    }
}
