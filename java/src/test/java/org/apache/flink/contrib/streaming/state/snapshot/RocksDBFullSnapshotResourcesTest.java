package org.apache.flink.contrib.streaming.state.snapshot;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueStateSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.ResourceGuard;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link RocksDBFullSnapshotResources}. */
public class RocksDBFullSnapshotResourcesTest {

    @ClassRule public static final TemporaryFolder TMP_DIR = new TemporaryFolder();

    @BeforeClass
    public static void loadRocksLibrary() throws Exception {
        NativeLibraryLoader.getInstance().loadLibrary(TMP_DIR.newFolder().getAbsolutePath());
    }

    // ----- helpers -----------------------------------------------------------

    private static RocksDBKeyedStateBackend.RocksDbKvStateInfo plainKvStateInfo(String name) {
        ColumnFamilyHandle handle = mock(ColumnFamilyHandle.class);
        RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, String> metaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.VALUE,
                        name,
                        VoidNamespaceSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        return new RocksDBKeyedStateBackend.RocksDbKvStateInfo(handle, metaInfo);
    }

    /** A kv-state info whose meta info is NOT a RegisteredKeyValueStateBackendMetaInfo. */
    private static RocksDBKeyedStateBackend.RocksDbKvStateInfo nonKvStateInfo(String name) {
        ColumnFamilyHandle handle = mock(ColumnFamilyHandle.class);
        RegisteredStateMetaInfoBase metaInfo = mock(RegisteredStateMetaInfoBase.class);
        when(metaInfo.getName()).thenReturn(name);
        when(metaInfo.snapshot()).thenReturn(mock(StateMetaInfoSnapshot.class));
        return new RocksDBKeyedStateBackend.RocksDbKvStateInfo(handle, metaInfo);
    }

    /** A kv-state info whose snapshot transform factory yields a transformer (covers else branch). */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RocksDBKeyedStateBackend.RocksDbKvStateInfo transformingKvStateInfo(String name) {
        ColumnFamilyHandle handle = mock(ColumnFamilyHandle.class);
        StateSnapshotTransformFactory<String> factory = mock(StateSnapshotTransformFactory.class);
        StateSnapshotTransformer<byte[]> transformer = mock(StateSnapshotTransformer.class);
        when(factory.createForSerializedState()).thenReturn(Optional.of(transformer));
        RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, String> metaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.VALUE,
                        name,
                        VoidNamespaceSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        factory);
        return new RocksDBKeyedStateBackend.RocksDbKvStateInfo(handle, metaInfo);
    }

    private RocksDBFullSnapshotResources<Integer> buildResources(
            List<RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStates,
            List<HeapPriorityQueueStateSnapshot<?>> pqSnapshots,
            RocksDB db,
            Snapshot snapshot,
            KeyGroupRange keyGroupRange) throws IOException {

        ResourceGuard rg = new ResourceGuard();
        ResourceGuard.Lease lease = rg.acquireResource();

        // Build StateMetaInfoSnapshot list to mirror what create() would do.
        java.util.List<StateMetaInfoSnapshot> metaSnapshots = new java.util.ArrayList<>();
        for (RocksDBKeyedStateBackend.RocksDbKvStateInfo info : kvStates) {
            metaSnapshots.add(info.metaInfo.snapshot());
        }

        return new RocksDBFullSnapshotResources<>(
                lease,
                snapshot,
                kvStates,
                pqSnapshots,
                metaSnapshots,
                db,
                /* keyGroupPrefixBytes= */ 1,
                keyGroupRange,
                IntSerializer.INSTANCE,
                UncompressedStreamCompressionDecorator.INSTANCE);
    }

    // ----- tests -------------------------------------------------------------

    @Test
    public void testCreateBuildsResourcesAndCallsDbApis() throws Exception {
        // arrange
        ColumnFamilyHandle h1 = mock(ColumnFamilyHandle.class);
        RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, String> meta1 =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.VALUE,
                        "kv-a",
                        VoidNamespaceSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        RocksDBKeyedStateBackend.RocksDbKvStateInfo kv1 =
                new RocksDBKeyedStateBackend.RocksDbKvStateInfo(h1, meta1);

        LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvMap =
                new LinkedHashMap<>();
        kvMap.put("kv-a", kv1);

        // priority queue wrapper
        HeapPriorityQueueSnapshotRestoreWrapper<?> pqWrapper =
                mock(HeapPriorityQueueSnapshotRestoreWrapper.class);
        RegisteredPriorityQueueStateBackendMetaInfo<?> pqMeta =
                mock(RegisteredPriorityQueueStateBackendMetaInfo.class);
        StateMetaInfoSnapshot pqSnap = mock(StateMetaInfoSnapshot.class);
        when(pqMeta.snapshot()).thenReturn(pqSnap);
        Mockito.doReturn(pqMeta).when(pqWrapper).getMetaInfo();
        HeapPriorityQueueStateSnapshot<?> pqStateSnapshot =
                mock(HeapPriorityQueueStateSnapshot.class);
        Mockito.doReturn(pqStateSnapshot).when(pqWrapper).stateSnapshot();

        java.util.Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> pqMap =
                new LinkedHashMap<>();
        pqMap.put("pq-a", pqWrapper);

        ResourceGuard rg = new ResourceGuard();
        Snapshot snapshot = mock(Snapshot.class);
        RocksDB db = mock(RocksDB.class);
        when(db.getSnapshot()).thenReturn(snapshot);

        KeyGroupRange kgr = new KeyGroupRange(0, 1);

        // act
        RocksDBFullSnapshotResources<Integer> res =
                RocksDBFullSnapshotResources.create(
                        kvMap,
                        pqMap,
                        db,
                        rg,
                        kgr,
                        IntSerializer.INSTANCE,
                        /* keyGroupPrefixBytes= */ 1,
                        UncompressedStreamCompressionDecorator.INSTANCE);

        // assert
        assertNotNull(res);
        assertEquals(2, res.getMetaInfoSnapshots().size());
        assertSame(kgr, res.getKeyGroupRange());
        assertSame(IntSerializer.INSTANCE, res.getKeySerializer());
        assertSame(
                UncompressedStreamCompressionDecorator.INSTANCE,
                res.getStreamCompressionDecorator());
        verify(db, times(1)).getSnapshot();
        // create() should have acquired the lease, so guard count is 1
        assertEquals(1, rg.getLeaseCount());
    }

    @Test
    public void testGettersExposeConstructorArgs() throws Exception {
        Snapshot snapshot = mock(Snapshot.class);
        RocksDB db = mock(RocksDB.class);
        KeyGroupRange kgr = new KeyGroupRange(0, 0);
        RocksDBKeyedStateBackend.RocksDbKvStateInfo kv = plainKvStateInfo("kv");

        RocksDBFullSnapshotResources<Integer> res =
                buildResources(
                        Collections.singletonList(kv),
                        Collections.emptyList(),
                        db,
                        snapshot,
                        kgr);

        assertEquals(1, res.getMetaInfoSnapshots().size());
        assertSame(kgr, res.getKeyGroupRange());
        assertSame(IntSerializer.INSTANCE, res.getKeySerializer());
        assertSame(
                UncompressedStreamCompressionDecorator.INSTANCE,
                res.getStreamCompressionDecorator());
    }

    @Test
    public void testReleaseClosesAllResources() throws Exception {
        Snapshot snapshot = mock(Snapshot.class);
        RocksDB db = mock(RocksDB.class);
        KeyGroupRange kgr = new KeyGroupRange(0, 0);

        ResourceGuard rg = new ResourceGuard();
        ResourceGuard.Lease lease = rg.acquireResource();
        assertEquals(1, rg.getLeaseCount());

        RocksDBFullSnapshotResources<Integer> res =
                new RocksDBFullSnapshotResources<>(
                        lease,
                        snapshot,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        db,
                        1,
                        kgr,
                        IntSerializer.INSTANCE,
                        UncompressedStreamCompressionDecorator.INSTANCE);

        res.release();

        verify(db, times(1)).releaseSnapshot(snapshot);
        verify(snapshot, atLeastOnce()).close();
        // closing the lease decrements the guard count back to 0
        assertEquals(0, rg.getLeaseCount());
    }

    @Test
    public void testFillMetaDataHandlesAllMetaInfoVariants() throws Exception {
        // mix all three branches: non-KV metaInfo, KV with no transform, KV with transform
        RocksDBKeyedStateBackend.RocksDbKvStateInfo plain = plainKvStateInfo("plain");
        RocksDBKeyedStateBackend.RocksDbKvStateInfo transforming = transformingKvStateInfo("xform");
        RocksDBKeyedStateBackend.RocksDbKvStateInfo nonKv = nonKvStateInfo("non-kv");

        Snapshot snapshot = mock(Snapshot.class);
        RocksDB db = mock(RocksDB.class);
        KeyGroupRange kgr = new KeyGroupRange(0, 1);

        RocksDBFullSnapshotResources<Integer> res =
                buildResources(
                        Arrays.asList(plain, transforming, nonKv),
                        Collections.emptyList(),
                        db,
                        snapshot,
                        kgr);

        assertEquals(3, res.getMetaInfoSnapshots().size());
    }

    @Test
    public void testCreateKVStateIteratorHappyPath() throws Exception {
        // Two KV states: one with transformer, one without -> exercises both wrapper variants.
        RocksDBKeyedStateBackend.RocksDbKvStateInfo plain = plainKvStateInfo("plain");
        RocksDBKeyedStateBackend.RocksDbKvStateInfo xform = transformingKvStateInfo("xform");

        Snapshot snapshot = mock(Snapshot.class);
        RocksDB db = mock(RocksDB.class);
        // db.newIterator(handle, readOptions) -> mock RocksIterator
        RocksIterator it1 = mock(RocksIterator.class);
        RocksIterator it2 = mock(RocksIterator.class);
        when(db.newIterator(any(ColumnFamilyHandle.class), any(ReadOptions.class)))
                .thenReturn(it1, it2);
        KeyGroupRange kgr = new KeyGroupRange(0, 0);

        RocksDBFullSnapshotResources<Integer> res =
                buildResources(
                        Arrays.asList(plain, xform),
                        Collections.emptyList(),
                        db,
                        snapshot,
                        kgr);

        try (KeyValueStateIterator merged = res.createKVStateIterator()) {
            assertNotNull(merged);
        }
        verify(db, times(2)).newIterator(any(ColumnFamilyHandle.class), any(ReadOptions.class));
    }

    @Test
    public void testCreateKVStateIteratorWithEmptyState() throws Exception {
        // Empty kv-state and empty PQ -> exercises the createKVStateIterator() path
        // without instantiating any per-state iterator.
        Snapshot snapshot = mock(Snapshot.class);
        RocksDB db = mock(RocksDB.class);
        KeyGroupRange kgr = new KeyGroupRange(0, 1);

        RocksDBFullSnapshotResources<Integer> res =
                buildResources(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        db,
                        snapshot,
                        kgr);

        try (KeyValueStateIterator merged = res.createKVStateIterator()) {
            assertNotNull(merged);
        }
        verify(db, never()).newIterator(any(ColumnFamilyHandle.class), any(ReadOptions.class));
    }

    @Test
    public void testCreateKVStateIteratorWrapsExceptionAsIOException() throws Exception {
        RocksDBKeyedStateBackend.RocksDbKvStateInfo plain = plainKvStateInfo("plain");

        Snapshot snapshot = mock(Snapshot.class);
        RocksDB db = mock(RocksDB.class);
        when(db.newIterator(any(ColumnFamilyHandle.class), any(ReadOptions.class)))
                .thenThrow(new RuntimeException("boom"));
        KeyGroupRange kgr = new KeyGroupRange(0, 0);

        RocksDBFullSnapshotResources<Integer> res =
                buildResources(
                        Collections.singletonList(plain),
                        Collections.emptyList(),
                        db,
                        snapshot,
                        kgr);

        try {
            res.createKVStateIterator();
            fail("expected IOException");
        } catch (IOException expected) {
            assertTrue(expected.getMessage().contains("Error creating merge iterator"));
        }
    }

    @Test
    public void testSetTotalOrderSeekDisabledByDefault() throws Exception {
        Snapshot snapshot = mock(Snapshot.class);
        RocksDB db = mock(RocksDB.class);
        KeyGroupRange kgr = new KeyGroupRange(0, 0);
        RocksDBFullSnapshotResources<Integer> res =
                buildResources(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        db,
                        snapshot,
                        kgr);

        ReadOptions ro = mock(ReadOptions.class);
        // default GlobalConfiguration -> empty config -> flag false -> setTotalOrderSeek not called
        res.setTotalOrderSeek(ro);
        verify(ro, never()).setTotalOrderSeek(true);
    }

    @Test
    public void testSetTotalOrderSeekEnabledViaConfig() throws Exception {
        Snapshot snapshot = mock(Snapshot.class);
        RocksDB db = mock(RocksDB.class);
        KeyGroupRange kgr = new KeyGroupRange(0, 0);
        RocksDBFullSnapshotResources<Integer> res =
                buildResources(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        db,
                        snapshot,
                        kgr);

        Configuration cfg = new Configuration();
        cfg.setBoolean("state.backend.rocksdb.falcon.use-hash-memtable", true);

        ReadOptions ro = mock(ReadOptions.class);
        try (MockedStatic<GlobalConfiguration> mocked =
                Mockito.mockStatic(GlobalConfiguration.class)) {
            mocked.when(GlobalConfiguration::loadConfiguration).thenReturn(cfg);
            res.setTotalOrderSeek(ro);
        }
        verify(ro, times(1)).setTotalOrderSeek(true);
    }
}
