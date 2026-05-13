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

package org.apache.flink.contrib.streaming.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for the {@link RocksDBOperationUtils}. */
public class RocksDBOperationsUtilsTest {

    @ClassRule
    public static final TemporaryFolder TMP_DIR = new TemporaryFolder();

    @BeforeClass
    public static void loadRocksLibrary() throws Exception {
        NativeLibraryLoader.getInstance().loadLibrary(TMP_DIR.newFolder().getAbsolutePath());
    }


    @Test
    public void testSanityCheckArenaBlockSize() {
        long testWriteBufferSize = 56 * 1024 * 1024L;
        long testDefaultArenaSize = RocksDBMemoryControllerUtils.calculateRocksDBDefaultArenaBlockSize(
            testWriteBufferSize
        );
        long testWriteBufferCapacityBoundary = (testDefaultArenaSize * 8) / 7;
        assertTrue(
            "The sanity check should pass with default arena block size",
            RocksDBOperationUtils.sanityCheckArenaBlockSize(testWriteBufferSize, 0, testWriteBufferCapacityBoundary)
        );
        assertTrue(
            "The sanity check should pass with default arena block size given as argument",
            RocksDBOperationUtils.sanityCheckArenaBlockSize(
                testWriteBufferSize,
                testDefaultArenaSize,
                testWriteBufferCapacityBoundary
            )
        );
        assertTrue(
            "The sanity check should pass when the configured arena block size is smaller than the boundary.",
            RocksDBOperationUtils.sanityCheckArenaBlockSize(
                testWriteBufferSize,
                testDefaultArenaSize - 1,
                testWriteBufferCapacityBoundary
            )
        );
        assertFalse(
            "The sanity check should fail when the configured arena block size is higher than the boundary.",
            RocksDBOperationUtils.sanityCheckArenaBlockSize(
                testWriteBufferSize,
                testDefaultArenaSize + 1,
                testWriteBufferCapacityBoundary
            )
        );
    }

    private static String getLongString(int numChars) {
        final StringBuilder builder = new StringBuilder();
        for (int i = numChars; i > 0; --i) {
            builder.append('a');
        }
        return builder.toString();
    }

    @Test
    public void testRegisterKvStateInformationWithoutMonitor() {
        Map<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> map = new HashMap<>();
        RocksDBKeyedStateBackend.RocksDbKvStateInfo info = new RocksDBKeyedStateBackend.RocksDbKvStateInfo(
            mock(ColumnFamilyHandle.class),
            new RegisteredKeyValueStateBackendMetaInfo<>(
                StateDescriptor.Type.VALUE,
                "foo",
                VoidNamespaceSerializer.INSTANCE,
                IntSerializer.INSTANCE
            )
        );
        RocksDBOperationUtils.registerKvStateInformation(map, null, "cf", info);
        assertEquals(info, map.get("cf"));
    }

    @Test
    public void testRegisterKvStateInformationWithMonitor() {
        Map<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> map = new HashMap<>();
        ColumnFamilyHandle handle = mock(ColumnFamilyHandle.class);
        RocksDBKeyedStateBackend.RocksDbKvStateInfo info = new RocksDBKeyedStateBackend.RocksDbKvStateInfo(
            handle,
            new RegisteredKeyValueStateBackendMetaInfo<>(
                StateDescriptor.Type.VALUE,
                "foo",
                VoidNamespaceSerializer.INSTANCE,
                IntSerializer.INSTANCE
            )
        );
        RocksDBNativeMetricMonitor monitor = mock(RocksDBNativeMetricMonitor.class);

        RocksDBOperationUtils.registerKvStateInformation(map, monitor, "cf", info);

        assertEquals(info, map.get("cf"));
        verify(monitor, times(1)).registerColumnFamily("cf", handle);
    }

    @Test
    public void testCreateColumnFamilyWrapsRocksDBException() throws Exception {
        ColumnFamilyDescriptor desc = new ColumnFamilyDescriptor("test".getBytes());
        RocksDB db = mock(RocksDB.class);
        RocksDBException expected = new RocksDBException("simulated open failure");
        when(db.createColumnFamily(any(ColumnFamilyDescriptor.class))).thenThrow(expected);

        RegisteredKeyValueStateBackendMetaInfo<org.apache.flink.runtime.state.VoidNamespace, Integer> meta =
            new RegisteredKeyValueStateBackendMetaInfo<>(
                StateDescriptor.Type.VALUE,
                "test",
                VoidNamespaceSerializer.INSTANCE,
                IntSerializer.INSTANCE
            );

        Function<String, ColumnFamilyOptions> factory = name -> new ColumnFamilyOptions();

        try {
            RocksDBOperationUtils.createStateInfo(meta, db, factory, null, null);
            org.junit.Assert.fail("expected FlinkRuntimeException");
        } catch (FlinkRuntimeException e) {
            assertTrue(e.getMessage().contains("Error creating ColumnFamilyHandle"));
            assertEquals(expected, e.getCause());
        }
    }

    @Test
    public void testAddColumnFamilyOptionsToCloseLaterNullHandle() {
        java.util.List<ColumnFamilyOptions> sink = new java.util.ArrayList<>();
        RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(sink, null);
        assertEquals(0, sink.size());
    }

    @Test
    public void testAddColumnFamilyOptionsToCloseLaterRocksDBExceptionSwallowed() throws Exception {
        java.util.List<ColumnFamilyOptions> sink = new java.util.ArrayList<>();
        ColumnFamilyHandle handle = mock(ColumnFamilyHandle.class);
        when(handle.getDescriptor()).thenThrow(new RocksDBException("boom"));
        // Must not throw
        RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(sink, handle);
        assertEquals(0, sink.size());
    }

    @Test
    public void testAddColumnFamilyOptionsToCloseLaterAddsOptions() throws Exception {
        java.util.List<ColumnFamilyOptions> sink = new java.util.ArrayList<>();
        ColumnFamilyHandle handle = mock(ColumnFamilyHandle.class);
        ColumnFamilyOptions opt = new ColumnFamilyOptions();
        ColumnFamilyDescriptor descriptor = mock(ColumnFamilyDescriptor.class);
        when(descriptor.getOptions()).thenReturn(opt);
        when(handle.getDescriptor()).thenReturn(descriptor);

        RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(sink, handle);
        assertEquals(1, sink.size());
        assertEquals(opt, sink.get(0));
        opt.close();
    }

    @Test
    public void testAllocateSharedCachesIfConfiguredReturnsNullWhenUnconfigured() throws Exception {
        RocksDBMemoryConfiguration cfg = mock(RocksDBMemoryConfiguration.class);
        when(cfg.isUsingFixedMemoryPerSlot()).thenReturn(false);
        when(cfg.isUsingManagedMemory()).thenReturn(false);
        Logger log = LoggerFactory.getLogger("test");
        OpaqueMemoryResource<RocksDBSharedResources> result = RocksDBOperationUtils.allocateSharedCachesIfConfigured(
            cfg,
            mock(MemoryManager.class),
            0.5,
            log
        );
        assertNull(result);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAllocateSharedCachesFixedSlotMemory() throws Exception {
        RocksDBMemoryConfiguration cfg = mock(RocksDBMemoryConfiguration.class);
        when(cfg.isUsingFixedMemoryPerSlot()).thenReturn(true);
        when(cfg.isUsingManagedMemory()).thenReturn(false);
        when(cfg.getHighPriorityPoolRatio()).thenReturn(0.1);
        when(cfg.getWriteBufferRatio()).thenReturn(0.5);
        when(cfg.isUsingPartitionedIndexFilters()).thenReturn(false);
        when(cfg.getFixedMemoryPerSlot()).thenReturn(MemorySize.parse("64mb"));

        MemoryManager mm = mock(MemoryManager.class);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        OpaqueMemoryResource<RocksDBSharedResources> stub = (OpaqueMemoryResource) mock(OpaqueMemoryResource.class);
        org.mockito.Mockito.doReturn(stub).when(mm).getExternalSharedMemoryResource(anyString(), any(), anyLong());

        OpaqueMemoryResource<RocksDBSharedResources> result = RocksDBOperationUtils.allocateSharedCachesIfConfigured(
            cfg,
            mm,
            0.5,
            LoggerFactory.getLogger("test")
        );
        assertEquals(stub, result);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAllocateSharedCachesManagedMemory() throws Exception {
        RocksDBMemoryConfiguration cfg = mock(RocksDBMemoryConfiguration.class);
        when(cfg.isUsingFixedMemoryPerSlot()).thenReturn(false);
        when(cfg.isUsingManagedMemory()).thenReturn(true);
        when(cfg.getHighPriorityPoolRatio()).thenReturn(0.1);
        when(cfg.getWriteBufferRatio()).thenReturn(0.5);
        when(cfg.isUsingPartitionedIndexFilters()).thenReturn(false);

        MemoryManager mm = mock(MemoryManager.class);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        OpaqueMemoryResource<RocksDBSharedResources> stub = (OpaqueMemoryResource) mock(OpaqueMemoryResource.class);
        org.mockito.Mockito.doReturn(stub)
            .when(mm)
            .getSharedMemoryResourceForManagedMemory(anyString(), any(), anyDouble());

        OpaqueMemoryResource<RocksDBSharedResources> result = RocksDBOperationUtils.allocateSharedCachesIfConfigured(
            cfg,
            mm,
            0.5,
            LoggerFactory.getLogger("test")
        );
        assertEquals(stub, result);
    }

    @Test
    public void testAllocateSharedCachesWrapsExceptionAsIOException() throws Exception {
        RocksDBMemoryConfiguration cfg = mock(RocksDBMemoryConfiguration.class);
        when(cfg.isUsingFixedMemoryPerSlot()).thenReturn(false);
        when(cfg.isUsingManagedMemory()).thenReturn(true);
        when(cfg.getHighPriorityPoolRatio()).thenReturn(0.1);
        when(cfg.getWriteBufferRatio()).thenReturn(0.5);
        when(cfg.isUsingPartitionedIndexFilters()).thenReturn(false);

        MemoryManager mm = mock(MemoryManager.class);
        when(mm.getSharedMemoryResourceForManagedMemory(anyString(), any(), anyDouble())).thenThrow(
            new RuntimeException("boom")
        );

        try {
            RocksDBOperationUtils.allocateSharedCachesIfConfigured(cfg, mm, 0.5, LoggerFactory.getLogger("test"));
            org.junit.Assert.fail("expected IOException");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("Failed to acquire shared cache resource"));
        }
    }

    @Test
    public void testCreateColumnFamilyDescriptorMergeOperatorPropagation() {
        Function<String, ColumnFamilyOptions> factory = name -> new ColumnFamilyOptions();
        RegisteredKeyValueStateBackendMetaInfo<org.apache.flink.runtime.state.VoidNamespace, Integer> meta =
            new RegisteredKeyValueStateBackendMetaInfo<>(
                StateDescriptor.Type.VALUE,
                "merge-test",
                VoidNamespaceSerializer.INSTANCE,
                IntSerializer.INSTANCE
            );
        meta.setMergeOperatorName("uint64add");

        ColumnFamilyDescriptor d = RocksDBOperationUtils.createColumnFamilyDescriptor(meta, factory, null, null);
        assertNotNull(d);
        d.getOptions().close();
    }

    @Test
    public void testCreateColumnFamilyDescriptorWithoutMergeUsesDefault() {
        Function<String, ColumnFamilyOptions> factory = name -> new ColumnFamilyOptions();
        RegisteredKeyValueStateBackendMetaInfo<org.apache.flink.runtime.state.VoidNamespace, Integer> meta =
            new RegisteredKeyValueStateBackendMetaInfo<>(
                StateDescriptor.Type.MAP,
                "no-merge",
                VoidNamespaceSerializer.INSTANCE,
                IntSerializer.INSTANCE
            );

        ColumnFamilyDescriptor d = RocksDBOperationUtils.createColumnFamilyDescriptor(meta, factory, null, null);
        assertNotNull(d);
        d.getOptions().close();
    }
}
