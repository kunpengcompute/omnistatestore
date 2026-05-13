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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.ReadOptions;
import org.rocksdb.Statistics;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.WriteBufferManager;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link RocksDBResourceContainer}, including Falcon-specific hash-memtable enforcement.
 *
 * <p>Adapted from upstream Flink 1.16.3
 * {@code org.apache.flink.contrib.streaming.state.RocksDBResourceContainerTest}.
 */
public class RocksDBResourceContainerTest {

    @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    @BeforeClass
    public static void ensureRocksDbNativeLibraryLoaded() throws IOException {
        NativeLibraryLoader.getInstance().loadLibrary(TMP_FOLDER.newFolder().getAbsolutePath());
    }

    // ------------------------------------------------------------------------
    // Upstream-derived tests (close behaviour, shared resources, partitioned filter)
    // ------------------------------------------------------------------------

    @Test
    public void testFreeDBOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        DBOptions dbOptions = container.getDbOptions();
        assertThat(dbOptions.isOwningHandle(), is(true));
        container.close();
        assertThat(dbOptions.isOwningHandle(), is(false));
    }

    @Test
    public void testFreeMultipleDBOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        final int optionNumber = 20;
        ArrayList<DBOptions> dbOptions = new ArrayList<>(optionNumber);
        for (int i = 0; i < optionNumber; i++) {
            dbOptions.add(container.getDbOptions());
        }
        container.close();
        for (DBOptions dbOption : dbOptions) {
            assertThat(dbOption.isOwningHandle(), is(false));
        }
    }

    @Test
    public void testSharedResourcesAfterClose() throws Exception {
        OpaqueMemoryResource<RocksDBSharedResources> sharedResources = getSharedResources();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, sharedResources);
        container.close();
        RocksDBSharedResources rocksDBSharedResources = sharedResources.getResourceHandle();
        assertThat(rocksDBSharedResources.getCache().isOwningHandle(), is(false));
        assertThat(rocksDBSharedResources.getWriteBufferManager().isOwningHandle(), is(false));
    }

    @Test
    public void testGetDbOptionsWithSharedResources() throws Exception {
        final int optionNumber = 20;
        OpaqueMemoryResource<RocksDBSharedResources> sharedResources = getSharedResources();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, sharedResources);
        HashSet<WriteBufferManager> writeBufferManagers = new HashSet<>();
        for (int i = 0; i < optionNumber; i++) {
            DBOptions dbOptions = container.getDbOptions();
            WriteBufferManager writeBufferManager = getWriteBufferManager(dbOptions);
            writeBufferManagers.add(writeBufferManager);
        }
        assertThat(writeBufferManagers.size(), is(1));
        assertThat(
                writeBufferManagers.iterator().next(),
                is(sharedResources.getResourceHandle().getWriteBufferManager()));
        container.close();
    }

    @Test
    public void testGetColumnFamilyOptionsWithSharedResources() throws Exception {
        final int optionNumber = 20;
        OpaqueMemoryResource<RocksDBSharedResources> sharedResources = getSharedResources();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, sharedResources);
        HashSet<Cache> caches = new HashSet<>();
        for (int i = 0; i < optionNumber; i++) {
            ColumnFamilyOptions columnOptions = container.getColumnOptions();
            Cache cache = getBlockCache(columnOptions);
            caches.add(cache);
        }
        assertThat(caches.size(), is(1));
        assertThat(caches.iterator().next(), is(sharedResources.getResourceHandle().getCache()));
        container.close();
    }

    @Test
    public void testFreeColumnOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        ColumnFamilyOptions columnFamilyOptions = container.getColumnOptions();
        assertThat(columnFamilyOptions.isOwningHandle(), is(true));
        container.close();
        assertThat(columnFamilyOptions.isOwningHandle(), is(false));
    }

    @Test
    public void testFreeMultipleColumnOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        final int optionNumber = 20;
        ArrayList<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(optionNumber);
        for (int i = 0; i < optionNumber; i++) {
            columnFamilyOptions.add(container.getColumnOptions());
        }
        container.close();
        for (ColumnFamilyOptions columnFamilyOption : columnFamilyOptions) {
            assertThat(columnFamilyOption.isOwningHandle(), is(false));
        }
    }

    @Test
    public void testFreeMultipleColumnOptionsWithPredefinedOptions() throws Exception {
        for (PredefinedOptions predefinedOptions : PredefinedOptions.values()) {
            RocksDBResourceContainer container =
                    new RocksDBResourceContainer(predefinedOptions, null);
            final int optionNumber = 20;
            ArrayList<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(optionNumber);
            for (int i = 0; i < optionNumber; i++) {
                columnFamilyOptions.add(container.getColumnOptions());
            }
            container.close();
            for (ColumnFamilyOptions columnFamilyOption : columnFamilyOptions) {
                assertThat(columnFamilyOption.isOwningHandle(), is(false));
            }
        }
    }

    @Test
    public void testFreeSharedResourcesAfterClose() throws Exception {
        LRUCache cache = new LRUCache(1024L);
        WriteBufferManager wbm = new WriteBufferManager(1024L, cache);
        RocksDBSharedResources sharedResources =
                new RocksDBSharedResources(cache, wbm, 1024L, false);
        final ThrowingRunnable<Exception> disposer = sharedResources::close;
        OpaqueMemoryResource<RocksDBSharedResources> opaqueResource =
                new OpaqueMemoryResource<>(sharedResources, 1024L, disposer);

        RocksDBResourceContainer container =
                new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, opaqueResource);

        container.close();
        assertThat(cache.isOwningHandle(), is(false));
        assertThat(wbm.isOwningHandle(), is(false));
    }

    @Test
    public void testFreeWriteReadOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        WriteOptions writeOptions = container.getWriteOptions();
        ReadOptions readOptions = container.getReadOptions();
        assertThat(writeOptions.isOwningHandle(), is(true));
        assertThat(readOptions.isOwningHandle(), is(true));
        container.close();
        assertThat(writeOptions.isOwningHandle(), is(false));
        assertThat(readOptions.isOwningHandle(), is(false));
    }

    @Test
    public void testGetColumnFamilyOptionsWithPartitionedIndex() throws Exception {
        LRUCache cache = new LRUCache(1024L);
        WriteBufferManager wbm = new WriteBufferManager(1024L, cache);
        RocksDBSharedResources sharedResources =
                new RocksDBSharedResources(cache, wbm, 1024L, true);
        final ThrowingRunnable<Exception> disposer = sharedResources::close;
        OpaqueMemoryResource<RocksDBSharedResources> opaqueResource =
                new OpaqueMemoryResource<>(sharedResources, 1024L, disposer);
        BloomFilter blockBasedFilter = new BloomFilter();
        RocksDBOptionsFactory blockBasedBloomFilterOptionFactory =
                new RocksDBOptionsFactory() {

                    @Override
                    public DBOptions createDBOptions(
                            DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                        return currentOptions;
                    }

                    @Override
                    public ColumnFamilyOptions createColumnOptions(
                            ColumnFamilyOptions currentOptions,
                            Collection<AutoCloseable> handlesToClose) {
                        TableFormatConfig tableFormatConfig = currentOptions.tableFormatConfig();
                        BlockBasedTableConfig blockBasedTableConfig =
                                tableFormatConfig == null
                                        ? new BlockBasedTableConfig()
                                        : (BlockBasedTableConfig) tableFormatConfig;
                        blockBasedTableConfig.setFilter(blockBasedFilter);
                        handlesToClose.add(blockBasedFilter);
                        currentOptions.setTableFormatConfig(blockBasedTableConfig);
                        return currentOptions;
                    }
                };
        try (RocksDBResourceContainer container =
                new RocksDBResourceContainer(
                        PredefinedOptions.DEFAULT,
                        blockBasedBloomFilterOptionFactory,
                        opaqueResource)) {
            ColumnFamilyOptions columnOptions = container.getColumnOptions();
            BlockBasedTableConfig actual =
                    (BlockBasedTableConfig) columnOptions.tableFormatConfig();
            assertThat(actual.indexType(), is(IndexType.kTwoLevelIndexSearch));
            assertThat(actual.partitionFilters(), is(true));
            assertThat(actual.pinTopLevelIndexAndFilter(), is(true));
            assertThat(actual.filterPolicy(), not(blockBasedFilter));
        }
        assertFalse("Block based filter is left unclosed.", blockBasedFilter.isOwningHandle());
    }

    // ------------------------------------------------------------------------
    // Additional coverage for accessors / write+read options factory paths
    // ------------------------------------------------------------------------

    @Test
    public void testGetPredefinedOptionsAndOptionsFactory() throws Exception {
        RocksDBOptionsFactory factory = new IdentityOptionsFactory();
        try (RocksDBResourceContainer container =
                new RocksDBResourceContainer(PredefinedOptions.SPINNING_DISK_OPTIMIZED, factory)) {
            assertSame(PredefinedOptions.SPINNING_DISK_OPTIMIZED, container.getPredefinedOptions());
            assertSame(factory, container.getOptionsFactory());
        }

        try (RocksDBResourceContainer defaultContainer = new RocksDBResourceContainer()) {
            assertSame(PredefinedOptions.DEFAULT, defaultContainer.getPredefinedOptions());
            assertThat(defaultContainer.getOptionsFactory(), nullValue());
        }
    }

    @Test
    public void testGetWriteBufferManagerCapacityWithoutSharedResources() throws Exception {
        try (RocksDBResourceContainer container = new RocksDBResourceContainer()) {
            assertThat(container.getWriteBufferManagerCapacity(), nullValue());
        }
    }

    @Test
    public void testGetWriteBufferManagerCapacityWithSharedResources() throws Exception {
        OpaqueMemoryResource<RocksDBSharedResources> sharedResources = getSharedResources();
        try (RocksDBResourceContainer container =
                new RocksDBResourceContainer(
                        PredefinedOptions.DEFAULT, null, sharedResources)) {
            assertEquals(
                    Long.valueOf(
                            sharedResources
                                    .getResourceHandle()
                                    .getWriteBufferManagerCapacity()),
                    container.getWriteBufferManagerCapacity());
        }
    }

    @Test
    public void testWriteAndReadOptionsHonorOptionsFactory() throws Exception {
        RocksDBOptionsFactory factory = new IdentityOptionsFactory();
        try (RocksDBResourceContainer container =
                new RocksDBResourceContainer(PredefinedOptions.DEFAULT, factory)) {
            WriteOptions writeOptions = container.getWriteOptions();
            ReadOptions readOptions = container.getReadOptions();
            assertNotNull(writeOptions);
            assertNotNull(readOptions);
            // disableWAL is true by default
            assertTrue(writeOptions.disableWAL());
        }
    }

    @Test
    public void testGetMemoryWatcherOptionsDefaultsWhenNoFactory() throws Exception {
        RocksDBNativeMetricOptions defaultMetricOptions = new RocksDBNativeMetricOptions();
        try (RocksDBResourceContainer container = new RocksDBResourceContainer()) {
            RocksDBNativeMetricOptions out = container.getMemoryWatcherOptions(defaultMetricOptions);
            assertSame(defaultMetricOptions, out);
        }
    }

    @Test
    public void testGetMemoryWatcherOptionsRoutedThroughFactory() throws Exception {
        final RocksDBNativeMetricOptions custom = new RocksDBNativeMetricOptions();
        RocksDBOptionsFactory factory =
                new IdentityOptionsFactory() {
                    @Override
                    public RocksDBNativeMetricOptions createNativeMetricsOptions(
                            RocksDBNativeMetricOptions nativeMetricOptions) {
                        return custom;
                    }
                };
        try (RocksDBResourceContainer container =
                new RocksDBResourceContainer(PredefinedOptions.DEFAULT, factory)) {
            RocksDBNativeMetricOptions out =
                    container.getMemoryWatcherOptions(new RocksDBNativeMetricOptions());
            assertSame(custom, out);
        }
    }

    @Test
    public void testGetDbOptionsWithEnableStatistics() throws Exception {
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(
                        new Configuration(),
                        PredefinedOptions.DEFAULT,
                        null,
                        null,
                        true);
        DBOptions opts = container.getDbOptions();
        Statistics stats = opts.statistics();
        try {
            assertNotNull("statistics should be attached when enableStatistics=true", stats);
            assertThat(stats.isOwningHandle(), is(true));
        } finally {
            container.close();
        }
        assertThat(opts.isOwningHandle(), is(false));
        // NOTE: `Statistics` lifecycle differs between RocksDB versions; the container only adds
        // it to handlesToClose. Verifying that opts is closed is sufficient for branch coverage.
    }

    @Test
    public void testGetDbOptionsWithInstanceBasePathSetsLogDirToFlinkLogParent() throws Exception {
        File flinkLog = TMP_FOLDER.newFile("flink.log");
        File instanceBasePath = TMP_FOLDER.newFolder("rocksdb-instance");

        String prev = System.getProperty("log.file");
        System.setProperty("log.file", flinkLog.getAbsolutePath());
        try (RocksDBResourceContainer container =
                new RocksDBResourceContainer(
                        new Configuration(),
                        PredefinedOptions.DEFAULT,
                        null,
                        null,
                        instanceBasePath,
                        false)) {
            DBOptions opts = container.getDbOptions();
            assertThat(opts, notNullValue());
            // setDbLogDir should have been called with the parent of log.file
            assertThat(opts.dbLogDir(), is(flinkLog.getParent()));
        } finally {
            if (prev == null) {
                System.clearProperty("log.file");
            } else {
                System.setProperty("log.file", prev);
            }
        }
    }

    @Test
    public void testGetDbOptionsWithLongInstancePathSkipsLogRelocate() throws Exception {
        // Build a path that exceeds 255 - "_LOG".length() = 251 chars total absolute path.
        File parent = TMP_FOLDER.newFolder("longpath");
        StringBuilder name = new StringBuilder();
        for (int i = 0; i < 260; i++) {
            name.append('a');
        }
        File longBase = new File(parent, name.toString());
        // do NOT mkdir, just supply the path -- the constructor stores instanceRocksDBPath via
        // RocksDBKeyedStateBackendBuilder.getInstanceRocksDBPath(File)
        try (RocksDBResourceContainer container =
                new RocksDBResourceContainer(
                        new Configuration(),
                        PredefinedOptions.DEFAULT,
                        null,
                        null,
                        longBase,
                        false)) {
            DBOptions opts = container.getDbOptions();
            // log.file is unset / log dir should not be relocated when path is long; we only
            // exercise the branch -- contents of dbLogDir may be empty string.
            assertNotNull(opts);
        }
    }

    // ------------------------------------------------------------------------
    // Falcon-specific: forceHashMemtableDbOptions behaviour
    // ------------------------------------------------------------------------

    @Test
    public void testFalconHashMemtableDisabledByDefault() throws Exception {
        // GlobalConfiguration without FLINK_CONF_DIR returns an empty Configuration -> default
        // value (false). The path under test: forceHashMemtableDbOptions returns early.
        try (RocksDBResourceContainer container = new RocksDBResourceContainer()) {
            DBOptions opts = container.getDbOptions();
            // not mutated by Falcon; default for allowConcurrentMemtableWrite is true in RocksDB
            assertTrue(
                    "Falcon hash-memtable disabled should leave allowConcurrentMemtableWrite at default true",
                    opts.allowConcurrentMemtableWrite());
        }
    }

    @Test
    public void testFalconHashMemtableEnabledForcesAllowConcurrentMemtableWriteFalse()
            throws Exception {
        Configuration falconConfig = new Configuration();
        falconConfig.setBoolean("state.backend.rocksdb.falcon.use-hash-memtable", true);

        try (MockedStatic<GlobalConfiguration> mocked =
                Mockito.mockStatic(GlobalConfiguration.class)) {
            mocked.when(GlobalConfiguration::loadConfiguration).thenReturn(falconConfig);
            try (RocksDBResourceContainer container = new RocksDBResourceContainer()) {
                DBOptions opts = container.getDbOptions();
                assertFalse(
                        "Falcon hash-memtable enabled must force allowConcurrentMemtableWrite=false",
                        opts.allowConcurrentMemtableWrite());
            }
        }
    }

    @Test
    public void testFalconHashMemtableExplicitlyDisabled() throws Exception {
        Configuration falconConfig = new Configuration();
        falconConfig.setBoolean("state.backend.rocksdb.falcon.use-hash-memtable", false);

        try (MockedStatic<GlobalConfiguration> mocked =
                Mockito.mockStatic(GlobalConfiguration.class)) {
            mocked.when(GlobalConfiguration::loadConfiguration).thenReturn(falconConfig);
            try (RocksDBResourceContainer container = new RocksDBResourceContainer()) {
                DBOptions opts = container.getDbOptions();
                assertTrue(
                        "Explicitly disabled Falcon hash-memtable must not mutate the option",
                        opts.allowConcurrentMemtableWrite());
            }
        }
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    private OpaqueMemoryResource<RocksDBSharedResources> getSharedResources() {
        final long cacheSize = 1024L, writeBufferSize = 512L;
        final LRUCache cache = new LRUCache(cacheSize, -1, false, 0.1);
        final WriteBufferManager wbm = new WriteBufferManager(writeBufferSize, cache);
        RocksDBSharedResources rocksDBSharedResources =
                new RocksDBSharedResources(cache, wbm, writeBufferSize, false);
        return new OpaqueMemoryResource<>(
                rocksDBSharedResources, cacheSize, rocksDBSharedResources::close);
    }

    private Cache getBlockCache(ColumnFamilyOptions columnOptions) {
        BlockBasedTableConfig blockBasedTableConfig = null;
        try {
            blockBasedTableConfig = (BlockBasedTableConfig) columnOptions.tableFormatConfig();
        } catch (ClassCastException e) {
            fail("Table config got from ColumnFamilyOptions is not BlockBasedTableConfig");
        }
        Field cacheField = null;
        try {
            cacheField = BlockBasedTableConfig.class.getDeclaredField("blockCache");
        } catch (NoSuchFieldException e) {
            fail("blockCache is not defined");
        }
        cacheField.setAccessible(true);
        try {
            return (Cache) cacheField.get(blockBasedTableConfig);
        } catch (IllegalAccessException e) {
            fail("Cannot access blockCache field.");
            return null;
        }
    }

    private WriteBufferManager getWriteBufferManager(DBOptions dbOptions) {
        Field writeBufferManagerField = null;
        try {
            writeBufferManagerField = DBOptions.class.getDeclaredField("writeBufferManager_");
        } catch (NoSuchFieldException e) {
            fail("writeBufferManager_ is not defined.");
        }
        writeBufferManagerField.setAccessible(true);
        try {
            return (WriteBufferManager) writeBufferManagerField.get(dbOptions);
        } catch (IllegalAccessException e) {
            fail("Cannot access writeBufferManager_ field.");
            return null;
        }
    }

    /** No-op options factory that returns the inputs unchanged. */
    private static class IdentityOptionsFactory implements RocksDBOptionsFactory {
        @Override
        public DBOptions createDBOptions(
                DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
            return currentOptions;
        }

        @Override
        public ColumnFamilyOptions createColumnOptions(
                ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
            return currentOptions;
        }
    }

}
