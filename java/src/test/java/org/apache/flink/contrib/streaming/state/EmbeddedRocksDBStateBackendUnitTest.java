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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.util.TernaryBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.io.File;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Standalone tests for {@link EmbeddedRocksDBStateBackend}.
 *
 * <p>These tests exercise the public configuration / accessor surface of {@link
 * EmbeddedRocksDBStateBackend} directly, without inheriting from {@code StateBackendTestBase}. The
 * heavy keyed-state-backend tests are intentionally avoided here because creating a {@code
 * RocksDBValueState} eagerly loads {@code com.huawei.falcon.state.cache.FalconValueState}, which in
 * turn requires {@code libfalcon.so} on the classpath. That native library is not available in
 * this unit-test environment (macOS dev / generic CI without the Kunpeng build) — see {@code
 * FalconValueStateTest} which documents the same constraint.
 */
public class EmbeddedRocksDBStateBackendUnitTest {

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    // ------------------------------------------------------------------------
    //  Constructors
    // ------------------------------------------------------------------------

    @Test
    public void testDefaultConstructorDefaults() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        // default for incremental checkpointing falls back to CheckpointingOptions default.
        assertEquals(
                CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue().booleanValue(),
                backend.isIncrementalCheckpointsEnabled());
        assertNull(backend.getDbStoragePaths());
        assertNull(backend.getRocksDBOptions());
        assertNotNull(backend.getMemoryConfiguration());
        // PriorityQueueStateType defaults to RocksDBOptions.TIMER_SERVICE_FACTORY.defaultValue()
        assertEquals(
                RocksDBOptions.TIMER_SERVICE_FACTORY.defaultValue(),
                backend.getPriorityQueueStateType());
        // PredefinedOptions falls back to DEFAULT.
        assertEquals(PredefinedOptions.DEFAULT, backend.getPredefinedOptions());
        // Number of transfer threads falls back to RocksDBOptions default.
        assertEquals(
                RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM.defaultValue().intValue(),
                backend.getNumberOfTransferThreads());
    }

    @Test
    public void testBooleanConstructorEnablesIncremental() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);
        assertTrue(backend.isIncrementalCheckpointsEnabled());

        EmbeddedRocksDBStateBackend backend2 = new EmbeddedRocksDBStateBackend(false);
        assertFalse(backend2.isIncrementalCheckpointsEnabled());
    }

    @Test
    public void testTernaryBooleanConstructor() {
        EmbeddedRocksDBStateBackend backend =
                new EmbeddedRocksDBStateBackend(TernaryBoolean.TRUE);
        assertTrue(backend.isIncrementalCheckpointsEnabled());
    }

    // ------------------------------------------------------------------------
    //  configure() / re-configuration
    // ------------------------------------------------------------------------

    @Test
    public void testConfigureCopiesIncrementalFlag() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();

        Configuration config = new Configuration();
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        EmbeddedRocksDBStateBackend reconfigured =
                backend.configure(config, getClass().getClassLoader());

        assertTrue(reconfigured.isIncrementalCheckpointsEnabled());
    }

    @Test
    public void testConfigurePreservesExistingOverrides() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);
        backend.setNumberOfTransferThreads(7);
        backend.setWriteBatchSize(123);

        Configuration config = new Configuration();
        EmbeddedRocksDBStateBackend reconfigured =
                backend.configure(config, getClass().getClassLoader());

        assertTrue(reconfigured.isIncrementalCheckpointsEnabled());
        assertEquals(7, reconfigured.getNumberOfTransferThreads());
        assertEquals(123L, reconfigured.getWriteBatchSize());
    }

    @Test
    public void testConfigurePicksUpLocalDirectoriesFromConfig() throws Exception {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();

        File dir1 = tempFolder.newFolder();
        File dir2 = tempFolder.newFolder();

        Configuration config = new Configuration();
        config.set(
                RocksDBOptions.LOCAL_DIRECTORIES,
                dir1.getAbsolutePath() + "," + dir2.getAbsolutePath());

        EmbeddedRocksDBStateBackend reconfigured =
                backend.configure(config, getClass().getClassLoader());

        String[] paths = reconfigured.getDbStoragePaths();
        assertNotNull(paths);
        assertEquals(2, paths.length);
        assertEquals(dir1.getAbsolutePath(), paths[0]);
        assertEquals(dir2.getAbsolutePath(), paths[1]);
    }

    @Test
    public void testConfigureWrapsRelativeDirectoryError() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();

        Configuration config = new Configuration();
        // relative paths are explicitly rejected by setDbStoragePaths().
        config.set(RocksDBOptions.LOCAL_DIRECTORIES, "relative/path");

        try {
            backend.configure(config, getClass().getClassLoader());
            fail("expected IllegalConfigurationException for relative local rocksdb dir");
        } catch (IllegalConfigurationException expected) {
            // ok
        }
    }

    @Test
    public void testConfigurePreservesOverriddenLocalDirectories() throws Exception {
        File dir = tempFolder.newFolder();
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        backend.setDbStoragePath(dir.getAbsolutePath());

        Configuration config = new Configuration();
        // even though the config sets a different path, the explicit setter wins.
        File otherDir = tempFolder.newFolder();
        config.set(RocksDBOptions.LOCAL_DIRECTORIES, otherDir.getAbsolutePath());

        EmbeddedRocksDBStateBackend reconfigured =
                backend.configure(config, getClass().getClassLoader());
        assertArrayEquals(
                new String[] {dir.getAbsolutePath()}, reconfigured.getDbStoragePaths());
    }

    @Test
    public void testConfigurePicksPriorityQueueFromConfig() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        Configuration config = new Configuration();
        config.set(
                RocksDBOptions.TIMER_SERVICE_FACTORY,
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP);
        EmbeddedRocksDBStateBackend reconfigured =
                backend.configure(config, getClass().getClassLoader());
        assertEquals(
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP,
                reconfigured.getPriorityQueueStateType());
    }

    @Test
    public void testConfigurePreservesPriorityQueueOverride() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        backend.setPriorityQueueStateType(
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
        Configuration config = new Configuration();
        config.set(
                RocksDBOptions.TIMER_SERVICE_FACTORY,
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP);
        EmbeddedRocksDBStateBackend reconfigured =
                backend.configure(config, getClass().getClassLoader());
        assertEquals(
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB,
                reconfigured.getPriorityQueueStateType());
    }

    @Test
    public void testConfigurePicksUpPredefinedOptions() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        Configuration config = new Configuration();
        config.set(RocksDBOptions.PREDEFINED_OPTIONS, PredefinedOptions.SPINNING_DISK_OPTIMIZED.name());
        EmbeddedRocksDBStateBackend reconfigured =
                backend.configure(config, getClass().getClassLoader());
        assertEquals(PredefinedOptions.SPINNING_DISK_OPTIMIZED, reconfigured.getPredefinedOptions());
    }

    @Test
    public void testConfigureLoadsOptionsFactoryByClassName() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        Configuration config = new Configuration();
        config.setString(RocksDBOptions.OPTIONS_FACTORY.key(), TestOptionsFactory.class.getName());
        EmbeddedRocksDBStateBackend reconfigured =
                backend.configure(config, getClass().getClassLoader());
        assertNotNull(reconfigured.getRocksDBOptions());
        assertTrue(reconfigured.getRocksDBOptions() instanceof TestOptionsFactory);
    }

    @Test
    public void testConfigureLoadsConfigurableOptionsFactoryByClassName() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        Configuration config = new Configuration();
        config.setString(
                RocksDBOptions.OPTIONS_FACTORY.key(),
                TestConfigurableOptionsFactory.class.getName());
        EmbeddedRocksDBStateBackend reconfigured =
                backend.configure(config, getClass().getClassLoader());
        assertNotNull(reconfigured.getRocksDBOptions());
        assertTrue(reconfigured.getRocksDBOptions() instanceof TestConfigurableOptionsFactory);
        // configure() was called during loading.
        assertTrue(((TestConfigurableOptionsFactory) reconfigured.getRocksDBOptions()).configured);
    }

    @Test
    public void testConfigureFailsForUnknownOptionsFactoryClassName() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        Configuration config = new Configuration();
        config.setString(
                RocksDBOptions.OPTIONS_FACTORY.key(), "no.such.Class$DefinitelyMissing");
        try {
            backend.configure(config, getClass().getClassLoader());
            fail("expected FlinkRuntimeException wrapping DynamicCodeLoadingException");
        } catch (RuntimeException expected) {
            // ok
        }
    }

    @Test
    public void testConfigureSkipsDeprecatedDefaultConfigurableOptionsFactory() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        Configuration config = new Configuration();
        config.setString(
                RocksDBOptions.OPTIONS_FACTORY.key(),
                DefaultConfigurableOptionsFactory.class.getName());
        EmbeddedRocksDBStateBackend reconfigured =
                backend.configure(config, getClass().getClassLoader());
        // The branch logs a warning and leaves the options factory unset.
        assertNull(reconfigured.getRocksDBOptions());
    }

    @Test
    public void testConfigurePreservesProgrammaticOptionsFactory() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        TestOptionsFactory factory = new TestOptionsFactory();
        backend.setRocksDBOptions(factory);
        EmbeddedRocksDBStateBackend reconfigured =
                backend.configure(new Configuration(), getClass().getClassLoader());
        assertSame(factory, reconfigured.getRocksDBOptions());
    }

    @Test
    public void testConfigureCallsConfigureOnConfigurableOptionsFactory() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        TestConfigurableOptionsFactory factory = new TestConfigurableOptionsFactory();
        backend.setRocksDBOptions(factory);
        EmbeddedRocksDBStateBackend reconfigured =
                backend.configure(new Configuration(), getClass().getClassLoader());
        assertTrue(factory.configured);
        // configure() returns `this`, so reconfigured backend uses same instance.
        assertSame(factory, reconfigured.getRocksDBOptions());
    }

    // ------------------------------------------------------------------------
    //  Setters / getters
    // ------------------------------------------------------------------------

    @Test
    public void testSetDbStoragePathSingle() throws Exception {
        File dir = tempFolder.newFolder();
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        backend.setDbStoragePath(dir.getAbsolutePath());
        assertArrayEquals(new String[] {dir.getAbsolutePath()}, backend.getDbStoragePaths());
    }

    @Test
    public void testSetDbStoragePathNullClearsPaths() throws Exception {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        backend.setDbStoragePath(tempFolder.newFolder().getAbsolutePath());
        assertNotNull(backend.getDbStoragePaths());
        backend.setDbStoragePath(null);
        assertNull(backend.getDbStoragePaths());
    }

    @Test
    public void testSetDbStoragePathFileUri() throws Exception {
        File dir = tempFolder.newFolder();
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        backend.setDbStoragePath(dir.toURI().toString());
        assertArrayEquals(new String[] {dir.getAbsolutePath()}, backend.getDbStoragePaths());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetDbStoragePathRejectsNonLocalScheme() {
        new EmbeddedRocksDBStateBackend().setDbStoragePath("hdfs:///foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetDbStoragePathRejectsRelativePath() {
        new EmbeddedRocksDBStateBackend().setDbStoragePath("relative/dir");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetDbStoragePathsRejectsEmptyArray() {
        new EmbeddedRocksDBStateBackend().setDbStoragePaths(new String[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetDbStoragePathsRejectsNullElement() {
        new EmbeddedRocksDBStateBackend().setDbStoragePaths(new String[] {null});
    }

    @Test
    public void testSetNumberOfTransferThreadsAccessor() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        backend.setNumberOfTransferThreads(4);
        assertEquals(4, backend.getNumberOfTransferThreads());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNumberOfTransferThreadsRejectsNonPositive() {
        new EmbeddedRocksDBStateBackend().setNumberOfTransferThreads(0);
    }

    @Test
    public void testSetWriteBatchSizeAccessor() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        backend.setWriteBatchSize(0);
        assertEquals(0L, backend.getWriteBatchSize());
        backend.setWriteBatchSize(1024);
        assertEquals(1024L, backend.getWriteBatchSize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetWriteBatchSizeRejectsNegative() {
        new EmbeddedRocksDBStateBackend().setWriteBatchSize(-1);
    }

    @Test
    public void testSetPredefinedOptionsAccessor() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        backend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED);
        assertEquals(PredefinedOptions.FLASH_SSD_OPTIMIZED, backend.getPredefinedOptions());
    }

    @Test(expected = NullPointerException.class)
    public void testSetPredefinedOptionsRejectsNull() {
        new EmbeddedRocksDBStateBackend().setPredefinedOptions(null);
    }

    @Test
    public void testSetRocksDBOptionsAccessor() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        TestOptionsFactory factory = new TestOptionsFactory();
        backend.setRocksDBOptions(factory);
        assertSame(factory, backend.getRocksDBOptions());
    }

    @Test
    public void testSetPriorityQueueStateTypeAccessor() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        backend.setPriorityQueueStateType(
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP);
        assertEquals(
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP,
                backend.getPriorityQueueStateType());
    }

    @Test(expected = NullPointerException.class)
    public void testSetPriorityQueueStateTypeRejectsNull() {
        new EmbeddedRocksDBStateBackend().setPriorityQueueStateType(null);
    }

    @Test
    public void testGetMemoryConfigurationReturnsNonNull() {
        assertNotNull(new EmbeddedRocksDBStateBackend().getMemoryConfiguration());
    }

    @Test
    public void testSupportsNoClaimRestoreModeAndSavepointFormat() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        assertTrue(backend.supportsNoClaimRestoreMode());
        for (SavepointFormatType type : SavepointFormatType.values()) {
            assertTrue(backend.supportsSavepointFormat(type));
        }
    }

    @Test
    public void testToStringDoesNotThrowAndContainsKeyFields() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);
        String s = backend.toString();
        assertNotNull(s);
        assertTrue(s.contains("EmbeddedRocksDBStateBackend"));
        assertTrue(s.contains("enableIncrementalCheckpointing"));
        assertTrue(s.contains("writeBatchSize"));
        assertTrue(s.contains("numberOfTransferThreads"));
    }

    @Test
    public void testGetOverlapFractionThresholdDefault() {
        EmbeddedRocksDBStateBackend backend =
                new EmbeddedRocksDBStateBackend()
                        .configure(new Configuration(), getClass().getClassLoader());
        // After configure(), the threshold should equal RESTORE_OVERLAP_FRACTION_THRESHOLD default.
        assertEquals(
                RocksDBConfigurableOptions.RESTORE_OVERLAP_FRACTION_THRESHOLD
                        .defaultValue()
                        .doubleValue(),
                backend.getOverlapFractionThreshold(),
                1e-9);
    }

    @Test
    public void testConfigureOverlapFractionFromConfig() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        Configuration config = new Configuration();
        config.set(RocksDBConfigurableOptions.RESTORE_OVERLAP_FRACTION_THRESHOLD, 0.4);
        EmbeddedRocksDBStateBackend reconfigured =
                backend.configure(config, getClass().getClassLoader());
        assertEquals(0.4, reconfigured.getOverlapFractionThreshold(), 1e-9);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConfigureRejectsOverlapFractionAboveOne() {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        Configuration config = new Configuration();
        config.set(RocksDBConfigurableOptions.RESTORE_OVERLAP_FRACTION_THRESHOLD, 1.4);
        backend.configure(config, getClass().getClassLoader());
    }

    // ------------------------------------------------------------------------
    //  createOptionsAndResourceContainer
    // ------------------------------------------------------------------------

    @Test
    public void testCreateOptionsAndResourceContainerVisibleForTesting() throws Exception {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        File dir = tempFolder.newFolder();
        try (RocksDBResourceContainer container =
                backend.createOptionsAndResourceContainer(dir)) {
            assertNotNull(container);
        }
    }

    @Test
    public void testCreateOptionsAndResourceContainerWithNullPath() throws Exception {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        try (RocksDBResourceContainer container =
                backend.createOptionsAndResourceContainer(null)) {
            assertNotNull(container);
        }
    }

    // ------------------------------------------------------------------------
    //  ensureRocksDBIsLoaded / resetRocksDBLoadedFlag
    // ------------------------------------------------------------------------

    @Test
    public void testEnsureRocksDBIsLoadedIsIdempotent() throws Exception {
        File dir = tempFolder.newFolder();
        EmbeddedRocksDBStateBackend.ensureRocksDBIsLoaded(dir.getAbsolutePath());
        // Calling twice must not throw or reload.
        EmbeddedRocksDBStateBackend.ensureRocksDBIsLoaded(dir.getAbsolutePath());
    }

    @Test
    public void testResetRocksDBLoadedFlagThenReload() throws Exception {
        File dir = tempFolder.newFolder();
        EmbeddedRocksDBStateBackend.ensureRocksDBIsLoaded(dir.getAbsolutePath());
        EmbeddedRocksDBStateBackend.resetRocksDBLoadedFlag();
        // After reset, re-loading must still succeed (the static flag is private; we use
        // the reset path on rocksdb internals only — the backend's own flag stays true,
        // which is the documented behaviour of this @VisibleForTesting helper).
        EmbeddedRocksDBStateBackend.ensureRocksDBIsLoaded(dir.getAbsolutePath());
    }

    // ------------------------------------------------------------------------
    //  createOperatorStateBackend
    // ------------------------------------------------------------------------

    @Test
    public void testCreateOperatorStateBackend() throws Exception {
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        try (MockEnvironment env = new MockEnvironmentBuilder().build();
                CloseableRegistry registry = new CloseableRegistry()) {
            OperatorStateBackend osb =
                    backend.createOperatorStateBackend(
                            env, "op-id", Collections.emptyList(), registry);
            try {
                assertNotNull(osb);
            } finally {
                osb.close();
                osb.dispose();
            }
        }
    }

    // ------------------------------------------------------------------------
    //  PriorityQueueStateType enum
    // ------------------------------------------------------------------------

    @Test
    public void testPriorityQueueStateTypeEnum() {
        for (EmbeddedRocksDBStateBackend.PriorityQueueStateType t :
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.values()) {
            assertNotNull(t.getDescription());
        }
        assertEquals(
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP,
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.valueOf("HEAP"));
        assertEquals(
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB,
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.valueOf("ROCKSDB"));
    }

    // ------------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------------

    /** Plain {@link RocksDBOptionsFactory} used by reflective loader tests. */
    public static final class TestOptionsFactory implements RocksDBOptionsFactory {
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

    /** {@link ConfigurableRocksDBOptionsFactory} that records whether {@code configure} was called. */
    public static final class TestConfigurableOptionsFactory
            implements ConfigurableRocksDBOptionsFactory {
        boolean configured = false;

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

        @Override
        public RocksDBOptionsFactory configure(
                org.apache.flink.configuration.ReadableConfig configuration) {
            this.configured = true;
            return this;
        }
    }
}
