package com.huawei.falcon.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mockStatic;

public class RocksDBOptOptionsFactoryTest {

    private RocksDBOptOptionsFactory factory;

    @Before
    public void setup() {
        factory = new RocksDBOptOptionsFactory();
    }

    private MockedStatic<GlobalConfiguration> stubGlobalConfig(Configuration conf) {
        MockedStatic<GlobalConfiguration> mocked = mockStatic(GlobalConfiguration.class);
        mocked.when(GlobalConfiguration::loadConfiguration).thenReturn(conf);
        return mocked;
    }

    @Test
    public void testCreateDBOptionsWithHashMemTable() {
        Configuration conf = new Configuration();
        conf.set(RocksDBOptOptionsFactory.USE_HASHMEMTABLE, true);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
             org.rocksdb.DBOptions options = new org.rocksdb.DBOptions()) {
            org.rocksdb.DBOptions result = factory.createDBOptions(options, new ArrayList<>());
            assertFalse(result.allowConcurrentMemtableWrite());
        }
    }

    @Test
    public void testCreateDBOptionsWithoutHashMemTable() {
        Configuration conf = new Configuration();
        conf.set(RocksDBOptOptionsFactory.USE_HASHMEMTABLE, false);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
             org.rocksdb.DBOptions options = new org.rocksdb.DBOptions()) {
            org.rocksdb.DBOptions result = factory.createDBOptions(options, new ArrayList<>());
            assertTrue(result.allowConcurrentMemtableWrite());
        }
    }

    @Test
    public void testCreateColumnOptionsSetsTableConfigWhenNull() {
        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(new Configuration());
             org.rocksdb.ColumnFamilyOptions options = new org.rocksdb.ColumnFamilyOptions()) {
            assertNull(options.tableFormatConfig());
            ArrayList<AutoCloseable> handlesToClose = new ArrayList<>();
            org.rocksdb.ColumnFamilyOptions result = factory.createColumnOptions(options, handlesToClose);
            assertNotNull(result.tableFormatConfig());
            assertTrue(result.tableFormatConfig() instanceof org.rocksdb.BlockBasedTableConfig);
            // No BloomFilter created when no filter config is set
            assertTrue(handlesToClose.isEmpty());
        }
    }

    @Test
    public void testCreateColumnOptionsKeepsExistingTableConfig() {
        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(new Configuration());
             org.rocksdb.ColumnFamilyOptions options = new org.rocksdb.ColumnFamilyOptions()) {
            org.rocksdb.BlockBasedTableConfig original = new org.rocksdb.BlockBasedTableConfig();
            options.setTableFormatConfig(original);
            ArrayList<AutoCloseable> handlesToClose = new ArrayList<>();
            org.rocksdb.ColumnFamilyOptions result = factory.createColumnOptions(options, handlesToClose);
            assertSame(original, result.tableFormatConfig());
            assertTrue(handlesToClose.isEmpty());
        }
    }

    @Test
    public void testCreateColumnOptionsWithPartitionFilter() {
        Configuration conf = new Configuration();
        conf.set(RocksDBOptOptionsFactory.USE_PARTITION_FILTER, true);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
             org.rocksdb.ColumnFamilyOptions options = new org.rocksdb.ColumnFamilyOptions()) {
            ArrayList<AutoCloseable> handlesToClose = new ArrayList<>();
            org.rocksdb.ColumnFamilyOptions result = factory.createColumnOptions(options, handlesToClose);
            org.rocksdb.BlockBasedTableConfig cfg =
                    (org.rocksdb.BlockBasedTableConfig) result.tableFormatConfig();
            assertTrue(cfg.partitionFilters());
            assertEquals(org.rocksdb.IndexType.kTwoLevelIndexSearch, cfg.indexType());
            assertNotNull(cfg.filterPolicy());
            assertEquals(0, handlesToClose.size());
        }
    }

    @Test
    public void testCreateColumnOptionsWithRangeFilterOnly() {
        Configuration conf = new Configuration();
        conf.set(RocksDBOptOptionsFactory.USE_RANGE_FILTER, true);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
             org.rocksdb.ColumnFamilyOptions options = new org.rocksdb.ColumnFamilyOptions()) {
            ArrayList<AutoCloseable> handlesToClose = new ArrayList<>();
            org.rocksdb.ColumnFamilyOptions result = factory.createColumnOptions(options, handlesToClose);
            org.rocksdb.BlockBasedTableConfig cfg =
                    (org.rocksdb.BlockBasedTableConfig) result.tableFormatConfig();
            assertNotNull(cfg.filterPolicy());
            assertFalse(cfg.partitionFilters());
            assertEquals(0, handlesToClose.size());
        }
    }

    @Test
    public void testCreateColumnOptionsWithL0L1Lz4CompressionDynamicLevelBytes() {
        Configuration conf = new Configuration();
        conf.set(RocksDBOptOptionsFactory.USE_L0_L1_LZ4_COMPRESSION, true);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
             org.rocksdb.ColumnFamilyOptions options = new org.rocksdb.ColumnFamilyOptions()) {
            options.setLevelCompactionDynamicLevelBytes(true);
            List<org.rocksdb.CompressionType> compressionTypes = new ArrayList<>();
            int numLevels = options.numLevels();
            for (int i = 0; i < numLevels; i++) {
                compressionTypes.add(options.compressionType());
            }
            options.setCompressionPerLevel(compressionTypes);

            org.rocksdb.ColumnFamilyOptions result = factory.createColumnOptions(options, new ArrayList<>());

            List<org.rocksdb.CompressionType> perLevel = result.compressionPerLevel();
            assertEquals(org.rocksdb.CompressionType.LZ4_COMPRESSION, perLevel.get(0));
        }
    }

    @Test
    public void testCreateColumnOptionsWithL0L1Lz4CompressionNonDynamicLevelBytes() {
        Configuration conf = new Configuration();
        conf.set(RocksDBOptOptionsFactory.USE_L0_L1_LZ4_COMPRESSION, true);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
             org.rocksdb.ColumnFamilyOptions options = new org.rocksdb.ColumnFamilyOptions()) {
            options.setLevelCompactionDynamicLevelBytes(false);
            List<org.rocksdb.CompressionType> compressionTypes = new ArrayList<>();
            int numLevels = options.numLevels();
            org.rocksdb.CompressionType globalCompression = options.compressionType();
            for (int i = 0; i < numLevels; i++) {
                compressionTypes.add(options.compressionType());
            }
            options.setCompressionPerLevel(compressionTypes);

            org.rocksdb.ColumnFamilyOptions result = factory.createColumnOptions(options, new ArrayList<>());

            List<org.rocksdb.CompressionType> perLevel = result.compressionPerLevel();
            assertEquals(org.rocksdb.CompressionType.LZ4_COMPRESSION, perLevel.get(0));
            assertEquals(org.rocksdb.CompressionType.LZ4_COMPRESSION, perLevel.get(1));
            // Levels beyond L0/L1 should retain the original global compression type
            for (int i = 2; i < numLevels; i++) {
                assertEquals(
                    "Level " + i + " should retain original compression type",
                    globalCompression,
                    perLevel.get(i));
            }
        }
    }

    @Test
    public void testCreateReadOptionsWithRangeFilter() {
        Configuration conf = new Configuration();
        conf.set(RocksDBOptOptionsFactory.USE_RANGE_FILTER, true);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
             org.rocksdb.ReadOptions options = new org.rocksdb.ReadOptions()) {
            org.rocksdb.ReadOptions result = factory.createReadOptions(options, new ArrayList<>());
            assertTrue(result.totalOrderSeek());
        }
    }

    @Test
    public void testCreateReadOptionsWithHashMemTable() {
        Configuration conf = new Configuration();
        conf.set(RocksDBOptOptionsFactory.USE_HASHMEMTABLE, true);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
             org.rocksdb.ReadOptions options = new org.rocksdb.ReadOptions()) {
            org.rocksdb.ReadOptions result = factory.createReadOptions(options, new ArrayList<>());
            assertTrue(result.totalOrderSeek());
        }
    }

    @Test
    public void testCreateReadOptionsWithNoConfig() {
        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(new Configuration());
             org.rocksdb.ReadOptions options = new org.rocksdb.ReadOptions()) {
            org.rocksdb.ReadOptions result = factory.createReadOptions(options, new ArrayList<>());
            assertFalse(result.totalOrderSeek());
        }
    }

    @Test
    public void testConfigure() {
        Configuration conf = new Configuration();
        RocksDBOptionsFactory result = factory.configure(conf);
        assertSame(factory, result);
    }

    /**
     * Exercises the {@code addCnt > 0} loop in {@code createColumnOptions} —
     * when {@code compressionPerLevel} is shorter than {@code numLevels},
     * the factory pads the list with the global compression type before
     * stamping LZ4 onto level 0/1.
     */
    @Test
    public void testCreateColumnOptionsWithLz4PadsCompressionPerLevel() {
        Configuration conf = new Configuration();
        conf.set(RocksDBOptOptionsFactory.USE_L0_L1_LZ4_COMPRESSION, true);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
             org.rocksdb.ColumnFamilyOptions options = new org.rocksdb.ColumnFamilyOptions()) {
            // do NOT pre-fill compressionPerLevel — it starts empty by default
            assertTrue(options.compressionPerLevel().isEmpty());
            options.setLevelCompactionDynamicLevelBytes(false);

            org.rocksdb.ColumnFamilyOptions result =
                    factory.createColumnOptions(options, new ArrayList<>());

            List<org.rocksdb.CompressionType> perLevel = result.compressionPerLevel();
            assertEquals(options.numLevels(), perLevel.size());
            assertEquals(org.rocksdb.CompressionType.LZ4_COMPRESSION, perLevel.get(0));
            assertEquals(org.rocksdb.CompressionType.LZ4_COMPRESSION, perLevel.get(1));
        }
    }

    /**
     * Both partition and range filter enabled — verifies the second branch
     * of the {@code (USE_PARTITION_FILTER || USE_RANGE_FILTER)} short-circuit
     * is exercised together with partitionFilter being on.
     */
    @Test
    public void testCreateColumnOptionsWithPartitionAndRangeFilter() {
        Configuration conf = new Configuration();
        conf.set(RocksDBOptOptionsFactory.USE_PARTITION_FILTER, true);
        conf.set(RocksDBOptOptionsFactory.USE_RANGE_FILTER, true);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
             org.rocksdb.ColumnFamilyOptions options = new org.rocksdb.ColumnFamilyOptions()) {
            ArrayList<AutoCloseable> handlesToClose = new ArrayList<>();
            org.rocksdb.ColumnFamilyOptions result =
                    factory.createColumnOptions(options, handlesToClose);
            org.rocksdb.BlockBasedTableConfig cfg =
                    (org.rocksdb.BlockBasedTableConfig) result.tableFormatConfig();
            assertNotNull(cfg.filterPolicy());
            assertTrue(cfg.partitionFilters());
            assertEquals(0, handlesToClose.size());
        }
    }

    @Test
    public void testConfigOptionMetadata() {
        assertEquals("state.backend.rocksdb.falcon.use-partition-filter",
                RocksDBOptOptionsFactory.USE_PARTITION_FILTER.key());
        assertEquals("state.backend.rocksdb.falcon.use-range-filter",
                RocksDBOptOptionsFactory.USE_RANGE_FILTER.key());
        assertEquals("state.backend.rocksdb.falcon.use-hash-memtable",
                RocksDBOptOptionsFactory.USE_HASHMEMTABLE.key());
        assertEquals("state.backend.rocksdb.falcon.l0-l1-use-lz4",
                RocksDBOptOptionsFactory.USE_L0_L1_LZ4_COMPRESSION.key());
        assertEquals(Boolean.FALSE,
                RocksDBOptOptionsFactory.USE_PARTITION_FILTER.defaultValue());
    }
}
