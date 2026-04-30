package com.huawei.falcon.state;

import java.util.List;
import java.util.Collection;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.ConfigurableRocksDBOptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.BLOOM_FILTER_BITS_PER_KEY;

/**
 * Custom implementation of ConfigurableRocksDBOptionsFactory to set
 * the RocksDB MemTable to HashSkipListMemTable.
 */
public class RocksDBOptOptionsFactory implements ConfigurableRocksDBOptionsFactory {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBOptOptionsFactory.class);

    public static final ConfigOption<Boolean> USE_PARTITION_FILTER =
            key("state.backend.rocksdb.falcon.use-partition-filter")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, every newly created SST file will contain a Partition Bloom filter. "
                                    + "It is disabled by default.");

    public static final ConfigOption<Boolean> USE_RANGE_FILTER =
            key("state.backend.rocksdb.falcon.use-range-filter")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, every newly created SST file will contain a Prefix Bloom filter. "
                                    + "It is disabled by default.");

    public static final ConfigOption<Boolean> USE_HASHMEMTABLE =
            key("state.backend.rocksdb.falcon.use-hash-memtable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, value state will use HashLinkList for memtable. "
                                    + "It is disabled by default.");

    public static final ConfigOption<Boolean> USE_L0_L1_LZ4_COMPRESSION =
            key("state.backend.rocksdb.falcon.l0-l1-use-lz4")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("If true, set compression type of rocksdb L0/L1 to LZ4.");

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions,
                                     Collection<AutoCloseable> handlesToClose) {
        Configuration config = GlobalConfiguration.loadConfiguration();
        if (config.get(USE_HASHMEMTABLE)) {
            // currentOptions.setAllowConcurrentMemtableWrite(false); // required for hash
        }

        return currentOptions;
    }
 
    @Override
    public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions,
                                                   Collection<AutoCloseable> handlesToClose) {
        Configuration config = GlobalConfiguration.loadConfiguration();

        TableFormatConfig tableFormatConfig = currentOptions.tableFormatConfig();

        BlockBasedTableConfig blockBasedTableConfig;
        if (tableFormatConfig == null) {
            blockBasedTableConfig = new BlockBasedTableConfig();
        } else {
            blockBasedTableConfig = (BlockBasedTableConfig) tableFormatConfig;
        }

        if (config.get(USE_PARTITION_FILTER) || config.get(USE_RANGE_FILTER)) {
            blockBasedTableConfig.setFilterPolicy(new BloomFilter(config.get(BLOOM_FILTER_BITS_PER_KEY), false));
        }

        if (config.get(USE_PARTITION_FILTER)) { // partition filter
            blockBasedTableConfig.setPartitionFilters(true);
            blockBasedTableConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
        }

        currentOptions.setTableFormatConfig(blockBasedTableConfig);

        // rocksdb num levels
        int numLevel = currentOptions.numLevels();
        // rocksdb perLevel compression type, which is not set by default. user can modify this param by calling
        // "currentOptions.setCompressionPerLevel(compressTypePerLevel)"
        List<CompressionType> perLevelCompressionType = currentOptions.compressionPerLevel();
        // rocksdb global compression type, which is set as snappy by default. user can modify this param be setting
        // "state.backend.rocksdb.compression.type"
        CompressionType globalCompressionType = currentOptions.compressionType();

        if (config.get(USE_L0_L1_LZ4_COMPRESSION)) {
            LOG.info("[FALCON] enable lz4 compression for rocksdb level0 and level1.");

            // resize perLevelCompressionType to numLevel
            int addCnt = numLevel - perLevelCompressionType.size();
            for (int i = 0; i < addCnt; i++) {
                perLevelCompressionType.add(globalCompressionType);
            }
            // if level_compaction_dynamic_level_bytes is true, compression_per_level[0] still determines L0, but other
            // elements of the array are based on base level (the level L0 files are merged to).
            if (currentOptions.levelCompactionDynamicLevelBytes()) {
                // set LZ4 for level0. Excessive part will be ignored.
                perLevelCompressionType.set(0, CompressionType.LZ4_COMPRESSION);
            } else {
                // set LZ4 for level0 and level1.
                perLevelCompressionType.set(0, CompressionType.LZ4_COMPRESSION);
                perLevelCompressionType.set(1, CompressionType.LZ4_COMPRESSION);
            }

            currentOptions.setCompressionPerLevel(perLevelCompressionType);
        }

        return currentOptions;
    }

    @Override
    public ReadOptions createReadOptions(
            ReadOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        Configuration config = GlobalConfiguration.loadConfiguration();
        if (config.get(USE_RANGE_FILTER)) {
            currentOptions.setTotalOrderSeek(true); // be careful if you use prefix filter for range query
        }

        return currentOptions;
    }
 
    @Override
    public RocksDBOptionsFactory configure(ReadableConfig configuration) {
        return this;
    }
}