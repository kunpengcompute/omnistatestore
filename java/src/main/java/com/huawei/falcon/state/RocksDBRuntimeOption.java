package com.huawei.falcon.state;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.huawei.falcon.state.RocksDBOptOptionsFactory.USE_RANGE_FILTER;
import static com.huawei.falcon.state.RocksDBOptOptionsFactory.USE_HASHMEMTABLE;

public class RocksDBRuntimeOption {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBRuntimeOption.class);

    public static final ConfigOption<String> custom_factory = ConfigOptions
            .key("state.backend.rocksdb.options-factory")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> prefixLength = ConfigOptions
            .key("state.backend.rocksdb.falcon.prefix-extractor.length")
            .intType()
            .defaultValue(13)
            .withDescription("prefix length of the capped prefix extractor");

    public static void optimizeValueOption(RegisteredStateMetaInfoBase metaInfoBase, ColumnFamilyOptions options){
        Configuration config = GlobalConfiguration.loadConfiguration();

        if (config.getOptional(custom_factory).isPresent() &&
                config.get(custom_factory).equals("com.huawei.falcon.state.RocksDBOptOptionsFactory") &&
                config.get(USE_HASHMEMTABLE)) {
            options.setMemTableConfig(new HashLinkedListMemTableConfig());
            options.useCappedPrefixExtractor(config.get(prefixLength));
            LOG.info("[FALCON] " + metaInfoBase.getName() + " is valueState, use HashLinkList as memTable structure.");
        } else {
            LOG.info("[FALCON] HashLinkList for memtable DISABLED. Check the value of factory and use-hash-memtable.");
        }
    }

    public static void optimizeMapOption(RegisteredStateMetaInfoBase metaInfoBase, ColumnFamilyOptions options){
        Configuration config = GlobalConfiguration.loadConfiguration();

        if (config.getOptional(custom_factory).isPresent() &&
                config.get(custom_factory).equals("com.huawei.falcon.state.RocksDBOptOptionsFactory") &&
                config.get(USE_RANGE_FILTER)) {
            options.useCappedPrefixExtractor(config.get(prefixLength));
            LOG.info("[FALCON] " + metaInfoBase.getName() + " is map, use range filter.");
        } else {
            LOG.info("[FALCON] Range Filter DISABLED. Check the value of factory and use-range-filter.");
        }
    }
}
