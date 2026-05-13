package com.huawei.falcon.state;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockStatic;

import com.huawei.falcon.state.utils.DummyRegisteredStateMetaInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.MemTableConfig;

/** Tests for {@link RocksDBRuntimeOption}. */
public class RocksDBRuntimeOptionTest {

    private static final String FACTORY_CLASS = "com.huawei.falcon.state.RocksDBOptOptionsFactory";

    private Configuration conf;

    @Before
    public void setup() {
        conf = new Configuration();
    }

    private MockedStatic<GlobalConfiguration> stubGlobalConfig(Configuration c) {
        MockedStatic<GlobalConfiguration> mocked = mockStatic(GlobalConfiguration.class);
        mocked.when(GlobalConfiguration::loadConfiguration).thenReturn(c);
        return mocked;
    }

    private RegisteredStateMetaInfoBase dummyMeta(String name) {
        return new DummyRegisteredStateMetaInfo(name);
    }

    // ---------- optimizeValueOption ----------

    /** Hash memtable enabled and state name is in the supported list -> options mutated. */
    @Test
    public void testOptimizeValueOptionEnabled_accState() {
        conf.set(RocksDBRuntimeOption.custom_factory, FACTORY_CLASS);
        conf.set(RocksDBOptOptionsFactory.USE_HASHMEMTABLE, true);
        conf.set(RocksDBRuntimeOption.prefixLength, 7);

        try (
            MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
            ColumnFamilyOptions options = new ColumnFamilyOptions()
        ) {
            RocksDBRuntimeOption.optimizeValueOption(dummyMeta("accState"), options);
            MemTableConfig mt = options.memTableConfig();
            assertNotNull(mt);
            assertTrue(mt instanceof HashLinkedListMemTableConfig);
        }
    }

    /** Hash memtable enabled with each remaining whitelisted name. */
    @Test
    public void testOptimizeValueOptionEnabled_allWhitelistedNames() {
        conf.set(RocksDBRuntimeOption.custom_factory, FACTORY_CLASS);
        conf.set(RocksDBOptOptionsFactory.USE_HASHMEMTABLE, true);
        conf.set(RocksDBRuntimeOption.prefixLength, 5);

        String[] names = {
            "Top1-Rank-State",
            "deduplicate-state",
            "distinctAcc_0_null_state",
            "distinctAcc_1_null_state",
        };

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf)) {
            for (String name : names) {
                try (ColumnFamilyOptions options = new ColumnFamilyOptions()) {
                    RocksDBRuntimeOption.optimizeValueOption(dummyMeta(name), options);
                    MemTableConfig mt = options.memTableConfig();
                    assertNotNull(mt);
                    assertTrue(mt instanceof HashLinkedListMemTableConfig);
                }
            }
        }
    }

    /** State name is not in the supported list — even with hash memtable on, no opt applied. */
    @Test
    public void testOptimizeValueOptionUnknownStateName() {
        conf.set(RocksDBRuntimeOption.custom_factory, FACTORY_CLASS);
        conf.set(RocksDBOptOptionsFactory.USE_HASHMEMTABLE, true);

        try (
            MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
            ColumnFamilyOptions options = new ColumnFamilyOptions()
        ) {
            // Falls into the else branch (no HashLinkedListMemTableConfig set).
            RocksDBRuntimeOption.optimizeValueOption(dummyMeta("not-a-known-state"), options);
            // No mutation: memTableConfig must remain null (default)
            assertNull(options.memTableConfig());
        }
    }

    /** custom_factory option absent -> no mutation. */
    @Test
    public void testOptimizeValueOptionNoFactory() {
        conf.set(RocksDBOptOptionsFactory.USE_HASHMEMTABLE, true);

        try (
            MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
            ColumnFamilyOptions options = new ColumnFamilyOptions()
        ) {
            RocksDBRuntimeOption.optimizeValueOption(dummyMeta("accState"), options);
            // No mutation: memTableConfig must remain null (default)
            assertNull(options.memTableConfig());
        }
    }

    /** custom_factory present but pointing to a different class -> no mutation. */
    @Test
    public void testOptimizeValueOptionFactoryMismatch() {
        conf.set(RocksDBRuntimeOption.custom_factory, "OtherFactory");
        conf.set(RocksDBOptOptionsFactory.USE_HASHMEMTABLE, true);

        try (
            MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
            ColumnFamilyOptions options = new ColumnFamilyOptions()
        ) {
            RocksDBRuntimeOption.optimizeValueOption(dummyMeta("accState"), options);
            // No mutation: memTableConfig must remain null (default)
            assertNull(options.memTableConfig());
        }
    }

    /** Factory is set but USE_HASHMEMTABLE is false -> no mutation. */
    @Test
    public void testOptimizeValueOptionNoHashMemTable() {
        conf.set(RocksDBRuntimeOption.custom_factory, FACTORY_CLASS);
        conf.set(RocksDBOptOptionsFactory.USE_HASHMEMTABLE, false);

        try (
            MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
            ColumnFamilyOptions options = new ColumnFamilyOptions()
        ) {
            RocksDBRuntimeOption.optimizeValueOption(dummyMeta("accState"), options);
            // No mutation: memTableConfig must remain null (default)
            assertNull(options.memTableConfig());
        }
    }

    // ---------- optimizeMapOption ----------

    /** Range filter enabled with the right factory -> capped prefix extractor applied. */
    @Test
    public void testOptimizeMapOptionEnabled() {
        conf.set(RocksDBRuntimeOption.custom_factory, FACTORY_CLASS);
        conf.set(RocksDBOptOptionsFactory.USE_RANGE_FILTER, true);
        conf.set(RocksDBRuntimeOption.prefixLength, 11);

        try (
            MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
            ColumnFamilyOptions options = new ColumnFamilyOptions()
        ) {
            RocksDBRuntimeOption.optimizeMapOption(dummyMeta("m1"), options);
            // optimizeMapOption does NOT set memTableConfig; it only calls useCappedPrefixExtractor.
            // Verify that memTableConfig was not set (stays null) and the method completed without error.
            assertNull(options.memTableConfig());
        }
    }

    /** Factory option absent -> map option unchanged. */
    @Test
    public void testOptimizeMapOptionNoFactory() {
        conf.set(RocksDBOptOptionsFactory.USE_RANGE_FILTER, true);

        try (
            MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
            ColumnFamilyOptions options = new ColumnFamilyOptions()
        ) {
            RocksDBRuntimeOption.optimizeMapOption(dummyMeta("m2"), options);
            // No mutation: memTableConfig must remain null (default)
            assertNull(options.memTableConfig());
        }
    }

    /** Factory present but mismatch -> map option unchanged. */
    @Test
    public void testOptimizeMapOptionFactoryMismatch() {
        conf.set(RocksDBRuntimeOption.custom_factory, "WrongFactory");
        conf.set(RocksDBOptOptionsFactory.USE_RANGE_FILTER, true);

        try (
            MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
            ColumnFamilyOptions options = new ColumnFamilyOptions()
        ) {
            RocksDBRuntimeOption.optimizeMapOption(dummyMeta("m3"), options);
            // No mutation: memTableConfig must remain null (default)
            assertNull(options.memTableConfig());
        }
    }

    /** Factory matches but range filter disabled -> map option unchanged. */
    @Test
    public void testOptimizeMapOptionNoRangeFilter() {
        conf.set(RocksDBRuntimeOption.custom_factory, FACTORY_CLASS);
        conf.set(RocksDBOptOptionsFactory.USE_RANGE_FILTER, false);

        try (
            MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf);
            ColumnFamilyOptions options = new ColumnFamilyOptions()
        ) {
            RocksDBRuntimeOption.optimizeMapOption(dummyMeta("m4"), options);
            // No mutation: memTableConfig must remain null (default)
            assertNull(options.memTableConfig());
        }
    }

    // ---------- ConfigOption metadata ----------

    /** Trivial constructor coverage — the class is logically static-only but exposes a default ctor. */
    @Test
    public void testDefaultConstructor() {
        org.junit.Assert.assertNotNull(new RocksDBRuntimeOption());
    }

    @Test
    public void testConfigOptionDefaults() {
        // Touch the static ConfigOptions to make sure their defaults are stable.
        org.junit.Assert.assertEquals(
            "state.backend.rocksdb.options-factory",
            RocksDBRuntimeOption.custom_factory.key()
        );
        org.junit.Assert.assertFalse(RocksDBRuntimeOption.custom_factory.hasDefaultValue());

        org.junit.Assert.assertEquals(
            "state.backend.rocksdb.falcon.prefix-extractor.length",
            RocksDBRuntimeOption.prefixLength.key()
        );
        org.junit.Assert.assertEquals(Integer.valueOf(13), RocksDBRuntimeOption.prefixLength.defaultValue());
    }
}
