package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mockStatic;

public class RegisteredKeyValueStateBackendMetaInfoTest {

    private static MockedStatic<GlobalConfiguration> stubGlobalConfig(Configuration conf) {
        MockedStatic<GlobalConfiguration> mocked = mockStatic(GlobalConfiguration.class);
        mocked.when(GlobalConfiguration::loadConfiguration).thenReturn(conf);
        return mocked;
    }

    @Test
    public void testMergeOperatorNameGetterSetter() {
        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(new Configuration())) {
            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> metaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            StateDescriptor.Type.VALUE,
                            "test-state",
                            VoidNamespaceSerializer.INSTANCE,
                            IntSerializer.INSTANCE);

            assertNull(metaInfo.getMergeOperatorName());
            metaInfo.setMergeOperatorName("uint64add");
            assertEquals("uint64add", metaInfo.getMergeOperatorName());
        }
    }

    @Test
    public void testGetOptionsMapWithoutMerge() {
        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(new Configuration())) {
            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> metaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            StateDescriptor.Type.VALUE,
                            "test-state",
                            VoidNamespaceSerializer.INSTANCE,
                            IntSerializer.INSTANCE);

            Map<String, String> optionsMap = metaInfo.getOptionsMap();
            assertEquals(1, optionsMap.size());
            assertTrue(optionsMap.containsKey(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString()));
            assertFalse(optionsMap.containsKey(StateMetaInfoSnapshot.CommonOptionsKeys.MERGE_OPERATOR_NAME.toString()));
        }
    }

    @Test
    public void testGetOptionsMapWithMergeEnabledButNullName() {
        Configuration conf = new Configuration();
        conf.setBoolean("state.backend.rocksdb.falcon.use-merge", true);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf)) {
            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> metaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            StateDescriptor.Type.MAP,
                            "test-merge-state",
                            VoidNamespaceSerializer.INSTANCE,
                            IntSerializer.INSTANCE);

            Map<String, String> optionsMap = metaInfo.getOptionsMap();
            assertEquals(1, optionsMap.size());
            assertFalse(optionsMap.containsKey(StateMetaInfoSnapshot.CommonOptionsKeys.MERGE_OPERATOR_NAME.toString()));
        }
    }

    @Test
    public void testGetOptionsMapWithMergeEnabledAndName() {
        Configuration conf = new Configuration();
        conf.setBoolean("state.backend.rocksdb.falcon.use-merge", true);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf)) {
            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> metaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            StateDescriptor.Type.MAP,
                            "test-merge-state",
                            VoidNamespaceSerializer.INSTANCE,
                            IntSerializer.INSTANCE);

            metaInfo.setMergeOperatorName("uint64add");
            Map<String, String> optionsMap = metaInfo.getOptionsMap();
            assertEquals(2, optionsMap.size());
            assertEquals("uint64add", optionsMap.get(StateMetaInfoSnapshot.CommonOptionsKeys.MERGE_OPERATOR_NAME.toString()));
        }
    }

    @Test
    public void testSnapshotIncludesMergeOperatorName() {
        Configuration conf = new Configuration();
        conf.setBoolean("state.backend.rocksdb.falcon.use-merge", true);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf)) {
            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> metaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            StateDescriptor.Type.VALUE,
                            "test-snapshot-state",
                            VoidNamespaceSerializer.INSTANCE,
                            IntSerializer.INSTANCE);

            metaInfo.setMergeOperatorName("uint64add");
            StateMetaInfoSnapshot snapshot = metaInfo.snapshot();

            assertEquals("uint64add",
                    snapshot.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.MERGE_OPERATOR_NAME));
            assertEquals("test-snapshot-state", snapshot.getName());
        }
    }

    @Test
    public void testRestoreFromSnapshotWithMerge() {
        Configuration conf = new Configuration();
        conf.setBoolean("state.backend.rocksdb.falcon.use-merge", true);

        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(conf)) {
            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> original =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            StateDescriptor.Type.VALUE,
                            "test-restore-state",
                            VoidNamespaceSerializer.INSTANCE,
                            IntSerializer.INSTANCE);

            original.setMergeOperatorName("uint64add");
            StateMetaInfoSnapshot snapshot = original.snapshot();

            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> restored =
                    new RegisteredKeyValueStateBackendMetaInfo<>(snapshot);

            assertEquals("uint64add", restored.getMergeOperatorName());
            assertEquals("test-restore-state", restored.getName());
        }
    }

    @Test
    public void testRestoreFromSnapshotWithoutMerge() {
        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(new Configuration())) {
            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> original =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            StateDescriptor.Type.VALUE,
                            "test-no-merge-state",
                            VoidNamespaceSerializer.INSTANCE,
                            IntSerializer.INSTANCE);

            StateMetaInfoSnapshot snapshot = original.snapshot();

            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> restored =
                    new RegisteredKeyValueStateBackendMetaInfo<>(snapshot);

            assertNull(restored.getMergeOperatorName());
        }
    }

    @Test
    public void testEqualsAndHashCode() {
        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(new Configuration())) {
            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> metaInfo1 =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            StateDescriptor.Type.VALUE,
                            "test-equals",
                            VoidNamespaceSerializer.INSTANCE,
                            IntSerializer.INSTANCE);

            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> metaInfo2 =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            StateDescriptor.Type.VALUE,
                            "test-equals",
                            VoidNamespaceSerializer.INSTANCE,
                            IntSerializer.INSTANCE);

            assertEquals(metaInfo1, metaInfo2);
            assertEquals(metaInfo1.hashCode(), metaInfo2.hashCode());
        }
    }

    @Test
    public void testToString() {
        try (MockedStatic<GlobalConfiguration> ignored = stubGlobalConfig(new Configuration())) {
            RegisteredKeyValueStateBackendMetaInfo<VoidNamespace, Integer> metaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            StateDescriptor.Type.VALUE,
                            "test-toString",
                            VoidNamespaceSerializer.INSTANCE,
                            IntSerializer.INSTANCE);

            String result = metaInfo.toString();
            assertNotNull(result);
            assertTrue(result.contains("test-toString"));
        }
    }
}
