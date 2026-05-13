package org.apache.flink.runtime.state.metainfo;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class StateMetaInfoSnapshotTest {
    private StateMetaInfoSnapshot snapshot;

    @Before
    public void setUp() throws Exception {
        Map<String, String> optionsMap = new HashMap<>(2);
        Map<String, TypeSerializerSnapshot<?>> serializerConfigSnapshotsMap = new HashMap<>(2);
        Map<String, TypeSerializer<?>> serializerMap = new HashMap<>(2);
        snapshot = new StateMetaInfoSnapshot(
                "name",
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                optionsMap,
                serializerConfigSnapshotsMap,
                serializerMap);
    }

    @Test
    public void testBackendStateTypeByCode() {
        assertEquals(StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                StateMetaInfoSnapshot.BackendStateType.byCode(0));
        assertEquals(StateMetaInfoSnapshot.BackendStateType.OPERATOR,
                StateMetaInfoSnapshot.BackendStateType.byCode(1));
        assertEquals(StateMetaInfoSnapshot.BackendStateType.BROADCAST,
                StateMetaInfoSnapshot.BackendStateType.byCode(2));
        assertEquals(StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE,
                StateMetaInfoSnapshot.BackendStateType.byCode(3));
    }

    @Test
    public void testBackendStateTypeGetCode() {
        assertEquals(0, StateMetaInfoSnapshot.BackendStateType.KEY_VALUE.getCode());
        assertEquals(1, StateMetaInfoSnapshot.BackendStateType.OPERATOR.getCode());
        assertEquals(2, StateMetaInfoSnapshot.BackendStateType.BROADCAST.getCode());
        assertEquals(3, StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE.getCode());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBackendStateTypeByCodeInvalid() {
        StateMetaInfoSnapshot.BackendStateType.byCode(99);
    }

    @Test
    public void testGetName() {
        assertEquals("name", snapshot.getName());
    }

    @Test
    public void testGetBackendStateType() {
        assertEquals(StateMetaInfoSnapshot.BackendStateType.KEY_VALUE, snapshot.getBackendStateType());
    }

    @Test
    public void testGetOptionsImmutable() {
        Map<String, String> options = snapshot.getOptionsImmutable();
        try {
            options.put("key", "value");
            fail("Should be immutable");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void testGetSerializerSnapshotsImmutable() {
        Map<String, TypeSerializerSnapshot<?>> snapshots = snapshot.getSerializerSnapshotsImmutable();
        try {
            snapshots.put("key", null);
            fail("Should be immutable");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void testGetOptionWithKey() {
        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(), "VALUE");
        optionsMap.put(StateMetaInfoSnapshot.CommonOptionsKeys.MERGE_OPERATOR_NAME.toString(), "uint64add");
        Map<String, TypeSerializerSnapshot<?>> serializerConfigSnapshotsMap = new HashMap<>();
        Map<String, TypeSerializer<?>> serializerMap = new HashMap<>();

        snapshot = new StateMetaInfoSnapshot(
                "test",
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                optionsMap,
                serializerConfigSnapshotsMap,
                serializerMap);

        assertEquals("VALUE", snapshot.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE));
        assertEquals("uint64add", snapshot.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.MERGE_OPERATOR_NAME));
    }

    @Test
    public void testGetOptionWithMissingKey() {
        assertNull(snapshot.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.MERGE_OPERATOR_NAME));
    }

    @Test
    public void testGetTypeSerializerSnapshotMissing() {
        assertNull(snapshot.getTypeSerializerSnapshot(StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER));
    }

    @Test
    public void testGetTypeSerializerMissing() {
        assertNull(snapshot.getTypeSerializer("nonexistent"));
    }

    @Test
    public void testCommonOptionsKeysValues() {
        assertEquals("KEYED_STATE_TYPE", StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString());
        assertEquals("MERGE_OPERATOR_NAME", StateMetaInfoSnapshot.CommonOptionsKeys.MERGE_OPERATOR_NAME.toString());
        assertEquals("OPERATOR_STATE_DISTRIBUTION_MODE",
                StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE.toString());
    }

    @Test
    public void testCommonSerializerKeysValues() {
        assertEquals("KEY_SERIALIZER", StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER.toString());
        assertEquals("NAMESPACE_SERIALIZER", StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString());
        assertEquals("VALUE_SERIALIZER", StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString());
    }
}
