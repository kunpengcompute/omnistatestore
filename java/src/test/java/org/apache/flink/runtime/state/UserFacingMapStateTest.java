package org.apache.flink.runtime.state;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link UserFacingMapState}. */
public class UserFacingMapStateTest {

    private UserFacingMapState<String, Integer> userFacingMapState;

    /** Simple HashMap-backed implementation of {@link MapState} for testing. */
    private static class HashMapMapState<K, V> implements MapState<K, V> {

        private final HashMap<K, V> map = new HashMap<>();

        @Override
        public void clear() {
            map.clear();
        }

        @Override
        public V get(K key) throws Exception {
            return map.get(key);
        }

        @Override
        public void put(K key, V value) throws Exception {
            map.put(key, value);
        }

        @Override
        public void putAll(Map<K, V> value) throws Exception {
            map.putAll(value);
        }

        @Override
        public void remove(K key) throws Exception {
            map.remove(key);
        }

        @Override
        public boolean contains(K key) throws Exception {
            return map.containsKey(key);
        }

        @Override
        public Iterable<Map.Entry<K, V>> entries() throws Exception {
            return map.entrySet();
        }

        @Override
        public Iterable<K> keys() throws Exception {
            return map.keySet();
        }

        @Override
        public Iterable<V> values() throws Exception {
            return map.values();
        }

        @Override
        public Iterator<Map.Entry<K, V>> iterator() throws Exception {
            return map.entrySet().iterator();
        }

        @Override
        public boolean isEmpty() throws Exception {
            return map.isEmpty();
        }
    }

    @Before
    public void setup() {
        userFacingMapState = new UserFacingMapState<>(new HashMapMapState<>());
    }

    @Test
    public void testGetOriginalState() throws Exception {
        MapState<String, Integer> original = userFacingMapState.getOriginalState();
        assertNotNull("getOriginalState should return the backing MapState", original);
    }

    @Test
    public void testGet() throws Exception {
        userFacingMapState.put("key1", 42);
        Integer value = userFacingMapState.get("key1");
        assertEquals(Integer.valueOf(42), value);
    }

    @Test
    public void testPut() throws Exception {
        userFacingMapState.put("key1", 100);
        assertEquals(Integer.valueOf(100), userFacingMapState.get("key1"));
    }

    @Test
    public void testPutAll() throws Exception {
        Map<String, Integer> data = new HashMap<>();
        data.put("a", 1);
        data.put("b", 2);
        data.put("c", 3);
        userFacingMapState.putAll(data);
        assertEquals(Integer.valueOf(1), userFacingMapState.get("a"));
        assertEquals(Integer.valueOf(2), userFacingMapState.get("b"));
        assertEquals(Integer.valueOf(3), userFacingMapState.get("c"));
    }

    @Test
    public void testClear() throws Exception {
        userFacingMapState.put("key1", 1);
        userFacingMapState.put("key2", 2);
        assertFalse("Map should not be empty after put", userFacingMapState.isEmpty());
        userFacingMapState.clear();
        assertTrue("Map should be empty after clear", userFacingMapState.isEmpty());
    }

    @Test
    public void testRemove() throws Exception {
        userFacingMapState.put("key1", 99);
        assertEquals(Integer.valueOf(99), userFacingMapState.get("key1"));
        userFacingMapState.remove("key1");
        assertNull("Get should return null after remove", userFacingMapState.get("key1"));
    }

    @Test
    public void testEntries() throws Exception {
        userFacingMapState.put("x", 10);
        userFacingMapState.put("y", 20);
        Iterable<Map.Entry<String, Integer>> entries = userFacingMapState.entries();
        assertNotNull(entries);
        int count = 0;
        for (Map.Entry<String, Integer> entry : entries) {
            count++;
            assertTrue("Unexpected key", entry.getKey().equals("x") || entry.getKey().equals("y"));
            assertTrue("Unexpected value", entry.getValue().equals(10) || entry.getValue().equals(20));
        }
        assertEquals("Entries should have size 2", 2, count);
    }

    @Test
    public void testContains() throws Exception {
        userFacingMapState.put("existing", 1);
        assertTrue("contains should return true for existing key", userFacingMapState.contains("existing"));
        assertFalse("contains should return false for absent key", userFacingMapState.contains("nonexistent"));
    }

    @Test
    public void testKeys() throws Exception {
        userFacingMapState.put("k1", 1);
        userFacingMapState.put("k2", 2);
        Iterable<String> keys = userFacingMapState.keys();
        assertNotNull(keys);
        int count = 0;
        for (String key : keys) {
            count++;
            assertTrue("Unexpected key", key.equals("k1") || key.equals("k2"));
        }
        assertEquals("Keys should have size 2", 2, count);
    }

    @Test
    public void testValues() throws Exception {
        userFacingMapState.put("k1", 100);
        userFacingMapState.put("k2", 200);
        Iterable<Integer> values = userFacingMapState.values();
        assertNotNull(values);
        int count = 0;
        for (Integer value : values) {
            count++;
            assertTrue("Unexpected value", value.equals(100) || value.equals(200));
        }
        assertEquals("Values should have size 2", 2, count);
    }

    @Test
    public void testIterator() throws Exception {
        userFacingMapState.put("a", 1);
        userFacingMapState.put("b", 2);
        Iterator<Map.Entry<String, Integer>> iterator = userFacingMapState.iterator();
        assertNotNull(iterator);
        int count = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, Integer> entry = iterator.next();
            count++;
            assertNotNull("Entry key should not be null", entry.getKey());
            assertNotNull("Entry value should not be null", entry.getValue());
        }
        assertEquals("Iterator should yield 2 entries", 2, count);
    }

    @Test
    public void testIsEmpty() throws Exception {
        assertTrue("Map should be empty initially", userFacingMapState.isEmpty());
        userFacingMapState.put("key", 1);
        assertFalse("Map should not be empty after put", userFacingMapState.isEmpty());
    }
}
