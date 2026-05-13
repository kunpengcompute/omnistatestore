package com.huawei.falcon.state.merge;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Contract tests for the {@link MergeableState} interface.
 * These tests use a test-only inner implementation ({@link TestMergeableState})
 * to document and validate the expected contract of the MergeableState interface.
 *
 * <p>Production implementations (e.g., RocksDBMapState) should have their own
 * integration tests since they depend on native RocksDB operations.</p>
 */
public class MergeableStateContractTest {

    /**
     * Test-only implementation of {@link MergeableState} that accumulates integer
     * values in an in-memory map. Used to validate the MergeableState contract.
     */
    static class TestMergeableState implements MergeableState<String, Integer> {

        private final Map<String, Integer> stateMap = new HashMap<>();

        @Override
        public void merge(String key, Integer operand) throws Exception {
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("Key cannot be null or empty");
            }
            if (operand == null) {
                throw new IllegalArgumentException("Operand cannot be null");
            }
            stateMap.put(key, stateMap.getOrDefault(key, 0) + operand);
        }

        public Integer getValue(String key) {
            return stateMap.getOrDefault(key, 0);
        }
    }

    @Test
    public void testMerge_NormalCase() throws Exception {
        TestMergeableState mergeableState = new TestMergeableState();

        mergeableState.merge("testKey", 10);
        mergeableState.merge("testKey", 20);
        mergeableState.merge("anotherKey", 5);

        assertEquals("testKey accumulated result should be 30", 30, mergeableState.getValue("testKey").intValue());
        assertEquals("anotherKey result should be 5", 5, mergeableState.getValue("anotherKey").intValue());
        assertEquals("non-existent key result should be 0", 0, mergeableState.getValue("nonExistKey").intValue());
    }

    @Test
    public void testMerge_NegativeValues() throws Exception {
        TestMergeableState mergeableState = new TestMergeableState();

        mergeableState.merge("counter", 100);
        mergeableState.merge("counter", -30);
        mergeableState.merge("counter", -70);

        assertEquals(
            "counter should be 0 after subtracting all additions",
            0,
            mergeableState.getValue("counter").intValue()
        );
    }

    @Test
    public void testMerge_NegativeValuesResultInNegativeTotal() throws Exception {
        TestMergeableState mergeableState = new TestMergeableState();

        mergeableState.merge("balance", 50);
        mergeableState.merge("balance", -100);

        assertEquals(
            "balance should be -50 after large withdrawal",
            -50,
            mergeableState.getValue("balance").intValue()
        );
    }

    @Test
    public void testMerge_IntegerOverflow() throws Exception {
        TestMergeableState mergeableState = new TestMergeableState();

        mergeableState.merge("overflow", Integer.MAX_VALUE);
        mergeableState.merge("overflow", 1);

        assertEquals(
            "overflow should wrap to Integer.MIN_VALUE after overflow",
            Integer.MIN_VALUE,
            mergeableState.getValue("overflow").intValue()
        );
    }

    @Test
    public void testMerge_IntegerUnderflow() throws Exception {
        TestMergeableState mergeableState = new TestMergeableState();

        mergeableState.merge("underflow", Integer.MIN_VALUE);
        mergeableState.merge("underflow", -1);

        assertEquals(
            "underflow should wrap to Integer.MAX_VALUE after underflow",
            Integer.MAX_VALUE,
            mergeableState.getValue("underflow").intValue()
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMerge_NullKey_ThrowsException() throws Exception {
        TestMergeableState mergeableState = new TestMergeableState();
        mergeableState.merge(null, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMerge_EmptyKey_ThrowsException() throws Exception {
        TestMergeableState mergeableState = new TestMergeableState();
        mergeableState.merge("", 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMerge_NullOperand_ThrowsException() throws Exception {
        TestMergeableState mergeableState = new TestMergeableState();
        mergeableState.merge("testKey", null);
    }

    @Test
    public void testMerge_ThrowGenericException() throws Exception {
        MergeableState<String, Integer> exceptionState = (key, operand) -> {
            throw new Exception("Generic merge exception");
        };

        try {
            exceptionState.merge("testKey", 10);
            fail("Should have thrown Exception with message 'Generic merge exception'");
        } catch (Exception e) {
            assertEquals("Generic merge exception", e.getMessage());
        }
    }
}
