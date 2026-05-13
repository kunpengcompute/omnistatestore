package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class StreamingSemiAntiJoinOperatorTest extends StreamingJoinOperatorTestBase {

    @Test
    public void testSemiJoinEmitsLeftRowWhenMatchingRightExists() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createSemiAntiHarness(false)) {
            harness.open();

            harness.processElement2(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement1(insertRow(1, 10), 1L);
            assertEquals(Arrays.asList("+I[1, 10]"), drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testAntiJoinRetractsLeftRowWhenMatchingRightArrives() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createSemiAntiHarness(true)) {
            harness.open();

            harness.processElement1(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList("+I[1, 10]"), drainOutputAsStrings(harness));

            harness.processElement2(insertRow(1, 10), 1L);
            assertEquals(Arrays.asList("-D[1, 10]"), drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testAntiJoinEmitsLeftRowAgainWhenLastMatchRetracts() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createSemiAntiHarness(true)) {
            harness.open();

            harness.processElement1(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList("+I[1, 10]"), drainOutputAsStrings(harness));

            harness.processElement2(insertRow(1, 10), 1L);
            assertEquals(Arrays.asList("-D[1, 10]"), drainOutputAsStrings(harness));

            harness.processElement2(deleteRow(1, 10), 2L);
            assertEquals(Arrays.asList("+I[1, 10]"), drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testSemiJoinHandlesUpdateMessages() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createSemiAntiHarness(false)) {
            harness.open();

            harness.processElement2(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement1(updateAfterRow(1, 10), 1L);
            assertEquals(Arrays.asList("+U[1, 10]"), drainOutputAsStrings(harness));

            harness.processElement1(updateBeforeRow(1, 10), 2L);
            assertEquals(Arrays.asList("-U[1, 10]"), drainOutputAsStrings(harness));

            harness.processElement1(insertRow(2, 20), 3L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement2(updateAfterRow(2, 20), 4L);
            assertEquals(Arrays.asList("+U[2, 20]"), drainOutputAsStrings(harness));

            harness.processElement2(updateBeforeRow(2, 20), 5L);
            assertEquals(Arrays.asList("-U[2, 20]"), drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testSemiAntiJoinWithMultipleMatchesOnRight() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createSemiAntiHarness(true)) {
            harness.open();

            harness.processElement1(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList("+I[1, 10]"), drainOutputAsStrings(harness));

            harness.processElement2(insertRow(1, 10), 1L);
            assertEquals(Arrays.asList("-D[1, 10]"), drainOutputAsStrings(harness));

            harness.processElement2(insertRow(1, 10), 2L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement2(deleteRow(1, 10), 3L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement2(deleteRow(1, 10), 4L);
            assertEquals(Arrays.asList("+I[1, 10]"), drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testAntiJoinSkipsMatchedLeftRowsAndUnmatchedRightRetracts() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createSemiAntiHarness(true)) {
            harness.open();

            harness.processElement2(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement1(insertRow(1, 10), 1L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement2(deleteRow(2, 20), 2L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));
        }
    }
}
