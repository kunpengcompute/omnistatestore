package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class StreamingJoinOperatorTest extends StreamingJoinOperatorTestBase {

    @Test
    public void testInnerJoinEmitsJoinedRowWhenSecondSideArrives() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createJoinHarness(false, false)) {
            harness.open();

            harness.processElement1(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement2(insertRow(1, 10), 1L);
            assertEquals(Arrays.asList("+I[1, 10, 1, 10]"), drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testLeftOuterJoinReplacesNullPaddingWhenMatchArrives() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createJoinHarness(true, false)) {
            harness.open();

            harness.processElement1(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList("+I[1, 10, null, null]"), drainOutputAsStrings(harness));

            harness.processElement2(insertRow(1, 10), 1L);
            assertEquals(
                    Arrays.asList("-D[1, 10, null, null]", "+I[1, 10, 1, 10]"),
                    drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testInnerJoinHandlesRetractMessages() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createJoinHarness(false, false)) {
            harness.open();

            harness.processElement2(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement1(updateAfterRow(1, 10), 1L);
            assertEquals(Arrays.asList("+U[1, 10, 1, 10]"), drainOutputAsStrings(harness));

            harness.processElement1(updateBeforeRow(1, 10), 2L);
            assertEquals(Arrays.asList("-U[1, 10, 1, 10]"), drainOutputAsStrings(harness));

            harness.processElement1(insertRow(1, 10), 3L);
            assertEquals(Arrays.asList("+I[1, 10, 1, 10]"), drainOutputAsStrings(harness));

            harness.processElement2(deleteRow(1, 10), 4L);
            assertEquals(Arrays.asList("-D[1, 10, 1, 10]"), drainOutputAsStrings(harness));

            getJoinOperator(harness).finish();
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testFullOuterJoinTransitions() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createJoinHarness(true, true)) {
            harness.open();

            harness.processElement1(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList("+I[1, 10, null, null]"), drainOutputAsStrings(harness));

            harness.processElement2(insertRow(2, 20), 1L);
            assertEquals(Arrays.asList("+I[null, null, 2, 20]"), drainOutputAsStrings(harness));

            harness.processElement2(insertRow(1, 10), 2L);
            assertEquals(
                    Arrays.asList("-D[1, 10, null, null]", "+I[1, 10, 1, 10]"),
                    drainOutputAsStrings(harness));

            harness.processElement1(insertRow(2, 20), 3L);
            assertEquals(
                    Arrays.asList("-D[null, null, 2, 20]", "+I[2, 20, 2, 20]"),
                    drainOutputAsStrings(harness));

            harness.processElement1(deleteRow(1, 10), 4L);
            assertEquals(
                    Arrays.asList("-D[1, 10, 1, 10]", "+I[null, null, 1, 10]"),
                    drainOutputAsStrings(harness));

            harness.processElement2(deleteRow(2, 20), 5L);
            assertEquals(
                    Arrays.asList("-D[2, 20, 2, 20]", "+I[2, 20, null, null]"),
                    drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testFalconMiniBatchPathFlushesOnFinish() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createJoinHarness(false, false, true)) {
            harness.open();

            harness.processElement2(insertRow(3, 30), 0L);
            getJoinOperator(harness).finish();
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement1(insertRow(2, 20), 1L);
            getJoinOperator(harness).finish();
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement1(insertRow(1, 10), 2L);
            harness.processElement1(insertRow(1, 10), 3L);
            harness.processElement1(insertRow(4, 40), 4L);
            harness.processElement2(insertRow(1, 10), 5L);
            harness.processElement2(insertRow(1, 10), 6L);
            harness.processElement2(insertRow(2, 20), 7L);

            getJoinOperator(harness).finish();
            assertEquals(
                    Arrays.asList(
                            "+I[1, 10, 1, 10]",
                            "+I[1, 10, 1, 10]",
                            "+I[1, 10, 1, 10]",
                            "+I[1, 10, 1, 10]",
                            "+I[2, 20, 2, 20]"),
                    drainSortedOutputAsStrings(harness));
        }
    }

    @Test
    public void testLeftOuterJoinHandlesRetractsAndMultipleMatches() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createJoinHarness(true, false)) {
            harness.open();

            harness.processElement1(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList("+I[1, 10, null, null]"), drainOutputAsStrings(harness));

            harness.processElement2(insertRow(1, 10), 1L);
            assertEquals(
                    Arrays.asList("-D[1, 10, null, null]", "+I[1, 10, 1, 10]"),
                    drainOutputAsStrings(harness));

            harness.processElement2(insertRow(1, 10), 2L);
            assertEquals(Arrays.asList("+I[1, 10, 1, 10]"), drainOutputAsStrings(harness));

            harness.processElement2(deleteRow(1, 10), 3L);
            assertEquals(Arrays.asList("-D[1, 10, 1, 10]"), drainOutputAsStrings(harness));

            harness.processElement2(deleteRow(1, 10), 4L);
            assertEquals(
                    Arrays.asList("-D[1, 10, 1, 10]", "+I[1, 10, null, null]"),
                    drainOutputAsStrings(harness));

            harness.processElement1(deleteRow(1, 10), 5L);
            assertEquals(Arrays.asList("-D[1, 10, null, null]"), drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testLeftOuterJoinProducesDirectJoinWhenRightAlreadyExists() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createJoinHarness(true, false)) {
            harness.open();

            harness.processElement2(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement1(insertRow(1, 10), 1L);
            assertEquals(Arrays.asList("+I[1, 10, 1, 10]"), drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testRightOuterJoinReplacesNullPaddingWhenLeftMatchArrives() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createJoinHarness(false, true)) {
            harness.open();

            harness.processElement2(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList("+I[null, null, 1, 10]"), drainOutputAsStrings(harness));

            harness.processElement1(insertRow(1, 10), 1L);
            assertEquals(
                    Arrays.asList("-D[null, null, 1, 10]", "+I[1, 10, 1, 10]"),
                    drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testFalconMiniBatchHandlesTimerAndRetracts() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createJoinHarness(false, false, true)) {
            harness.open();

            harness.processElement2(insertRow(1, 10), 0L);
            getJoinOperator(harness).finish();
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

//            setLongField(getJoinOperator(harness), "stateUpdateTimer", 0L);
//            harness.processElement1(insertRow(1, 10), 1L);
//            assertEquals(Arrays.asList("+I[1, 10, 1, 10]"), drainOutputAsStrings(harness));

            harness.processElement1(deleteRow(1, 10), 2L);
            assertEquals(Arrays.asList("-D[1, 10, 1, 10]"), drainOutputAsStrings(harness));

            harness.processElement1(insertRow(2, 20), 3L);
            getJoinOperator(harness).finish();
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement1(deleteRow(2, 20), 4L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testFalconMiniBatchFlushesWhenBufferSizeThresholdIsReached() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createJoinHarness(false, false, true)) {
            harness.open();

            harness.processElement2(insertRow(1, 10), 0L);
            getJoinOperator(harness).finish();
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            setIntField(getJoinOperator(harness), "bufferSize", 2999);
//            setLongField(getJoinOperator(harness), "stateUpdateTimer", Long.MAX_VALUE);

            harness.processElement1(insertRow(1, 10), 1L);
            assertEquals(Arrays.asList("+I[1, 10, 1, 10]"), drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testFalconMiniBatchSkipsRowsWithSameKeyButUnmatchedCondition() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createJoinHarness(false, false, true)) {
            harness.open();

            harness.processElement2(insertRow(1, 11), 0L);
            getJoinOperator(harness).finish();
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));

            harness.processElement1(insertRow(1, 10), 1L);
            harness.processElement2(insertRow(1, 11), 2L);
            getJoinOperator(harness).finish();
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testMiniBatchFlagDoesNotEnableFalconPathForOuterJoinVariants() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> leftOuterHarness =
                        createJoinHarness(true, false, true);
                KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData>
                        rightOuterHarness = createJoinHarness(false, true, true)) {
            leftOuterHarness.open();
            leftOuterHarness.processElement1(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList("+I[1, 10, null, null]"), drainOutputAsStrings(leftOuterHarness));
            leftOuterHarness.processElement2(insertRow(2, 20), 1L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(leftOuterHarness));
            getJoinOperator(leftOuterHarness).finish();
            assertEquals(Arrays.asList(), drainOutputAsStrings(leftOuterHarness));

            rightOuterHarness.open();
            rightOuterHarness.processElement2(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList("+I[null, null, 1, 10]"), drainOutputAsStrings(rightOuterHarness));
            rightOuterHarness.processElement1(insertRow(2, 20), 1L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(rightOuterHarness));
            getJoinOperator(rightOuterHarness).finish();
            assertEquals(Arrays.asList(), drainOutputAsStrings(rightOuterHarness));
        }
    }

    @Test
    public void testFullOuterJoinSkipsNullRetractionWhenMatchedOuterRowAlreadyAssociated()
            throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createJoinHarness(true, true)) {
            harness.open();

            harness.processElement1(insertRow(1, 10), 0L);
            assertEquals(Arrays.asList("+I[1, 10, null, null]"), drainOutputAsStrings(harness));

            harness.processElement2(insertRow(1, 10), 1L);
            assertEquals(
                    Arrays.asList("-D[1, 10, null, null]", "+I[1, 10, 1, 10]"),
                    drainOutputAsStrings(harness));

            harness.processElement1(insertRow(1, 10), 2L);
            assertEquals(Arrays.asList("+I[1, 10, 1, 10]"), drainOutputAsStrings(harness));

            harness.processElement2(insertRow(1, 10), 3L);
            assertEquals(
                    Arrays.asList("+I[1, 10, 1, 10]", "+I[1, 10, 1, 10]"),
                    drainOutputAsStrings(harness));
        }
    }

    @Test
    public void testInnerJoinIgnoresRetractWithoutMatches() throws Exception {
        try (KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData> harness =
                createJoinHarness(false, false)) {
            harness.open();

            harness.processElement1(deleteRow(1, 10), 0L);
            assertEquals(Arrays.asList(), drainOutputAsStrings(harness));
        }
    }
}
