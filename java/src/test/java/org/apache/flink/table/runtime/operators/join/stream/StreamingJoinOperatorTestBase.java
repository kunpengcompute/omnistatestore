package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.types.RowKind;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

abstract class StreamingJoinOperatorTestBase {

    protected static final InternalTypeInfo<RowData> INPUT_ROW_TYPE =
            InternalTypeInfo.ofFields(new IntType(), new IntType());

    protected static final KeySelector<RowData, Integer> KEY_SELECTOR =
            row -> row.getInt(0);

    protected static final boolean[] FILTER_NULL_KEYS = new boolean[] {false};

    protected static final long STATE_RETENTION_TIME = 0L;

    protected KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData>
            createSemiAntiHarness(boolean isAntiJoin) throws Exception {
        StreamingSemiAntiJoinOperator operator =
                new StreamingSemiAntiJoinOperator(
                        isAntiJoin,
                        INPUT_ROW_TYPE,
                        INPUT_ROW_TYPE,
                        testingJoinCondition(),
                        inputSideSpec(),
                        inputSideSpec(),
                        FILTER_NULL_KEYS,
                        STATE_RETENTION_TIME);

        return new KeyedTwoInputStreamOperatorTestHarness<>(
                operator, KEY_SELECTOR, KEY_SELECTOR, Types.INT);
    }

    protected KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData>
            createJoinHarness(boolean leftIsOuter, boolean rightIsOuter) throws Exception {
        return createJoinHarness(leftIsOuter, rightIsOuter, false);
    }

    protected KeyedTwoInputStreamOperatorTestHarness<Integer, RowData, RowData, RowData>
            createJoinHarness(boolean leftIsOuter, boolean rightIsOuter, boolean enableMiniBatchJoin)
                    throws Exception {
        StreamingJoinOperator operator =
                new StreamingJoinOperator(
                        INPUT_ROW_TYPE,
                        INPUT_ROW_TYPE,
                        testingJoinCondition(),
                        inputSideSpec(),
                        inputSideSpec(),
                        leftIsOuter,
                        rightIsOuter,
                        FILTER_NULL_KEYS,
                        STATE_RETENTION_TIME);
        if (enableMiniBatchJoin) {
            setBooleanField(operator, "enableMiniBatchJoin", true);
        }

        return new KeyedTwoInputStreamOperatorTestHarness<>(
                operator, KEY_SELECTOR, KEY_SELECTOR, Types.INT);
    }

    protected JoinInputSideSpec inputSideSpec() {
        return JoinInputSideSpec.withoutUniqueKey();
    }

    protected GeneratedJoinCondition testingJoinCondition() {
        return new GeneratedJoinCondition("TestingJoinCondition", "", new Object[0]) {
            private static final long serialVersionUID = 1L;

            @Override
            public JoinCondition newInstance(ClassLoader classLoader) {
                return new TestingJoinCondition();
            }
        };
    }

    protected RowData insertRow(int key, int value) {
        return row(RowKind.INSERT, key, value);
    }

    protected RowData deleteRow(int key, int value) {
        return row(RowKind.DELETE, key, value);
    }

    protected RowData updateBeforeRow(int key, int value) {
        return row(RowKind.UPDATE_BEFORE, key, value);
    }

    protected RowData updateAfterRow(int key, int value) {
        return row(RowKind.UPDATE_AFTER, key, value);
    }

    protected List<String> drainOutputAsStrings(AbstractStreamOperatorTestHarness<RowData> harness) {
        List<String> output = new ArrayList<>();
        for (RowData row : harness.extractOutputValues()) {
            output.add(toRowString(row));
        }
        harness.getOutput().clear();
        return output;
    }

    protected List<String> drainSortedOutputAsStrings(
            AbstractStreamOperatorTestHarness<RowData> harness) {
        List<String> output = drainOutputAsStrings(harness);
        Collections.sort(output);
        return output;
    }

    protected String toRowString(RowData row) {
        StringBuilder builder = new StringBuilder();
        builder.append(shortRowKind(row.getRowKind())).append("[");
        for (int i = 0; i < row.getArity(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            if (row.isNullAt(i)) {
                builder.append("null");
            } else {
                builder.append(row.getInt(i));
            }
        }
        builder.append("]");
        return builder.toString();
    }

    protected StreamingJoinOperator getJoinOperator(
            AbstractStreamOperatorTestHarness<RowData> harness) {
        return (StreamingJoinOperator) harness.getOperator();
    }

    protected void setBooleanField(Object target, String fieldName, boolean value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setBoolean(target, value);
    }

    protected void setLongField(Object target, String fieldName, long value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setLong(target, value);
    }

    protected void setIntField(Object target, String fieldName, int value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setInt(target, value);
    }

    private static RowData row(RowKind rowKind, int key, int value) {
        return GenericRowData.ofKind(rowKind, key, value);
    }

    private static String shortRowKind(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
                return "+I";
            case DELETE:
                return "-D";
            case UPDATE_BEFORE:
                return "-U";
            case UPDATE_AFTER:
                return "+U";
            default:
                throw new IllegalArgumentException("Unsupported row kind: " + rowKind);
        }
    }

    private static final class TestingJoinCondition extends AbstractRichFunction
            implements JoinCondition {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean apply(RowData left, RowData right) {
            return left.getInt(1) == right.getInt(1);
        }
    }
}
