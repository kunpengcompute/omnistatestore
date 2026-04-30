/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join.stream;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.GlobalConfiguration;

import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateViews;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

public class StreamingJoinOperator extends AbstractStreamingJoinOperator implements Triggerable<Object, VoidNamespace> {

    // -------------------------------- FALCON Implementation --------------------------------
    boolean enableMerge = GlobalConfiguration.loadConfiguration().get(
            ConfigOptions.key("state.backend.rocksdb.falcon.use-merge")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("If 'true' then get-update-write cycles in join without primary key will "
                            + "be switched in favor of rocksdb's merge operator. "
                            + "It will significantly improve join performance but won't work "
                            + "with restoration from previous flink versions. "
                            + "If use this option you must turn on record deduplication and rocksdb state backend.")
    );
    // if enableMiniBatchJoin is true, enable miniBatch Join optimization.
    private final boolean enableMiniBatchJoin = GlobalConfiguration.loadConfiguration().get(
            ConfigOptions.key("state.backend.rocksdb.falcon.use-opt-join")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("If true, miniBatch join will be used for StreamingJoinOperator")
    );
    // falcon miniBatch join record cache, records is grouped by joinKey
    private Map<Object, List<RowData>> leftBuffer = new HashMap<>(500);
    private Map<Object, List<RowData>> rightBuffer = new HashMap<>(500);
    private int bufferSize = 0; // total number of records in falcon cache
    private transient InternalTimerService<VoidNamespace> internalTimerService; // time service to trigger miniBatchJoin
    private long nextTriggerTimer = -1L;  // when current time reaches nextTriggerTimer, reset internalTimerService

    /** When currentTime reached nextTriggerTimer, internalTimerService will call these func to clear buffer records. */
    @Override
    public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
        triggerMiniBatchJoin();
    }

    @Override
    public void onProcessingTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
        triggerMiniBatchJoin();
    }


    /**
     * Falcon miniBatch join function. If input record type is delete/update_before, trigger miniBatch join and then
     * retract input record; If input record is insert/update_after, insert the record into buffer. When buffer size
     * reaches upper limit or trigger timer fires, trigger miniBatch join.
     * <p> If input record type is delete/update_before, it means that join result corresponding this record should be
     * retracted, thus these records should not be inserted into buffer.
     * @brief FALCON implementation
     * @param input [IN] input record
     * @param inputSideStateView [IN] input side rocksdb instance
     * @param otherSideStateView [IN] other side rocksdb instance
     * @param inputIsLeft [IN] whether input record is from left table
     * */
    private void processElementFalcon(RowData input, JoinRecordStateView inputSideStateView,
                                      JoinRecordStateView otherSideStateView, boolean inputIsLeft) throws Exception {
        if (RowDataUtil.isRetractMsg(input)) {
            triggerMiniBatchJoin();
            retractElement(input, inputSideStateView, otherSideStateView, inputIsLeft);
        } else {
            long currentTime = System.currentTimeMillis();
            // if timer is not set, or it's time to trigger miniBatch, register a register timer.
            if (nextTriggerTimer == -1 || currentTime >= nextTriggerTimer) {
                nextTriggerTimer = currentTime + 200;
                internalTimerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, nextTriggerTimer);
            }
            insertIntoBuffer(input, inputIsLeft);
            if (bufferSize >= 1000) {
                triggerMiniBatchJoin();
            }
        }
    }

    /**
     * Retract join results of the input record, first process join process for the input element, then mark output type
     * as DELETE and output results. This function will be called in falcon miniBatch join.
     * <p> For inner join case, raw flink implementation use inputRowKind as output rowType. But in falcon record cache,
     * we do not know record in left buffer comes earlier or record in right buffer comes earlier, it means join result
     * may be different if we change join order.
     * <p> To handle this problem, all insert join result will be marked as INSERT but not UPDATE_AFTER, and all retract
     * join result will be marked as DELETE.
     * @brief FALCON implementation
     * @param input [IN] input record
     * @param inputSideStateView [IN] input side rocksdb instance
     * @param otherSideStateView [IN] other side rocksdb instance
     * @param inputIsLeft [IN] whether input record is from left table
     */
    private void retractElement(RowData input, JoinRecordStateView inputSideStateView,
                                JoinRecordStateView otherSideStateView, boolean inputIsLeft) throws Exception {
        input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating
        AssociatedRecords associatedRecords =
                AssociatedRecords.of(input, inputIsLeft, otherSideStateView, joinCondition);
        inputSideStateView.retractRecord(input);
        if (!associatedRecords.isEmpty()) {
            outRow.setRowKind(RowKind.DELETE); // all retracted element will be marked as DELETE type
            for (RowData other : associatedRecords.getRecords()) {
                output(input, other, inputIsLeft);
            }
        }
    }

    /**
     * Insert input record into falcon record buffer, records are grouped by joinKey.
     * @brief FALCON implementation
     * @param input input record
     * @param inputIsLeft whether input record is from left table
     */
    private void insertIntoBuffer(RowData input, boolean inputIsLeft) {
        Object key = getCurrentKey();
        if (inputIsLeft) { // insert record into left table record buffer
            if (leftBuffer.containsKey(key)) {
                leftBuffer.get(key).add(input);
            } else {
                List<RowData> inputList = new ArrayList<>();
                inputList.add(input);
                leftBuffer.put(key, inputList);
            }
        } else { // insert record into right table record buffer
            if (rightBuffer.containsKey(key)) {
                rightBuffer.get(key).add(input);
            } else {
                List<RowData> inputList = new ArrayList<>();
                inputList.add(input);
                rightBuffer.put(key, inputList);
            }
        }
        bufferSize++;
    }

    /**
     * Buffer records join with other side RocksDB records. Traverse records in buffer according to joinKey, for several
     * records with the same joinKey, first range query records with the same joinKey from other side rocksdb instance,
     * then check join condition one by one. If join condition matches, output join results.
     * <p> For inner join case, all insert join result will be marked as INSERT, and all retract join result will be
     * marked as DELETE.
     * @brief FALCON implementation
     * @param inputBuffer record buffer
     * @param bufferIsLeft whether record buffer is from left table
     * @param otherSideStateView other side rocksdb instance
     */
    private void bufferJoinDB(Map<Object, List<RowData>> inputBuffer, boolean bufferIsLeft,
                              JoinRecordStateView otherSideStateView) throws Exception {
        for (Object key : inputBuffer.keySet()) {
            // set current key and range queries records of the same joinKey from other side rocksdb instance
            setCurrentKey(key);
            AssociatedRecords recordsWithSameKey = AssociatedRecords.innerJoinOfSameKey(otherSideStateView);

            // check join condition between buffer records and other side rocksdb records
            for (RowData recordInBuffer : inputBuffer.get(key)) {
                for (RowData recordInDB : recordsWithSameKey.getRecords()) {
                    boolean matched = bufferIsLeft ?
                            joinCondition.apply(recordInBuffer, recordInDB) :
                            joinCondition.apply(recordInDB, recordInBuffer);
                    if (matched) {
                        outRow.setRowKind(RowKind.INSERT); // all insert element will be marked as INSERT type
                        output(recordInBuffer, recordInDB, bufferIsLeft);
                    }
                }
            }
        }
    }

    /**
     * Left buffer records join with Right buffer records.
     * @brief FALCON implementation
     */
    private void bufferJoinBuffer() {
        for (Object key : leftBuffer.keySet()) {
            if (!rightBuffer.containsKey(key)) {
                continue;
            }
            // Check join condition one by one.
            for (RowData leftRecord : leftBuffer.get(key)) {
                for (RowData rightRecord : rightBuffer.get(key)) {
                    boolean matched = joinCondition.apply(leftRecord, rightRecord);
                    if (matched) {
                        outRow.setRowKind(RowKind.INSERT);
                        output(leftRecord, rightRecord, true);
                    }
                }
            }
        }
    }

    /**
     * Buffer records state update, i.e., update cnt of each buffer record in rocksdb. Note that before state updating,
     * record row kind should be erased.
     * @brief FALCON implementation
     */
    private void bufferStateUpdate() throws Exception {
        for (Object key : leftBuffer.keySet()) {
            setCurrentKey(key);
            for (RowData record : leftBuffer.get(key)) {
                record.setRowKind(RowKind.INSERT);
                leftRecordStateView.addRecord(record);
            }
        }
        for (Object key : rightBuffer.keySet()) {
            setCurrentKey(key);
            for (RowData record : rightBuffer.get(key)) {
                record.setRowKind(RowKind.INSERT);
                rightRecordStateView.addRecord(record);
            }
        }
    }

    /**
     * Falcon miniBatch join main function, including buffer join db, buffer join buffer and buffer state update. Note
     * that, only inner join case is supported.
     * @brief FALCON implementation
     * */
    public void triggerMiniBatchJoin() throws Exception {
        bufferJoinDB(leftBuffer, true, rightRecordStateView);
        bufferJoinDB(rightBuffer, false, leftRecordStateView);
        bufferJoinBuffer();
        bufferStateUpdate();

        // clear record buffer and reset trigger timer
        bufferSize = 0;
        leftBuffer.clear();
        rightBuffer.clear();
        internalTimerService.deleteProcessingTimeTimer(VoidNamespace.INSTANCE, nextTriggerTimer);
        nextTriggerTimer = -1;
    }

    /**
     * When the last record comes, StreamOperator will call finish function, trigger miniBatch join for the last time.
     * @brief FALCON implementation
     * */
    public void finish() throws Exception {
        if (enableMiniBatchJoin && !leftIsOuter && !rightIsOuter) {
            LOG.info("[FALCON] finish miniBatch process for StreamingJoinOperator.");
            triggerMiniBatchJoin();
        }
    }

    /**
     * Called when the operator should do a snapshot, trigger miniBatchJoin before the operator emits checkpoint barrier
     * @param checkpointId checkpoint id
     */
    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        if (enableMiniBatchJoin && !leftIsOuter && !rightIsOuter) {
            triggerMiniBatchJoin();
        }
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    // ---------------------------------------------------------------------------------------

    private static final long serialVersionUID = -376944622236540545L;

    // whether left side is outer side, e.g. left is outer but right is not when LEFT OUTER JOIN
    private final boolean leftIsOuter;
    // whether right side is outer side, e.g. right is outer but left is not when RIGHT OUTER JOIN
    private final boolean rightIsOuter;

    private transient JoinedRowData outRow;
    private transient RowData leftNullRow;
    private transient RowData rightNullRow;

    // left join state
    private transient JoinRecordStateView leftRecordStateView;
    // right join state
    private transient JoinRecordStateView rightRecordStateView;

    public StreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean leftIsOuter,
            boolean rightIsOuter,
            boolean[] filterNullKeys,
            long stateRetentionTime) {
        super(
                leftType,
                rightType,
                generatedJoinCondition,
                leftInputSideSpec,
                rightInputSideSpec,
                filterNullKeys,
                stateRetentionTime);
        this.leftIsOuter = leftIsOuter;
        this.rightIsOuter = rightIsOuter;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.internalTimerService = getInternalTimerService("falcon-miniBatch-join-timer",
                VoidNamespaceSerializer.INSTANCE, this);

        this.outRow = new JoinedRowData();
        this.leftNullRow = new GenericRowData(leftType.toRowSize());
        this.rightNullRow = new GenericRowData(rightType.toRowSize());

        // initialize states
        if (leftIsOuter) {
            this.leftRecordStateView =
                    OuterJoinRecordStateViews.create(
                            getRuntimeContext(),
                            "left-records",
                            leftInputSideSpec,
                            leftType,
                            stateRetentionTime);
        } else {
            this.leftRecordStateView =
                    JoinRecordStateViews.create(
                            getRuntimeContext(),
                            "left-records",
                            leftInputSideSpec,
                            leftType,
                            stateRetentionTime,
                            enableMerge);
        }

        if (rightIsOuter) {
            this.rightRecordStateView =
                    OuterJoinRecordStateViews.create(
                            getRuntimeContext(),
                            "right-records",
                            rightInputSideSpec,
                            rightType,
                            stateRetentionTime);
        } else {
            this.rightRecordStateView =
                    JoinRecordStateViews.create(
                            getRuntimeContext(),
                            "right-records",
                            rightInputSideSpec,
                            rightType,
                            stateRetentionTime,
                            enableMerge);
        }
        if (enableMiniBatchJoin && !leftIsOuter && !rightIsOuter) {
            LOG.info("[FALCON] enable miniBatch process for StreamingJoinOperator.");
        }
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        if (enableMiniBatchJoin && !leftIsOuter && !rightIsOuter) {
            processElementFalcon(element.getValue(), leftRecordStateView, rightRecordStateView, true);
        } else {
            processElement(element.getValue(), leftRecordStateView, rightRecordStateView, true);
        }
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        if (enableMiniBatchJoin && !leftIsOuter && !rightIsOuter) {
            processElementFalcon(element.getValue(), rightRecordStateView, leftRecordStateView, false);
        } else {
            processElement(element.getValue(), rightRecordStateView, leftRecordStateView, false);
        }
    }

    /**
     * Process an input element and output incremental joined records, retraction messages will be
     * sent in some scenarios.
     *
     * <p>Following is the pseudo code to describe the core logic of this method. The logic of this
     * method is too complex, so we provide the pseudo code to help understand the logic. We should
     * keep sync the following pseudo code with the real logic of the method.
     *
     * <p>Note: "+I" represents "INSERT", "-D" represents "DELETE", "+U" represents "UPDATE_AFTER",
     * "-U" represents "UPDATE_BEFORE". We forward input RowKind if it is inner join, otherwise, we
     * always send insert and delete for simplification. We can optimize this to send -U & +U
     * instead of D & I in the future (see FLINK-17337). They are equivalent in this join case. It
     * may need some refactoring if we want to send -U & +U, so we still keep -D & +I for now for
     * simplification. See {@code
     * FlinkChangelogModeInferenceProgram.SatisfyModifyKindSetTraitVisitor}.
     *
     * <pre>
     * if input record is accumulate
     * |  if input side is outer
     * |  |  if there is no matched rows on the other side, send +I[record+null], state.add(record, 0)
     * |  |  if there are matched rows on the other side
     * |  |  | if other side is outer
     * |  |  | |  if the matched num in the matched rows == 0, send -D[null+other]
     * |  |  | |  if the matched num in the matched rows > 0, skip
     * |  |  | |  otherState.update(other, old + 1)
     * |  |  | endif
     * |  |  | send +I[record+other]s, state.add(record, other.size)
     * |  |  endif
     * |  endif
     * |  if input side not outer
     * |  |  state.add(record)
     * |  |  if there is no matched rows on the other side, skip
     * |  |  if there are matched rows on the other side
     * |  |  |  if other side is outer
     * |  |  |  |  if the matched num in the matched rows == 0, send -D[null+other]
     * |  |  |  |  if the matched num in the matched rows > 0, skip
     * |  |  |  |  otherState.update(other, old + 1)
     * |  |  |  |  send +I[record+other]s
     * |  |  |  else
     * |  |  |  |  send +I/+U[record+other]s (using input RowKind)
     * |  |  |  endif
     * |  |  endif
     * |  endif
     * endif
     *
     * if input record is retract
     * |  state.retract(record)
     * |  if there is no matched rows on the other side
     * |  | if input side is outer, send -D[record+null]
     * |  endif
     * |  if there are matched rows on the other side, send -D[record+other]s if outer, send -D/-U[record+other]s if inner.
     * |  |  if other side is outer
     * |  |  |  if the matched num in the matched rows == 0, this should never happen!
     * |  |  |  if the matched num in the matched rows == 1, send +I[null+other]
     * |  |  |  if the matched num in the matched rows > 1, skip
     * |  |  |  otherState.update(other, old - 1)
     * |  |  endif
     * |  endif
     * endif
     * </pre>
     *
     * @param input the input element
     * @param inputSideStateView state of input side
     * @param otherSideStateView state of other side
     * @param inputIsLeft whether input side is left side
     */
    private void processElement(
            RowData input,
            JoinRecordStateView inputSideStateView,
            JoinRecordStateView otherSideStateView,
            boolean inputIsLeft)
            throws Exception {
        boolean inputIsOuter = inputIsLeft ? leftIsOuter : rightIsOuter;
        boolean otherIsOuter = inputIsLeft ? rightIsOuter : leftIsOuter;
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
        RowKind inputRowKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating

        AssociatedRecords associatedRecords =
                AssociatedRecords.of(input, inputIsLeft, otherSideStateView, joinCondition);
        if (isAccumulateMsg) { // record is accumulate
            if (inputIsOuter) { // input side is outer
                OuterJoinRecordStateView inputSideOuterStateView =
                        (OuterJoinRecordStateView) inputSideStateView;
                if (associatedRecords.isEmpty()) { // there is no matched rows on the other side
                    // send +I[record+null]
                    outRow.setRowKind(RowKind.INSERT);
                    outputNullPadding(input, inputIsLeft);
                    // state.add(record, 0)
                    inputSideOuterStateView.addRecord(input, 0);
                } else { // there are matched rows on the other side
                    if (otherIsOuter) { // other side is outer
                        OuterJoinRecordStateView otherSideOuterStateView =
                                (OuterJoinRecordStateView) otherSideStateView;
                        for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                            RowData other = outerRecord.record;
                            // if the matched num in the matched rows == 0
                            if (outerRecord.numOfAssociations == 0) {
                                // send -D[null+other]
                                outRow.setRowKind(RowKind.DELETE);
                                outputNullPadding(other, !inputIsLeft);
                            } // ignore matched number > 0
                            // otherState.update(other, old + 1)
                            otherSideOuterStateView.updateNumOfAssociations(
                                    other, outerRecord.numOfAssociations + 1);
                        }
                    }
                    // send +I[record+other]s
                    outRow.setRowKind(RowKind.INSERT);
                    for (RowData other : associatedRecords.getRecords()) {
                        output(input, other, inputIsLeft);
                    }
                    // state.add(record, other.size)
                    inputSideOuterStateView.addRecord(input, associatedRecords.size());
                }
            } else { // input side not outer
                // state.add(record)
                inputSideStateView.addRecord(input);
                if (!associatedRecords.isEmpty()) { // if there are matched rows on the other side
                    if (otherIsOuter) { // if other side is outer
                        OuterJoinRecordStateView otherSideOuterStateView =
                                (OuterJoinRecordStateView) otherSideStateView;
                        for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                            if (outerRecord.numOfAssociations
                                    == 0) { // if the matched num in the matched rows == 0
                                // send -D[null+other]
                                outRow.setRowKind(RowKind.DELETE);
                                outputNullPadding(outerRecord.record, !inputIsLeft);
                            }
                            // otherState.update(other, old + 1)
                            otherSideOuterStateView.updateNumOfAssociations(
                                    outerRecord.record, outerRecord.numOfAssociations + 1);
                        }
                        // send +I[record+other]s
                        outRow.setRowKind(RowKind.INSERT);
                    } else {
                        // send +I/+U[record+other]s (using input RowKind)
                        outRow.setRowKind(inputRowKind);
                    }
                    for (RowData other : associatedRecords.getRecords()) {
                        output(input, other, inputIsLeft);
                    }
                }
                // skip when there is no matched rows on the other side
            }
        } else { // input record is retract
            // state.retract(record)
            inputSideStateView.retractRecord(input);
            if (associatedRecords.isEmpty()) { // there is no matched rows on the other side
                if (inputIsOuter) { // input side is outer
                    // send -D[record+null]
                    outRow.setRowKind(RowKind.DELETE);
                    outputNullPadding(input, inputIsLeft);
                }
                // nothing to do when input side is not outer
            } else { // there are matched rows on the other side
                if (inputIsOuter) {
                    // send -D[record+other]s
                    outRow.setRowKind(RowKind.DELETE);
                } else {
                    // send -D/-U[record+other]s (using input RowKind)
                    outRow.setRowKind(inputRowKind);
                }
                for (RowData other : associatedRecords.getRecords()) {
                    output(input, other, inputIsLeft);
                }
                // if other side is outer
                if (otherIsOuter) {
                    OuterJoinRecordStateView otherSideOuterStateView =
                            (OuterJoinRecordStateView) otherSideStateView;
                    for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                        if (outerRecord.numOfAssociations == 1) {
                            // send +I[null+other]
                            outRow.setRowKind(RowKind.INSERT);
                            outputNullPadding(outerRecord.record, !inputIsLeft);
                        } // nothing else to do when number of associations > 1
                        // otherState.update(other, old - 1)
                        otherSideOuterStateView.updateNumOfAssociations(
                                outerRecord.record, outerRecord.numOfAssociations - 1);
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------------------

    private void output(RowData inputRow, RowData otherRow, boolean inputIsLeft) {
        if (inputIsLeft) {
            outRow.replace(inputRow, otherRow);
        } else {
            outRow.replace(otherRow, inputRow);
        }
        collector.collect(outRow);
    }

    private void outputNullPadding(RowData row, boolean isLeft) {
        if (isLeft) {
            outRow.replace(row, rightNullRow);
        } else {
            outRow.replace(leftNullRow, row);
        }
        collector.collect(outRow);
    }
}
