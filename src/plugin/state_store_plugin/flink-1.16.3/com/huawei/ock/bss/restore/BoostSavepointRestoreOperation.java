/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.huawei.ock.bss.restore;

import com.huawei.ock.bss.common.BoostStateDB;
import com.huawei.ock.bss.common.conf.BoostConfig;
import com.huawei.ock.bss.common.exception.BSSRuntimeException;
import com.huawei.ock.bss.common.memory.DirectDataOutputSerializer;
import com.huawei.ock.bss.state.internal.KeyedListState;
import com.huawei.ock.bss.state.internal.KeyedListStateImpl;
import com.huawei.ock.bss.state.internal.KeyedMapState;
import com.huawei.ock.bss.state.internal.KeyedMapStateImpl;
import com.huawei.ock.bss.state.internal.KeyedState;
import com.huawei.ock.bss.state.internal.KeyedValueState;
import com.huawei.ock.bss.state.internal.KeyedValueStateImpl;
import com.huawei.ock.bss.state.internal.descriptor.InternalStateType;
import com.huawei.ock.bss.state.internal.descriptor.keyed.KeyedListStateDescriptor;
import com.huawei.ock.bss.state.internal.descriptor.keyed.KeyedMapStateDescriptor;
import com.huawei.ock.bss.state.internal.descriptor.keyed.KeyedStateDescriptor;
import com.huawei.ock.bss.state.internal.descriptor.keyed.KeyedValueStateDescriptor;
import com.huawei.ock.bss.state.internal.descriptor.nskeyed.NSKeyedListStateDescriptor;
import com.huawei.ock.bss.state.internal.descriptor.nskeyed.NSKeyedMapStateDescriptor;
import com.huawei.ock.bss.state.internal.descriptor.nskeyed.NSKeyedStateDescriptor;
import com.huawei.ock.bss.state.internal.descriptor.nskeyed.NSKeyedValueStateDescriptor;
import com.huawei.ock.bss.state.internal.namespace.NSKeyedListState;
import com.huawei.ock.bss.state.internal.namespace.NSKeyedListStateImpl;
import com.huawei.ock.bss.state.internal.namespace.NSKeyedMapState;
import com.huawei.ock.bss.state.internal.namespace.NSKeyedMapStateImpl;
import com.huawei.ock.bss.state.internal.namespace.NSKeyedState;
import com.huawei.ock.bss.state.internal.namespace.NSKeyedValueState;
import com.huawei.ock.bss.state.internal.namespace.NSKeyedValueStateImpl;
import com.huawei.ock.bss.table.KeyPair;
import com.huawei.ock.bss.table.TtlSavepointRestore;
import com.huawei.ock.bss.table.api.KListTable;
import com.huawei.ock.bss.table.api.KMapTable;
import com.huawei.ock.bss.table.api.KVTable;
import com.huawei.ock.bss.table.api.Table;
import com.huawei.ock.bss.table.api.NsKListTable;
import com.huawei.ock.bss.table.api.NsKMapTable;
import com.huawei.ock.bss.table.api.NsKVTable;
import com.huawei.ock.bss.table.api.SerializedKVRestoreTable;
import com.huawei.ock.bss.table.api.SerializedListRestoreTable;
import com.huawei.ock.bss.table.api.SerializedMapRestoreTable;
import com.huawei.ock.bss.table.api.TableDescription;
import com.huawei.ock.bss.util.DescriptionUtils;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.ListDelimitedSerializer;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.restore.FullSnapshotRestoreOperation;
import org.apache.flink.runtime.state.restore.KeyGroup;
import org.apache.flink.runtime.state.restore.KeyGroupEntry;
import org.apache.flink.runtime.state.restore.SavepointRestoreResult;
import org.apache.flink.runtime.state.restore.ThrowingIterator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.StateMigrationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * savepoint restore
 *
 * @param <K> key
 * @since BeiMing 25.0.T1
 */
@SuppressWarnings("rawtypes")
public class BoostSavepointRestoreOperation<K> extends AbstractBoostRestoreOperation<K> {
    private static final Logger LOG = LoggerFactory.getLogger(BoostSavepointRestoreOperation.class);

    private static final int LIST_RESTORE_MAX_ELEMENTS = 100;

    private static final int LIST_RESTORE_MAX_BYTES = 64 * 1024;

    private final int keyGroupPrefixBytes;

    private final BoostStateDB db;

    private final KeyGroupRange keyGroupRange;

    private final FullSnapshotRestoreOperation<K> restoreOperation;

    private final StateSerializerProvider<K> serializerProvider;

    private final DataInputDeserializer keyDeserializer;

    private final DataInputDeserializer valueDeserializer;

    private final ListDelimitedSerializer listDelimitedSerializer;

    private DirectDataOutputSerializer normalizationOutput;

    private final PriorityQueueSetFactory priorityQueueSetFactory;

    private final Map<String, Table> tables;

    private final Map<String, KeyedState> keyedStateMap;

    private final Map<String, NSKeyedState> nsKeyedStateMap;

    private final Map<String, KeyedStateDescriptor> keyedStateDescriptorMap;

    private final Map<String, NSKeyedStateDescriptor> nsKeyedStateDescriptorMap;

    private final Map<String, RegisteredStateMetaInfoBase> registeredKVStateMetas;

    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;

    public BoostSavepointRestoreOperation(
        KeyGroupRange keyGroupRange,
        int keyGroupPrefixBytes,
        ClassLoader userCodeClassLoader,
        StateSerializerProvider<K> serializerProvider,
        BoostStateDB db,
        @Nonnull Collection<KeyedStateHandle> restoreStateHandles,
        Map<String, Table> tables,
        Map<String, RegisteredStateMetaInfoBase> registeredKVStateMetas,
        Map<String, KeyedStateDescriptor> keyedStateDescriptorMap,
        Map<String, NSKeyedStateDescriptor> nsKeyedStateDescriptorMap,
        Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
        PriorityQueueSetFactory priorityQueueFactory) {
        this.restoreOperation =
            new FullSnapshotRestoreOperation<>(keyGroupRange, userCodeClassLoader, restoreStateHandles,
                serializerProvider);
        this.keyedStateMap = new HashMap<>();
        this.nsKeyedStateMap = new HashMap<>();
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.serializerProvider = serializerProvider;
        this.keyDeserializer = new DataInputDeserializer();
        this.valueDeserializer = new DataInputDeserializer();
        this.listDelimitedSerializer = new ListDelimitedSerializer();
        this.priorityQueueSetFactory = priorityQueueFactory;
        this.keyGroupRange = keyGroupRange;
        this.db = db;
        this.keyedStateDescriptorMap = keyedStateDescriptorMap;
        this.nsKeyedStateDescriptorMap = nsKeyedStateDescriptorMap;
        this.tables = tables;
        this.registeredKVStateMetas = registeredKVStateMetas;
        this.registeredPQStates = registeredPQStates;
        LOG.info("Finished new BoostSavepointRestoreOperation");
    }

    @Override
    public BoostRestoreResult restore() throws Exception {
        LOG.info("Begin savepoint restore.");
        try (ThrowingIterator<SavepointRestoreResult> restore = this.restoreOperation.restore()) {
            while (restore.hasNext()) {
                Map<Integer, RegisteredStateMetaInfoBase> restoredKvStates = new HashMap<>();
                Map<Integer, HeapPriorityQueueSnapshotRestoreWrapper<?>> restoredPQStates = new HashMap<>();
                Map<Integer, KeyGroupedInternalPriorityQueue<?>> pqStates = new HashMap<>();
                SavepointRestoreResult restoreResult = restore.next();
                List<StateMetaInfoSnapshot> restoredMetaInfos = restoreResult.getStateMetaInfoSnapshots();
                restoreStateMeta(restoredMetaInfos, restoredKvStates, restoredPQStates, pqStates);
                restoreTable();
                try (ThrowingIterator<KeyGroup> keyGroups = restoreResult.getRestoredKeyGroups()) {
                    restoreStateData(keyGroups, restoredKvStates, restoredPQStates, pqStates);
                }
            }
        }
        LOG.info("Finish savepoint restore.");
        return BoostRestoreResult.EMPTY_RESULT;
    }

    @SuppressWarnings("unchecked")
    private <T extends org.apache.flink.runtime.state.heap.HeapPriorityQueueElement
        & org.apache.flink.runtime.state.PriorityComparable<? super T>> void restoreStateMeta(
        List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
        Map<Integer, RegisteredStateMetaInfoBase> restoredKVStates,
        Map<Integer, HeapPriorityQueueSnapshotRestoreWrapper<?>> restoredPQStates,
        Map<Integer, KeyGroupedInternalPriorityQueue<?>> pqStates) {
        LOG.info("Begin savepoint restore restoreStateMeta.");
        for (int i = 0; i < stateMetaInfoSnapshots.size(); i++) {
            RegisteredStateMetaInfoBase metaInfo;
            HeapPriorityQueueSnapshotRestoreWrapper<?> pqInfo;
            StateMetaInfoSnapshot stateMetaInfoSnapshot = stateMetaInfoSnapshots.get(i);
            String name = stateMetaInfoSnapshot.getName();
            switch (stateMetaInfoSnapshot.getBackendStateType()) {
                case KEY_VALUE:
                    metaInfo = registeredKVStateMetas.computeIfAbsent(name,
                        key -> new RegisteredKeyValueStateBackendMetaInfo<>(stateMetaInfoSnapshot));
                    restoredKVStates.put(i, metaInfo);
                    break;
                case PRIORITY_QUEUE:
                    if (this.priorityQueueSetFactory instanceof HeapPriorityQueueSetFactory) {
                        RegisteredPriorityQueueStateBackendMetaInfo pqMetaInfo =
                                new RegisteredPriorityQueueStateBackendMetaInfo<>(stateMetaInfoSnapshot);
                        HeapPriorityQueueSet<T> priorityQueueSet =
                            ((HeapPriorityQueueSetFactory) this.priorityQueueSetFactory).create(pqMetaInfo.getName(),
                            pqMetaInfo.getElementSerializer());
                        pqInfo = registeredPQStates.computeIfAbsent(name,
                                key -> new HeapPriorityQueueSnapshotRestoreWrapper(priorityQueueSet, pqMetaInfo,
                                        KeyExtractorFunction.forKeyedObjects(), this.keyGroupRange,
                                        this.db.getConfig().getMaxParallelism()));
                        restoredPQStates.put(i, pqInfo);
                        break;
                    }
                    Object object = registeredKVStateMetas.computeIfAbsent(name,
                            key -> new RegisteredPriorityQueueStateBackendMetaInfo<>(stateMetaInfoSnapshot));
                    RegisteredPriorityQueueStateBackendMetaInfo pqMetaInfo;
                    if (object instanceof RegisteredPriorityQueueStateBackendMetaInfo) {
                        pqMetaInfo = (RegisteredPriorityQueueStateBackendMetaInfo) object;
                    } else {
                        throw new IllegalStateException("Failed to restore Meta info, invalid pqMetaInfo type: "
                                + object.getClass().getSimpleName() + ".");
                    }
                    KeyGroupedInternalPriorityQueue<?> queue =
                            priorityQueueSetFactory.create(pqMetaInfo.getName(),
                                    pqMetaInfo.getPreviousElementSerializer());
                    restoredKVStates.put(i, pqMetaInfo);
                    pqStates.put(i, queue);
                    break;
                default:
                    throw new IllegalStateException("Failed to restore Meta info because of invalid state type: "
                        + stateMetaInfoSnapshot.getBackendStateType() + ".");
            }
        }
        LOG.info("Finish savepoint restore restoreStateMeta.");
    }

    @SuppressWarnings("unchecked")
    private void restoreStateData(ThrowingIterator<KeyGroup> keyGroups,
        Map<Integer, RegisteredStateMetaInfoBase> restoredKVStates,
        Map<Integer, HeapPriorityQueueSnapshotRestoreWrapper<?>> restoredPQStates,
        Map<Integer, KeyGroupedInternalPriorityQueue<?>> pqStates)
        throws StateMigrationException, IOException {
        RegisteredKeyValueStateBackendMetaInfo<?, ?> kvMetaInfo = null;
        RegisteredPriorityQueueStateBackendMetaInfo<?> pqNativeMetaInfo = null;
        HeapPriorityQueueSnapshotRestoreWrapper<HeapPriorityQueueElement> pqMetaInfo = null;
        KeyGroupedInternalPriorityQueue<?> queue = null;
        while (keyGroups.hasNext()) {
            KeyGroup keyGroup = keyGroups.next();
            if (keyGroup == null) {
                LOG.error("keyGroup is null while trying to restoreStateData.");
                continue;
            }
            try (ThrowingIterator<KeyGroupEntry> entries = keyGroup.getKeyGroupEntries()) {
                int prevStateId = -1;
                while (entries.hasNext()) {
                    KeyGroupEntry entry = entries.next();
                    if (entry == null) {
                        LOG.error("KeyGroupEntry is null while trying to restoreStateData.");
                        continue;
                    }
                    int kvStateId = entry.getKvStateId();
                    if (kvStateId != prevStateId) {
                        prevStateId = kvStateId;
                        RegisteredStateMetaInfoBase base = restoredKVStates.get(kvStateId);
                        if (base instanceof RegisteredKeyValueStateBackendMetaInfo) {
                            kvMetaInfo = (RegisteredKeyValueStateBackendMetaInfo<?, ?>) base;
                            pqNativeMetaInfo = null;
                            queue = null;
                        } else if (base instanceof RegisteredPriorityQueueStateBackendMetaInfo) {
                            pqNativeMetaInfo = (RegisteredPriorityQueueStateBackendMetaInfo) base;
                            queue = pqStates.get(kvStateId);
                            kvMetaInfo = null;
                        } else if (base == null) {
                            pqNativeMetaInfo = null;
                            queue = pqStates.get(kvStateId);
                            kvMetaInfo = null;
                        } else {
                            LOG.error("RegisteredStateMetaInfoBase type not match.");
                            continue;
                        }
                        pqMetaInfo =
                            (HeapPriorityQueueSnapshotRestoreWrapper<HeapPriorityQueueElement>)
                                restoredPQStates.get(kvStateId);
                    }

                    if (pqMetaInfo != null) {
                        restoreQueueElement(pqMetaInfo, entry);
                        continue;
                    }
                    if (pqNativeMetaInfo != null && queue != null) {
                        restoreQueueElement(pqNativeMetaInfo, entry, queue);
                        continue;
                    }

                    if (kvMetaInfo != null) {
                        // 实际无作用，仅根据name check了map中对应state是否为null
                        readKvStateData(kvMetaInfo, entry);
                        continue;
                    }

                    throw new IllegalStateException("Unknown state id: " + kvStateId);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void restoreTable() {
        for (RegisteredStateMetaInfoBase backendMetaInfo : this.registeredKVStateMetas.values()) {
            if (backendMetaInfo == null) {
                LOG.error("backendMetaInfo is null while trying to restoreTable.");
                continue;
            }
            if (!(backendMetaInfo instanceof RegisteredKeyValueStateBackendMetaInfo)) {
                continue;
            }
            switch (((RegisteredKeyValueStateBackendMetaInfo) backendMetaInfo).getStateType()) {
                case LIST:
                    restoreKeyedListStateTable((RegisteredKeyValueStateBackendMetaInfo) backendMetaInfo);
                    continue;
                case MAP:
                    restoreKeyedMapStateTable((RegisteredKeyValueStateBackendMetaInfo) backendMetaInfo);
                    continue;
                case VALUE:
                case REDUCING:
                case AGGREGATING:
                    restoreKeyedValueStateTable((RegisteredKeyValueStateBackendMetaInfo) backendMetaInfo);
                    continue;
                default:
                    throw new UnsupportedOperationException("Restore table failed because of invalid state type "
                            + ((RegisteredKeyValueStateBackendMetaInfo) backendMetaInfo).getStateType());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <N, V> void restoreKeyedValueStateTable(RegisteredKeyValueStateBackendMetaInfo<N, V> backendMetaInfo) {
        String name = backendMetaInfo.getName();
        InternalStateType internalStateType = getInternalValueStateType(backendMetaInfo);
        if (internalStateType.isKeyedState()) {
            KeyedValueStateDescriptor<K, V> descriptor =
                new KeyedValueStateDescriptor<>(name, this.serializerProvider.previousSchemaSerializer(),
                    backendMetaInfo.getPreviousStateSerializer(), internalStateType);
            TableDescription tableDescription =
                DescriptionUtils.createTableDescription(this.db.getConfig().getMaxParallelism(), descriptor);
            KVTable<K, V> table = (KVTable<K, V>) this.db.getTableOrCreate(tableDescription);
            KeyedValueState<K, V> valueState = new KeyedValueStateImpl<>(descriptor, table);

            this.keyedStateDescriptorMap.put(name, descriptor);
            this.tables.put(name, table);
            this.keyedStateMap.put(name, valueState);
            return;
        }

        NSKeyedValueStateDescriptor<K, N, V> descriptor =
            new NSKeyedValueStateDescriptor<>(name, this.serializerProvider.previousSchemaSerializer(),
                backendMetaInfo.getPreviousNamespaceSerializer(), backendMetaInfo.getPreviousStateSerializer(),
                internalStateType);
        TableDescription tableDescription =
            DescriptionUtils.createTableDescription(this.db.getConfig().getMaxParallelism(), descriptor);
        NsKVTable<K, N, V> table = (NsKVTable<K, N, V>) this.db.getTableOrCreate(tableDescription);
        NSKeyedValueState<K, N, V> nsValueState = new NSKeyedValueStateImpl<>(descriptor, table);

        this.nsKeyedStateDescriptorMap.put(name, descriptor);
        this.tables.put(name, table);
        this.nsKeyedStateMap.put(name, nsValueState);
    }

    @SuppressWarnings("unchecked")
    private <N, E> void restoreKeyedListStateTable(RegisteredKeyValueStateBackendMetaInfo<N, List<E>> backendMetaInfo) {
        String name = backendMetaInfo.getName();
        if (backendMetaInfo.getPreviousNamespaceSerializer() instanceof VoidNamespaceSerializer) {
            KeyedListStateDescriptor<K, E> descriptor =
                new KeyedListStateDescriptor<>(name, this.serializerProvider.previousSchemaSerializer(),
                    ((ListSerializer<E>) backendMetaInfo.getPreviousStateSerializer()).getElementSerializer());
            TableDescription tableDescription =
                DescriptionUtils.createTableDescription(this.db.getConfig().getMaxParallelism(), descriptor);
            KListTable<K, E> table = (KListTable<K, E>) this.db.getTableOrCreate(tableDescription);
            KeyedListState<K, E> listState = new KeyedListStateImpl<>(descriptor, table);

            this.keyedStateDescriptorMap.put(name, descriptor);
            this.tables.put(name, table);
            this.keyedStateMap.put(name, listState);
            return;
        }

        NSKeyedListStateDescriptor<K, N, E> descriptor =
            new NSKeyedListStateDescriptor<>(name, this.serializerProvider.previousSchemaSerializer(),
                backendMetaInfo.getPreviousNamespaceSerializer(),
                ((ListSerializer<E>) backendMetaInfo.getPreviousStateSerializer()).getElementSerializer());
        TableDescription tableDescription =
            DescriptionUtils.createTableDescription(this.db.getConfig().getMaxParallelism(), descriptor);
        NsKListTable<K, N, E> table = (NsKListTable<K, N, E>) this.db.getTableOrCreate(tableDescription);
        NSKeyedListState<K, N, E> nsListState = new NSKeyedListStateImpl<>(descriptor, table);

        this.nsKeyedStateDescriptorMap.put(name, descriptor);
        this.tables.put(name, table);
        this.nsKeyedStateMap.put(name, nsListState);
    }

    @SuppressWarnings("unchecked")
    private <N, UK, UV> void restoreKeyedMapStateTable(
        RegisteredKeyValueStateBackendMetaInfo<N, Map<UK, UV>> backendMetaInfo) {
        String name = backendMetaInfo.getName();
        if (backendMetaInfo.getPreviousNamespaceSerializer() instanceof VoidNamespaceSerializer) {
            KeyedMapStateDescriptor<K, UK, UV> descriptor =
                new KeyedMapStateDescriptor<>(name, this.serializerProvider.previousSchemaSerializer(),
                    backendMetaInfo.getPreviousStateSerializer());
            TableDescription tableDescription =
                DescriptionUtils.createTableDescription(this.db.getConfig().getMaxParallelism(), descriptor);
            KMapTable<K, UK, UV, Map<UK, UV>> table =
                (KMapTable<K, UK, UV, Map<UK, UV>>) this.db.getTableOrCreate(tableDescription);
            KeyedMapState<K, UK, UV> mapState = new KeyedMapStateImpl<>(descriptor, table);

            this.keyedStateDescriptorMap.put(name, descriptor);
            this.tables.put(name, table);
            this.keyedStateMap.put(name, mapState);
            return;
        }

        NSKeyedMapStateDescriptor<K, N, UK, UV> descriptor =
            new NSKeyedMapStateDescriptor<>(name, this.serializerProvider.previousSchemaSerializer(),
                backendMetaInfo.getPreviousNamespaceSerializer(), backendMetaInfo.getPreviousStateSerializer());
        TableDescription tableDescription =
            DescriptionUtils.createTableDescription(this.db.getConfig().getMaxParallelism(), descriptor);
        NsKMapTable<K, N, UK, UV, Map<UK, UV>> table =
            (NsKMapTable<K, N, UK, UV, Map<UK, UV>>) this.db.getTableOrCreate(tableDescription);
        NSKeyedMapState<K, N, UK, UV> nsMapState = new NSKeyedMapStateImpl<>(descriptor, table);
        this.nsKeyedStateDescriptorMap.put(name, descriptor);
        this.tables.put(name, table);
        this.nsKeyedStateMap.put(name, nsMapState);
    }

    private void restoreQueueElement(HeapPriorityQueueSnapshotRestoreWrapper<HeapPriorityQueueElement> restoredPQ,
        KeyGroupEntry groupEntry) throws IOException {
        this.keyDeserializer.setBuffer(groupEntry.getKey(), this.keyGroupPrefixBytes,
                groupEntry.getKey().length - this.keyGroupPrefixBytes);
        HeapPriorityQueueElement timer =
            restoredPQ.getMetaInfo().getElementSerializer().deserialize(this.keyDeserializer);
        restoredPQ.getPriorityQueue().add(timer);
    }

    private void restoreQueueElement(RegisteredPriorityQueueStateBackendMetaInfo<?> metaInfo,
        KeyGroupEntry groupEntry, KeyGroupedInternalPriorityQueue<?> queue)
            throws IOException {
        this.keyDeserializer.setBuffer(groupEntry.getKey());
        this.keyDeserializer.skipBytesToRead(this.keyGroupPrefixBytes);
        Object ele = metaInfo.getElementSerializer().deserialize(keyDeserializer);
        KeyGroupedInternalPriorityQueue<Object> typedQueue = (KeyGroupedInternalPriorityQueue<Object>) queue;
        typedQueue.add(ele);
    }

    private <N> void readKvStateData(RegisteredKeyValueStateBackendMetaInfo<N, ?> backendMetaInfo,
        KeyGroupEntry groupEntry) throws IOException {
        TypeSerializer<K> keySerializer = this.serializerProvider.previousSchemaSerializer();
        String stateName = backendMetaInfo.getName();
        TypeSerializer<N> namespaceSerializer = backendMetaInfo.getPreviousNamespaceSerializer();
        TypeSerializer<?> stateSerializer = backendMetaInfo.getPreviousStateSerializer();
        Preconditions.checkNotNull(namespaceSerializer,
            "PreviousNamespaceSerializer should not be null when readKvStateData.");
        Preconditions.checkNotNull(stateSerializer,
            "PreviousStateSerializer should not be null when readKvStateData.");

        boolean ambiguousKeyPossible = (keySerializer.getLength() < 0 && namespaceSerializer.getLength() < 0);
        this.keyDeserializer.setBuffer(groupEntry.getKey());
        this.valueDeserializer.setBuffer(groupEntry.getValue());
        // 此处不读取会导致偏移量无法正确增加，后续key和namespace无法正确读取
        CompositeKeySerializationUtils.readKeyGroup(this.keyGroupPrefixBytes, this.keyDeserializer);
        int keyStart = this.keyDeserializer.getPosition();
        K key = keySerializer.deserialize(this.keyDeserializer);
        int keyEnd = this.keyDeserializer.getPosition();
        if (ambiguousKeyPossible) {
            CompositeKeySerializationUtils.readVariableIntBytes(this.keyDeserializer, keyEnd - keyStart);
        }
        int namespaceStart = this.keyDeserializer.getPosition();
        N namespace = namespaceSerializer.deserialize(this.keyDeserializer);
        int namespaceEnd = this.keyDeserializer.getPosition();
        if (ambiguousKeyPossible) {
            CompositeKeySerializationUtils.readVariableIntBytes(this.keyDeserializer,
                namespaceEnd - namespaceStart);
        }
        Preconditions.checkNotNull(key, "key should not be null when readKvStateData.");
        Preconditions.checkNotNull(namespace, "namespace should not be null when readKvStateData.");
        byte[] canonicalKey = groupEntry.getKey();
        byte[] keyBytes = fieldBytes(keySerializer, key, canonicalKey, keyStart, keyEnd);
        byte[] namespaceBytes = fieldBytes(namespaceSerializer, namespace, canonicalKey, namespaceStart, namespaceEnd);
        int keyJavaHash = key.hashCode();
        int namespaceJavaHash = namespace.hashCode();
        int mixedKeyHash = MathUtils.murmurHash(keyJavaHash);

        StateDescriptor.Type stateType = backendMetaInfo.getStateType();
        switch (stateType) {
            case LIST:
                restoreListState(stateName, key, namespace, keyBytes, namespaceBytes, keyJavaHash,
                    namespaceJavaHash, mixedKeyHash, groupEntry.getValue(), (ListSerializer<?>) stateSerializer);
                return;
            case MAP:
                restoreMapState(stateName, key, namespace, keyBytes, namespaceBytes, keyJavaHash,
                    namespaceJavaHash, mixedKeyHash, canonicalKey, groupEntry.getValue(),
                    (MapSerializer<?, ?>) stateSerializer);
                return;
            case VALUE:
            case REDUCING:
            case AGGREGATING:
                restoreKeyedAndNSKeyedState(stateName, key, namespace, keyBytes, namespaceBytes, keyJavaHash,
                    namespaceJavaHash, mixedKeyHash, stateSerializer, groupEntry.getValue());
                return;
            default:
                throw new UnsupportedEncodingException("Invalid state type " + stateType);
        }
    }

    @SuppressWarnings("unchecked")
    private <N, V> void restoreKeyedAndNSKeyedState(String stateName, K key, N namespace, byte[] keyBytes,
        byte[] namespaceBytes, int keyJavaHash, int namespaceJavaHash, int mixedKeyHash,
        TypeSerializer<V> valueSerializer, byte[] originalValueBytes) throws IOException {
        if (TtlSavepointRestore.isTtlSerializer(valueSerializer)) {
            V value = valueSerializer.deserialize(this.valueDeserializer);
            TtlSavepointRestore.restoreValue(this.tables.get(stateName), key,
                namespace instanceof VoidNamespace ? null : namespace, value);
            return;
        }
        if (BoostConfig.DEBUG_MODE) {
            V value = valueSerializer.deserialize(this.valueDeserializer);
            restoreValueLegacy(stateName, key, namespace, value);
            return;
        }
        byte[] valueBytes = normalizedValueBytes(valueSerializer, originalValueBytes);
        if (namespace instanceof VoidNamespace) {
            ((SerializedKVRestoreTable<K>) getTableWithCheck(stateName)).restoreSerialized(keyBytes, mixedKeyHash,
                keyJavaHash, valueBytes);
            return;
        }
        ((SerializedMapRestoreTable<K, N>) getTableWithCheck(stateName)).restoreSerialized(keyBytes, mixedKeyHash,
            keyJavaHash, namespaceBytes, namespaceJavaHash, valueBytes);
    }

    private <N, V> void restoreValueLegacy(String stateName, K key, N namespace, V value) {
        if (namespace instanceof VoidNamespace) {
            ((KeyedValueState<K, V>) getKeyedStateWithCheck(stateName)).put(key, value);
        } else {
            ((NSKeyedValueState<K, N, V>) getNSKeyedStateWithCheck(stateName)).put(key, namespace, value);
        }
    }

    @SuppressWarnings("unchecked")
    private <N, E> void restoreListState(String stateName, K key, N namespace, byte[] keyBytes,
        byte[] namespaceBytes, int keyJavaHash, int namespaceJavaHash, int mixedKeyHash, byte[] valueBytes,
        TypeSerializer<List<E>> stateSerializer) throws IOException {
        if (valueBytes == null) {
            LOG.warn("input valueBytes is null.");
            return;
        }

        TypeSerializer<E> elementSerializer = ((ListSerializer<E>) stateSerializer).getElementSerializer();
        if (TtlSavepointRestore.isTtlSerializer(elementSerializer)) {
            List<E> values = this.listDelimitedSerializer.deserializeList(valueBytes, elementSerializer);
            TtlSavepointRestore.restoreList(this.tables.get(stateName), key,
                namespace instanceof VoidNamespace ? null : namespace, values);
            return;
        }
        if (BoostConfig.DEBUG_MODE) {
            List<E> values = this.listDelimitedSerializer.deserializeList(valueBytes, elementSerializer);
            if (namespace instanceof VoidNamespace) {
                KeyedListState<K, E> state = (KeyedListState<K, E>) getKeyedStateWithCheck(stateName);
                values.forEach(e -> state.add(key, e));
            } else {
                NSKeyedListState<K, N, E> state = (NSKeyedListState<K, N, E>) getNSKeyedStateWithCheck(stateName);
                values.forEach(e -> state.add(key, namespace, e));
            }
            return;
        }
        if (namespace instanceof VoidNamespace) {
            ((SerializedListRestoreTable<K>) getTableWithCheck(stateName)).restoreSerialized(keyBytes, mixedKeyHash,
                keyJavaHash, consumer -> transcodeList(valueBytes, elementSerializer, consumer));
        } else {
            ((SerializedListRestoreTable<KeyPair<K, N>>) getTableWithCheck(stateName)).restoreSerialized(
                keyPairBytes(keyBytes, namespaceBytes, namespaceJavaHash), mixedKeyHash,
                keyJavaHash ^ namespaceJavaHash,
                consumer -> transcodeList(valueBytes, elementSerializer, consumer));
        }
    }

    @SuppressWarnings("unchecked")
    private <N, UK, UV> void restoreMapState(String stateName, K key, N namespace, byte[] keyBytes,
        byte[] namespaceBytes, int keyJavaHash, int namespaceJavaHash, int mixedKeyHash, byte[] canonicalKey,
        byte[] canonicalValue, MapSerializer<UK, UV> stateSerializer) throws IOException {
        int userKeyStart = this.keyDeserializer.getPosition();
        UK mapEntryKey = stateSerializer.getKeySerializer().deserialize(this.keyDeserializer);
        int userKeyEnd = this.keyDeserializer.getPosition();
        int userKeyHash = mapEntryKey.hashCode();
        byte[] serializedUserKey = fieldBytes(stateSerializer.getKeySerializer(), mapEntryKey,
            canonicalKey, userKeyStart, userKeyEnd);
        boolean isNull = this.valueDeserializer.readBoolean();
        UV mapEntryValue = null;
        if (TtlSavepointRestore.isTtlSerializer(stateSerializer.getValueSerializer())) {
            if (!isNull) {
                mapEntryValue = stateSerializer.getValueSerializer().deserialize(this.valueDeserializer);
            }
            TtlSavepointRestore.restoreMapEntry(this.tables.get(stateName), key,
                namespace instanceof VoidNamespace ? null : namespace, mapEntryKey, mapEntryValue);
            return;
        }
        if (isNull) {
            restoreMapLegacy(stateName, key, namespace, mapEntryKey, null);
            return;
        }
        if (BoostConfig.DEBUG_MODE) {
            mapEntryValue = stateSerializer.getValueSerializer().deserialize(this.valueDeserializer);
            restoreMapLegacy(stateName, key, namespace, mapEntryKey, mapEntryValue);
            return;
        }
        byte[] serializedValue;
        if (isCanonicalSerializer(stateSerializer.getValueSerializer())) {
            serializedValue = Arrays.copyOfRange(canonicalValue, 1, canonicalValue.length);
            validateSerialized(stateSerializer.getValueSerializer(), serializedValue);
        } else {
            mapEntryValue = stateSerializer.getValueSerializer().deserialize(this.valueDeserializer);
            if (this.valueDeserializer.available() != 0) {
                throw new IOException("Map value serializer did not consume the complete savepoint value.");
            }
            serializedValue = serializeNormalized(stateSerializer.getValueSerializer(), mapEntryValue);
        }
        if (namespace instanceof VoidNamespace) {
            ((SerializedMapRestoreTable<K, UK>) getTableWithCheck(stateName)).restoreSerialized(keyBytes,
                mixedKeyHash, keyJavaHash, serializedUserKey, userKeyHash, serializedValue);
        } else {
            ((SerializedMapRestoreTable<KeyPair<K, N>, UK>) getTableWithCheck(stateName)).restoreSerialized(
                keyPairBytes(keyBytes, namespaceBytes, namespaceJavaHash), mixedKeyHash,
                keyJavaHash ^ namespaceJavaHash, serializedUserKey, userKeyHash, serializedValue);
        }
    }

    private <N, UK, UV> void restoreMapLegacy(String stateName, K key, N namespace, UK userKey, UV value) {
        if (namespace instanceof VoidNamespace) {
            ((KeyedMapState<K, UK, UV>) getKeyedStateWithCheck(stateName)).add(key, userKey, value);
        } else {
            ((NSKeyedMapState<K, N, UK, UV>) getNSKeyedStateWithCheck(stateName)).add(key, namespace, userKey, value);
        }
    }

    private Table getTableWithCheck(String stateName) {
        Table table = this.tables.get(stateName);
        if (table == null) {
            throw new BSSRuntimeException("Failed to get Table, stateName: " + stateName);
        }
        return table;
    }

    private <T> byte[] fieldBytes(TypeSerializer<T> serializer, T value, byte[] source, int start, int end)
        throws IOException {
        if (isCanonicalSerializer(serializer)) {
            return Arrays.copyOfRange(source, start, end);
        }
        return serializeNormalized(serializer, value);
    }

    private <T> byte[] normalizedValueBytes(TypeSerializer<T> serializer, byte[] bytes) throws IOException {
        if (isCanonicalSerializer(serializer)) {
            validateSerialized(serializer, bytes);
            return bytes;
        }
        DataInputDeserializer input = new DataInputDeserializer(bytes);
        T value = serializer.deserialize(input);
        if (input.available() != 0) {
            throw new IOException("State serializer did not consume the complete savepoint value.");
        }
        return serializeNormalized(serializer, value);
    }

    private <T> byte[] serializeNormalized(TypeSerializer<T> serializer, T value) throws IOException {
        if (normalizationOutput == null) {
            normalizationOutput = new DirectDataOutputSerializer(128);
        } else {
            normalizationOutput.setPosition(0);
        }
        serializer.serialize(value, normalizationOutput);
        return normalizationOutput.getCopyOfBuffer();
    }

    private static void validateSerialized(TypeSerializer<?> serializer, byte[] bytes) throws IOException {
        DataInputDeserializer input = new DataInputDeserializer(bytes);
        serializer.copy(input, DiscardingDataOutputView.INSTANCE);
        if (input.available() != 0) {
            throw new IOException("Serializer did not consume the complete savepoint value.");
        }
    }

    private static boolean isCanonicalSerializer(TypeSerializer<?> serializer) {
        String name = serializer.getClass().getName();
        return name.equals("org.apache.flink.api.common.typeutils.base.BooleanSerializer")
            || name.equals("org.apache.flink.api.common.typeutils.base.ByteSerializer")
            || name.equals("org.apache.flink.api.common.typeutils.base.CharSerializer")
            || name.equals("org.apache.flink.api.common.typeutils.base.IntSerializer")
            || name.equals("org.apache.flink.api.common.typeutils.base.LongSerializer")
            || name.equals("org.apache.flink.api.common.typeutils.base.ShortSerializer")
            || name.equals("org.apache.flink.api.common.typeutils.base.StringSerializer")
            || name.equals("org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer")
            || serializer instanceof VoidNamespaceSerializer;
    }

    private static byte[] keyPairBytes(byte[] keyBytes, byte[] namespaceBytes, int namespaceHash) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + keyBytes.length + namespaceBytes.length)
            .order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(namespaceHash);
        buffer.put(keyBytes);
        buffer.put(namespaceBytes);
        buffer.putInt(keyBytes.length);
        return buffer.array();
    }

    private <E> void transcodeList(byte[] canonicalBytes, TypeSerializer<E> serializer,
        SerializedListRestoreTable.ChunkConsumer consumer) throws IOException {
        DataInputDeserializer input = new DataInputDeserializer(canonicalBytes);
        DataOutputSerializer payload = new DataOutputSerializer(Math.min(Math.max(canonicalBytes.length, 1),
            LIST_RESTORE_MAX_BYTES));
        GrowableDataOutputView element = new GrowableDataOutputView(128);
        int count = 0;
        boolean rawCopy = isCanonicalSerializer(serializer);
        while (input.available() > 0) {
            element.clear();
            byte[] bytes;
            if (rawCopy) {
                serializer.copy(input, element);
                bytes = element.getCopyOfBuffer();
            } else {
                E value = serializer.deserialize(input);
                bytes = serializeNormalized(serializer, value);
            }
            if (count > 0 && (count >= LIST_RESTORE_MAX_ELEMENTS
                || payload.length() + bytes.length > LIST_RESTORE_MAX_BYTES)) {
                consumer.accept(buildListChunk(payload, count));
                payload.clear();
                count = 0;
            }
            payload.write(bytes);
            count++;
            if (input.available() > 0) {
                if (input.readByte() != ',' || input.available() == 0) {
                    throw new IOException("Invalid list delimiter in savepoint value.");
                }
            }
        }
        if (count > 0) {
            consumer.accept(buildListChunk(payload, count));
        }
    }

    private static byte[] buildListChunk(DataOutputSerializer payload, int count) throws IOException {
        // Match KVSerializerUtil.serList: element count followed by serialized elements.
        DataOutputSerializer chunk = new DataOutputSerializer(Integer.BYTES + payload.length());
        chunk.writeInt(count);
        chunk.write(payload.getSharedBuffer(), 0, payload.length());
        return chunk.getCopyOfBuffer();
    }

    private KeyedState getKeyedStateWithCheck(String stateName) {
        KeyedState keyedState = this.keyedStateMap.get(stateName);
        if (keyedState == null) {
            throw new BSSRuntimeException("Failed to get KeyedState, stateName: " + stateName);
        }
        return keyedState;
    }

    private NSKeyedState getNSKeyedStateWithCheck(String stateName) {
        NSKeyedState nsKeyedState = this.nsKeyedStateMap.get(stateName);
        if (nsKeyedState == null) {
            throw new BSSRuntimeException("Failed to get NSKeyedState, stateName: " + stateName);
        }
        return nsKeyedState;
    }

    private <N, V> InternalStateType getInternalValueStateType(
        RegisteredKeyValueStateBackendMetaInfo<N, V> backendMetaInfo) {
        if (backendMetaInfo.getNamespaceSerializer() instanceof VoidNamespaceSerializer) {
            switch (backendMetaInfo.getStateType()) {
                case VALUE:
                    return InternalStateType.KEYED_VALUE;
                case REDUCING:
                    return InternalStateType.KEYED_REDUCING;
                case AGGREGATING:
                    return InternalStateType.KEYED_AGGREGATING;
            }
            throw new UnsupportedOperationException("Invalid state type: " + backendMetaInfo.getStateType());
        }

        switch (backendMetaInfo.getStateType()) {
            case VALUE:
                return InternalStateType.NSKEYED_VALUE;
            case REDUCING:
                return InternalStateType.NSKEYED_REDUCING;
            case AGGREGATING:
                return InternalStateType.NSKEYED_AGGREGATING;
        }
        throw new UnsupportedOperationException("Invalid state type: " + backendMetaInfo.getStateType());
    }

    @Override
    public void close() throws Exception {
        LOG.info("Savepoint restore closed.");
    }
}
