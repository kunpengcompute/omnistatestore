/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistatestore is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */
package com.huawei.ock.bss.iterator;

import com.huawei.ock.bss.common.BinaryKeyValueItem;
import com.huawei.ock.bss.common.BoostStateType;
import com.huawei.ock.bss.common.exception.BSSRuntimeException;
import com.huawei.ock.bss.iterator.serializer.KeyValueBuilder;
import com.huawei.ock.bss.snapshot.SavepointDBResult;

import org.apache.flink.contrib.streaming.state.iterator.SingleStateIterator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

public final class BoostSortedKeyValueIterator<K> implements SingleStateIterator {
    private static final Logger LOG = LoggerFactory.getLogger(BoostSortedKeyValueIterator.class);

    private CloseableIterator<BinaryKeyValueItem> iterator;

    private KeyValueItem currentItem;

    private KeyValueBuilder<K> keyValueBuilder;

    private boolean isValid;

    private KeyGroupRange keyGroupRange;

    private Record pending;

    private static final byte LIST_DELIMITER = ',';

    public BoostSortedKeyValueIterator(SavepointDBResult savepointDBResult, KeyValueBuilder<K> keyValueBuilder,
        KeyGroupRange keyGroupRange) throws IOException {
        this(savepointDBResult.iterator(), keyValueBuilder, keyGroupRange);
    }

    BoostSortedKeyValueIterator(CloseableIterator<BinaryKeyValueItem> iterator,
        KeyValueBuilder<K> keyValueBuilder, KeyGroupRange keyGroupRange) throws IOException {
        this.keyValueBuilder = keyValueBuilder;
        this.isValid = false;
        this.iterator = iterator;

        this.keyGroupRange = keyGroupRange;
        next();
    }

    @Override
    public void next() {
        try {
            Record first = pending == null ? readNext() : pending;
            pending = null;
            if (first == null) {
                currentItem = null;
                isValid = false;
                return;
            }
            currentItem = first.item;
            if (first.type == BoostStateType.LIST || first.type == BoostStateType.SUB_LIST) {
                List<byte[]> fragments = new ArrayList<>();
                fragments.add(first.item.getValue());
                Record following;
                while ((following = readNext()) != null) {
                    if (following.type != first.type || following.item.getStateId() != first.item.getStateId()
                        || following.item.getKeyGroup() != first.item.getKeyGroup()
                        || !Arrays.equals(following.item.getKey(), first.item.getKey())) {
                        pending = following;
                        break;
                    }
                    fragments.add(following.item.getValue());
                }
                if (fragments.size() > 1) {
                    currentItem = new KeyValueItem(first.item.getKeyGroup(), first.item.getStateId(),
                        first.item.getKey(), combineListFragments(fragments));
                }
            }
        } catch (IOException e) {
            throw new BSSRuntimeException("Failed to get next kv item.", e);
        }
        isValid = currentItem != null;
    }

    private Record readNext() throws IOException {
        while (iterator.hasNext()) {
            BinaryKeyValueItem binaryItem = iterator.next();
            KeyValueItem item = keyValueBuilder.buildKeyValueItem(binaryItem);
            if (item != null) {
                return new Record(binaryItem.getStateType(), item);
            }
        }
        return null;
    }

    // Native savepoint sections arrive newest first. Flink's ListDelimitedSerializer
    // requires one value per key/state, with a comma between serialized elements.
    static byte[] combineListFragments(List<byte[]> newestFirst) {
        long length = 0;
        int nonEmpty = 0;
        for (byte[] fragment : newestFirst) {
            if (fragment.length > 0) {
                length += fragment.length;
                nonEmpty++;
            }
        }
        if (nonEmpty == 0) {
            return new byte[0];
        }
        length += nonEmpty - 1;
        if (length > Integer.MAX_VALUE) {
            throw new BSSRuntimeException("Serialized ListState exceeds Java array limit: " + length);
        }
        byte[] combined = new byte[(int) length];
        int position = 0;
        for (int index = newestFirst.size() - 1; index >= 0; index--) {
            byte[] fragment = newestFirst.get(index);
            if (fragment.length == 0) {
                continue;
            }
            if (position > 0) {
                combined[position++] = LIST_DELIMITER;
            }
            System.arraycopy(fragment, 0, combined, position, fragment.length);
            position += fragment.length;
        }
        return combined;
    }

    private static final class Record {
        private final BoostStateType type;
        private final KeyValueItem item;

        private Record(BoostStateType type, KeyValueItem item) {
            this.type = type;
            this.item = item;
        }
    }

    private boolean checkKeyGroup(int keyGroup) {
        return this.keyGroupRange.contains(keyGroup);
    }

    @Override
    public boolean isValid() {
        return isValid;
    }

    @Override
    public byte[] key() {
        return currentItem.getKey();
    }

    @Override
    public byte[] value() {
        return currentItem.getValue();
    }

    @Override
    public int getKvStateId() {
        return currentItem.getStateId();
    }

    @Override
    public void close() {
        try {
            iterator.close();
        } catch (Exception e) {
            throw new BSSRuntimeException("Failed to close iterator.", e);
        }
    }

    public static class KeyValueItem {
        private final int keyGroup;

        private final int stateId;

        private final byte[] key;

        private final byte[] value;

        public KeyValueItem(int keyGroup, int stateId, byte[] key, byte[] value) {
            this.keyGroup = keyGroup;
            this.stateId = stateId;
            this.key = key;
            this.value = value;
        }

        /**
         * getKeyGroup
         *
         * @return int
         */
        public int getKeyGroup() {
            return this.keyGroup;
        }

        /**
         * getStateId
         *
         * @return int
         */
        public int getStateId() {
            return this.stateId;
        }

        /**
         * getKey
         *
         * @return byte[]
         */
        public byte[] getKey() {
            return this.key;
        }

        /**
         * getValue
         *
         * @return byte[]
         */
        public byte[] getValue() {
            return this.value;
        }
    }
}
