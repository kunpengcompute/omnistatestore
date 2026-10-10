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

package com.huawei.ock.bss.transformer;

import static org.apache.flink.runtime.state.StateSnapshotTransformer.CollectionStateSnapshotTransformer.TransformStrategy.STOP_ON_FIRST_INCLUDED;

import com.huawei.ock.bss.common.exception.BSSRuntimeException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

/**
 * ListStateSnapshotTransformerWrapper
 *
 * @since BeiMing 25.0.T1
 */
public class ListStateSnapshotTransformerWrapper<T> implements StateSnapshotTransformer<byte[]> {
    private final DataInputDeserializer deserializer;

    private final TypeSerializer<T> elementSerializer;

    private final StateSnapshotTransformer<T> elementTransformer;

    private final CollectionStateSnapshotTransformer.TransformStrategy transformStrategy;

    ListStateSnapshotTransformerWrapper(
        StateSnapshotTransformer<T> elementTransformer,
        TypeSerializer<T> elementSerializer) {
        this.elementTransformer = elementTransformer;
        this.elementSerializer = elementSerializer;
        this.deserializer = new DataInputDeserializer();
        this.transformStrategy = elementTransformer instanceof CollectionStateSnapshotTransformer
            ? ((CollectionStateSnapshotTransformer<?>) elementTransformer).getFilterStrategy()
            : CollectionStateSnapshotTransformer.TransformStrategy.TRANSFORM_ALL;
    }

    @Override
    @Nullable
    public byte[] filterOrTransform(@Nullable byte[] value) {
        if (value == null) {
            return null;
        }

        List<T> result = new ArrayList<>();
        deserializer.setBuffer(value);
        try {
            return handleListValue(value, result);
        } catch (IOException e) {
            throw new BSSRuntimeException("Failed to serialize transformed list", e);
        } finally {
            deserializer.releaseArrays();
        }
    }

    @Nullable
    private byte[] handleListValue(byte[] value, List<T> result) throws IOException {
        while (deserializer.available() != 0) {
            int subListStart = deserializer.getPosition();
            int subListSize = deserializer.readInt();
            for (int i = 0; i < subListSize; i++) {
                int elementStart = deserializer.getPosition();
                T element = elementTransformer.filterOrTransform(elementSerializer.deserialize(deserializer));
                if (element == null) {
                    continue;
                }
                if (transformStrategy == STOP_ON_FIRST_INCLUDED) {
                    if (i == 0) {
                        return Arrays.copyOfRange(value, subListStart, value.length);
                    }
                    // Preserve the remaining serialized elements with their updated sub-list count.
                    DataOutputSerializer outputView = new DataOutputSerializer(Integer.BYTES);
                    outputView.writeInt(subListSize - i);
                    outputView.write(value, elementStart, value.length - elementStart);
                    return outputView.getCopyOfBuffer();
                }
                result.add(element);
            }
        }
        return result.isEmpty() ? null : serializeList(result, elementSerializer);
    }

    private <E> byte[] serializeList(List<? extends E> elements, TypeSerializer<E> elementSerializer)
        throws IOException {
        DataOutputSerializer outputView = new DataOutputSerializer(1);
        outputView.writeInt(elements.size());

        for (E element : elements) {
            elementSerializer.serialize(element, outputView);
        }

        return outputView.getCopyOfBuffer();
    }
}