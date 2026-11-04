/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.huawei.ock.bss.common.serialize;

import com.huawei.ock.bss.table.api.TableDescription;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.ttl.TtlValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** TTL metadata for native compaction; never changes the timestamp stored in a value. */
public final class FlinkTtlListCompactionFilter {
    private final TypeSerializer<?> serializer;
    private final TtlTimeProvider timeProvider;
    private final ClassLoader userClassLoader;

    private FlinkTtlListCompactionFilter(TypeSerializer<?> serializer, TtlTimeProvider timeProvider) {
        this.serializer = serializer.duplicate();
        this.timeProvider = timeProvider;
        this.userClassLoader = Thread.currentThread().getContextClassLoader();
    }

    public static FlinkTtlListCompactionFilter create(TableDescription description, TtlTimeProvider timeProvider) {
        TypeSerializer<?> serializer = description.getTableSerializer().getValueSerializer();
        return serializer instanceof TtlStateFactory.TtlSerializer
            ? new FlinkTtlListCompactionFilter(serializer, timeProvider) : null;
    }

    public long currentTimestamp() {
        return timeProvider.currentTimestamp();
    }

    /**
     * Called for lists only. Each native list chunk is count followed by serialized elements.
     * Deserialization locates variable-length elements; survivors are copied verbatim, including TTL.
     * Returning the input denotes no change; null denotes an empty result.
     */
    public synchronized byte[] filterList(byte[] bytes, long ttl, long now) throws IOException {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(userClassLoader);
        try {
            return filterListWithUserClassLoader(bytes, ttl, now);
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    private byte[] filterListWithUserClassLoader(byte[] bytes, long ttl, long now) throws IOException {
        DataInputDeserializer input = new DataInputDeserializer(bytes);
        List<int[]> survivors = new ArrayList<>();
        boolean changed = false;
        while (input.available() > 0) {
            int count = input.readInt();
            if (count < 0 || count > input.available() / Long.BYTES) {
                throw new IOException("Invalid TTL list element count: " + count);
            }
            for (int i = 0; i < count; i++) {
                int start = input.getPosition();
                TtlValue<?> value = (TtlValue<?>) serializer.deserialize(input);
                long timestamp = value.getLastAccessTimestamp();
                long expires = ttl > 0 && timestamp > Long.MAX_VALUE - ttl ? Long.MAX_VALUE : timestamp + ttl;
                boolean expired = ttl > 0 && expires <= now;
                if (expired) {
                    changed = true;
                } else {
                    survivors.add(new int[] {start, input.getPosition() - start});
                }
            }
        }
        if (!changed) {
            return bytes;
        }
        if (survivors.isEmpty()) {
            return null;
        }
        DataOutputSerializer output = new DataOutputSerializer(bytes.length);
        output.writeInt(survivors.size());
        for (int[] survivor : survivors) {
            output.write(bytes, survivor[0], survivor[1]);
        }
        return output.getCopyOfBuffer();
    }
}
