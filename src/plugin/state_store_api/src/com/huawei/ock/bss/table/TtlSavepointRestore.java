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

package com.huawei.ock.bss.table;

import com.huawei.ock.bss.table.api.Table;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlValue;

import java.util.List;

/** Restore-only bridge. Ordinary writes do not inspect serializers or TTL values. */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class TtlSavepointRestore {
    private TtlSavepointRestore() {
    }

    public static boolean isTtlSerializer(TypeSerializer<?> serializer) {
        return serializer instanceof TtlStateFactory.TtlSerializer;
    }

    public static void restoreValue(Table table, Object key, Object namespace, Object value) {
        long timestamp = timestamp(value);
        if (namespace == null) {
            ((KVTableImpl) table).restore(key, value, timestamp);
        } else {
            ((AbstractKMapTable) table).restore(key, namespace, value, timestamp);
        }
    }

    public static void restoreMapEntry(Table table, Object key, Object namespace, Object mapKey, Object value) {
        Object primaryKey = namespace == null ? key : new KeyPair<>(key, namespace);
        ((AbstractKMapTable) table).restore(primaryKey, mapKey, value, timestamp(value));
    }

    public static void restoreList(Table table, Object key, Object namespace, List<?> values) {
        if (values.isEmpty()) {
            return;
        }
        // Native TTL applies to the whole list. Use the latest element time so
        // restoring cannot prematurely collect a still-live element. A single
        // PUT also preserves list order without ordering APPENDs by timestamp.
        long timestamp = 0;
        for (Object value : values) {
            timestamp = Math.max(timestamp, timestamp(value));
        }
        Object primaryKey = namespace == null ? key : new KeyPair<>(key, namespace);
        ((KListTableImpl) table).restore(primaryKey, values, timestamp);
    }

    private static long timestamp(Object value) {
        if (!(value instanceof TtlValue)) {
            throw new IllegalArgumentException("TTL savepoint entry must contain a TtlValue");
        }
        long timestamp = ((TtlValue<?>) value).getLastAccessTimestamp();
        // Native sequence IDs reserve 16 counter bits and keep the sign bit clear.
        // Leave the maximal timestamp unused so a subsequent write can advance.
        if (timestamp < 0 || timestamp >= 0x7FFFFFFFFFFFL) {
            throw new IllegalArgumentException("TTL timestamp is outside the native range: " + timestamp);
        }
        return timestamp;
    }
}
