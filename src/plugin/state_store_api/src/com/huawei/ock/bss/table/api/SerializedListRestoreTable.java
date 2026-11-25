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
package com.huawei.ock.bss.table.api;

import java.io.IOException;

/** Restore-only entry point for bounded chunks in the native list format. */
public interface SerializedListRestoreTable<K> {
    void restoreSerialized(byte[] keyBytes, int mixedKeyHash, int bloomKeyHash, ChunkProducer producer)
        throws IOException;

    interface ChunkProducer {
        void writeTo(ChunkConsumer consumer) throws IOException;
    }

    interface ChunkConsumer {
        void accept(byte[] chunk) throws IOException;
    }
}
