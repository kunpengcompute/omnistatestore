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
package com.huawei.ock.bss.restore;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** Stateless sink used to validate serialized input without retaining a second payload copy. */
final class DiscardingDataOutputView implements DataOutputView {
    static final DiscardingDataOutputView INSTANCE = new DiscardingDataOutputView();

    private DiscardingDataOutputView() {
    }

    @Override public void skipBytesToWrite(int numBytes) { }
    @Override public void write(DataInputView source, int numBytes) throws IOException {
        source.skipBytesToRead(numBytes);
    }
    @Override public void write(int value) { }
    @Override public void write(byte[] value) { }
    @Override public void write(byte[] value, int offset, int length) { }
    @Override public void writeBoolean(boolean value) { }
    @Override public void writeByte(int value) { }
    @Override public void writeShort(int value) { }
    @Override public void writeChar(int value) { }
    @Override public void writeInt(int value) { }
    @Override public void writeLong(long value) { }
    @Override public void writeFloat(float value) { }
    @Override public void writeDouble(double value) { }
    @Override public void writeBytes(String value) { }
    @Override public void writeChars(String value) { }
    @Override public void writeUTF(String value) { }
}
