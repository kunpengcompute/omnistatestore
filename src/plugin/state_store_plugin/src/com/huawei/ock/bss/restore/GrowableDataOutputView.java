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
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** Growable serializer output whose bulk-copy path does not require preallocated capacity. */
final class GrowableDataOutputView implements DataOutputView {
    private static final int COPY_BUFFER_SIZE = 8192;

    private final DataOutputSerializer output;

    private final byte[] copyBuffer;

    GrowableDataOutputView(int initialSize) {
        output = new DataOutputSerializer(initialSize);
        copyBuffer = new byte[COPY_BUFFER_SIZE];
    }

    void clear() {
        output.clear();
    }

    byte[] getCopyOfBuffer() {
        return output.getCopyOfBuffer();
    }

    @Override
    public void skipBytesToWrite(int numBytes) throws IOException {
        if (numBytes < 0) {
            throw new IOException("Cannot skip a negative number of bytes: " + numBytes);
        }
        int remaining = numBytes;
        while (remaining > 0) {
            int copyLength = Math.min(remaining, copyBuffer.length);
            output.write(copyBuffer, 0, copyLength);
            remaining -= copyLength;
        }
    }

    @Override
    public void write(DataInputView source, int numBytes) throws IOException {
        if (numBytes < 0) {
            throw new IOException("Cannot copy a negative number of bytes: " + numBytes);
        }
        int remaining = numBytes;
        while (remaining > 0) {
            int copyLength = Math.min(remaining, copyBuffer.length);
            source.readFully(copyBuffer, 0, copyLength);
            output.write(copyBuffer, 0, copyLength);
            remaining -= copyLength;
        }
    }

    @Override public void write(int value) throws IOException { output.write(value); }
    @Override public void write(byte[] value) throws IOException { output.write(value); }
    @Override public void write(byte[] value, int offset, int length) throws IOException {
        output.write(value, offset, length);
    }
    @Override public void writeBoolean(boolean value) throws IOException { output.writeBoolean(value); }
    @Override public void writeByte(int value) throws IOException { output.writeByte(value); }
    @Override public void writeShort(int value) throws IOException { output.writeShort(value); }
    @Override public void writeChar(int value) throws IOException { output.writeChar(value); }
    @Override public void writeInt(int value) throws IOException { output.writeInt(value); }
    @Override public void writeLong(long value) throws IOException { output.writeLong(value); }
    @Override public void writeFloat(float value) throws IOException { output.writeFloat(value); }
    @Override public void writeDouble(double value) throws IOException { output.writeDouble(value); }
    @Override public void writeBytes(String value) throws IOException { output.writeBytes(value); }
    @Override public void writeChars(String value) throws IOException { output.writeChars(value); }
    @Override public void writeUTF(String value) throws IOException { output.writeUTF(value); }
}
