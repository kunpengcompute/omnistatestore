package com.huawei.falcon.state.merge;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.Before;
import org.junit.Test;

/** A test for the {@link UInt64RocksDbSerializer}. */
public class UInt64RocksDbSerializerTest {

    private UInt64RocksDbSerializer serializer;

    @Before
    public void setup() {
        serializer = new UInt64RocksDbSerializer();
    }

    @Test
    public void testIsImmutableType() {
        assertTrue(serializer.isImmutableType());
    }

    @Test
    public void testCreateInstance() {
        assertEquals(0L, serializer.createInstance().longValue());
    }

    @Test
    public void testCopy1() {
        Long temp = 1L;
        assertEquals(temp, serializer.copy(temp));
    }

    @Test
    public void testCopy2() {
        Long temp1 = 1L;
        Long temp2 = 2L;
        assertEquals(temp1, serializer.copy(temp1, temp2));
    }

    @Test
    public void testGetLength() {
        assertEquals(Long.BYTES, serializer.getLength());
    }

    @Test
    public void testSerialize() throws IOException {
        long testRecord = 123456789L;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.BYTES);
        DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(baos);
        serializer.serialize(testRecord, outView);

        byte[] result = baos.toByteArray();
        assertEquals(Long.BYTES, result.length);

        // The serializer writes values in native byte order so that the on-wire bytes
        // match RocksDB's Uint64 merge operator expectations. Verify by reading the
        // bytes back with native byte order.
        ByteBuffer buf = ByteBuffer.wrap(result).order(ByteOrder.nativeOrder());
        assertEquals(testRecord, buf.getLong());
    }

    @Test
    public void testDeserialize() throws IOException {
        long testRecord = 123456789L;
        // Create bytes in the same native byte order format that serialize() produces.
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.nativeOrder());
        buf.putLong(testRecord);
        ByteArrayInputStream bais = new ByteArrayInputStream(buf.array());
        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bais);

        Long result = serializer.deserialize(inView);
        assertEquals(testRecord, result.longValue());
    }

    @Test
    public void testCopy3() throws IOException {
        long testRecord = 123456789L;
        // Serialize the test record to get bytes in the serializer's format
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.BYTES);
        DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(baos);
        serializer.serialize(testRecord, outView);

        // Copy the serialized data from input to output
        ByteArrayInputStream inputBais = new ByteArrayInputStream(baos.toByteArray());
        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(inputBais);
        ByteArrayOutputStream outputBaos = new ByteArrayOutputStream(Long.BYTES);
        DataOutputViewStreamWrapper outView2 = new DataOutputViewStreamWrapper(outputBaos);
        serializer.copy(inView, outView2);

        // Deserialize the copy and verify it matches the original
        ByteArrayInputStream resultBais = new ByteArrayInputStream(outputBaos.toByteArray());
        DataInputViewStreamWrapper resultInView = new DataInputViewStreamWrapper(resultBais);
        Long result = serializer.deserialize(resultInView);
        assertEquals(testRecord, result.longValue());
    }

    @Test
    public void testSnapshotConfiguration() {
        TypeSerializerSnapshot<Long> snapshot = serializer.snapshotConfiguration();
        assertTrue(snapshot instanceof UInt64RocksDbSerializer.UInt64RocksDbSerializerSnapshot);
    }

    @Test
    public void testRoundTrip() throws IOException {
        long testRecord = 9876543210L;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.BYTES);
        DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(baos);
        serializer.serialize(testRecord, outView);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bais);
        Long deserialized = serializer.deserialize(inView);

        assertEquals(testRecord, deserialized.longValue());
    }

    @Test
    public void testBoundaryValues() throws IOException {
        long[] boundaryValues = { Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L };
        for (long value : boundaryValues) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.BYTES);
            DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(baos);
            serializer.serialize(value, outView);

            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bais);
            Long deserialized = serializer.deserialize(inView);

            assertEquals("Round-trip failed for value " + value, value, deserialized.longValue());
        }
    }
}
