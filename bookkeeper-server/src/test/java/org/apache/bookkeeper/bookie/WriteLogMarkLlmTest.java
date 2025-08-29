package org.apache.bookkeeper.bookie;

import org.junit.Test;
import java.nio.ByteBuffer;
import static org.junit.Assert.*;

public class WriteLogMarkLlmTest {

    @Test
    public void testWriteLogMarkToBuffer() {
        LogMark mark = new LogMark(111L, 222L);
        ByteBuffer buffer = ByteBuffer.allocate(16);
        mark.writeLogMark(buffer);
        buffer.flip();

        assertEquals(111L, buffer.getLong());
        assertEquals(222L, buffer.getLong());
    }

    @Test(expected = java.nio.BufferOverflowException.class)
    public void testWriteToSmallBufferThrowsException() {
        LogMark mark = new LogMark(1L, 2L);
        ByteBuffer buffer = ByteBuffer.allocate(8); // too small
        mark.writeLogMark(buffer);
    }
}
