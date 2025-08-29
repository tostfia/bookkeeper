package org.apache.bookkeeper.bookie;

import org.junit.Test;
import java.nio.ByteBuffer;
import static org.junit.Assert.*;

public class ReadLogMarkLlmTest {

    @Test
    public void testReadValidLogMark() {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(999L);
        buffer.putLong(888L);
        buffer.flip();

        LogMark mark = new LogMark();
        mark.readLogMark(buffer);

        assertEquals(999L, mark.getLogFileId());
        assertEquals(888L, mark.getLogFileOffset());
    }

    @Test(expected = java.nio.BufferUnderflowException.class)
    public void testReadFromEmptyBufferThrowsException() {
        ByteBuffer buffer = ByteBuffer.allocate(0);
        LogMark mark = new LogMark();
        mark.readLogMark(buffer);
    }
}
