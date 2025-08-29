package org.apache.bookkeeper.bookie;

import org.junit.Test;
import static org.junit.Assert.*;

public class GetLogFileOffsetLlmTest {

    @Test
    public void testDefaultConstructorReturnsZeroOffset() {
        LogMark mark = new LogMark();
        assertEquals(0, mark.getLogFileOffset());
    }

    @Test
    public void testNegativeOffset() {
        LogMark mark = new LogMark(0L, -100L);
        assertEquals(-100L, mark.getLogFileOffset());
    }

    @Test
    public void testMaxLongOffset() {
        LogMark mark = new LogMark(0L, Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, mark.getLogFileOffset());
    }
}
