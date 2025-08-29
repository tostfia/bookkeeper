package org.apache.bookkeeper.bookie;

import org.junit.Test;
import static org.junit.Assert.*;

public class GetLogFileIdLlmTest {

    @Test
    public void testDefaultConstructorReturnsZero() {
        LogMark mark = new LogMark();
        assertEquals(0, mark.getLogFileId());
    }

    @Test
    public void testCustomConstructorReturnsCorrectId() {
        LogMark mark = new LogMark(123L, 456L);
        assertEquals(123L, mark.getLogFileId());
    }
}
