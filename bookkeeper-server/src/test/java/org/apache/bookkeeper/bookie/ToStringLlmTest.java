package org.apache.bookkeeper.bookie;

import org.junit.Test;
import static org.junit.Assert.*;

public class ToStringLlmTest {

    @Test
    public void testToStringFormat() {
        LogMark mark = new LogMark(42L, 84L);
        String expected = "LogMark: logFileId - 42 , logFileOffset - 84";
        assertEquals(expected, mark.toString());
    }

    @Test
    public void testToStringWithNegativeValues() {
        LogMark mark = new LogMark(-1L, -999L);
        String result = mark.toString();
        assertTrue(result.contains("-1"));
        assertTrue(result.contains("-999"));
    }

    @Test
    public void testToStringWithMaxValues() {
        LogMark mark = new LogMark(Long.MAX_VALUE, Long.MAX_VALUE);
        String result = mark.toString();
        assertTrue(result.contains(String.valueOf(Long.MAX_VALUE)));
    }
}
