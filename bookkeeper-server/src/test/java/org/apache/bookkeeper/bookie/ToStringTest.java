package org.apache.bookkeeper.bookie;


import org.junit.Test;
import org.junit.jupiter.api.AfterEach;


import static org.junit.Assert.assertEquals;

public class ToStringTest {

    private LogMark logMark;
    @AfterEach
    public void tearDown() {
        logMark = null;
    }

    @Test
    public void testToStringWithPositiveValues() {
        logMark = new LogMark(123, 456L);
        String expected = "LogMark: logFileId - 123 , logFileOffset - 456";
        assertEquals(expected, logMark.toString());
    }

    @Test
    public void testToStringWithZeroValues() {
        logMark = new LogMark(); // logFileId = 0, logFileOffset = 0
        String expected = "LogMark: logFileId - 0 , logFileOffset - 0";
        assertEquals(expected, logMark.toString());
    }

    @Test
    public void testToStringWithNegativeValues() {
        logMark = new LogMark(-1, -100L);
        String expected = "LogMark: logFileId - -1 , logFileOffset - -100";
        assertEquals(expected, logMark.toString());
    }

    @Test
    public void testToStringWithLargeValues() {
        logMark = new LogMark(Integer.MAX_VALUE, Long.MAX_VALUE);
        String expected = "LogMark: logFileId - " + Integer.MAX_VALUE +
                " , logFileOffset - " + Long.MAX_VALUE;
        assertEquals(expected, logMark.toString());
    }
}

