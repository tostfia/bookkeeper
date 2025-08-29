package org.apache.bookkeeper.bookie;

import org.junit.Test;
import static org.junit.Assert.*;

public class SetLogMarkLlmTest {

    @Test
    public void testSetLogMarkUpdatesValues() {
        LogMark mark = new LogMark();
        mark.setLogMark(555L, 666L);

        assertEquals(555L, mark.getLogFileId());
        assertEquals(666L, mark.getLogFileOffset());
    }

    @Test
    public void testSetNegativeValues() {
        LogMark mark = new LogMark();
        mark.setLogMark(-10L, -20L);

        assertEquals(-10L, mark.getLogFileId());
        assertEquals(-20L, mark.getLogFileOffset());
    }
}
