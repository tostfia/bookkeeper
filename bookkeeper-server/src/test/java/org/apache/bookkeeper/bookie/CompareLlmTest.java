package org.apache.bookkeeper.bookie;

import org.junit.Test;
import static org.junit.Assert.*;

public class CompareLlmTest {

    @Test
    public void testCompareSmallerFileId() {
        LogMark a = new LogMark(1L, 100L);
        LogMark b = new LogMark(2L, 50L);
        assertEquals(-1, a.compare(b));
    }

    @Test
    public void testCompareEqualFileIdSmallerOffset() {
        LogMark a = new LogMark(5L, 100L);
        LogMark b = new LogMark(5L, 200L);
        assertEquals(-1, a.compare(b));
    }

    @Test
    public void testCompareEqualFileIdAndOffset() {
        LogMark a = new LogMark(7L, 300L);
        LogMark b = new LogMark(7L, 300L);
        assertEquals(0, a.compare(b));
    }

    @Test
    public void testCompareWithNegativeValues() {
        LogMark a = new LogMark(-1L, -100L);
        LogMark b = new LogMark(0L, 0L);
        assertEquals(-1, a.compare(b));
    }

    @Test
    public void testCompareWithMaxValues() {
        LogMark a = new LogMark(Long.MAX_VALUE, Long.MAX_VALUE);
        LogMark b = new LogMark(0L, 0L);
        assertEquals(1, a.compare(b));
    }
}
