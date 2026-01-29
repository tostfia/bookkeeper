package org.apache.bookkeeper.bookie;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;



import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CompareTest {

    private final LogMark thisMark;
    private final LogMark otherMark;
    private final int expectedResult;
    private final String description;

    public CompareTest(LogMark thisMark, LogMark otherMark, int expectedResult, String description) {
        this.thisMark = thisMark;
        this.otherMark = otherMark;
        this.expectedResult = expectedResult;
        this.description = description;
    }

    @Parameterized.Parameters(name = "{index}: {3}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // Categoria 1: logFileId diversi
                {new LogMark(1, 100), new LogMark(2, 50), -1, "LogFileId minore → -1"},
                {new LogMark(3, 10), new LogMark(2, 999), 1, "LogFileId maggiore → 1"},

                // Categoria 2: logFileId uguali, offset diversi
                {new LogMark(5, 100), new LogMark(5, 200), -1, "LogFileId uguale, offset minore → -1"},
                {new LogMark(5, 300), new LogMark(5, 200), 1, "LogFileId uguale, offset maggiore → 1"},
                {new LogMark(7, 400), new LogMark(7, 400), 0, "LogFileId uguale, offset uguale → 0"},

                // Boundary: valori estremi
                {new LogMark(Long.MIN_VALUE, 0), new LogMark(Long.MAX_VALUE, 0), 1, "Boundary: MIN vs MAX id"},
                {new LogMark(Long.MAX_VALUE, Long.MAX_VALUE), new LogMark(Long.MAX_VALUE, Long.MIN_VALUE), -1, "Boundary: offset MAX vs MIN con stesso id"},

                //valori identici
                {new LogMark(12345, 67890), new LogMark(12345, 67890), 0, "Valori identici → 0"},

                //overflow potenziale
                {new LogMark(Long.MAX_VALUE, Long.MAX_VALUE), new LogMark(Long.MIN_VALUE, Long.MIN_VALUE), -1, "Overflow potenziale: MAX vs MIN"},
                {new LogMark(Long.MIN_VALUE, Long.MIN_VALUE), new LogMark(Long.MAX_VALUE, Long.MAX_VALUE), 1, "Overflow potenziale: MIN vs MAX"}
        });
    }

    @Test
    public void testCompare() {
        int result = thisMark.compare(otherMark);
        assertEquals("Errore nel caso: " + description, expectedResult, result);
    }

    @Test(expected = NullPointerException.class)
    public void testCompareWithNull() {
        thisMark.compare(null);
    }


}

