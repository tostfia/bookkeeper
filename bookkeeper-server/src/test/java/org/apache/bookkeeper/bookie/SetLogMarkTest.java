package org.apache.bookkeeper.bookie;



import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SetLogMarkTest {

    private final long inputLogFileId;
    private final long inputLogFileOffset;
    private final String description;

    public SetLogMarkTest(long inputLogFileId, long inputLogFileOffset, String description) {
        this.inputLogFileId = inputLogFileId;
        this.inputLogFileOffset = inputLogFileOffset;
        this.description = description;
    }

    @Parameterized.Parameters(name = "{index}: {2}")
    public static Object[][] data() {
        return new Object[][]{
                // Partizione 1: valori normali
                {10L, 100L, "Valori normali positivi"},
                {1L, 1L, "Valori piccoli"},

                // Partizione 2: zero
                {0L, 0L, "Entrambi zero"},
                {0L, 123L, "Solo logFileId zero"},
                {456L, 0L, "Solo logFileOffset zero"},

                // Partizione 3: valori negativi
                {-1L, -100L, "Entrambi negativi"},
                {-1L, 50L, "Solo logFileId negativo"},
                {70L, -200L, "Solo logFileOffset negativo"},

                // Partizione 4: valori estremi
                {Long.MAX_VALUE, Long.MIN_VALUE, "Limiti estremi dei long"},
                {Long.MIN_VALUE, Long.MAX_VALUE, "Limiti invertiti"}
        };
    }

    @Test
    public void testSetLogMark() {
        LogMark mark = new LogMark(0L, 0L);

        // Azione in test
        mark.setLogMark(inputLogFileId, inputLogFileOffset);

        // Verifica post-condizione
        assertEquals("logFileId non settato correttamente - " + description,
                inputLogFileId, mark.getLogFileId());
        assertEquals("logFileOffset non settato correttamente - " + description,
                inputLogFileOffset, mark.getLogFileOffset());
    }
}
