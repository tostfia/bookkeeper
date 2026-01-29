package org.apache.bookkeeper.bookie;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class SetLogMarkTest {

    private final long inputLogFileId;
    private final long inputLogFileOffset;
    private final Class<? extends Throwable> expectedException;
    private final String description;

    public SetLogMarkTest(long inputLogFileId,
                          long inputLogFileOffset,
                          Class<? extends Throwable> expectedException,
                          String description) {
        this.inputLogFileId = inputLogFileId;
        this.inputLogFileOffset = inputLogFileOffset;
        this.expectedException = expectedException;
        this.description = description;
    }

    @Parameterized.Parameters(name = "{index}: {3}")
    public static Object[][] data() {
        return new Object[][]{
                // Partizione 1: valori normali
                {10L, 100L, null, "Valori normali positivi"},
                {1L, 1L, null, "Valori piccoli"},

                // Partizione 2: zero
                {0L, 0L, null, "Entrambi zero"},

                // Partizione 3: valori negativi
                //{-5L, -50L, IllegalArgumentException.class, "Entrambi negativi"},

                // Partizione 4: valori estremi
                //{Long.MIN_VALUE, Long.MIN_VALUE, IllegalArgumentException.class, "Limiti estremi dei long"},
                {Long.MIN_VALUE, Long.MAX_VALUE, null, "Limiti invertiti"}


        };
    }

    @Test
    public void testSetLogMark() {
        try {
            LogMark mark = new LogMark(0L, 0L);

            // Azione in test
            mark.setLogMark(inputLogFileId, inputLogFileOffset);

            if (expectedException != null) {
                fail("Attesa eccezione: " + expectedException.getSimpleName() + " ma non è stata lanciata. Caso: " + description);
            }

            // Verifica post-condizione
            assertEquals("logFileId non settato correttamente - " + description,
                    inputLogFileId, mark.getLogFileId());
            assertEquals("logFileOffset non settato correttamente - " + description,
                    inputLogFileOffset, mark.getLogFileOffset());

        } catch (Throwable t) {
            if (expectedException == null || !expectedException.isInstance(t)) {
                throw new AssertionError("Eccezione inattesa nel caso '" + description + "': " + t, t);
            }
            // eccezione attesa -> test passa
        }
    }
}