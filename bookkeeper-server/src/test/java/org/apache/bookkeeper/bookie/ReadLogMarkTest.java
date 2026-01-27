package org.apache.bookkeeper.bookie;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ReadLogMarkTest {

    private final ByteBuffer inputBuffer;
    private final Long expectedLogFileId;
    private final Long expectedLogFileOffset;
    private final Class<? extends Throwable> expectedException;
    private final String description;

    public ReadLogMarkTest(ByteBuffer inputBuffer,
                                    Long expectedLogFileId,
                                    Long expectedLogFileOffset,
                                    Class<? extends Throwable> expectedException,
                                    String description) {
        this.inputBuffer = inputBuffer;
        this.expectedLogFileId = expectedLogFileId;
        this.expectedLogFileOffset = expectedLogFileOffset;
        this.expectedException = expectedException;
        this.description = description;
    }

    @Parameterized.Parameters(name = "{index}: {4}")
    public static Object[][] data() {
        return new Object[][] {
                // Partizione 1: buffer valido
                {ByteBuffer.allocate(16).putLong(10L).putLong(20L).flip(),
                        10L, 20L, null, "Buffer valido con valori normali"},
                {ByteBuffer.allocate(16).putLong(-5L).putLong(-50L).flip(),
                        -5L, -50L, null, "Buffer valido con valori negativi"},
                {ByteBuffer.allocate(16).putLong(Long.MAX_VALUE).putLong(Long.MIN_VALUE).flip(),
                        Long.MAX_VALUE, Long.MIN_VALUE, null, "Buffer valido con estremi long"},

                // Partizione 2: offset iniziale non zero
                {ByteBuffer.allocate(24)
                        .putLong(999L)     // "spazzatura"
                        .putLong(30L)
                        .putLong(40L)
                        .flip()
                        .position(8),
                        30L, 40L, null, "Buffer con offset iniziale"},

                // Partizione 3: buffer incompleto
                {ByteBuffer.allocate(8).putLong(10L).flip(),
                        null, null, java.nio.BufferUnderflowException.class,
                        "Buffer troppo corto (solo un long)"},

                // Partizione 4: buffer vuoto
                {ByteBuffer.allocate(0),
                        null, null, java.nio.BufferUnderflowException.class,
                        "Buffer vuoto"},

                // Partizione 5: buffer più grande del necessario
                {ByteBuffer.allocate(24)
                        .putLong(123L)
                        .putLong(456L)
                        .putLong(789L)
                        .flip(),
                        123L, 456L, null, "Buffer con dati extra ignorati"},

                //caso null
                {null,null,null,NullPointerException.class,"Parametri nulli"}

        };
    }

    @Test
    public void testReadLogMark() {
        LogMark mark = new LogMark(0L, 0L);

        try {
            mark.readLogMark(inputBuffer);

            if (expectedException != null) {
                throw new AssertionError("Attesa eccezione: " + expectedException.getSimpleName() +
                        " ma nessuna eccezione è stata lanciata. Caso: " + description);
            }

            // Verifica valori attesi
            assertEquals("logFileId non corretto - " + description,
                    expectedLogFileId.longValue(), mark.getLogFileId());
            assertEquals("logFileOffset non corretto - " + description,
                    expectedLogFileOffset.longValue(), mark.getLogFileOffset());

        } catch (Throwable t) {
            if (expectedException == null || !expectedException.isInstance(t)) {
                throw new AssertionError("Eccezione inattesa nel caso '" + description + "': " + t, t);
            }
        }
    }
}

