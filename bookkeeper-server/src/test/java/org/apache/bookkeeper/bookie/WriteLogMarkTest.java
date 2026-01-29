package org.apache.bookkeeper.bookie;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class WriteLogMarkTest {

    private final ByteBuffer inputBuffer;
    private final long logFileId;
    private final long logFileOffset;
    private final int initialPosition;
    private final Class<? extends Throwable> exception;
    private final String description;

    public WriteLogMarkTest(ByteBuffer inputBuffer,long logFileId, long logFileOffset,
                                     int initialPosition, Class<? extends Throwable> exception,String description) {
        this.inputBuffer = inputBuffer;
        this.logFileId = logFileId;
        this.logFileOffset = logFileOffset;
        this.initialPosition = initialPosition;
        this.exception = exception;
        this.description = description;
    }

    @Parameterized.Parameters
    public static Object[][] data() {
        return new Object[][]{

                // Partizione 1: Buffer troppo piccolo
                { ByteBuffer.allocate(8), 10L, 100L, 0, java.nio.BufferOverflowException.class,
                        "Buffer troppo piccolo (<16 byte)" },

                // Partizione 2: Buffer esattamente 16 byte
                { ByteBuffer.allocate(16), 20L, 200L, 0, null,
                        "Buffer esattamente 16 byte" },

                // Partizione 3: Buffer più grande di 16 byte
                { ByteBuffer.allocate(32), 30L, 300L, 0, null,
                        "Buffer grande abbastanza (>16 byte)" },

                // Partizione 4: Buffer con offset iniziale valido
                { ByteBuffer.allocate(32), 40L, 400L, 4, null,
                        "Buffer con offset iniziale e spazio sufficiente" },

                // Partizione 5: Buffer con offset ma spazio insufficiente
                { ByteBuffer.allocate(18), 50L, 500L, 4, java.nio.BufferOverflowException.class,
                        "Buffer con offset ma spazio insufficiente" },

                // Partizione 6: Valori estremi
                { ByteBuffer.allocate(32), Long.MAX_VALUE, Long.MIN_VALUE, 0, null,
                        "Valori estremi di long" },

                // Partizione 7: buffer nullo
                { null, 1L, 1L, 0, NullPointerException.class,
                        "Buffer nullo" },


                //Partizione 8: buffer di dimensione zero
                { ByteBuffer.allocate(0), 1L, 1L, 0, java.nio.BufferOverflowException.class,
                        "Buffer di dimensione zero" }


        };
    }

    @Test
    public void testWriteLogMark() {
        LogMark mark = new LogMark(logFileId, logFileOffset);
        ByteBuffer buffer = inputBuffer;
        if (buffer != null) {
            buffer.position(initialPosition);
        }

        try {
            mark.writeLogMark(buffer);

            if (exception != null) {
                throw new AssertionError("Attesa eccezione: " + exception.getSimpleName() +
                        " ma nessuna eccezione è stata lanciata. Caso: " + description);
            }

            // Lettura dei valori scritti
            buffer.flip();
            buffer.position(initialPosition);

            long writtenId = buffer.getLong();
            long writtenOffset = buffer.getLong();

            assertEquals("logFileId scritto incorrettamente (" + description + ")",
                    logFileId, writtenId);
            assertEquals("logFileOffset scritto incorrettamente (" + description + ")",
                    logFileOffset, writtenOffset);

        } catch (Throwable t) {
            if (exception == null || !exception.isInstance(t)) {
                throw new AssertionError("Eccezione inattesa nel caso '" + description + "': " + t, t);
            }
        }
    }





}
