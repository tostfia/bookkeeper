package org.apache.bookkeeper.bookie;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class WriteLogMarkTest {

    private final long logFileId;
    private final long logFileOffset;
    private final int bufferSize;
    private final int initialPosition;
    private final boolean expectException;
    private final String description;

    public WriteLogMarkTest(long logFileId, long logFileOffset,
                                     int bufferSize, int initialPosition,
                                     boolean expectException, String description) {
        this.logFileId = logFileId;
        this.logFileOffset = logFileOffset;
        this.bufferSize = bufferSize;
        this.initialPosition = initialPosition;
        this.expectException = expectException;
        this.description = description;
    }

    @Parameterized.Parameters
    public static Object[][] data() {
        return new Object[][]{
                // --- Partizione 1: Buffer troppo piccolo ---
                {10L, 100L, 8, 0, true, "Buffer troppo piccolo (<16 byte)"},

                // --- Partizione 2: Buffer esattamente 16 byte ---
                {20L, 200L, 16, 0, false, "Buffer esattamente 16 byte"},

                // --- Partizione 3: Buffer più grande di 16 byte ---
                {30L, 300L, 32, 0, false, "Buffer grande abbastanza (>16 byte)"},

                // --- Partizione 4: Buffer con offset iniziale valido ---
                {40L, 400L, 32, 4, false, "Buffer con offset iniziale e spazio sufficiente"},

                // --- Partizione 5: Buffer con offset ma spazio insufficiente ---
                {50L, 500L, 18, 4, true, "Buffer con offset ma spazio insufficiente"},

                // --- Partizione 6: Valori estremi ---
                {Long.MAX_VALUE, Long.MIN_VALUE, 32, 0, false, "Valori estremi di long"},


        };
    }

    @Test
    public void testWriteLogMark() {
        LogMark mark = new LogMark(logFileId, logFileOffset);
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.position(initialPosition);

        try {
            mark.writeLogMark(buffer);

            if (expectException) {
                fail("Ci si aspettava un'eccezione ma il caso è passato: " + description);
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

        } catch (BufferOverflowException e) {
            if (!expectException) {
                fail("Eccezione inattesa in " + description + ": " + e);
            }
        } catch (Exception e) {
            fail("Eccezione inattesa in " + description + ": " + e);
        }
    }


    @Test(expected = BufferOverflowException.class)
    public void testWriteLogMarkInvalidCase() {
        LogMark mark = new LogMark(1L, 1L);
        // buffer di dimensione zero
        ByteBuffer tiny = ByteBuffer.allocate(0);
        mark.writeLogMark(tiny);

    }


}
